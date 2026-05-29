//! Compact execution log parser.
//!
//! `--execution_log_compact_file=<path>` (also
//! `--experimental_execution_log_compact_file=<path>`) produces a single zstd
//! frame containing length-delimited [`ExecLogEntry`] messages. Entries
//! reference each other by ID (files, directories, input sets, runfiles
//! trees) and a [`Spawn`](spawn_proto::tools::protos::exec_log_entry::Spawn)
//! entry only carries integer IDs for its inputs/outputs; the consumer is
//! expected to chase those references and rebuild a [`SpawnExec`]-shaped
//! record.
//!
//! See `bazel/src/main/java/com/google/devtools/build/lib/exec/SpawnLogReconstructor.java`
//! for the canonical algorithm. Conduit only needs output paths, the inputs
//! count, and the spawn's own scalar fields, so this implementation skips the
//! Java reconstructor's runfiles-tree expansion math (paths still surface via
//! the underlying File entries; runfiles symlink layout is not reconstructed).

use std::collections::HashMap;
use std::fs::File as FsFile;
use std::io::{BufReader, Read};
use std::path::Path;

use prost::Message;

use spawn_proto::tools::protos::exec_log_entry::{
    self, Directory as DirEntry, File as FileEntry, RunfilesTree, Spawn as SpawnEntry,
    UnresolvedSymlink as SymlinkEntry,
};
use spawn_proto::tools::protos::{Digest, ExecLogEntry, File as ProtoFile, SpawnExec};

use super::read_message_len;

/// One entry from the dedup table, keyed by [`ExecLogEntry::id`]. Only the
/// variants the spawn reconstructor reads back through `output_id` are kept;
/// `input_set_id`, symlink-entry-set, and the invocation envelope are
/// consumed inline because conduit doesn't materialise input listings.
enum Stored {
    /// A regular file or unresolved symlink: stored as a [`ProtoFile`].
    File(ProtoFile),
    /// A directory entry, paired with the per-file paths it expands to.
    Dir(DirEntry, Vec<ProtoFile>),
    /// A runfiles tree -- conduit only uses its top-level path.
    RunfilesTree(RunfilesTree),
}

impl Stored {
    /// Single output-side path for this entry. For directories and runfiles
    /// trees this is the tree root, not the contained files.
    fn primary_path(&self) -> &str {
        match self {
            Stored::File(f) => &f.path,
            Stored::Dir(d, _) => &d.path,
            Stored::RunfilesTree(t) => &t.path,
        }
    }
}

/// Read every spawn from a compact execution log, expanded to [`SpawnExec`].
///
/// * `max_message_bytes` caps a single length-delimited entry, preventing
///   OOM from a malformed varint prefix.
/// * `max_decompressed_bytes` caps the total number of bytes pulled out of
///   the zstd frame, preventing a small malicious file from inflating into
///   GiB worth of valid-but-bogus messages (zstd's `Decoder` is otherwise
///   unbounded).
pub fn read_all(
    path: &Path,
    max_message_bytes: usize,
    max_decompressed_bytes: usize,
) -> std::io::Result<Vec<SpawnExec>> {
    let file = FsFile::open(path)?;
    let decoder = zstd::Decoder::new(BufReader::new(file))?;
    let limited = LimitedReader::new(decoder, max_decompressed_bytes);
    let mut reader = BufReader::with_capacity(64 * 1024, limited);

    let mut state = State::default();
    let mut spawns = Vec::new();
    let mut buf = Vec::new();

    while let Some(len) = read_message_len(&mut reader, max_message_bytes)? {
        buf.resize(len, 0);
        reader.read_exact(&mut buf)?;
        let entry = ExecLogEntry::decode(buf.as_slice())
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        if let Some(spawn_exec) = state.ingest(entry)? {
            spawns.push(spawn_exec);
        }
    }
    Ok(spawns)
}

/// `Read` adapter that errors with `InvalidData` once `max_bytes` total
/// bytes have been delivered to the consumer. The internal budget is
/// `max_bytes + 1` so that a stream of exactly `max_bytes` reads cleanly
/// (final inner `Ok(0)` propagates as a clean EOF); the +1 byte is only
/// ever consumed when the underlying stream is actually larger.
pub(super) struct LimitedReader<R: Read> {
    inner: R,
    remaining: u64,
}

impl<R: Read> LimitedReader<R> {
    pub(super) fn new(inner: R, max_bytes: usize) -> Self {
        Self {
            inner,
            remaining: (max_bytes as u64).saturating_add(1),
        }
    }
}

impl<R: Read> Read for LimitedReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if self.remaining == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "execlog decompressed payload exceeds cap",
            ));
        }
        let cap = self.remaining.min(buf.len() as u64) as usize;
        let n = self.inner.read(&mut buf[..cap])?;
        self.remaining -= n as u64;
        Ok(n)
    }
}

/// Incremental decoder state. Owns the dedup table built up as
/// [`ExecLogEntry::File`] / [`ExecLogEntry::Directory`] / ... entries flow in,
/// and turns each [`ExecLogEntry::Spawn`] into a [`SpawnExec`] using the
/// entries it has seen so far. Drive it with [`Self::ingest`] from any source
/// that produces `ExecLogEntry`s in order — the post-build batch reader
/// ([`read_all`]) and the live tail-follow ([`crate::exec_log::tailer`]) both
/// share this code path.
#[derive(Default)]
pub struct State {
    table: HashMap<u32, Stored>,
    hash_function_name: String,
}

impl State {
    /// Consume one [`ExecLogEntry`]. Returns `Ok(Some(SpawnExec))` when the
    /// entry was a [`exec_log_entry::Type::Spawn`] that we successfully
    /// resolved against the dedup table; otherwise `Ok(None)`. An error here
    /// means the log is corrupt (e.g. a spawn references an unknown output id)
    /// — callers should warn and abort, not skip the entry.
    pub fn ingest(&mut self, entry: ExecLogEntry) -> std::io::Result<Option<SpawnExec>> {
        let id = entry.id;
        let Some(payload) = entry.r#type else {
            return Ok(None);
        };
        match payload {
            exec_log_entry::Type::Invocation(inv) => {
                self.hash_function_name = inv.hash_function_name;
                Ok(None)
            }
            exec_log_entry::Type::File(f) => {
                self.put(id, Stored::File(self.convert_file(f, None)));
                Ok(None)
            }
            exec_log_entry::Type::Directory(d) => {
                let files = d
                    .files
                    .iter()
                    .cloned()
                    .map(|sub| self.convert_file(sub, Some(&d.path)))
                    .collect();
                self.put(id, Stored::Dir(d, files));
                Ok(None)
            }
            exec_log_entry::Type::UnresolvedSymlink(s) => {
                self.put(id, Stored::File(symlink_to_proto_file(s)));
                Ok(None)
            }
            exec_log_entry::Type::InputSet(_) => {
                // Dropped: spawn `input_set_id` is only used to materialise
                // the inputs listing, which conduit skips. SpawnMetrics still
                // carries `input_files`/`input_bytes` for the spawn span.
                Ok(None)
            }
            exec_log_entry::Type::RunfilesTree(tree) => {
                self.put(id, Stored::RunfilesTree(tree));
                Ok(None)
            }
            exec_log_entry::Type::SymlinkEntrySet(_) => {
                // Only used by RunfilesTree expansion, which conduit skips.
                Ok(None)
            }
            exec_log_entry::Type::SymlinkAction(_) => {
                // Symlink actions don't appear in expanded format. Drop.
                Ok(None)
            }
            exec_log_entry::Type::Spawn(spawn) => Ok(Some(self.reconstruct_spawn(spawn)?)),
        }
    }

    fn put(&mut self, id: u32, stored: Stored) {
        if id != 0 {
            self.table.insert(id, stored);
        }
    }

    fn convert_file(&self, entry: FileEntry, dir_path: Option<&str>) -> ProtoFile {
        let path = match dir_path {
            Some(parent) => format!("{parent}/{}", entry.path),
            None => entry.path,
        };
        let digest = entry.digest.map(|d| Digest {
            hash: d.hash,
            size_bytes: d.size_bytes,
            hash_function_name: self.hash_function_name.clone(),
        });
        ProtoFile {
            path,
            symlink_target_path: String::new(),
            digest,
            is_tool: false,
        }
    }

    fn reconstruct_spawn(&self, entry: SpawnEntry) -> std::io::Result<SpawnExec> {
        let mut listed_outputs: Vec<String> = Vec::new();
        let mut actual_outputs: Vec<ProtoFile> = Vec::new();

        for output in &entry.outputs {
            let Some(ty) = &output.r#type else { continue };
            match ty {
                exec_log_entry::output::Type::OutputId(out_id) => {
                    let stored = self.table.get(out_id).ok_or_else(|| {
                        std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            format!("missing output entry id={out_id}"),
                        )
                    })?;
                    listed_outputs.push(stored.primary_path().to_string());
                    match stored {
                        Stored::File(f) => actual_outputs.push(f.clone()),
                        Stored::Dir(_, files) => actual_outputs.extend(files.iter().cloned()),
                        Stored::RunfilesTree(t) => actual_outputs.push(ProtoFile {
                            path: t.path.clone(),
                            ..Default::default()
                        }),
                    }
                }
                exec_log_entry::output::Type::InvalidOutputPath(p) => {
                    listed_outputs.push(p.clone());
                }
            }
        }

        listed_outputs.sort();
        listed_outputs.dedup();

        let digest = entry.digest.map(|d| Digest {
            hash: d.hash,
            size_bytes: d.size_bytes,
            hash_function_name: self.hash_function_name.clone(),
        });

        Ok(SpawnExec {
            command_args: entry.args,
            environment_variables: entry.env_vars,
            platform: entry.platform,
            inputs: Vec::new(),
            listed_outputs,
            remotable: entry.remotable,
            cacheable: entry.cacheable,
            timeout_millis: entry.timeout_millis,
            mnemonic: entry.mnemonic,
            actual_outputs,
            runner: entry.runner,
            cache_hit: entry.cache_hit,
            status: entry.status,
            exit_code: entry.exit_code,
            remote_cacheable: entry.remote_cacheable,
            target_label: entry.target_label,
            digest,
            metrics: entry.metrics,
        })
    }
}

fn symlink_to_proto_file(entry: SymlinkEntry) -> ProtoFile {
    ProtoFile {
        path: entry.path,
        symlink_target_path: entry.target_path,
        digest: None,
        is_tool: false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exec_log::{
        DEFAULT_EXECLOG_MAX_DECOMPRESSED_BYTES, DEFAULT_EXECLOG_MAX_MESSAGE_BYTES,
    };
    use prost::Message;
    use spawn_proto::tools::protos::exec_log_entry::{
        File as FileEntryProto, Invocation, Output, Type as EntryType,
    };
    use std::io::Write;
    use tempfile::NamedTempFile;

    fn write_log(entries: &[ExecLogEntry]) -> NamedTempFile {
        let f = NamedTempFile::new().unwrap();
        let mut enc = zstd::Encoder::new(f.reopen().unwrap(), 0).unwrap();
        for e in entries {
            let mut buf = Vec::with_capacity(e.encoded_len() + 5);
            let len = e.encoded_len() as u64;
            let mut tmp = [0u8; 10];
            let n = encode_varint(len, &mut tmp);
            buf.extend_from_slice(&tmp[..n]);
            e.encode(&mut buf).unwrap();
            enc.write_all(&buf).unwrap();
        }
        enc.finish().unwrap();
        f
    }

    fn encode_varint(mut value: u64, buf: &mut [u8; 10]) -> usize {
        let mut i = 0;
        while value >= 0x80 {
            buf[i] = (value as u8) | 0x80;
            value >>= 7;
            i += 1;
        }
        buf[i] = value as u8;
        i + 1
    }

    fn entry(id: u32, t: EntryType) -> ExecLogEntry {
        ExecLogEntry {
            id,
            r#type: Some(t),
        }
    }

    #[test]
    fn reconstructs_spawn_with_cache_hit_and_runner() {
        let log = write_log(&[
            entry(
                0,
                EntryType::Invocation(Invocation {
                    hash_function_name: "SHA256".into(),
                    workspace_runfiles_directory: "_main".into(),
                    sibling_repository_layout: false,
                    id: "inv-1".into(),
                }),
            ),
            entry(
                1,
                EntryType::File(FileEntryProto {
                    path: "bazel-out/k8/bin/foo.o".into(),
                    digest: None,
                }),
            ),
            entry(
                2,
                EntryType::Spawn(SpawnEntry {
                    args: vec!["/usr/bin/cc".into(), "-c".into(), "foo.c".into()],
                    target_label: "//pkg:foo".into(),
                    mnemonic: "CppCompile".into(),
                    runner: "remote cache hit".into(),
                    cache_hit: true,
                    remotable: true,
                    cacheable: true,
                    remote_cacheable: true,
                    exit_code: 0,
                    outputs: vec![Output {
                        r#type: Some(exec_log_entry::output::Type::OutputId(1)),
                    }],
                    ..Default::default()
                }),
            ),
        ]);

        let spawns = read_all(
            log.path(),
            DEFAULT_EXECLOG_MAX_MESSAGE_BYTES,
            DEFAULT_EXECLOG_MAX_DECOMPRESSED_BYTES,
        )
        .expect("compact log parses");
        assert_eq!(spawns.len(), 1);
        let s = &spawns[0];
        assert_eq!(s.target_label, "//pkg:foo");
        assert_eq!(s.mnemonic, "CppCompile");
        assert_eq!(s.runner, "remote cache hit");
        assert!(s.cache_hit);
        assert_eq!(s.listed_outputs, vec!["bazel-out/k8/bin/foo.o".to_string()]);
        assert_eq!(s.actual_outputs.len(), 1);
        assert_eq!(s.actual_outputs[0].path, "bazel-out/k8/bin/foo.o");
    }

    #[test]
    fn directory_output_expands_to_contained_files() {
        let log = write_log(&[
            entry(
                0,
                EntryType::Invocation(Invocation {
                    hash_function_name: "SHA256".into(),
                    workspace_runfiles_directory: "_main".into(),
                    sibling_repository_layout: false,
                    id: "inv-2".into(),
                }),
            ),
            entry(
                1,
                EntryType::Directory(DirEntry {
                    path: "bazel-out/k8/bin/tree".into(),
                    files: vec![
                        FileEntryProto {
                            path: "a.txt".into(),
                            digest: None,
                        },
                        FileEntryProto {
                            path: "b.txt".into(),
                            digest: None,
                        },
                    ],
                }),
            ),
            entry(
                2,
                EntryType::Spawn(SpawnEntry {
                    target_label: "//pkg:tree".into(),
                    mnemonic: "MakeTree".into(),
                    outputs: vec![Output {
                        r#type: Some(exec_log_entry::output::Type::OutputId(1)),
                    }],
                    ..Default::default()
                }),
            ),
        ]);

        let spawns = read_all(
            log.path(),
            DEFAULT_EXECLOG_MAX_MESSAGE_BYTES,
            DEFAULT_EXECLOG_MAX_DECOMPRESSED_BYTES,
        )
        .unwrap();
        assert_eq!(spawns.len(), 1);
        let s = &spawns[0];
        assert_eq!(s.listed_outputs, vec!["bazel-out/k8/bin/tree".to_string()]);
        let paths: Vec<_> = s.actual_outputs.iter().map(|f| f.path.clone()).collect();
        assert_eq!(
            paths,
            vec![
                "bazel-out/k8/bin/tree/a.txt".to_string(),
                "bazel-out/k8/bin/tree/b.txt".to_string(),
            ]
        );
    }

    #[test]
    fn rejects_message_length_exceeding_cap() {
        let f = NamedTempFile::new().unwrap();
        let mut enc = zstd::Encoder::new(f.reopen().unwrap(), 0).unwrap();
        let mut tmp = [0u8; 10];
        let oversize = (DEFAULT_EXECLOG_MAX_MESSAGE_BYTES as u64) + 1;
        let n = encode_varint(oversize, &mut tmp);
        enc.write_all(&tmp[..n]).unwrap();
        enc.finish().unwrap();

        let err = read_all(
            f.path(),
            DEFAULT_EXECLOG_MAX_MESSAGE_BYTES,
            DEFAULT_EXECLOG_MAX_DECOMPRESSED_BYTES,
        )
        .expect_err("oversize message length must be rejected");
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
        assert!(
            err.to_string().contains("exceeds"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn rejects_decompressed_payload_exceeding_cap() {
        // Pack 64 KiB of zero bytes (highly compressible) into a zstd frame
        // that on-disk is well under 1 KiB, then read it back with a 1 KiB
        // decompressed cap. The reader should error before exhausting the
        // frame -- this is the zip-bomb defence.
        let f = NamedTempFile::new().unwrap();
        let mut enc = zstd::Encoder::new(f.reopen().unwrap(), 0).unwrap();
        let payload = vec![0u8; 64 * 1024];
        enc.write_all(&payload).unwrap();
        enc.finish().unwrap();

        let err = read_all(f.path(), DEFAULT_EXECLOG_MAX_MESSAGE_BYTES, 1024)
            .expect_err("decompressed bomb must be rejected");
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
        assert!(
            err.to_string().contains("decompressed"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn invalid_output_path_is_recorded_verbatim() {
        let log = write_log(&[
            entry(
                0,
                EntryType::Invocation(Invocation {
                    hash_function_name: "SHA256".into(),
                    workspace_runfiles_directory: "_main".into(),
                    sibling_repository_layout: false,
                    id: "inv-3".into(),
                }),
            ),
            entry(
                1,
                EntryType::Spawn(SpawnEntry {
                    target_label: "//pkg:bad".into(),
                    mnemonic: "BadAction".into(),
                    outputs: vec![Output {
                        r#type: Some(exec_log_entry::output::Type::InvalidOutputPath(
                            "bazel-out/k8/bin/missing".into(),
                        )),
                    }],
                    ..Default::default()
                }),
            ),
        ]);

        let spawns = read_all(
            log.path(),
            DEFAULT_EXECLOG_MAX_MESSAGE_BYTES,
            DEFAULT_EXECLOG_MAX_DECOMPRESSED_BYTES,
        )
        .unwrap();
        assert_eq!(spawns.len(), 1);
        assert_eq!(
            spawns[0].listed_outputs,
            vec!["bazel-out/k8/bin/missing".to_string()]
        );
        assert!(spawns[0].actual_outputs.is_empty());
    }
}

