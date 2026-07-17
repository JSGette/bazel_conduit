//! Live tail-follow of Bazel's compact execution log.
//!
//! Bazel writes the compact log via
//! `AsynchronousMessageOutputStream → ZstdOutputStream → BufferedOutputStream(default 8 KiB) → FileOutputStream`
//! ([`CompactSpawnLogContext.java:204`]). zstd-jni's `ZstdOutputStream` only
//! emits a compressed block once its internal input buffer fills at 128 KiB
//! of *uncompressed* proto data, so this is "best-effort streaming" rather
//! than "every spawn live": a chunk lands every ~200–600 spawns on big
//! builds, and the last <128 KiB of input is always invisible until
//! `close()` at `BuildCompleteEvent`.
//!
//! The tailer is one [`tokio::task::spawn_blocking`] worker per invocation:
//!
//! 1. Open the file (with backoff if Bazel hasn't created it yet — BEP
//!    `OptionsParsed` arrives before `SpawnLogModule.executorInit`).
//! 2. Wrap in a small `Read` adapter that **blocks** (sleeps 100 ms then
//!    retries) when the inner file is momentarily at EOF, instead of
//!    propagating `Ok(0)`. This is the linchpin: `zstd::Decoder<R: Read>`
//!    keeps asking `R::read()` for bytes; as long as we never falsely
//!    signal EOF, the decoder happily resumes when Bazel flushes the next
//!    block.
//! 3. Drive [`compact::State::ingest`] over each length-delimited
//!    [`ExecLogEntry`], forwarding any [`SpawnExec`] it resolves through
//!    an mpsc to the mapper.
//!
//! Cooperative shutdown via [`TailerHandle::shutdown`]: the adapter checks
//! the flag on every sleep tick. The mapper sets the flag from `finish()`
//! after giving the tailer a bounded drain window (so the post-`close()`
//! final chunk + frame terminator can flow through first).

use std::fs::File;
use std::io::{BufReader, Read};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use prost::Message;
use spawn_proto::tools::protos::{ExecLogEntry, SpawnExec};
use tokio::sync::mpsc;
use tracing::{debug, warn};

use super::compact::{LimitedReader, State};
use super::{
    DEFAULT_EXECLOG_MAX_DECOMPRESSED_BYTES, DEFAULT_EXECLOG_MAX_MESSAGE_BYTES, read_message_len,
};

/// How long to wait between EOF-poll attempts when the inner file has no
/// new bytes. Matched to Bazel's ~seconds-scale flush cadence (one chunk
/// per 128 KiB of input means flushes are typically several seconds apart).
const TAIL_POLL_INTERVAL: Duration = Duration::from_millis(100);

/// How long to wait between attempts to open the file when it doesn't exist
/// yet. Capped at [`MAX_OPEN_RETRIES`] so a typo / missing-file scenario
/// doesn't hang forever.
const OPEN_RETRY_INTERVAL: Duration = Duration::from_millis(200);

/// Cap on how many times we retry opening the file before giving up. With
/// [`OPEN_RETRY_INTERVAL`] of 200 ms this gives a ~30 s budget, which is
/// plenty for the gap between `OptionsParsed` and `SpawnLogModule` opening
/// the file (typically milliseconds).
const MAX_OPEN_RETRIES: u32 = 150;

/// Channel depth between the tailer and the mapper. Sized large enough to
/// hold the final post-`close()` burst — a 128 KiB chunk holds roughly
/// 200–600 spawn entries, and we want the tailer to never block on send
/// once `close()` has flushed the tail.
const CHANNEL_DEPTH: usize = 4096;

/// Handle returned by [`spawn`] so the mapper can shut the tailer down and
/// receive the stream of [`SpawnExec`] records as they're decoded.
pub struct TailerHandle {
    /// Stream of `SpawnExec` records, in the order they appeared in the
    /// compact log. Dropping the receiver signals the tailer to terminate
    /// (via the dropped sender's `send` errors) as a back-stop in addition
    /// to the explicit shutdown flag.
    pub rx: mpsc::Receiver<SpawnExec>,
    shutdown: Arc<AtomicBool>,
    /// Terminal error from the worker, populated once `run()` returns Err.
    /// Read by the mapper after shutdown to surface a diagnostic event on
    /// the root span.
    last_error: Arc<Mutex<Option<String>>>,
    join: tokio::task::JoinHandle<()>,
}

impl TailerHandle {
    /// Signal the tailer to terminate. The adapter wakes from its 100 ms
    /// sleep, observes the flag, returns `Ok(0)` to the decoder, the
    /// decoder finalises the frame, and the task exits cleanly.
    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::SeqCst);
    }

    /// Take the worker's terminal error, if any. Called by the mapper after
    /// [`Self::shutdown`] + drain has completed.
    pub fn take_error(&self) -> Option<String> {
        self.last_error.lock().ok().and_then(|mut g| g.take())
    }

    /// True once the spawn_blocking worker thread has returned. The mapper
    /// polls this after [`Self::shutdown`] to know when [`Self::take_error`]
    /// will reflect the final state — without it, a worker mid-sleep at
    /// shutdown time can race the error-slot write against the read.
    pub fn is_finished(&self) -> bool {
        self.join.is_finished()
    }

    /// Await the worker's exit. The mapper calls this after [`Self::shutdown`]
    /// (with a timeout) to clean up the blocking thread on `finish()`.
    pub async fn join(self) {
        let _ = self.join.await;
    }
}

/// Spawn a tailer worker. Returns a handle the mapper drives.
///
/// `max_message_bytes` and `max_decompressed_bytes` cap pathological logs
/// (length-prefix attack and zstd zip-bomb respectively); both default to
/// the same values as the post-build batch reader so a config that worked
/// for `read_all` will also work live.
pub fn spawn(
    path: PathBuf,
    max_message_bytes: usize,
    max_decompressed_bytes: usize,
) -> TailerHandle {
    let (tx, rx) = mpsc::channel(CHANNEL_DEPTH);
    let shutdown = Arc::new(AtomicBool::new(false));
    let shutdown_for_task = Arc::clone(&shutdown);
    let last_error = Arc::new(Mutex::new(None));
    let last_error_for_task = Arc::clone(&last_error);

    let join = tokio::task::spawn_blocking(move || {
        if let Err(err) = run(&path, max_message_bytes, max_decompressed_bytes, &shutdown_for_task, &tx) {
            warn!(
                path = %path.display(),
                error = %err,
                "Compact exec log tailer aborted",
            );
            if let Ok(mut slot) = last_error_for_task.lock() {
                *slot = Some(err.to_string());
            }
        } else {
            debug!(path = %path.display(), "Compact exec log tailer exited cleanly");
        }
    });

    TailerHandle {
        rx,
        shutdown,
        last_error,
        join,
    }
}

/// Same as [`spawn`] but with the default caps. Convenience for callers
/// that don't override.
pub fn spawn_with_defaults(path: PathBuf) -> TailerHandle {
    spawn(
        path,
        DEFAULT_EXECLOG_MAX_MESSAGE_BYTES,
        DEFAULT_EXECLOG_MAX_DECOMPRESSED_BYTES,
    )
}

fn run(
    path: &Path,
    max_message_bytes: usize,
    max_decompressed_bytes: usize,
    shutdown: &Arc<AtomicBool>,
    tx: &mpsc::Sender<SpawnExec>,
) -> std::io::Result<()> {
    let file = open_with_backoff(path, shutdown)?;
    let tailed = TailingReader::new(file, Arc::clone(shutdown));
    let limited = LimitedReader::new(tailed, max_decompressed_bytes);
    let decoder = zstd::Decoder::new(limited)?;
    let mut reader = BufReader::with_capacity(64 * 1024, decoder);

    let mut state = State::default();
    let mut buf = Vec::new();

    while let Some(len) = read_message_len(&mut reader, max_message_bytes)? {
        buf.resize(len, 0);
        reader.read_exact(&mut buf)?;
        let entry = ExecLogEntry::decode(buf.as_slice())
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        if let Some(spawn) = state.ingest(entry)? {
            if tx.blocking_send(spawn).is_err() {
                // Receiver dropped: mapper is gone. Exit quietly.
                return Ok(());
            }
        }
    }
    Ok(())
}

fn open_with_backoff(path: &Path, shutdown: &Arc<AtomicBool>) -> std::io::Result<File> {
    let mut tries = 0u32;
    loop {
        match File::open(path) {
            Ok(f) => return Ok(f),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                tries += 1;
                if tries > MAX_OPEN_RETRIES || shutdown.load(Ordering::SeqCst) {
                    return Err(e);
                }
                std::thread::sleep(OPEN_RETRY_INTERVAL);
            }
            Err(e) => return Err(e),
        }
    }
}

/// A synchronous [`Read`] over a `File` that **blocks on EOF** instead of
/// propagating it. zstd's `Decoder` calls `read()` repeatedly; if we
/// returned `Ok(0)` mid-frame the decoder would treat the frame as
/// truncated and either fail or stop. Instead we sleep 100 ms and retry —
/// when Bazel flushes the next compressed block (or runs `close()`), the
/// underlying file returns new bytes and the decoder keeps going. The
/// shutdown flag is the one and only way to break out: once set, we
/// propagate `Ok(0)` so the decoder finalises the frame and we exit.
struct TailingReader {
    file: File,
    shutdown: Arc<AtomicBool>,
}

impl TailingReader {
    fn new(file: File, shutdown: Arc<AtomicBool>) -> Self {
        Self { file, shutdown }
    }
}

impl Read for TailingReader {
    fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
        loop {
            let n = self.file.read(out)?;
            if n != 0 {
                return Ok(n);
            }
            if self.shutdown.load(Ordering::SeqCst) {
                return Ok(0);
            }
            std::thread::sleep(TAIL_POLL_INTERVAL);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use spawn_proto::tools::protos::exec_log_entry::{
        File as FileEntryProto, Invocation, Output, Spawn as SpawnEntry, Type as EntryType,
        output::Type as OutputType,
    };
    use std::io::Write;
    use tempfile::NamedTempFile;

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

    /// Encode entries through a fresh zstd frame and append to `path`.
    /// Each call closes its own frame — Bazel only ever emits one frame
    /// per invocation, but our decoder doesn't actually care because the
    /// underlying file is read as a continuous byte stream.
    fn append_frame(path: &Path, entries: &[ExecLogEntry]) {
        let file = std::fs::OpenOptions::new()
            .append(true)
            .open(path)
            .unwrap();
        let mut enc = zstd::Encoder::new(file, 0).unwrap();
        for e in entries {
            let len = e.encoded_len() as u64;
            let mut tmp = [0u8; 10];
            let n = encode_varint(len, &mut tmp);
            enc.write_all(&tmp[..n]).unwrap();
            let mut buf = Vec::with_capacity(e.encoded_len());
            e.encode(&mut buf).unwrap();
            enc.write_all(&buf).unwrap();
        }
        enc.finish().unwrap();
    }

    fn entry(id: u32, t: EntryType) -> ExecLogEntry {
        ExecLogEntry {
            id,
            r#type: Some(t),
        }
    }

    /// End-to-end: write a frame, spawn the tailer, drain the channel,
    /// shut down. Catches that the blocking-on-EOF reader actually wakes
    /// up when shutdown fires.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn drains_existing_frame_then_shuts_down() {
        let f = NamedTempFile::new().unwrap();
        append_frame(
            f.path(),
            &[
                entry(
                    0,
                    EntryType::Invocation(Invocation {
                        hash_function_name: "SHA256".into(),
                        workspace_runfiles_directory: "_main".into(),
                        sibling_repository_layout: false,
                        id: "inv".into(),
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
                        target_label: "//pkg:foo".into(),
                        mnemonic: "CppCompile".into(),
                        runner: "linux-sandbox".into(),
                        outputs: vec![Output {
                            r#type: Some(OutputType::OutputId(1)),
                        }],
                        ..Default::default()
                    }),
                ),
            ],
        );

        let mut handle = spawn_with_defaults(f.path().to_path_buf());
        // Allow the worker to read the closed frame.
        let spawn = tokio::time::timeout(Duration::from_secs(2), handle.rx.recv())
            .await
            .expect("tailer should produce a spawn within 2s")
            .expect("tailer should not drop the channel before shutdown");
        assert_eq!(spawn.target_label, "//pkg:foo");
        assert_eq!(spawn.runner, "linux-sandbox");
        handle.shutdown();
        handle.join.await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn shutdown_drains_records_already_on_disk() {
        let f = NamedTempFile::new().unwrap();
        append_frame(
            f.path(),
            &[
                entry(
                    0,
                    EntryType::Invocation(Invocation {
                        hash_function_name: "SHA256".into(),
                        workspace_runfiles_directory: "_main".into(),
                        sibling_repository_layout: false,
                        id: "inv".into(),
                    }),
                ),
                entry(
                    1,
                    EntryType::Spawn(SpawnEntry {
                        target_label: "//pkg:final".into(),
                        mnemonic: "Rustc".into(),
                        ..Default::default()
                    }),
                ),
            ],
        );

        let mut handle = spawn_with_defaults(f.path().to_path_buf());
        handle.shutdown();

        let spawn = tokio::time::timeout(Duration::from_secs(2), handle.rx.recv())
            .await
            .expect("tailer should stop within 2s")
            .expect("record already on disk must survive shutdown");
        assert_eq!(spawn.target_label, "//pkg:final");
        handle.join().await;
    }

    /// File doesn't exist at spawn time. Tailer retries the open until
    /// the file appears.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn waits_for_file_to_appear() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("not-yet.log.zst");
        let mut handle = spawn_with_defaults(path.clone());

        // Wait a bit longer than the open backoff, then create the file.
        tokio::time::sleep(Duration::from_millis(400)).await;
        append_frame(
            {
                std::fs::File::create(&path).unwrap();
                &path
            },
            &[
                entry(
                    0,
                    EntryType::Invocation(Invocation {
                        hash_function_name: "SHA256".into(),
                        workspace_runfiles_directory: "_main".into(),
                        sibling_repository_layout: false,
                        id: "inv".into(),
                    }),
                ),
                entry(
                    1,
                    EntryType::Spawn(SpawnEntry {
                        target_label: "//pkg:late".into(),
                        mnemonic: "GoCompile".into(),
                        ..Default::default()
                    }),
                ),
            ],
        );

        let spawn = tokio::time::timeout(Duration::from_secs(2), handle.rx.recv())
            .await
            .expect("tailer should produce a spawn once file exists")
            .expect("tailer should not drop the channel before shutdown");
        assert_eq!(spawn.target_label, "//pkg:late");
        handle.shutdown();
        handle.join.await.unwrap();
    }
}
