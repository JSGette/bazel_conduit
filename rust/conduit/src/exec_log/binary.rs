//! Binary execution log parser.
//!
//! Reads `--execution_log_binary_file=<path>` -- a stream of length-delimited
//! [`SpawnExec`] protobuf messages, each prefixed by a varint encoding its
//! byte size. No compression, no dedup table, no top-level framing -- just
//! `(varint length, payload)*`.

use std::fs::File;
use std::io::{BufReader, Read};
use std::path::Path;

use prost::Message;
use spawn_proto::tools::protos::SpawnExec;

use super::read_message_len;

/// Default buffer size for chunked reading of the exec log file (64 KiB).
const DEFAULT_READER_CAPACITY: usize = 64 * 1024;

/// Chunked parser for the binary execution log.
pub struct ExecLogParser {
    reader: BufReader<File>,
    buffer: Vec<u8>,
    max_message_bytes: usize,
}

impl ExecLogParser {
    pub fn open(path: &Path, max_message_bytes: usize) -> std::io::Result<Self> {
        let file = File::open(path)?;
        let reader = BufReader::with_capacity(DEFAULT_READER_CAPACITY, file);
        Ok(Self {
            reader,
            buffer: Vec::new(),
            max_message_bytes,
        })
    }

    /// Read the next [`SpawnExec`] message. Returns `Ok(None)` at EOF.
    pub fn next_entry(&mut self) -> std::io::Result<Option<SpawnExec>> {
        let Some(len) = read_message_len(&mut self.reader, self.max_message_bytes)? else {
            return Ok(None);
        };
        self.buffer.resize(len, 0);
        self.reader.read_exact(&mut self.buffer)?;
        let msg = SpawnExec::decode(self.buffer.as_slice())
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        Ok(Some(msg))
    }
}

/// Read every [`SpawnExec`] in the file. Stops at the first I/O or decode
/// error and returns it; partial results are discarded. `max_message_bytes`
/// caps the per-message size to prevent OOM from a malformed varint prefix.
pub fn read_all(path: &Path, max_message_bytes: usize) -> std::io::Result<Vec<SpawnExec>> {
    let mut parser = ExecLogParser::open(path, max_message_bytes)?;
    let mut out = Vec::new();
    while let Some(entry) = parser.next_entry()? {
        out.push(entry);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exec_log::DEFAULT_EXECLOG_MAX_MESSAGE_BYTES;
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

    #[test]
    fn rejects_message_length_exceeding_cap() {
        let f = NamedTempFile::new().unwrap();
        let mut tmp = [0u8; 10];
        let oversize = (DEFAULT_EXECLOG_MAX_MESSAGE_BYTES as u64) + 1;
        let n = encode_varint(oversize, &mut tmp);
        std::fs::write(f.path(), &tmp[..n]).unwrap();

        let err = read_all(f.path(), DEFAULT_EXECLOG_MAX_MESSAGE_BYTES)
            .expect_err("oversize message length must be rejected");
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
        assert!(
            err.to_string().contains("exceeds"),
            "unexpected error: {err}"
        );
    }
}
