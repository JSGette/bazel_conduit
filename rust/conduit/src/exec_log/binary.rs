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

/// Default buffer size for chunked reading of the exec log file (64 KiB).
const DEFAULT_READER_CAPACITY: usize = 64 * 1024;

/// Read a varint encoding a non-negative `u64` from the stream. Returns
/// `Ok(None)` on a clean EOF before any bytes were consumed (i.e. at message
/// boundaries) so callers can distinguish end-of-stream from a truncated
/// length prefix.
fn read_varint<R: Read>(r: &mut R) -> std::io::Result<Option<u64>> {
    let mut buf = [0u8; 1];
    let mut n: u64 = 0;
    let mut shift: u32 = 0;
    loop {
        if r.read(&mut buf)? == 0 {
            return Ok(if shift == 0 { None } else { Some(n) });
        }
        let b = buf[0];
        n |= u64::from(b & 0x7F) << shift;
        shift += 7;
        if (b & 0x80) == 0 {
            return Ok(Some(n));
        }
        if shift >= 64 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "varint overflow",
            ));
        }
    }
}

/// Chunked parser for the binary execution log.
pub struct ExecLogParser {
    reader: BufReader<File>,
    buffer: Vec<u8>,
}

impl ExecLogParser {
    pub fn open(path: &Path) -> std::io::Result<Self> {
        let file = File::open(path)?;
        let reader = BufReader::with_capacity(DEFAULT_READER_CAPACITY, file);
        Ok(Self {
            reader,
            buffer: Vec::new(),
        })
    }

    /// Read the next [`SpawnExec`] message. Returns `Ok(None)` at EOF.
    pub fn next_entry(&mut self) -> std::io::Result<Option<SpawnExec>> {
        let len = match read_varint(&mut self.reader)? {
            Some(l) => l,
            None => return Ok(None),
        };
        let len_usize = len.try_into().map_err(|_| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, "message too large")
        })?;
        self.buffer.resize(len_usize, 0);
        self.reader.read_exact(&mut self.buffer)?;
        let msg = SpawnExec::decode(self.buffer.as_slice())
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        Ok(Some(msg))
    }
}

/// Read every [`SpawnExec`] in the file. Stops at the first I/O or decode
/// error and returns it; partial results are discarded.
pub fn read_all(path: &Path) -> std::io::Result<Vec<SpawnExec>> {
    let mut parser = ExecLogParser::open(path)?;
    let mut out = Vec::new();
    while let Some(entry) = parser.next_entry()? {
        out.push(entry);
    }
    Ok(out)
}
