// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

use crate::otel::span::{LogData, Severity};
use super::{strip_ansi, truncate_to_byte_limit, OtelMapper};
use std::fmt;
use std::time::SystemTime;

pub(super) const LOG_RECORD_CAP_BYTES: usize = 64 * 1024;
pub(super) const MAX_LOG_RECORDS: u64 = 256;
const TRUNCATED: &str = "...(truncated)";

/// Which BEP `Progress` output becomes OTLP log records (`--logs`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum LogMode {
    /// `ERROR:`/`FAIL:`/`WARNING:` blocks and failed test output.
    #[default]
    Errors,
    /// Every progress event, one record per non-empty stream.
    All,
    None,
}

impl LogMode {
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "errors" => Some(Self::Errors),
            "all" => Some(Self::All),
            "none" => Some(Self::None),
            _ => None,
        }
    }
}

impl fmt::Display for LogMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Errors => "errors",
            Self::All => "all",
            Self::None => "none",
        })
    }
}

pub(super) struct Record {
    severity: Severity,
    body: String,
}

pub(super) fn handle_progress(mapper: &mut OtelMapper, stderr: Option<&str>, stdout: Option<&str>) {
    if mapper.log_mode == LogMode::None {
        return;
    }
    let clean = |s: Option<&str>| match s {
        Some(s) if !s.is_empty() => mapper.redactor.scrub_text(&strip_ansi(s)).into_owned(),
        _ => String::new(),
    };
    let (stderr, stdout) = (clean(stderr), clean(stdout));
    let records = match mapper.log_mode {
        LogMode::All => all_records(stderr, stdout),
        _ => mapper.error_logs.feed(&stderr, &stdout),
    };
    emit(mapper, records);
}

/// Emit the error block still open when the invocation ends.
pub(super) fn flush_progress(mapper: &mut OtelMapper) {
    let records = mapper.error_logs.flush();
    emit(mapper, records);
}

fn emit(mapper: &mut OtelMapper, records: Vec<Record>) {
    let (Some(logger), Some(cx)) = (mapper.logger.as_ref(), mapper.root_context.as_ref()) else {
        return;
    };
    let span_context = cx.span().span_context();
    for record in records {
        if mapper.log_records >= MAX_LOG_RECORDS {
            mapper.suppressed_logs += 1;
            continue;
        }
        mapper.log_records += 1;
        logger.emit(LogData {
            timestamp: SystemTime::now(),
            span_context,
            severity: record.severity,
            body: cap(&record.body),
            attributes: Vec::new(),
        });
    }
}

pub(super) fn cap(s: &str) -> String {
    truncate_to_byte_limit(s, LOG_RECORD_CAP_BYTES - TRUNCATED.len(), TRUNCATED)
}

fn all_records(stderr: String, stdout: String) -> Vec<Record> {
    [stderr, stdout]
        .into_iter()
        .filter(|body| !body.is_empty())
        .map(|body| Record { severity: Severity::Info, body })
        .collect()
}

/// `--logs=errors` classifier. Keeps `ERROR:`/`FAIL:`/`WARNING:` blocks from
/// stderr, each with the lines that follow it up to the next Bazel UI line (a
/// failed action's own stderr is printed unindented right after its
/// `ERROR:`), plus `--test_output` blocks from either stream. Progress events
/// carry console deltas, so blocks and even lines can span events.
#[derive(Default)]
pub(super) struct ErrorLogParser {
    stderr_tail: String,
    stdout_tail: String,
    block: Option<Record>,
    stderr_test_output: Option<String>,
    stdout_test_output: Option<String>,
}

const TEST_OUTPUT_BANNER: &str = "==================== Test output for ";

impl ErrorLogParser {
    pub(super) fn feed(&mut self, stderr: &str, stdout: &str) -> Vec<Record> {
        let mut out = Vec::new();
        for line in complete_lines(&mut self.stderr_tail, stderr) {
            self.stderr_line(&line, &mut out);
        }
        for line in complete_lines(&mut self.stdout_tail, stdout) {
            test_output_line(&mut self.stdout_test_output, &line, &mut out);
        }
        out
    }

    pub(super) fn flush(&mut self) -> Vec<Record> {
        let mut out = Vec::new();
        let (stderr, stdout) = (std::mem::take(&mut self.stderr_tail), std::mem::take(&mut self.stdout_tail));
        if !stderr.is_empty() {
            self.stderr_line(&stderr, &mut out);
        }
        if !stdout.is_empty() {
            test_output_line(&mut self.stdout_test_output, &stdout, &mut out);
        }
        out.extend(self.block.take().map(finished));
        for test_output in [self.stderr_test_output.take(), self.stdout_test_output.take()] {
            out.extend(test_output.map(error_record));
        }
        out
    }

    fn stderr_line(&mut self, line: &str, out: &mut Vec<Record>) {
        if line.starts_with(TEST_OUTPUT_BANNER) {
            out.extend(self.block.take().map(finished));
        }
        if test_output_line(&mut self.stderr_test_output, line, out) {
            return;
        }
        let severity = if line.starts_with("ERROR:") || line.starts_with("FAIL:") {
            Some(Severity::Error)
        } else if line.starts_with("WARNING:") {
            Some(Severity::Warn)
        } else {
            None
        };
        if severity.is_some() || is_ui_line(line) {
            out.extend(self.block.take().map(finished));
        }
        if let Some(severity) = severity {
            self.block = Some(Record { severity, body: line.to_string() });
        } else if let Some(block) = self.block.as_mut() {
            append_line(&mut block.body, line);
        }
    }
}

/// Feeds `line` to a `--test_output` block; returns whether it belonged to one.
fn test_output_line(state: &mut Option<String>, line: &str, out: &mut Vec<Record>) -> bool {
    if line.starts_with(TEST_OUTPUT_BANNER) {
        out.extend(state.take().map(error_record));
        *state = Some(line.to_string());
        return true;
    }
    let Some(body) = state.as_mut() else {
        return false;
    };
    append_line(body, line);
    if line.len() >= 20 && line.bytes().all(|b| b == b'=') {
        out.extend(state.take().map(error_record));
    }
    true
}

fn error_record(body: String) -> Record {
    finished(Record { severity: Severity::Error, body })
}

/// Appends `chunk` to `tail` and returns its complete lines, leaving an
/// unterminated last line in `tail` (forced out once it exceeds the cap).
fn complete_lines(tail: &mut String, chunk: &str) -> Vec<String> {
    tail.push_str(chunk);
    let rest = match tail.rfind('\n') {
        Some(end) => tail.split_off(end + 1),
        None if tail.len() > LOG_RECORD_CAP_BYTES => String::new(),
        None => return Vec::new(),
    };
    let lines = tail.lines().map(str::to_string).collect();
    *tail = rest;
    lines
}

/// Bodies stop growing past the record cap; [`cap`] marks the truncation.
fn append_line(body: &mut String, line: &str) {
    if body.len() <= LOG_RECORD_CAP_BYTES {
        body.push('\n');
        body.push_str(line);
    }
}

fn finished(mut record: Record) -> Record {
    record.body.truncate(record.body.trim_end().len());
    record
}

/// Bazel's own progress/info output, which ends an error block. `FAILED:`
/// is the final progress bar, not a message.
fn is_ui_line(line: &str) -> bool {
    const PREFIXES: [&str; 7] = [
        "INFO:",
        "DEBUG:",
        "FAILED:",
        "Loading:",
        "Analyzing:",
        "Computing main repo mapping",
        "Fetching ",
    ];
    PREFIXES.iter().any(|p| line.starts_with(p))
        || (line.starts_with('[') && line[1..].starts_with(|c: char| c.is_ascii_digit()))
}

#[cfg(test)]
pub(super) fn error_record_summaries(chunks: &[(&str, &str)]) -> Vec<(Severity, String)> {
    let mut parser = ErrorLogParser::default();
    let mut records = Vec::new();
    for (stderr, stdout) in chunks {
        records.extend(parser.feed(stderr, stdout));
    }
    records.extend(parser.flush());
    records.into_iter().map(|r| (r.severity, r.body)).collect()
}
