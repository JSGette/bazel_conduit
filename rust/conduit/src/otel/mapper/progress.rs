// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

use crate::otel::span::{LogData, Severity};
use super::{strip_ansi, truncate_to_byte_limit, OtelMapper};
use std::collections::HashMap;
use std::fmt;
use std::time::SystemTime;

pub(super) const LOG_RECORD_CAP_BYTES: usize = 64 * 1024;
pub(super) const MAX_LOG_RECORDS: u64 = 256;
const TRUNCATED: &str = "...(truncated)";
const TRUNCATED_HEAD: &str = "\n(truncated)...";

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

pub(super) fn handle_test_summary(mapper: &mut OtelMapper, label: &str, failed: bool) {
    if mapper.log_mode == LogMode::Errors {
        let records = mapper.error_logs.test_summary(label, failed);
        emit(mapper, records);
    }
}

/// Emit the error block still open when the invocation ends.
pub(super) fn flush_progress(mapper: &mut OtelMapper) {
    let records = mapper.error_logs.flush();
    mapper.suppressed_logs += std::mem::take(&mut mapper.error_logs.dropped);
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
/// `--test_output=all` prints passing tests too, so a test output block is
/// held until its label's `TestSummary`: shards, runs and attempts finish in
/// any order, and only the summary knows whether any of them failed. A label
/// built in several configurations has several summaries; any failure wins,
/// but output arriving after a passing summary is dropped even if another
/// configuration later fails (the banner does not name the configuration).
#[derive(Default)]
pub(super) struct ErrorLogParser {
    stderr_tail: String,
    stdout_tail: String,
    block: Option<Record>,
    stderr_test_output: Option<String>,
    stdout_test_output: Option<String>,
    test_failed: HashMap<String, bool>,
    pending_test_output: HashMap<String, Vec<String>>,
    /// Held blocks are capped at what could still be emitted; drops count as
    /// suppressed only once their label fails (or never gets a summary).
    pub(super) pending_blocks: usize,
    dropped_by_label: HashMap<String, u64>,
    pub(super) dropped: u64,
}

const TEST_OUTPUT_BANNER: &str = "==================== Test output for ";

impl ErrorLogParser {
    pub(super) fn feed(&mut self, stderr: &str, stdout: &str) -> Vec<Record> {
        let mut out = Vec::new();
        let mut test_output = Vec::new();
        for line in complete_lines(&mut self.stderr_tail, stderr) {
            self.stderr_line(&line, &mut out, &mut test_output);
        }
        for line in complete_lines(&mut self.stdout_tail, stdout) {
            test_output_line(&mut self.stdout_test_output, &line, &mut test_output);
        }
        self.route_test_output(test_output, &mut out);
        out
    }

    pub(super) fn test_summary(&mut self, label: &str, failed: bool) -> Vec<Record> {
        let failed = *self.test_failed.entry(label.to_string()).and_modify(|f| *f |= failed).or_insert(failed);
        let pending = self.pending_test_output.remove(label).unwrap_or_default();
        self.pending_blocks -= pending.len();
        let dropped = self.dropped_by_label.remove(label).unwrap_or_default();
        if !failed {
            return Vec::new();
        }
        self.dropped += dropped;
        pending.into_iter().map(error_record).collect()
    }

    fn route_test_output(&mut self, blocks: Vec<String>, out: &mut Vec<Record>) {
        for body in blocks {
            let label = test_output_label(&body).to_string();
            match self.test_failed.get(&label) {
                Some(true) => out.push(error_record(body)),
                Some(false) => {}
                None if self.pending_blocks >= MAX_LOG_RECORDS as usize => {
                    *self.dropped_by_label.entry(label).or_default() += 1;
                }
                None => {
                    self.pending_blocks += 1;
                    self.pending_test_output.entry(label).or_default().push(body);
                }
            }
        }
    }

    pub(super) fn flush(&mut self) -> Vec<Record> {
        let mut out = Vec::new();
        let mut test_output = Vec::new();
        let (stderr, stdout) = (std::mem::take(&mut self.stderr_tail), std::mem::take(&mut self.stdout_tail));
        if !stderr.is_empty() {
            self.stderr_line(&stderr, &mut out, &mut test_output);
        }
        if !stdout.is_empty() {
            test_output_line(&mut self.stdout_test_output, &stdout, &mut test_output);
        }
        out.extend(self.block.take().map(finished));
        test_output.extend(self.stderr_test_output.take());
        test_output.extend(self.stdout_test_output.take());
        self.route_test_output(test_output, &mut out);
        // No TestSummary arrived (e.g. an interrupted build): keep the output.
        let mut pending: Vec<_> = self.pending_test_output.drain().collect();
        self.pending_blocks = 0;
        self.dropped += self.dropped_by_label.drain().map(|(_, n)| n).sum::<u64>();
        pending.sort();
        out.extend(pending.into_iter().flat_map(|(_, bodies)| bodies).map(error_record));
        out
    }

    fn stderr_line(&mut self, line: &str, out: &mut Vec<Record>, test_output: &mut Vec<String>) {
        if line.starts_with(TEST_OUTPUT_BANNER) {
            out.extend(self.block.take().map(finished));
        }
        if test_output_line(&mut self.stderr_test_output, line, test_output) {
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
fn test_output_line(state: &mut Option<String>, line: &str, done: &mut Vec<String>) -> bool {
    if line.starts_with(TEST_OUTPUT_BANNER) {
        done.extend(state.take());
        *state = Some(line.to_string());
        return true;
    }
    let Some(body) = state.as_mut() else {
        return false;
    };
    body.push('\n');
    body.push_str(line);
    if body.len() > 2 * LOG_RECORD_CAP_BYTES {
        keep_banner_and_tail(body);
    }
    if line.len() >= 20 && line.bytes().all(|b| b == b'=') {
        done.extend(state.take());
    }
    true
}

/// `==================== Test output for //pkg:t (shard 1 of 2):` -> `//pkg:t`.
fn test_output_label(body: &str) -> &str {
    let banner = body.lines().next().unwrap_or_default();
    let name = banner.strip_prefix(TEST_OUTPUT_BANNER).unwrap_or(banner);
    let name = name.strip_suffix(':').unwrap_or(name);
    name.split(" (").next().unwrap_or(name)
}

/// Test output record: a test reports its failure at the end of its log.
fn error_record(mut body: String) -> Record {
    keep_banner_and_tail(&mut body);
    finished(Record { severity: Severity::Error, body })
}

/// Shrink a test output block to what [`cap`] passes unchanged, keeping the
/// end of the output and the banner line, which names the test and routes
/// the block to its `TestSummary`.
fn keep_banner_and_tail(body: &mut String) {
    let max = LOG_RECORD_CAP_BYTES - TRUNCATED.len();
    if body.len() <= max {
        return;
    }
    let mut banner = body.find('\n').unwrap_or(body.len()).min(max / 2);
    while !body.is_char_boundary(banner) {
        banner -= 1;
    }
    let mut start = body.len() - (max - banner - TRUNCATED_HEAD.len());
    while !body.is_char_boundary(start) {
        start += 1;
    }
    *body = format!("{}{TRUNCATED_HEAD}{}", &body[..banner], &body[start..]);
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
