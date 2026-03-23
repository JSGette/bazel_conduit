//! Execution log parsing and trace enrichment.
//!
//! Parses Bazel's binary execution log (length-delimited SpawnExec protos from
//! `--execution_log_binary_file`) and enriches the BEP-derived trace with spawn
//! child spans containing full command lines, I/O metrics, timing breakdowns,
//! and proper span hierarchy.
//!
//! Spawns are linked to their BEP action spans when possible. Unmatched spawns
//! are grouped under synthetic target spans derived from the exec log's
//! `target_label`, preserving the logical structure instead of dumping
//! everything under the root invocation.

use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, Read};
use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use opentelemetry::trace::{Span, SpanContext, SpanKind, TraceContextExt, Tracer};
use opentelemetry::{Context, KeyValue};
use prost::Message;
use tracing::{debug, info, warn};

use crate::otel::attributes::*;
use crate::otel::mapper::{ActionSpanInfo, ActionSpanKey, clamp_time_range};
use spawn_proto::tools::protos::SpawnExec;

fn nanos_to_system_time(nanos: i64) -> SystemTime {
    if nanos >= 0 {
        UNIX_EPOCH + Duration::from_nanos(nanos as u64)
    } else {
        UNIX_EPOCH
    }
}

/// Default buffer size for chunked reading of the exec log file (64 KiB).
const DEFAULT_READER_CAPACITY: usize = 64 * 1024;

/// Reads length-delimited protobuf messages from a byte stream.
/// Each message is prefixed by a varint encoding its length in bytes.
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

/// Chunked parser for binary execution log (length-delimited SpawnExec protos).
pub struct ExecLogParser {
    reader: BufReader<File>,
    buffer: Vec<u8>,
}

impl ExecLogParser {
    /// Open the execution log file for reading. Uses a 64 KiB buffer for chunked I/O.
    pub fn open(path: &Path) -> std::io::Result<Self> {
        let file = File::open(path)?;
        let reader = BufReader::with_capacity(DEFAULT_READER_CAPACITY, file);
        Ok(Self {
            reader,
            buffer: Vec::new(),
        })
    }

    /// Read the next SpawnExec message. Returns `Ok(None)` at EOF.
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

/// Normalize label for matching: strip leading `@` so BEP `@@repo//:t` and
/// exec log `@repo//:t` match.
fn normalize_label(s: &str) -> &str {
    s.trim_start_matches('@')
}

/// Candidate keys for cache lookup: (target_label, mnemonic, output) for each
/// output in listed_outputs and actual_outputs.
fn spawn_exec_candidate_keys(s: &SpawnExec) -> Vec<ActionSpanKey> {
    let label = normalize_label(s.target_label.as_str());
    let mnemonic = s.mnemonic.as_str();
    let mut keys = Vec::new();
    for out in &s.listed_outputs {
        keys.push(ActionSpanKey::new(Some(label), Some(mnemonic), Some(out)));
    }
    for f in &s.actual_outputs {
        if !f.path.is_empty() && !keys.iter().any(|k| k.primary_output == f.path) {
            keys.push(ActionSpanKey::new(
                Some(label),
                Some(mnemonic),
                Some(f.path.as_str()),
            ));
        }
    }
    if keys.is_empty() {
        keys.push(ActionSpanKey::new(Some(label), Some(mnemonic), Some("")));
    }
    keys
}

// ---------------------------------------------------------------------------
// Indexes
// ---------------------------------------------------------------------------

/// (normalized_label, mnemonic) -> list of (primary_output, ActionSpanInfo).
fn build_label_mnemonic_index(
    cache: &HashMap<ActionSpanKey, ActionSpanInfo>,
) -> HashMap<(String, String), Vec<(String, ActionSpanInfo)>> {
    let mut index: HashMap<(String, String), Vec<(String, ActionSpanInfo)>> = HashMap::new();
    for (k, info) in cache {
        let key = (k.target_label.clone(), k.mnemonic.clone());
        index
            .entry(key)
            .or_default()
            .push((k.primary_output.clone(), info.clone()));
    }
    index
}

/// output_path -> ActionSpanInfo, for matching by output alone when labels diverge.
fn build_output_index(cache: &HashMap<ActionSpanKey, ActionSpanInfo>) -> HashMap<String, ActionSpanInfo> {
    let mut index = HashMap::new();
    for (k, info) in cache {
        if !k.primary_output.is_empty() {
            index.insert(k.primary_output.clone(), info.clone());
        }
    }
    index
}

// ---------------------------------------------------------------------------
// Matching
// ---------------------------------------------------------------------------

/// Parent directory of a path (everything before the last `/`). Empty if no slash.
fn output_dir(path: &str) -> &str {
    path.rsplit_once('/').map(|(dir, _)| dir).unwrap_or("")
}

/// Find a parent ActionSpanInfo for this SpawnExec.
///
/// Strategy (in order):
///   1. Exact ActionSpanKey match (label + mnemonic + output)
///   2. Output-only match (handles label mismatches across targets)
///   3. (label, mnemonic) fuzzy output match (exact / ends_with)
///   4. (label, mnemonic) match by output directory (e.g. TestRunner: test.log vs test.xml)
fn find_parent_for_spawn(
    cache: &HashMap<ActionSpanKey, ActionSpanInfo>,
    label_mnemonic_index: &HashMap<(String, String), Vec<(String, ActionSpanInfo)>>,
    output_index: &HashMap<String, ActionSpanInfo>,
    s: &SpawnExec,
) -> Option<ActionSpanInfo> {
    // 1. exact key match
    for k in spawn_exec_candidate_keys(s) {
        if let Some(info) = cache.get(&k) {
            return Some(info.clone());
        }
    }

    // 2. output-only match
    for out in &s.listed_outputs {
        if let Some(info) = output_index.get(out.as_str()) {
            return Some(info.clone());
        }
    }
    for f in &s.actual_outputs {
        if let Some(info) = output_index.get(f.path.as_str()) {
            return Some(info.clone());
        }
    }

    // 3. (label, mnemonic) fuzzy output match
    let label = normalize_label(s.target_label.as_str()).to_string();
    let mnemonic = s.mnemonic.to_string();
    let candidates = label_mnemonic_index.get(&(label, mnemonic))?;
    if candidates.len() == 1 {
        return Some(candidates[0].1.clone());
    }
    let outputs: Vec<&str> = s
        .listed_outputs
        .iter()
        .map(String::as_str)
        .chain(s.actual_outputs.iter().map(|f| f.path.as_str()))
        .collect();
    for out in &outputs {
        for (cached_out, info) in candidates {
            if cached_out == *out || cached_out.ends_with(out) || out.ends_with(cached_out.as_str())
            {
                return Some(info.clone());
            }
        }
    }

    // 4. Match by output directory (e.g. TestRunner: action has test.log, spawn has test.xml)
    if !outputs.is_empty() {
        for out in &outputs {
            let spawn_dir = output_dir(out);
            if spawn_dir.is_empty() {
                continue;
            }
            for (cached_out, info) in candidates {
                if !cached_out.is_empty() && output_dir(cached_out) == spawn_dir {
                    return Some(info.clone());
                }
            }
        }
    }
    None
}

// ---------------------------------------------------------------------------
// Span name helpers
// ---------------------------------------------------------------------------

/// Build a descriptive operation name from the SpawnExec.
/// e.g. "spawn CppCompile zlib/adler32.c" instead of just "spawn CppCompile".
fn spawn_operation_name(s: &SpawnExec) -> String {
    let mnemonic = if s.mnemonic.is_empty() {
        "unknown"
    } else {
        s.mnemonic.as_str()
    };

    let short_id = derive_short_identifier(s);
    if let Some(id) = short_id {
        format!("spawn {} {}", mnemonic, id)
    } else {
        format!("spawn {}", mnemonic)
    }
}

/// Derive a short human-readable identifier for the spawn.
/// For compiles: the source file basename. For archives/links: the output name.
fn derive_short_identifier(s: &SpawnExec) -> Option<String> {
    // For CppCompile-like actions, the source file is typically the last `-c <file>` arg,
    // or the last non-flag arg before `-o`.
    if s.mnemonic.contains("Compile") || s.mnemonic.contains("compile") {
        if let Some(source) = find_source_in_command(s) {
            return Some(short_path(&source));
        }
    }

    // Fall back to primary output basename
    let primary = s
        .listed_outputs
        .first()
        .map(String::as_str)
        .or_else(|| s.actual_outputs.first().map(|f| f.path.as_str()));
    primary.map(|p| short_path(p))
}

/// Find the source file being compiled from command_args.
fn find_source_in_command(s: &SpawnExec) -> Option<String> {
    let args = &s.command_args;
    for (i, arg) in args.iter().enumerate() {
        if arg == "-c" {
            if let Some(next) = args.get(i + 1) {
                return Some(next.clone());
            }
        }
    }
    None
}

/// Shorten a bazel output path to its most informative tail.
/// "bazel-out/darwin_arm64-fastbuild/bin/external/+_repo_rules+zlib/_objs/zlib/adler32.o"
///  -> "zlib/adler32.o"
fn short_path(path: &str) -> String {
    let parts: Vec<&str> = path.split('/').collect();
    if parts.len() >= 2 {
        format!("{}/{}", parts[parts.len() - 2], parts[parts.len() - 1])
    } else {
        path.to_string()
    }
}

// ---------------------------------------------------------------------------
// Duration helpers
// ---------------------------------------------------------------------------

/// Convert a proto Duration (seconds + nanos) to milliseconds.
/// Works with any generated Duration type that has `.seconds: i64` and `.nanos: i32`.
fn duration_to_ms_raw(seconds: i64, nanos: i32) -> i64 {
    seconds * 1000 + i64::from(nanos) / 1_000_000
}

// ---------------------------------------------------------------------------
// Enrichment entry point
// ---------------------------------------------------------------------------

/// Enrich the trace by parsing the exec log and creating spawn child spans.
///
/// Two-pass approach:
///   1. Parse all entries, attempt to match each to a BEP action span.
///   2. For unmatched entries, create synthetic target spans grouped by
///      target_label, then emit spawn spans under them.
///
/// `root_start_nanos` / `root_end_nanos` are the root span's time bounds,
/// used to clamp synthetic target spans so they don't escape the build window.
pub fn enrich_trace(
    path: &Path,
    action_span_cache: &HashMap<ActionSpanKey, ActionSpanInfo>,
    tracer: &opentelemetry_sdk::trace::Tracer,
    root_span_context: Option<&SpanContext>,
    root_start_nanos: Option<i64>,
    root_end_nanos: Option<i64>,
) {
    let mut parser = match ExecLogParser::open(path) {
        Ok(p) => p,
        Err(e) => {
            warn!(path = %path.display(), error = %e, "Failed to open execution log");
            return;
        }
    };

    let label_mnemonic_index = build_label_mnemonic_index(action_span_cache);
    let output_index = build_output_index(action_span_cache);

    let mut matched: Vec<(ActionSpanInfo, SpawnExec)> = Vec::new();
    let mut unmatched: Vec<SpawnExec> = Vec::new();

    // Pass 1: parse and classify
    while let Ok(Some(entry)) = parser.next_entry() {
        let parent = find_parent_for_spawn(
            action_span_cache,
            &label_mnemonic_index,
            &output_index,
            &entry,
        );
        match parent {
            Some(info) => matched.push((info, entry)),
            None => unmatched.push(entry),
        }
    }

    let total = matched.len() + unmatched.len();

    // Emit matched spans under their BEP action parent
    for (parent_info, entry) in &matched {
        emit_spawn_span(tracer, parent_info, entry);
    }

    // Pass 2: group unmatched by target_label, create synthetic parents
    let unmatched_count = unmatched.len();
    if !unmatched.is_empty() {
        if let Some(root_cx) = root_span_context {
            emit_unmatched_under_synthetic_targets(
                tracer,
                root_cx,
                unmatched,
                root_start_nanos,
                root_end_nanos,
            );
        } else {
            debug!(
                count = unmatched_count,
                "Dropping unmatched exec log entries (no root span)"
            );
        }
    }

    info!(
        total = total,
        matched = matched.len(),
        unmatched = unmatched_count,
        cache_size = action_span_cache.len(),
        "Exec log enrichment complete"
    );
}

/// Group unmatched spawns by target_label and emit under synthetic target spans.
/// Timing is clamped to root span bounds to prevent synthetic targets from
/// escaping the build time window (e.g. cached actions with old timestamps).
fn emit_unmatched_under_synthetic_targets(
    tracer: &opentelemetry_sdk::trace::Tracer,
    root_cx: &SpanContext,
    entries: Vec<SpawnExec>,
    root_start_nanos: Option<i64>,
    root_end_nanos: Option<i64>,
) {
    let mut by_target: HashMap<String, Vec<SpawnExec>> = HashMap::new();
    for entry in entries {
        let label = normalize_label(&entry.target_label).to_string();
        let label = if label.is_empty() {
            "(unknown)".to_string()
        } else {
            label
        };
        by_target.entry(label).or_default().push(entry);
    }

    let root_ctx = Context::new().with_remote_span_context(root_cx.clone());

    for (label, spawns) in &by_target {
        let (raw_start, raw_end) = compute_time_bounds(spawns);
        let (clamped_start, clamped_end) =
            clamp_time_range(raw_start, raw_end, root_start_nanos, root_end_nanos);

        let span_name = format!("target {}", shorten_label(label));
        let attrs = vec![
            KeyValue::new(BAZEL_TARGET_LABEL, label.clone()),
            KeyValue::new(BAZEL_TARGET_LABEL_SHORT, shorten_label(label).to_string()),
            KeyValue::new(BAZEL_TARGET_SYNTHETIC, true),
        ];

        let mut builder = tracer
            .span_builder(span_name)
            .with_kind(SpanKind::Internal)
            .with_attributes(attrs);
        if let Some(start) = clamped_start {
            builder = builder.with_start_time(nanos_to_system_time(start));
        }

        let synthetic_info = ActionSpanInfo {
            span_context: SpanContext::empty_context(),
            start_nanos: clamped_start,
            end_nanos: clamped_end,
        };

        let mut target_span = tracer.build_with_context(builder, &root_ctx);
        let target_cx = target_span.span_context().clone();
        let target_info = ActionSpanInfo {
            span_context: target_cx,
            start_nanos: synthetic_info.start_nanos,
            end_nanos: synthetic_info.end_nanos,
        };

        for entry in spawns {
            emit_spawn_span(tracer, &target_info, entry);
        }

        if let Some(end) = clamped_end {
            target_span.end_with_timestamp(nanos_to_system_time(end));
        } else {
            target_span.end();
        }
    }
}

/// Compute (min_start_nanos, max_end_nanos) across a set of SpawnExec entries.
fn compute_time_bounds(spawns: &[SpawnExec]) -> (Option<i64>, Option<i64>) {
    let mut min_start: Option<i64> = None;
    let mut max_end: Option<i64> = None;

    for s in spawns {
        if let Some(ref m) = s.metrics {
            if let Some(ref t) = m.start_time {
                let start = t.seconds * 1_000_000_000 + i64::from(t.nanos);
                min_start = Some(min_start.map_or(start, |cur: i64| cur.min(start)));

                let total = m
                    .total_time
                    .as_ref()
                    .map(|d| d.seconds * 1_000_000_000 + i64::from(d.nanos))
                    .unwrap_or(0);
                let end = start + total;
                max_end = Some(max_end.map_or(end, |cur: i64| cur.max(end)));
            }
        }
    }
    (min_start, max_end)
}

// ---------------------------------------------------------------------------
// Span emission with full exec log data
// ---------------------------------------------------------------------------

/// Create a spawn child span under the given parent action.
/// Spawn timing from the exec log is clamped to the parent action's bounds
/// so child spans don't start before or end after their parent (required by
/// Datadog's flame graph, which uses temporal containment for nesting).
fn emit_spawn_span(
    tracer: &opentelemetry_sdk::trace::Tracer,
    parent_info: &ActionSpanInfo,
    s: &SpawnExec,
) {
    let parent_ctx = Context::new().with_remote_span_context(parent_info.span_context.clone());
    let name = spawn_operation_name(s);

    let mut attrs = build_spawn_attributes(s);

    let (raw_start, raw_end) = extract_timing(s);
    let (start_time, end_time) =
        clamp_time_range(raw_start, raw_end, parent_info.start_nanos, parent_info.end_nanos);

    attrs.reserve(4);

    let mut builder = tracer
        .span_builder(name)
        .with_kind(SpanKind::Internal)
        .with_attributes(attrs);
    if let Some(nanos) = start_time {
        builder = builder.with_start_time(nanos_to_system_time(nanos));
    }
    let mut span = tracer.build_with_context(builder, &parent_ctx);

    if s.exit_code != 0 {
        span.set_status(opentelemetry::trace::Status::Error {
            description: std::borrow::Cow::Owned(format!(
                "exit_code={} status={}",
                s.exit_code, s.status
            )),
        });
    }

    if let Some(nanos) = end_time {
        span.end_with_timestamp(nanos_to_system_time(nanos));
    } else {
        span.end();
    }
}

/// Build the full attribute set from a SpawnExec.
fn build_spawn_attributes(s: &SpawnExec) -> Vec<KeyValue> {
    let mut attrs = Vec::with_capacity(32);

    // Identity
    if !s.target_label.is_empty() {
        attrs.push(KeyValue::new(
            BAZEL_SPAWN_TARGET_LABEL,
            s.target_label.clone(),
        ));
        attrs.push(KeyValue::new(
            BAZEL_SPAWN_TARGET_LABEL_SHORT,
            shorten_label(&s.target_label).to_string(),
        ));
    }
    if !s.mnemonic.is_empty() {
        attrs.push(KeyValue::new(BAZEL_SPAWN_MNEMONIC, s.mnemonic.clone()));
    }

    // Primary output
    let primary_output = s
        .listed_outputs
        .first()
        .cloned()
        .or_else(|| s.actual_outputs.first().map(|f| f.path.clone()))
        .unwrap_or_default();
    if !primary_output.is_empty() {
        attrs.push(KeyValue::new(BAZEL_SPAWN_PRIMARY_OUTPUT, primary_output));
    }

    // All listed outputs
    if !s.listed_outputs.is_empty() {
        attrs.push(KeyValue::new(
            BAZEL_SPAWN_LISTED_OUTPUTS,
            s.listed_outputs.join(", "),
        ));
    }

    // Execution properties
    attrs.push(KeyValue::new(BAZEL_SPAWN_RUNNER, s.runner.clone()));
    attrs.push(KeyValue::new(BAZEL_SPAWN_CACHE_HIT, s.cache_hit));
    attrs.push(KeyValue::new(BAZEL_SPAWN_REMOTABLE, s.remotable));
    attrs.push(KeyValue::new(BAZEL_SPAWN_CACHEABLE, s.cacheable));
    attrs.push(KeyValue::new(BAZEL_SPAWN_REMOTE_CACHEABLE, s.remote_cacheable));

    if !s.status.is_empty() {
        attrs.push(KeyValue::new(BAZEL_SPAWN_STATUS, s.status.clone()));
    }
    if s.exit_code != 0 {
        attrs.push(KeyValue::new(BAZEL_SPAWN_EXIT_CODE, s.exit_code as i64));
    }
    if s.timeout_millis != 0 {
        attrs.push(KeyValue::new(BAZEL_SPAWN_TIMEOUT_MS, s.timeout_millis));
    }

    // Digest
    if let Some(ref d) = s.digest {
        if !d.hash.is_empty() {
            attrs.push(KeyValue::new(BAZEL_SPAWN_DIGEST, d.hash.clone()));
        }
        if d.size_bytes != 0 {
            attrs.push(KeyValue::new(BAZEL_SPAWN_DIGEST_SIZE_BYTES, d.size_bytes));
        }
    }

    // Command (join args; cap at 4KB to avoid blowing up span size)
    if !s.command_args.is_empty() {
        let cmd = s.command_args.join(" ");
        let cmd = if cmd.len() > 4096 {
            format!("{}...", &cmd[..4096])
        } else {
            cmd
        };
        attrs.push(KeyValue::new(BAZEL_SPAWN_COMMAND, cmd));
    }

    // I/O counts
    if !s.inputs.is_empty() {
        attrs.push(KeyValue::new(
            BAZEL_SPAWN_INPUT_COUNT,
            s.inputs.len() as i64,
        ));
    }
    if !s.actual_outputs.is_empty() {
        attrs.push(KeyValue::new(
            BAZEL_SPAWN_OUTPUT_COUNT,
            s.actual_outputs.len() as i64,
        ));
    }

    // SpawnMetrics
    if let Some(ref m) = s.metrics {
        if m.input_bytes != 0 {
            attrs.push(KeyValue::new(BAZEL_SPAWN_INPUT_BYTES, m.input_bytes));
        }
        if m.input_files != 0 {
            attrs.push(KeyValue::new(BAZEL_SPAWN_INPUT_FILES, m.input_files as i64));
        }
        if m.memory_estimate_bytes != 0 {
            attrs.push(KeyValue::new(
                BAZEL_SPAWN_MEMORY_ESTIMATE_BYTES,
                m.memory_estimate_bytes,
            ));
        }

        // Timing breakdown (only emit non-zero)
        if let Some(ref d) = m.execution_wall_time {
            let ms = duration_to_ms_raw(d.seconds, d.nanos);
            if ms > 0 {
                attrs.push(KeyValue::new(BAZEL_SPAWN_EXEC_WALL_TIME_MS, ms));
            }
        }
        if let Some(ref d) = m.parse_time {
            let ms = duration_to_ms_raw(d.seconds, d.nanos);
            if ms > 0 {
                attrs.push(KeyValue::new(BAZEL_SPAWN_PARSE_TIME_MS, ms));
            }
        }
        if let Some(ref d) = m.network_time {
            let ms = duration_to_ms_raw(d.seconds, d.nanos);
            if ms > 0 {
                attrs.push(KeyValue::new(BAZEL_SPAWN_NETWORK_TIME_MS, ms));
            }
        }
        if let Some(ref d) = m.fetch_time {
            let ms = duration_to_ms_raw(d.seconds, d.nanos);
            if ms > 0 {
                attrs.push(KeyValue::new(BAZEL_SPAWN_FETCH_TIME_MS, ms));
            }
        }
        if let Some(ref d) = m.queue_time {
            let ms = duration_to_ms_raw(d.seconds, d.nanos);
            if ms > 0 {
                attrs.push(KeyValue::new(BAZEL_SPAWN_QUEUE_TIME_MS, ms));
            }
        }
        if let Some(ref d) = m.setup_time {
            let ms = duration_to_ms_raw(d.seconds, d.nanos);
            if ms > 0 {
                attrs.push(KeyValue::new(BAZEL_SPAWN_SETUP_TIME_MS, ms));
            }
        }
        if let Some(ref d) = m.upload_time {
            let ms = duration_to_ms_raw(d.seconds, d.nanos);
            if ms > 0 {
                attrs.push(KeyValue::new(BAZEL_SPAWN_UPLOAD_TIME_MS, ms));
            }
        }
        if let Some(ref d) = m.process_outputs_time {
            let ms = duration_to_ms_raw(d.seconds, d.nanos);
            if ms > 0 {
                attrs.push(KeyValue::new(BAZEL_SPAWN_PROCESS_OUTPUTS_TIME_MS, ms));
            }
        }
        if let Some(ref d) = m.retry_time {
            let ms = duration_to_ms_raw(d.seconds, d.nanos);
            if ms > 0 {
                attrs.push(KeyValue::new(BAZEL_SPAWN_RETRY_TIME_MS, ms));
            }
        }
    }

    attrs
}

/// Extract (start_nanos, end_nanos) from SpawnMetrics.
fn extract_timing(s: &SpawnExec) -> (Option<i64>, Option<i64>) {
    if let Some(ref m) = s.metrics {
        let start = m
            .start_time
            .as_ref()
            .map(|t| t.seconds * 1_000_000_000 + i64::from(t.nanos));
        let total_nanos = m
            .total_time
            .as_ref()
            .map(|d| d.seconds * 1_000_000_000 + i64::from(d.nanos))
            .unwrap_or(0);
        let end = start.map(|s| s + total_nanos);
        (start, end)
    } else {
        (None, None)
    }
}
