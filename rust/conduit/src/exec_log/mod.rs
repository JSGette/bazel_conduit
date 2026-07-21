// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

//! Compact execution-log helpers shared by the live tailer and mapper.
//!
//! Conduit only consumes `--execution_log_compact_file` (and the experimental
//! alias). The compact format stores deduped `ExecLogEntry` records in a zstd
//! frame; the tailer decodes entries incrementally and emits reconstructed
//! [`SpawnExec`] messages to the mapper.
//!
//! The mapper matches each spawn to an action span by
//! `(target_label, mnemonic, primary_output)`, emits a child `spawn {mnemonic}`
//! span, and synthesizes orphan parent actions for unmatched spawns at
//! `finish()`.

use std::io::Read;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use opentelemetry::trace::{Span, SpanKind, TraceContextExt, Tracer};
use opentelemetry::{Context, KeyValue};

use crate::otel::attributes::*;
use crate::otel::mapper::{ActionSpanInfo, ActionSpanKey, clamp_time_range, truncate_to_byte_limit};
use crate::otel::Redactor;
use spawn_proto::tools::protos::SpawnExec;

pub mod compact;
pub mod tailer;

/// Cap for string attributes built from variable-length lists (listed_outputs,
/// command args) so a single span can't single-handedly blow the OTLP payload.
const SPAWN_ATTR_CAP_BYTES: usize = 4096;

/// Default per-message cap on length-delimited compact-log entries.
/// Without this, a malformed (or hostile)
/// varint length prefix would be fed straight to `Vec::resize`, OOM'ing the
/// process before any payload is read. 64 MiB is roughly 10x the largest
/// realistic entry observed in Bazel exec logs (a `SpawnExec` with
/// thousands of input files and a long argv); operators can override via
/// `--exec-log-max-message-mib` when they ingest truly pathological logs.
pub const DEFAULT_EXECLOG_MAX_MESSAGE_BYTES: usize = 64 * 1024 * 1024;

/// Default cap on total **decompressed** bytes pulled out of a
/// `--execution_log_compact_file` zstd frame. The per-message cap above
/// only bounds one entry; without this second cap, a hostile sender could
/// pack millions of valid-but-bogus small messages into a tiny zstd frame
/// (zstd routinely hits 100:1 ratios on repetitive protobufs) and OOM us
/// via the in-memory `Vec<SpawnExec>`. 2 GiB is ~4x the largest realistic
/// decompressed compact log (kernel-monorepo-sized builds: ~500 MiB);
/// override via `--exec-log-max-decompressed-mib`.
pub const DEFAULT_EXECLOG_MAX_DECOMPRESSED_BYTES: usize = 2 * 1024 * 1024 * 1024;

/// Read a varint-encoded message length and validate it against `max_bytes`.
/// `Ok(None)` is returned on a clean EOF at a message boundary (no bytes
/// consumed yet); a length exceeding the cap or a varint that fails to
/// terminate within 10 bytes returns an `InvalidData` error before any
/// payload allocation is attempted.
pub fn read_message_len<R: Read>(r: &mut R, max_bytes: usize) -> std::io::Result<Option<usize>> {
    let Some(len) = read_varint(r)? else {
        return Ok(None);
    };
    if len > max_bytes as u64 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("execlog message length {len} exceeds {max_bytes} byte cap"),
        ));
    }
    Ok(Some(len as usize))
}

/// Read an unsigned LEB128 varint (protobuf wire format). `Ok(None)` on a
/// clean EOF at a message boundary. Truncated mid-varint reads return the
/// bytes accumulated so far, which the message-len wrapper then rejects
/// either via the cap or via the subsequent `read_exact` failing.
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

fn nanos_to_system_time(nanos: i64) -> SystemTime {
    if nanos >= 0 {
        UNIX_EPOCH + Duration::from_nanos(nanos as u64)
    } else {
        UNIX_EPOCH
    }
}

/// Normalize label for matching: strip leading `@` so BEP `@@repo//:t` and
/// exec log `@repo//:t` match.
fn normalize_label(s: &str) -> &str {
    s.trim_start_matches('@')
}

/// Candidate keys for cache lookup: (target_label, mnemonic, output) for each
/// output in listed_outputs and actual_outputs.
pub(crate) fn spawn_exec_candidate_keys(s: &SpawnExec) -> Vec<ActionSpanKey> {
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

/// Build a descriptive operation name from the SpawnExec.
/// e.g. "spawn CppCompile zlib/adler32.c" instead of just "spawn CppCompile".
fn spawn_operation_name(s: &SpawnExec) -> String {
    let mnemonic = if s.mnemonic.is_empty() {
        "unknown"
    } else {
        s.mnemonic.as_str()
    };

    if let Some(id) = derive_short_identifier(s) {
        format!("spawn {} {}", mnemonic, id)
    } else {
        format!("spawn {}", mnemonic)
    }
}

/// Derive a short human-readable identifier for the spawn.
/// For compiles: the source file basename. For archives/links: the output name.
fn derive_short_identifier(s: &SpawnExec) -> Option<String> {
    if s.mnemonic.contains("Compile") || s.mnemonic.contains("compile") {
        if let Some(source) = find_source_in_command(s) {
            return Some(short_path(&source));
        }
    }

    let primary = s
        .listed_outputs
        .first()
        .map(String::as_str)
        .or_else(|| s.actual_outputs.first().map(|f| f.path.as_str()));
    primary.map(|p| short_path(p))
}

/// Find the source file being compiled from command_args (looks for `-c <file>`).
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
/// "bazel-out/.../bin/external/+_repo_rules+zlib/_objs/zlib/adler32.o"
///  -> "zlib/adler32.o"
fn short_path(path: &str) -> String {
    let parts: Vec<&str> = path.split('/').collect();
    if parts.len() >= 2 {
        format!("{}/{}", parts[parts.len() - 2], parts[parts.len() - 1])
    } else {
        path.to_string()
    }
}

/// Convert a proto Duration (seconds + nanos) to milliseconds.
fn duration_to_ms_raw(seconds: i64, nanos: i32) -> i64 {
    seconds * 1000 + i64::from(nanos) / 1_000_000
}

/// Backfill curated spawn-derived attributes onto the parent action span.
///
/// Only called for the **first** matching [`SpawnExec`] per action — subsequent
/// spawns (retries, dynamic-exec races) only emit child spans. The set is a
/// summary mirror of [`build_spawn_attributes`]: the fields engineers reach
/// for first when triaging a slow action (which runner ran it, was it a
/// cache hit, where did the time go).
pub(crate) fn apply_spawn_attrs_to_action(
    span: &mut opentelemetry_sdk::trace::Span,
    s: &SpawnExec,
) {
    if !s.runner.is_empty() {
        span.set_attribute(KeyValue::new(BAZEL_ACTION_SPAWN_RUNNER, s.runner.clone()));
    }
    span.set_attribute(KeyValue::new(BAZEL_ACTION_SPAWN_CACHE_HIT, s.cache_hit));
    span.set_attribute(KeyValue::new(
        BAZEL_ACTION_SPAWN_REMOTE_CACHEABLE,
        s.remote_cacheable,
    ));
    if let Some(d) = s.digest.as_ref() {
        if !d.hash.is_empty() {
            span.set_attribute(KeyValue::new(BAZEL_ACTION_SPAWN_DIGEST, d.hash.clone()));
        }
    }
    if let Some(m) = s.metrics.as_ref() {
        for (key, dur) in [
            (BAZEL_ACTION_SPAWN_EXEC_WALL_TIME_MS, &m.execution_wall_time),
            (BAZEL_ACTION_SPAWN_QUEUE_TIME_MS, &m.queue_time),
            (BAZEL_ACTION_SPAWN_FETCH_TIME_MS, &m.fetch_time),
            (BAZEL_ACTION_SPAWN_NETWORK_TIME_MS, &m.network_time),
            (BAZEL_ACTION_SPAWN_UPLOAD_TIME_MS, &m.upload_time),
            (BAZEL_ACTION_SPAWN_SETUP_TIME_MS, &m.setup_time),
            (
                BAZEL_ACTION_SPAWN_PROCESS_OUTPUTS_TIME_MS,
                &m.process_outputs_time,
            ),
        ] {
            if let Some(d) = dur.as_ref() {
                let ms = duration_to_ms_raw(d.seconds, d.nanos);
                if ms > 0 {
                    span.set_attribute(KeyValue::new(key, ms));
                }
            }
        }
    }
}

/// Create a spawn child span under the given parent action.
/// Spawn timing from the exec log is clamped to the parent action's bounds
/// so child spans don't start before or end after their parent (required by
/// Datadog's flame graph, which uses temporal containment for nesting).
pub(crate) fn emit_spawn_span(
    tracer: &opentelemetry_sdk::trace::Tracer,
    parent_info: &ActionSpanInfo,
    s: &SpawnExec,
    redactor: &Redactor,
) {
    let parent_ctx = Context::new().with_remote_span_context(parent_info.span_context.clone());
    let name = spawn_operation_name(s);

    let attrs = build_spawn_attributes(s, redactor);

    let (raw_start, raw_end) = extract_timing(s);
    let (start_time, end_time) = clamp_time_range(
        raw_start,
        raw_end,
        parent_info.start_nanos,
        parent_info.end_nanos,
    );

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

/// Build the full attribute set from a SpawnExec. `redactor` is applied to
/// `s.command_args` before it is joined into the `bazel.spawn.command`
/// attribute -- defence in depth for rules that smuggle a Bazel-style
/// `--client_env=NAME=VALUE` flag into the spawned process argv.
fn build_spawn_attributes(s: &SpawnExec, redactor: &Redactor) -> Vec<KeyValue> {
    let mut attrs = Vec::with_capacity(32);

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

    let primary_output = s
        .listed_outputs
        .first()
        .cloned()
        .or_else(|| s.actual_outputs.first().map(|f| f.path.clone()))
        .unwrap_or_default();
    if !primary_output.is_empty() {
        attrs.push(KeyValue::new(BAZEL_SPAWN_PRIMARY_OUTPUT, primary_output));
    }

    if !s.listed_outputs.is_empty() {
        let joined = s.listed_outputs.join(", ");
        let total = s.listed_outputs.len();
        let suffix = format!("... ({total} outputs total)");
        attrs.push(KeyValue::new(
            BAZEL_SPAWN_LISTED_OUTPUTS,
            truncate_to_byte_limit(&joined, SPAWN_ATTR_CAP_BYTES, &suffix),
        ));
    }

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

    if let Some(ref d) = s.digest {
        if !d.hash.is_empty() {
            attrs.push(KeyValue::new(BAZEL_SPAWN_DIGEST, d.hash.clone()));
        }
        if d.size_bytes != 0 {
            attrs.push(KeyValue::new(BAZEL_SPAWN_DIGEST_SIZE_BYTES, d.size_bytes));
        }
    }

    if !s.command_args.is_empty() {
        let cmd = redactor.scrub_args(&s.command_args).join(" ");
        attrs.push(KeyValue::new(
            BAZEL_SPAWN_COMMAND,
            truncate_to_byte_limit(&cmd, SPAWN_ATTR_CAP_BYTES, "..."),
        ));
    }

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
