// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

use crate::otel::span::{KeyValue, LogData};
use super::super::attributes::BAZEL_PROGRESS_STDOUT;
use super::{append_progress_capped, strip_ansi, truncate_to_byte_limit, OtelMapper, PROGRESS_CAP_BYTES};
use std::time::SystemTime;
use tracing::debug;

pub(super) fn handle_progress(mapper: &mut OtelMapper, stderr: Option<&str>, stdout: Option<&str>) {
    let stderr = stderr.unwrap_or("");
    let stdout = stdout.unwrap_or("");
    if stderr.is_empty() && stdout.is_empty() {
        return;
    }

    if mapper.logger.is_some() && mapper.root_context.is_some() {
        emit_progress_log(mapper, stderr, stdout);
        return;
    }

    if !stderr.is_empty() {
        let cleaned = mapper.redactor.scrub_text(&strip_ansi(stderr)).into_owned();
        append_progress_capped(&mut mapper.progress_stderr, &cleaned);
    }
    if !stdout.is_empty() {
        let cleaned = mapper.redactor.scrub_text(&strip_ansi(stdout)).into_owned();
        append_progress_capped(&mut mapper.progress_stdout, &cleaned);
    }
}

fn emit_progress_log(mapper: &OtelMapper, stderr: &str, stdout: &str) {
    let Some(logger) = mapper.logger.as_ref() else {
        return;
    };
    let Some(cx) = mapper.root_context.as_ref() else {
        return;
    };
    let span_cx = cx.span().span_context();

    let stderr = if stderr.is_empty() {
        String::new()
    } else {
        let scrubbed = mapper.redactor.scrub_text(&strip_ansi(stderr)).into_owned();
        truncate_to_byte_limit(&scrubbed, PROGRESS_CAP_BYTES, "...(truncated)")
    };
    let stdout = if stdout.is_empty() {
        String::new()
    } else {
        let scrubbed = mapper.redactor.scrub_text(&strip_ansi(stdout)).into_owned();
        truncate_to_byte_limit(&scrubbed, PROGRESS_CAP_BYTES, "...(truncated)")
    };

    let stderr_len = stderr.len();
    let stdout_len = stdout.len();
    let mut attributes = vec![KeyValue::new("bazel.log.kind", "progress")];
    let body = match (stderr.is_empty(), stdout.is_empty()) {
        (false, false) => {
            attributes.push(KeyValue::new(BAZEL_PROGRESS_STDOUT, stdout));
            stderr
        }
        (false, true) => stderr,
        (true, false) => stdout,
        (true, true) => return,
    };

    logger.emit(LogData {
        timestamp: SystemTime::now(),
        span_context: span_cx,
        body,
        attributes,
    });
    debug!(
        trace_id = %span_cx.trace_id(),
        stderr_len,
        stdout_len,
        "Streamed bazel.progress log record"
    );
}

