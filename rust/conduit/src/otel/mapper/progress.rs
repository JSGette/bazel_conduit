use super::super::attributes::BAZEL_PROGRESS_STDOUT;
use super::{append_progress_capped, strip_ansi, truncate_to_byte_limit, OtelMapper, PROGRESS_CAP_BYTES};
use opentelemetry::logs::{AnyValue, LogRecord as _, Logger as _, Severity};
use opentelemetry::trace::TraceContextExt;
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
    let span_cx = cx.span().span_context().clone();

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

    let mut record = logger.create_log_record();
    record.set_trace_context(
        span_cx.trace_id(),
        span_cx.span_id(),
        Some(span_cx.trace_flags()),
    );
    record.set_severity_number(Severity::Info);
    record.set_severity_text("INFO");
    record.add_attribute("bazel.log.kind", AnyValue::String("progress".into()));

    let stderr_len = stderr.len();
    let stdout_len = stdout.len();
    match (!stderr.is_empty(), !stdout.is_empty()) {
        (true, true) => {
            record.set_body(AnyValue::String(stderr.into()));
            record.add_attribute(BAZEL_PROGRESS_STDOUT, AnyValue::String(stdout.into()));
        }
        (true, false) => record.set_body(AnyValue::String(stderr.into())),
        (false, true) => record.set_body(AnyValue::String(stdout.into())),
        (false, false) => return,
    }

    logger.emit(record);
    debug!(
        trace_id = %span_cx.trace_id(),
        stderr_len,
        stdout_len,
        "Streamed bazel.progress log record"
    );
}

