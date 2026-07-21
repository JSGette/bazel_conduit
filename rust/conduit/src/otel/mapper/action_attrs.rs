// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

use std::borrow::Cow;
use std::collections::HashMap;

use opentelemetry::trace::Status;
use opentelemetry::KeyValue;

use super::super::attributes::*;
use super::{shorten_label, truncate_to_byte_limit, ActionCompletedEvent, Redactor, COMMAND_LINE_CAP_BYTES};

pub(super) fn build_action_span_name(ev: &ActionCompletedEvent<'_>) -> String {
    match (ev.label, ev.mnemonic) {
        (Some(label), Some(mnemonic)) => format!("action {mnemonic} {}", shorten_label(label)),
        (Some(label), None) => format!("action {}", shorten_label(label)),
        (None, Some(mnemonic)) => format!("action {mnemonic}"),
        _ => "action".to_string(),
    }
}

pub(super) fn action_status(success: bool) -> Status {
    if success {
        Status::Ok
    } else {
        Status::Error {
            description: Cow::Borrowed("Action failed"),
        }
    }
}

pub(super) fn build_action_attrs(
    ev: &ActionCompletedEvent<'_>,
    configurations: &HashMap<String, String>,
    redactor: &Redactor,
) -> Vec<KeyValue> {
    let mut attrs = vec![KeyValue::new(BAZEL_ACTION_SUCCESS, ev.success)];

    if let Some(v) = ev.mnemonic {
        attrs.push(KeyValue::new(BAZEL_ACTION_MNEMONIC, v.to_string()));
    }
    if let Some(v) = ev.exit_code {
        attrs.push(KeyValue::new(BAZEL_ACTION_EXIT_CODE, i64::from(v)));
    }
    if let Some(v) = ev.exit_code_name.filter(|s| !s.is_empty()) {
        attrs.push(KeyValue::new(BAZEL_ACTION_EXIT_CODE_NAME, v.to_string()));
    }
    if let Some(v) = ev.cached {
        attrs.push(KeyValue::new(BAZEL_ACTION_CACHED, v));
    }
    if let Some(v) = ev.hostname.filter(|s| !s.is_empty()) {
        attrs.push(KeyValue::new(BAZEL_ACTION_HOSTNAME, v.to_string()));
    }
    if let Some(v) = ev.cached_remotely {
        attrs.push(KeyValue::new(BAZEL_ACTION_CACHED_REMOTELY, v));
    }
    if let Some(v) = ev.runner.filter(|s| !s.is_empty()) {
        attrs.push(KeyValue::new(BAZEL_ACTION_RUNNER, v.to_string()));
    }
    if let Some(v) = ev.primary_output {
        attrs.push(KeyValue::new(BAZEL_ACTION_PRIMARY_OUTPUT, v.to_string()));
    }
    if let Some(v) = ev.label {
        attrs.push(KeyValue::new(BAZEL_ACTION_LABEL, v.to_string()));
        attrs.push(KeyValue::new(BAZEL_ACTION_LABEL_SHORT, shorten_label(v).to_string()));
    }
    if let Some(cfg) = ev.configuration {
        let display_val = configurations
            .get(cfg)
            .map_or_else(|| cfg.to_string(), Clone::clone);
        attrs.push(KeyValue::new(BAZEL_ACTION_CONFIGURATION, display_val));
    }
    if !ev.command_line.is_empty() {
        let scrubbed = redactor.scrub_args(ev.command_line);
        let joined = scrubbed.join(" ");
        let capped = truncate_to_byte_limit(&joined, COMMAND_LINE_CAP_BYTES, "...(truncated)");
        attrs.push(KeyValue::new(BAZEL_ACTION_COMMAND_LINE, capped));
    }
    if let Some(v) = ev.stdout_path {
        attrs.push(KeyValue::new(BAZEL_ACTION_STDOUT, v.to_string()));
    }
    if let Some(v) = ev.stderr_path {
        attrs.push(KeyValue::new(BAZEL_ACTION_STDERR, v.to_string()));
    }

    attrs
}

