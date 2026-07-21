// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

use super::super::attributes::BAZEL_CRITICAL_PATH_LOG_URI;
use super::OtelMapper;
use opentelemetry::KeyValue;
use tracing::debug;

pub(super) fn apply_build_metadata(mapper: &OtelMapper, metadata: &serde_json::Value) {
    if let Some(entries) = metadata.get("metadata").and_then(|m| m.as_object()) {
        for (key, value) in entries {
            if let Some(v) = value.as_str() {
                mapper.set_root_attr(KeyValue::new(format!("bazel.metadata.{key}"), v.to_string()));
            }
        }
    }
}

pub(super) fn apply_build_tool_logs(mapper: &OtelMapper, logs: &serde_json::Value) {
    if let Some(entries) = logs.get("log").and_then(|v| v.as_array()) {
        for entry in entries {
            let name = entry.get("name").and_then(|v| v.as_str()).unwrap_or("");
            if name == "critical path" || name.contains("critical") {
                let uri = entry.get("uri").and_then(|v| v.as_str()).unwrap_or("");
                if !uri.is_empty() {
                    mapper.set_root_attr(KeyValue::new(BAZEL_CRITICAL_PATH_LOG_URI, uri.to_string()));
                }
                mapper.add_root_event(
                    "build_tool_log",
                    vec![
                        KeyValue::new("log.name", name.to_string()),
                        KeyValue::new("log.uri", uri.to_string()),
                    ],
                );
                debug!("Recorded build tool log: {name}");
            }
        }
    }
}

