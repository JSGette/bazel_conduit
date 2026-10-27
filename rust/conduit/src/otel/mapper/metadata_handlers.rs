// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

use super::super::attributes::BAZEL_CRITICAL_PATH_LOG_URI;
use super::OtelMapper;
use crate::build_event_stream::{BuildMetadata, BuildToolLogs};
use opentelemetry::KeyValue;
use tracing::debug;

pub(super) fn apply_build_metadata(mapper: &OtelMapper, bm: &BuildMetadata) {
    for (key, value) in &bm.metadata {
        mapper.set_root_attr(KeyValue::new(format!("bazel.metadata.{key}"), value.clone()));
    }
}

pub(super) fn apply_build_tool_logs(mapper: &OtelMapper, logs: &BuildToolLogs) {
    for f in &logs.log {
        if !f.name.contains("critical") {
            continue;
        }
        let uri = crate::grpc::server::bep_file_uri(f).unwrap_or_default();
        if !uri.is_empty() {
            mapper.set_root_attr(KeyValue::new(BAZEL_CRITICAL_PATH_LOG_URI, uri.clone()));
        }
        mapper.add_root_event(
            "build_tool_log",
            vec![
                KeyValue::new("log.name", f.name.clone()),
                KeyValue::new("log.uri", uri),
            ],
        );
        debug!("Recorded build tool log: {}", f.name);
    }
}
