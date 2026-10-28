// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

use crate::otel::span::{KeyValue, SpanKind};

use tracing::{debug, info, warn};

use super::super::attributes::*;
use super::{build_test_attrs, build_test_span_name, test_status};
use super::{OtelMapper, TestResultEvent};

impl OtelMapper {
    /// Fetch -> root counters only: BEP `Fetch` carries no timing, so a span
    /// per download would be fake.
    pub fn on_fetch(&mut self, success: bool) {
        self.fetch_count += 1;
        if !success {
            self.fetch_failed += 1;
        }
    }

    /// TestResult -> child span under target span for a single test attempt.
    /// When timing fields are present, span uses accurate timing.
    pub fn on_test_result(&mut self, ev: &TestResultEvent<'_>) {
        let raw_end = match (ev.start_time_nanos, ev.duration_nanos) {
            (Some(start), Some(dur)) => Some(start + dur),
            _ => None,
        };
        // Cached test results carry the original execution timestamps; clamp
        // so they don't escape the current invocation's window.
        let (start_nanos, end_nanos) = self.clamp_to_invocation(ev.start_time_nanos, raw_end);

        let parent = self
            .target_parent_context(ev.label, start_nanos)
            .or_else(|| self.root_context.clone());
        let Some(parent) = parent else {
            warn!("TestResult with no parent context for {}", ev.label);
            return;
        };
        self.record_synthetic_target_end(ev.label, end_nanos);

        let span_name = build_test_span_name(ev);
        let attrs = build_test_attrs(ev);
        let status = test_status(ev);

        self.build_and_end_child_span(
            &parent,
            span_name,
            SpanKind::Internal,
            attrs,
            status,
            start_nanos,
            end_nanos,
        );

        info!(label = ev.label, status = ?ev.status, "Created test result span");
    }

    /// TestSummary -> span event on root (target span is already ended).
    #[allow(clippy::too_many_arguments)]
    pub fn on_test_summary(
        &mut self,
        label: &str,
        overall_status: Option<&str>,
        total_run_count: Option<i32>,
        run_count: Option<i32>,
        attempt_count: Option<i32>,
        shard_count: Option<i32>,
        total_num_cached: Option<i32>,
    ) {
        let mut attrs = vec![
            KeyValue::new(BAZEL_TARGET_LABEL, label.to_string()),
            KeyValue::new(BAZEL_TARGET_LABEL_SHORT, shorten_label(label).to_string()),
        ];
        if let Some(s) = overall_status {
            attrs.push(KeyValue::new(BAZEL_TEST_OVERALL_STATUS, s.to_string()));
        }
        if let Some(c) = total_run_count {
            attrs.push(KeyValue::new(BAZEL_TEST_TOTAL_RUN_COUNT, c as i64));
        }
        if let Some(v) = run_count {
            attrs.push(KeyValue::new(BAZEL_TEST_RUN_COUNT, v as i64));
        }
        if let Some(v) = attempt_count {
            attrs.push(KeyValue::new(BAZEL_TEST_ATTEMPT_COUNT, v as i64));
        }
        if let Some(v) = shard_count {
            attrs.push(KeyValue::new(BAZEL_TEST_SHARD_COUNT, v as i64));
        }
        if let Some(v) = total_num_cached {
            attrs.push(KeyValue::new(BAZEL_TEST_TOTAL_NUM_CACHED, v as i64));
        }
        self.add_root_event("test_summary", attrs);
        debug!("Added test summary event for {label}");
    }
}
