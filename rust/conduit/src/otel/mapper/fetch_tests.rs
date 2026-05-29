use std::borrow::Cow;
use std::time::{Duration, SystemTime};

use opentelemetry::trace::{Span, SpanKind, Status, Tracer};
use opentelemetry::KeyValue;
use tracing::{debug, info, warn};

use super::super::attributes::*;
use super::{build_test_attrs, build_test_span_name, test_status};
use super::{BufferedTestResult, OtelMapper, TestResultEvent};

impl OtelMapper {
    /// Fetch -> child span under root for external resource fetching.
    ///
    /// In gRPC mode, fetch events may arrive *before* the BuildStarted event
    /// (during Bazel module resolution).  These are buffered and replayed once
    /// the root span exists.
    pub fn on_fetch(&mut self, url: &str, success: bool, downloader: Option<&str>) {
        let arrival = SystemTime::now();
        if self.root_context.is_none() {
            debug!("Buffering fetch event (root span not yet created): {url}");
            self.pending_fetches.push((
                url.to_string(),
                success,
                arrival,
                downloader.map(String::from),
            ));
            return;
        }
        self.emit_fetch_span(url, success, arrival, downloader);
    }

    /// Ensure the `fetches` parent span exists (lazily created).
    fn ensure_fetches_span(&mut self) {
        Self::ensure_group_span(
            &mut self.fetches_context,
            &self.root_context,
            &self.tracer,
            "fetches",
        );
    }

    /// Internal: create + end a fetch span under the `fetches` parent.
    ///
    /// `arrival` is the wallclock time the fetch event was received.
    /// BEP `Fetch` carries no timing data, so we use the arrival time as the
    /// span start and end it 1 ms later to give Jaeger a visible duration.
    pub(super) fn emit_fetch_span(
        &mut self,
        url: &str,
        success: bool,
        arrival: SystemTime,
        downloader: Option<&str>,
    ) {
        self.ensure_fetches_span();

        let parent = match &self.fetches_context {
            Some(cx) => cx.clone(),
            None => return,
        };

        let mut attrs = vec![
            KeyValue::new(BAZEL_FETCH_URL, url.to_string()),
            KeyValue::new(BAZEL_FETCH_SUCCESS, success),
        ];
        if let Some(dl) = downloader {
            attrs.push(KeyValue::new(BAZEL_FETCH_DOWNLOADER, dl.to_string()));
        }

        let builder = self
            .tracer
            .span_builder(format!("fetch {url}"))
            .with_kind(SpanKind::Client)
            .with_start_time(arrival)
            .with_attributes(attrs);

        let mut span = self.tracer.build_with_context(builder, &parent);

        if success {
            span.set_status(Status::Ok);
        } else {
            span.set_status(Status::Error {
                description: Cow::Borrowed("fetch failed"),
            });
        }
        // End 1 ms after arrival so the span has visible duration in Jaeger.
        span.end_with_timestamp(arrival + Duration::from_millis(1));

        debug!("Created fetch span for {url} (success={success})");
    }

    /// TestResult -> child span under target span for a single test attempt.
    /// When timing fields are present, span uses accurate timing.
    /// Buffers and replays if root span is not yet created (BES can send test events before Started).
    pub fn on_test_result(&mut self, ev: &TestResultEvent<'_>) {
        if self.root_context.is_none() {
            self.pending_test_results.push(BufferedTestResult {
                label: ev.label.to_string(),
                status: ev.status.map(String::from),
                attempt: ev.attempt,
                run: ev.run,
                shard: ev.shard,
                cached: ev.cached,
                strategy: ev.strategy.map(String::from),
                start_time_nanos: ev.start_time_nanos,
                duration_nanos: ev.duration_nanos,
                hostname: ev.hostname.map(String::from),
                cached_remotely: ev.cached_remotely,
            });
            debug!("Buffered test result (root not ready): {}", ev.label);
            return;
        }

        let parent = self
            .target_parent_context(ev.label, None)
            .or_else(|| self.root_context.clone());
        let Some(parent) = parent else {
            warn!("TestResult with no parent context for {}", ev.label);
            return;
        };

        let span_name = build_test_span_name(ev);
        let attrs = build_test_attrs(ev);
        let status = test_status(ev);

        let raw_end = match (ev.start_time_nanos, ev.duration_nanos) {
            (Some(start), Some(dur)) => Some(start + dur),
            _ => None,
        };
        // Cached test results carry the original execution timestamps; clamp
        // so they don't escape the current invocation's window.
        let (start_nanos, end_nanos) = self.clamp_to_invocation(ev.start_time_nanos, raw_end);

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

    /// TargetSummary -> span event on root with overall build/test status for the target.
    pub fn on_target_summary(
        &mut self,
        label: &str,
        overall_build_success: Option<bool>,
        overall_test_status: Option<&str>,
    ) {
        let mut attrs = vec![
            KeyValue::new(BAZEL_TARGET_LABEL, label.to_string()),
            KeyValue::new(BAZEL_TARGET_LABEL_SHORT, shorten_label(label).to_string()),
        ];
        if let Some(s) = overall_build_success {
            attrs.push(KeyValue::new(BAZEL_TARGET_OVERALL_BUILD_SUCCESS, s));
        }
        if let Some(s) = overall_test_status {
            attrs.push(KeyValue::new(BAZEL_TARGET_OVERALL_TEST_STATUS, s.to_string()));
        }
        self.add_root_event("target_summary", attrs);
        debug!("Added target summary event for {label}");
    }
}
