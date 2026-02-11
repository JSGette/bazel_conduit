//! OTel Mapper – converts BEP events into OpenTelemetry spans.
//!
//! Manages the lifecycle of spans for a single Bazel build invocation.
//! Spans are created / updated / ended as BEP events flow through the router.
//!
//! Span hierarchy (lightweight mode):
//! ```text
//! bazel.invocation (root, BuildStarted → finish)
//! ├── target {label} (TargetConfigured → TargetCompleted)
//! │   ├── action {mnemonic} {label} (failed actions only)
//! │   └── test {label} (testResult spans)
//! └── fetches (single parent span grouping all fetch events)
//!     └── fetch {url} (individual fetch spans)
//!
//! Events on bazel.invocation:
//!   - build.log (accumulated progress stderr/stdout)
//! ```

use std::borrow::Cow;
use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use opentelemetry::trace::{Span, SpanKind, Status, TraceContextExt, Tracer};
use opentelemetry::{Context, KeyValue};
use tracing::{debug, info, warn};

use super::attributes::*;
use super::trace_context;

/// Strip ANSI escape sequences (colors, cursor movement, etc.) from a string.
///
/// Handles CSI sequences (`ESC[...X`), OSC sequences (`ESC]...BEL/ST`), and
/// bare ESC + single-char sequences.
fn strip_ansi(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    let mut chars = input.chars().peekable();

    while let Some(ch) = chars.next() {
        if ch == '\x1b' {
            // ESC — consume the sequence.
            match chars.peek() {
                Some('[') => {
                    // CSI sequence: ESC [ <params> <final byte 0x40–0x7E>
                    chars.next(); // consume '['
                    while let Some(&c) = chars.peek() {
                        if c.is_ascii() && (0x40..=0x7E).contains(&(c as u8)) {
                            chars.next(); // consume final byte
                            break;
                        }
                        chars.next(); // consume parameter/intermediate byte
                    }
                }
                Some(']') => {
                    // OSC sequence: ESC ] ... (terminated by BEL or ST)
                    chars.next(); // consume ']'
                    while let Some(&c) = chars.peek() {
                        chars.next();
                        if c == '\x07' {
                            break; // BEL
                        }
                        if c == '\x1b' {
                            // ST = ESC backslash
                            if chars.peek() == Some(&'\\') {
                                chars.next();
                            }
                            break;
                        }
                    }
                }
                Some(_) => {
                    // Two-character escape (e.g. ESC M, ESC 7)
                    chars.next();
                }
                None => {} // stray ESC at end
            }
        } else {
            out.push(ch);
        }
    }

    out
}

/// Holds data for a target whose `TargetCompleted` arrived before all
/// referenced `NamedSet` events.  Resolved lazily in [`OtelMapper::on_named_set`]
/// or force-resolved in [`OtelMapper::finish`].
struct PendingTargetCompletion {
    cx: Context,
    success: bool,
    file_set_ids: Vec<String>,
    end_time_nanos: Option<i64>,
}

/// Maps BEP events to OpenTelemetry spans.
pub struct OtelMapper {
    tracer: opentelemetry_sdk::trace::Tracer,

    /// Root span context (`bazel.invocation`).
    root_context: Option<Context>,

    /// Open target spans, keyed by target label.
    target_contexts: HashMap<String, Context>,

    /// Targets whose `TargetCompleted` arrived but whose `NamedSet` data
    /// is not yet in the cache.  Keyed by target label.
    pending_completions: HashMap<String, PendingTargetCompletion>,

    /// Single `fetches` parent span grouping all fetch child spans.
    /// Created lazily on first fetch event, ended in [`finish`].
    fetches_context: Option<Context>,

    /// Accumulated stderr / stdout from all progress messages.
    /// Flushed as a single `build.log` event on the root span in [`finish`].
    progress_stderr: String,
    progress_stdout: String,

    /// Cached exit code from BuildFinished (root span ends in [`finish`]).
    exit_code: Option<i32>,

    /// Cached finish timestamp from BuildFinished.
    finish_time_millis: Option<i64>,

    /// Named set cache: set_id → file names.
    /// Used to resolve output files when target spans are completed.
    named_set_cache: HashMap<String, Vec<String>>,

    /// Fetch events that arrived before the root span was created.
    /// Replayed once `on_build_started` fires.
    /// Tuple: (url, success, wallclock arrival time).
    pending_fetches: Vec<(String, bool, SystemTime)>,
}

impl OtelMapper {
    pub fn new(tracer: opentelemetry_sdk::trace::Tracer) -> Self {
        Self {
            tracer,
            root_context: None,
            target_contexts: HashMap::new(),
            pending_completions: HashMap::new(),
            fetches_context: None,
            progress_stderr: String::new(),
            progress_stdout: String::new(),
            exit_code: None,
            finish_time_millis: None,
            named_set_cache: HashMap::new(),
            pending_fetches: Vec::new(),
        }
    }

    // =====================================================================
    // Lifecycle events
    // =====================================================================

    /// BuildStarted → create root span `bazel.invocation`.
    pub fn on_build_started(
        &mut self,
        uuid: &str,
        command: &str,
        start_time_millis: Option<i64>,
    ) {
        let trace_id = trace_context::uuid_to_trace_id(uuid);
        let parent_cx = trace_context::make_root_context(trace_id);

        let mut builder = self
            .tracer
            .span_builder("bazel.invocation")
            .with_kind(SpanKind::Internal)
            .with_attributes(vec![
                KeyValue::new(BAZEL_INVOCATION_ID, uuid.to_string()),
                KeyValue::new(BAZEL_COMMAND, command.to_string()),
            ]);

        if let Some(millis) = start_time_millis {
            builder = builder.with_start_time(millis_to_system_time(millis));
        }

        let span = self.tracer.build_with_context(builder, &parent_cx);
        let cx = Context::new().with_span(span);

        info!(trace_id = %trace_id, "Created root span for invocation {uuid}");
        self.root_context = Some(cx);

        // Replay any fetch events that arrived before the root span.
        if !self.pending_fetches.is_empty() {
            let buffered: Vec<_> = std::mem::take(&mut self.pending_fetches);
            info!("Replaying {} buffered fetch events", buffered.len());
            for (url, success, arrival) in buffered {
                self.emit_fetch_span(&url, success, arrival);
            }
        }
    }

    /// OptionsParsed → add attributes to root span.
    pub fn on_options_parsed(
        &mut self,
        startup_options: &[String],
        explicit_cmd_line: &[String],
    ) {
        if let Some(cx) = &self.root_context {
            if !startup_options.is_empty() {
                cx.span().set_attribute(KeyValue::new(
                    "bazel.startup_options",
                    startup_options.join(" "),
                ));
            }
            if !explicit_cmd_line.is_empty() {
                cx.span().set_attribute(KeyValue::new(
                    "bazel.explicit_cmd_line",
                    explicit_cmd_line.join(" "),
                ));
            }
        }
    }

    /// WorkspaceStatus → add workspace attributes to root span.
    pub fn on_workspace_status(&mut self, items: &HashMap<String, String>) {
        if let Some(cx) = &self.root_context {
            for (key, value) in items {
                let attr_key = match key.as_str() {
                    "BUILD_USER" => BAZEL_WORKSPACE_USER.to_string(),
                    "BUILD_HOST" => BAZEL_WORKSPACE_HOST.to_string(),
                    _ => format!("bazel.workspace.{}", key.to_lowercase()),
                };
                cx.span()
                    .set_attribute(KeyValue::new(attr_key, value.clone()));
            }
        }
    }

    /// UnstructuredCommandLine → add full command line as attribute.
    pub fn on_command_line(&mut self, args: &[String]) {
        if let Some(cx) = &self.root_context {
            if !args.is_empty() {
                cx.span()
                    .set_attribute(KeyValue::new(BAZEL_COMMAND_LINE, args.join(" ")));
            }
        }
    }

    /// Pattern expanded → add build patterns as attribute.
    pub fn on_pattern(&mut self, patterns: &[String]) {
        if let Some(cx) = &self.root_context {
            if !patterns.is_empty() {
                cx.span()
                    .set_attribute(KeyValue::new(BAZEL_PATTERNS, patterns.join(", ")));
            }
        }
    }

    // =====================================================================
    // Configuration events
    // =====================================================================

    /// Configuration → span event on root span.
    pub fn on_configuration(
        &mut self,
        config_id: &str,
        mnemonic: Option<&str>,
        platform: Option<&str>,
    ) {
        if let Some(cx) = &self.root_context {
            let mut attrs = vec![KeyValue::new(BAZEL_CONFIG_ID, config_id.to_string())];
            if let Some(m) = mnemonic {
                attrs.push(KeyValue::new(BAZEL_CONFIG_MNEMONIC, m.to_string()));
            }
            if let Some(p) = platform {
                attrs.push(KeyValue::new(BAZEL_CONFIG_PLATFORM, p.to_string()));
            }
            cx.span().add_event("configuration", attrs);
            debug!("Added configuration event: {config_id}");
        }
    }

    // =====================================================================
    // Target events
    // =====================================================================

    /// TargetConfigured → create child span `target {label}`.
    ///
    /// If `event_time_nanos` is provided (gRPC mode), it is used as the
    /// span's start time for accurate, sub-millisecond duration measurement.
    pub fn on_target_configured(
        &mut self,
        label: &str,
        kind: Option<&str>,
        tags: &[String],
        event_time_nanos: Option<i64>,
    ) {
        let parent = match &self.root_context {
            Some(cx) => cx,
            None => {
                warn!("TargetConfigured before BuildStarted for {label}");
                return;
            }
        };

        let mut attrs = vec![KeyValue::new(BAZEL_TARGET_LABEL, label.to_string())];
        if let Some(k) = kind {
            attrs.push(KeyValue::new(BAZEL_TARGET_KIND, k.to_string()));
        }
        if !tags.is_empty() {
            attrs.push(KeyValue::new(BAZEL_TARGET_TAGS, tags.join(", ")));
        }

        let mut builder = self
            .tracer
            .span_builder(format!("target {label}"))
            .with_kind(SpanKind::Internal)
            .with_attributes(attrs);

        if let Some(nanos) = event_time_nanos {
            builder = builder.with_start_time(nanos_to_system_time(nanos));
        }

        let span = self.tracer.build_with_context(builder, parent);
        let cx = Context::new().with_span(span);

        debug!("Created target span for {label}");
        self.target_contexts.insert(label.to_string(), cx);
    }

    /// TargetCompleted → end target span with status, outputs, tags, and resolved files.
    ///
    /// If all referenced `NamedSet` IDs are already in the cache the span is
    /// ended immediately.  Otherwise completion is deferred — the span stays
    /// open and is ended later when the missing `NamedSet` events arrive (see
    /// [`on_named_set`]) or at build end (see [`finish`]).
    ///
    /// If `event_time_nanos` is provided (gRPC mode), it is used as the
    /// span's end time so the duration accurately reflects Bazel timestamps.
    pub fn on_target_completed(
        &mut self,
        label: &str,
        success: bool,
        file_set_ids: &[String],
        tags: &[String],
        event_time_nanos: Option<i64>,
    ) {
        if let Some(cx) = self.target_contexts.remove(label) {
            cx.span()
                .set_attribute(KeyValue::new(BAZEL_TARGET_SUCCESS, success));

            // Merge tags from TargetCompleted (may overlap with Configured).
            if !tags.is_empty() {
                cx.span()
                    .set_attribute(KeyValue::new(BAZEL_TARGET_TAGS, tags.join(", ")));
            }

            // Check whether all referenced file sets are in the cache.
            let all_resolved = file_set_ids.is_empty()
                || file_set_ids
                    .iter()
                    .all(|id| self.named_set_cache.contains_key(id));

            if all_resolved {
                // Resolve immediately.
                Self::apply_file_sets_and_end(&self.named_set_cache, &cx, label, success, file_set_ids, event_time_nanos);
            } else {
                // Defer — keep the span open until the missing sets arrive.
                debug!(
                    "Deferring target completion for {label} (waiting for {} named sets)",
                    file_set_ids.iter().filter(|id| !self.named_set_cache.contains_key(*id)).count()
                );
                self.pending_completions.insert(
                    label.to_string(),
                    PendingTargetCompletion {
                        cx,
                        success,
                        file_set_ids: file_set_ids.to_vec(),
                        end_time_nanos: event_time_nanos,
                    },
                );
            }
        } else {
            warn!("TargetCompleted for unknown target {label}");
        }
    }

    /// Resolve file sets from cache and end the target span.
    fn apply_file_sets_and_end(
        cache: &HashMap<String, Vec<String>>,
        cx: &Context,
        label: &str,
        success: bool,
        file_set_ids: &[String],
        end_time_nanos: Option<i64>,
    ) {
        let mut all_files: Vec<String> = Vec::new();
        for set_id in file_set_ids {
            if let Some(files) = cache.get(set_id) {
                all_files.extend(files.iter().cloned());
            }
        }

        if !all_files.is_empty() {
            cx.span().set_attribute(KeyValue::new(
                BAZEL_TARGET_OUTPUT_COUNT,
                all_files.len() as i64,
            ));
            let file_list = if all_files.len() <= 20 {
                all_files.join(", ")
            } else {
                format!(
                    "{} ... and {} more",
                    all_files[..20].join(", "),
                    all_files.len() - 20
                )
            };
            cx.span()
                .set_attribute(KeyValue::new(BAZEL_TARGET_OUTPUT_FILES, file_list));
        }

        let status = if success {
            Status::Ok
        } else {
            Status::Error {
                description: Cow::Borrowed("target failed"),
            }
        };
        cx.span().set_status(status);

        if let Some(nanos) = end_time_nanos {
            cx.span().end_with_timestamp(nanos_to_system_time(nanos));
        } else {
            cx.span().end();
        }

        debug!(
            "Ended target span for {label} (success={success}, files={})",
            all_files.len()
        );
    }

    // =====================================================================
    // Named set cache
    // =====================================================================

    /// Cache a NamedSet so it can be resolved when targets complete.
    ///
    /// After caching, any pending target completions that were waiting for
    /// this set (and now have all their sets resolved) are finalized.
    pub fn on_named_set(&mut self, set_id: &str, files: Vec<String>) {
        self.named_set_cache.insert(set_id.to_string(), files);

        // Check if any pending target completions can now be resolved.
        let ready_labels: Vec<String> = self
            .pending_completions
            .iter()
            .filter(|(_, pending)| {
                pending
                    .file_set_ids
                    .iter()
                    .all(|id| self.named_set_cache.contains_key(id))
            })
            .map(|(label, _)| label.clone())
            .collect();

        for label in ready_labels {
            if let Some(pending) = self.pending_completions.remove(&label) {
                debug!("Resolving deferred target completion for {label}");
                Self::apply_file_sets_and_end(
                    &self.named_set_cache,
                    &pending.cx,
                    &label,
                    pending.success,
                    &pending.file_set_ids,
                    pending.end_time_nanos,
                );
            }
        }
    }

    // =====================================================================
    // Action events (lightweight: failed only)
    // =====================================================================

    /// Failed action → create + immediately end `action {mnemonic} {label}`.
    pub fn on_action_failed(
        &mut self,
        label: Option<&str>,
        mnemonic: Option<&str>,
        exit_code: Option<i32>,
        primary_output: Option<&str>,
    ) {
        // Prefer the target span as parent; fall back to root.
        let parent = label
            .and_then(|l| self.target_contexts.get(l))
            .or(self.root_context.as_ref());

        let parent = match parent {
            Some(cx) => cx.clone(),
            None => {
                warn!("ActionFailed with no parent context");
                return;
            }
        };

        let span_name = match (label, mnemonic) {
            (Some(l), Some(m)) => format!("action {m} {l}"),
            (Some(l), None) => format!("action {l}"),
            (None, Some(m)) => format!("action {m}"),
            _ => "action".to_string(),
        };

        let mut attrs = vec![KeyValue::new(BAZEL_ACTION_SUCCESS, false)];
        if let Some(m) = mnemonic {
            attrs.push(KeyValue::new(BAZEL_ACTION_MNEMONIC, m.to_string()));
        }
        if let Some(code) = exit_code {
            attrs.push(KeyValue::new(BAZEL_ACTION_EXIT_CODE, code as i64));
        }
        if let Some(output) = primary_output {
            attrs.push(KeyValue::new(
                BAZEL_ACTION_PRIMARY_OUTPUT,
                output.to_string(),
            ));
        }
        if let Some(l) = label {
            attrs.push(KeyValue::new(BAZEL_TARGET_LABEL, l.to_string()));
        }

        let builder = self
            .tracer
            .span_builder(span_name)
            .with_kind(SpanKind::Internal)
            .with_attributes(attrs);

        let mut span = self.tracer.build_with_context(builder, &parent);
        span.set_status(Status::Error {
            description: Cow::Borrowed("Action failed"),
        });
        span.end();

        debug!("Created failed action span for {:?}", label);
    }

    // =====================================================================
    // Fetch events
    // =====================================================================

    /// Fetch → child span under root for external resource fetching.
    ///
    /// In gRPC mode, fetch events may arrive *before* the BuildStarted event
    /// (during Bazel module resolution).  These are buffered and replayed once
    /// the root span exists.
    pub fn on_fetch(&mut self, url: &str, success: bool) {
        let arrival = SystemTime::now();
        if self.root_context.is_none() {
            debug!("Buffering fetch event (root span not yet created): {url}");
            self.pending_fetches.push((url.to_string(), success, arrival));
            return;
        }
        self.emit_fetch_span(url, success, arrival);
    }

    /// Ensure the `fetches` parent span exists (lazily created).
    fn ensure_fetches_span(&mut self) {
        if self.fetches_context.is_some() {
            return;
        }
        if let Some(root_cx) = &self.root_context {
            let builder = self
                .tracer
                .span_builder("fetches")
                .with_kind(SpanKind::Internal);

            let span = self.tracer.build_with_context(builder, root_cx);
            let cx = Context::new().with_span(span);
            self.fetches_context = Some(cx);
            debug!("Created fetches parent span");
        }
    }

    /// Internal: create + end a fetch span under the `fetches` parent.
    ///
    /// `arrival` is the wallclock time the fetch event was received.
    /// BEP `Fetch` carries no timing data, so we use the arrival time as the
    /// span start and end it 1 ms later to give Jaeger a visible duration.
    fn emit_fetch_span(&mut self, url: &str, success: bool, arrival: SystemTime) {
        self.ensure_fetches_span();

        let parent = match &self.fetches_context {
            Some(cx) => cx.clone(),
            None => return,
        };

        let attrs = vec![
            KeyValue::new(BAZEL_FETCH_URL, url.to_string()),
            KeyValue::new(BAZEL_FETCH_SUCCESS, success),
        ];

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

    // =====================================================================
    // Test events
    // =====================================================================

    /// TestResult → child span under target span for a single test attempt.
    pub fn on_test_result(
        &mut self,
        label: &str,
        status: Option<&str>,
        attempt: Option<i32>,
        run: Option<i32>,
        shard: Option<i32>,
        cached: Option<bool>,
        strategy: Option<&str>,
    ) {
        // Parent is the target span; fall back to root.
        let parent = self
            .target_contexts
            .get(label)
            .or(self.root_context.as_ref());

        let parent = match parent {
            Some(cx) => cx.clone(),
            None => {
                warn!("TestResult with no parent context for {label}");
                return;
            }
        };

        let span_name = match (attempt, run, shard) {
            (Some(a), Some(r), Some(s)) => {
                format!("test {label} attempt={a} run={r} shard={s}")
            }
            (Some(a), _, _) => format!("test {label} attempt={a}"),
            _ => format!("test {label}"),
        };

        let mut attrs = vec![KeyValue::new(BAZEL_TARGET_LABEL, label.to_string())];
        if let Some(s) = status {
            attrs.push(KeyValue::new(BAZEL_TEST_STATUS, s.to_string()));
        }
        if let Some(a) = attempt {
            attrs.push(KeyValue::new(BAZEL_TEST_ATTEMPT, a as i64));
        }
        if let Some(r) = run {
            attrs.push(KeyValue::new(BAZEL_TEST_RUN, r as i64));
        }
        if let Some(s) = shard {
            attrs.push(KeyValue::new(BAZEL_TEST_SHARD, s as i64));
        }
        if let Some(c) = cached {
            attrs.push(KeyValue::new(BAZEL_TEST_CACHED, c));
        }
        if let Some(st) = strategy {
            attrs.push(KeyValue::new(BAZEL_TEST_STRATEGY, st.to_string()));
        }

        let builder = self
            .tracer
            .span_builder(span_name)
            .with_kind(SpanKind::Internal)
            .with_attributes(attrs);

        let mut span = self.tracer.build_with_context(builder, &parent);

        let is_pass = status.map(|s| s == "PASSED").unwrap_or(false);
        if is_pass {
            span.set_status(Status::Ok);
        } else {
            span.set_status(Status::Error {
                description: Cow::Owned(format!(
                    "test {}",
                    status.unwrap_or("UNKNOWN")
                )),
            });
        }
        span.end();

        debug!("Created test result span for {label}");
    }

    /// TestSummary → span event on root (target span is already ended).
    pub fn on_test_summary(
        &mut self,
        label: &str,
        overall_status: Option<&str>,
        total_run_count: Option<i32>,
    ) {
        if let Some(cx) = &self.root_context {
            let mut attrs = vec![KeyValue::new(BAZEL_TARGET_LABEL, label.to_string())];
            if let Some(s) = overall_status {
                attrs.push(KeyValue::new(BAZEL_TEST_OVERALL_STATUS, s.to_string()));
            }
            if let Some(c) = total_run_count {
                attrs.push(KeyValue::new(BAZEL_TEST_TOTAL_RUN_COUNT, c as i64));
            }
            cx.span().add_event("test_summary", attrs);
            debug!("Added test summary event for {label}");
        }
    }

    // =====================================================================
    // Progress events
    // =====================================================================

    /// Progress with stderr/stdout → text is buffered and flushed as a
    /// single `build.log` event on the root `bazel.invocation` span in
    /// [`finish`].
    pub fn on_progress(&mut self, stderr: Option<&str>, stdout: Option<&str>) {
        if let Some(err) = stderr {
            if !err.is_empty() {
                self.progress_stderr.push_str(&strip_ansi(err));
            }
        }
        if let Some(out) = stdout {
            if !out.is_empty() {
                self.progress_stdout.push_str(&strip_ansi(out));
            }
        }
    }

    // =====================================================================
    // Build metadata
    // =====================================================================

    /// BuildMetadata → add as attributes on root span.
    pub fn on_build_metadata(&mut self, metadata: &serde_json::Value) {
        if let Some(cx) = &self.root_context {
            if let Some(entries) = metadata.get("metadata").and_then(|m| m.as_object()) {
                for (key, value) in entries {
                    if let Some(v) = value.as_str() {
                        let attr_key = format!("bazel.metadata.{key}");
                        cx.span()
                            .set_attribute(KeyValue::new(attr_key, v.to_string()));
                    }
                }
            }
        }
    }

    // =====================================================================
    // Build finish / metrics
    // =====================================================================

    /// BuildFinished → record exit code (root span is ended in [`finish`]).
    pub fn on_build_finished(&mut self, exit_code: Option<i32>, finish_time_millis: Option<i64>) {
        self.exit_code = exit_code;
        self.finish_time_millis = finish_time_millis;

        // Set exit code attribute immediately (metrics may add more before finish).
        if let Some(cx) = &self.root_context {
            if let Some(code) = exit_code {
                cx.span()
                    .set_attribute(KeyValue::new(BAZEL_EXIT_CODE, code as i64));
            }
        }
    }

    /// BuildMetrics → add metrics attributes to root span.
    pub fn on_build_metrics(&mut self, metrics: &serde_json::Value) {
        if let Some(cx) = &self.root_context {
            if let Some(summary) = metrics.as_object() {
                if let Some(created) = summary.get("actionsCreated").and_then(|v| v.as_i64()) {
                    cx.span()
                        .set_attribute(KeyValue::new(BAZEL_METRICS_ACTIONS_CREATED, created));
                }
                if let Some(executed) = summary.get("actionsExecuted").and_then(|v| v.as_i64()) {
                    cx.span()
                        .set_attribute(KeyValue::new(BAZEL_METRICS_ACTIONS_EXECUTED, executed));
                }
            }
        }
    }

    // =====================================================================
    // Finalization
    // =====================================================================

    /// End all remaining spans (call after last BEP event).
    pub fn finish(&mut self) {
        // End the `fetches` parent span.
        if let Some(cx) = self.fetches_context.take() {
            cx.span().set_status(Status::Ok);
            cx.span().end();
            debug!("Ended fetches span");
        }

        // Force-resolve any remaining deferred target completions.
        let pending_labels: Vec<String> = self.pending_completions.keys().cloned().collect();
        for label in pending_labels {
            if let Some(pending) = self.pending_completions.remove(&label) {
                debug!("Force-resolving deferred target completion for {label} at finish");
                Self::apply_file_sets_and_end(
                    &self.named_set_cache,
                    &pending.cx,
                    &label,
                    pending.success,
                    &pending.file_set_ids,
                    pending.end_time_nanos,
                );
            }
        }

        // End any remaining target spans (configured but never completed).
        let labels: Vec<String> = self.target_contexts.keys().cloned().collect();
        for label in labels {
            if let Some(cx) = self.target_contexts.remove(&label) {
                cx.span().set_status(Status::Unset);
                cx.span().end();
                warn!("Force-ended orphaned target span for {label}");
            }
        }

        // Flush accumulated progress text as a single `build.log` event on root.
        if let Some(cx) = &self.root_context {
            let mut attrs = Vec::new();
            if !self.progress_stderr.is_empty() {
                attrs.push(KeyValue::new(
                    BAZEL_PROGRESS_STDERR,
                    std::mem::take(&mut self.progress_stderr),
                ));
            }
            if !self.progress_stdout.is_empty() {
                attrs.push(KeyValue::new(
                    BAZEL_PROGRESS_STDOUT,
                    std::mem::take(&mut self.progress_stdout),
                ));
            }
            if !attrs.is_empty() {
                cx.span().add_event("build.log", attrs);
                debug!("Added build.log event to root span");
            }
        }

        // End root span with proper status + timestamp.
        if let Some(cx) = self.root_context.take() {
            let status = match self.exit_code {
                Some(0) => Status::Ok,
                Some(code) => Status::Error {
                    description: Cow::Owned(format!("exit code {code}")),
                },
                None => Status::Unset,
            };
            cx.span().set_status(status);

            if let Some(millis) = self.finish_time_millis {
                cx.span().end_with_timestamp(millis_to_system_time(millis));
            } else {
                cx.span().end();
            }

            info!("Root span ended (exit_code={:?})", self.exit_code);
        }
    }
}

fn millis_to_system_time(millis: i64) -> SystemTime {
    UNIX_EPOCH + Duration::from_millis(millis as u64)
}

fn nanos_to_system_time(nanos: i64) -> SystemTime {
    UNIX_EPOCH + Duration::from_nanos(nanos as u64)
}
