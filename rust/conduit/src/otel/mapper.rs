//! OTel Mapper – converts BEP events into OpenTelemetry spans.
//!
//! Manages the lifecycle of spans for a single Bazel build invocation.
//! Spans are created / updated / ended as BEP events flow through the router.
//!
//! Span hierarchy:
//! ```text
//! bazel.invocation (root, BuildStarted → finish)
//! ├── target {label} (TargetConfigured → TargetCompleted)
//! │   ├── action {mnemonic} {label} (lightweight: failed only / full: all)
//! │   └── test {label} (testResult spans)
//! ├── fetches (single parent span grouping all fetch events)
//! │   └── fetch {url} (individual fetch spans)
//! └── skipped targets (parent span grouping all skipped/aborted targets)
//!     └── target {label} (aborted by Bazel)
//!
//! Correlated OTel log records:
//!   - build.log (accumulated progress stderr/stdout, emitted as a Log
//!     record correlated with the trace via trace_id/span_id — falls back
//!     to a span event when no LoggerProvider is configured)
//!
//! Action processing modes:
//!   - lightweight (default): only failed actions create spans
//!   - full (--build_event_publish_all_actions): every action gets a span
//!     with accurate start_time / end_time from the ActionExecuted event
//! ```

use std::borrow::Cow;
use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use opentelemetry::logs::{AnyValue, LogRecord as _, Logger as _, Severity};
use opentelemetry::trace::{Span, SpanKind, Status, TraceContextExt, Tracer};
use opentelemetry::{Context, KeyValue};
use tracing::{debug, info, warn};

use crate::state::BufferedAction;
use super::attributes::*;
use super::trace_context;

/// Strip ANSI escape sequences (colors, cursor movement, etc.) from a string.
///
/// Handles CSI sequences (`ESC[...X`), OSC sequences (`ESC]...BEL/ST`), and
/// bare ESC + single-char sequences.
fn strip_ansi(input: &str) -> Cow<'_, str> {
    if !input.contains('\x1b') {
        return Cow::Borrowed(input);
    }
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

    Cow::Owned(out)
}

/// Metadata captured at `TargetConfigured` time.
///
/// Span creation is **deferred** until `TargetCompleted` so the mapper can
/// decide whether the target belongs under the root span or the "skipped"
/// parent span.  If an action arrives before the target completes, the
/// target span is lazily created under root (see
/// [`OtelMapper::ensure_target_span`]).
struct ConfiguredTarget {
    kind: Option<String>,
    tags: Vec<String>,
    event_time_nanos: Option<i64>,
}

/// Target span kept open until output file sets are resolved so we can
/// set output attributes on it before ending.
struct PendingOutputResolution {
    parent_cx: Context,
    file_set_ids: Vec<String>,
    event_time_nanos: Option<i64>,
}

/// A single entry in the NamedSet cache.
///
/// `NamedSetOfFiles` in BEP can contain both direct `files` and references
/// to other NamedSets (`file_sets`).  We store both so we can recursively
/// resolve the transitive closure of files.
struct NamedSetEntry {
    /// Direct files in this set.
    files: Vec<String>,
    /// IDs of child NamedSets whose files also belong to this set.
    child_set_ids: Vec<String>,
}

/// Maps BEP events to OpenTelemetry spans.
pub struct OtelMapper {
    tracer: opentelemetry_sdk::trace::Tracer,

    /// Root span context (`bazel.invocation`).
    root_context: Option<Context>,

    /// Targets whose `TargetConfigured` arrived but whose span has **not**
    /// yet been created.  Span creation is deferred until `TargetCompleted`
    /// so we can choose the correct parent (root vs. "skipped").
    configured_targets: HashMap<String, ConfiguredTarget>,

    /// Open target spans (created either lazily by an action or at
    /// `TargetCompleted` time), keyed by target label.
    target_contexts: HashMap<String, Context>,

    /// Targets whose output file sets are not yet fully resolved; the target
    /// span is still open.  Keyed by target label.
    pending_output_resolutions: HashMap<String, PendingOutputResolution>,

    /// Single `fetches` parent span grouping all fetch child spans.
    /// Created lazily on first fetch event, ended in [`finish`].
    fetches_context: Option<Context>,

    /// Single `skipped` parent span grouping all skipped/aborted targets.
    /// Created lazily on first skipped target, ended in [`finish`].
    skipped_context: Option<Context>,

    /// Accumulated stderr / stdout from all progress messages.
    /// Flushed as an OTel log record correlated with the trace in [`finish`].
    /// Falls back to a span event if no logger is available.
    progress_stderr: String,
    progress_stdout: String,

    /// Optional OTel logger for emitting build logs as log records.
    /// When present, build.log is emitted as a correlated log record
    /// instead of a span event — much friendlier for large build output.
    logger: Option<opentelemetry_sdk::logs::Logger>,

    /// Cached exit code from BuildFinished (root span ends in [`finish`]).
    exit_code: Option<i32>,

    /// Cached finish timestamp (nanos since epoch) from BuildFinished.
    finish_time_nanos: Option<i64>,

    /// Named set cache: set_id → entry with direct files + child set refs.
    /// Used to resolve output files (transitively) when target spans complete.
    named_set_cache: HashMap<String, NamedSetEntry>,

    /// Fetch events that arrived before the root span was created.
    /// Replayed once `on_build_started` fires.
    /// Tuple: (url, success, wallclock arrival time, downloader).
    pending_fetches: Vec<(String, bool, SystemTime, Option<String>)>,
}

impl OtelMapper {
    pub fn new(
        tracer: opentelemetry_sdk::trace::Tracer,
        logger: Option<opentelemetry_sdk::logs::Logger>,
    ) -> Self {
        Self {
            tracer,
            root_context: None,
            configured_targets: HashMap::new(),
            target_contexts: HashMap::new(),
            pending_output_resolutions: HashMap::new(),
            fetches_context: None,
            skipped_context: None,
            progress_stderr: String::new(),
            progress_stdout: String::new(),
            logger,
            exit_code: None,
            finish_time_nanos: None,
            named_set_cache: HashMap::new(),
            pending_fetches: Vec::new(),
        }
    }

    // =====================================================================
    // Lifecycle events
    // =====================================================================

    /// BuildStarted → create root span `bazel.invocation`.
    /// Prefers event_time_nanos (BES event time) when available so the trace reflects when the
    /// build was observed; otherwise uses start_time_nanos from the payload.
    pub fn on_build_started(
        &mut self,
        uuid: &str,
        command: &str,
        start_time_nanos: Option<i64>,
        event_time_nanos: Option<i64>,
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

        // Sanity-check: if the BEP start time is more than 5 minutes
        // from wall clock, it's probably stale (long-lived Bazel daemon).
        // Fall back to wall clock to avoid multi-day trace durations.
        let now_nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as i64;
        let five_min_nanos: i64 = 5 * 60 * 1_000_000_000;

        let effective_start = event_time_nanos.or(start_time_nanos);
        let sanitized_start = match effective_start {
            Some(nanos) if (now_nanos - nanos).abs() > five_min_nanos => {
                warn!(
                    bep_start_nanos = nanos,
                    wall_clock_nanos = now_nanos,
                    drift_seconds = (now_nanos - nanos) / 1_000_000_000,
                    "BEP start time deviates from wall clock by >5 min; using wall clock"
                );
                Some(now_nanos)
            }
            other => other,
        };
        if let Some(nanos) = sanitized_start {
            builder = builder.with_start_time(nanos_to_system_time(nanos));
        }
        let span = self.tracer.build_with_context(builder, &parent_cx);
        let cx = Context::new().with_span(span);

        info!(trace_id = %trace_id, "Created root span for invocation {uuid}");
        self.root_context = Some(cx);

        // Replay any fetch events that arrived before the root span.
        if !self.pending_fetches.is_empty() {
            let buffered: Vec<_> = std::mem::take(&mut self.pending_fetches);
            info!("Replaying {} buffered fetch events", buffered.len());
            for (url, success, arrival, downloader) in buffered {
                self.emit_fetch_span(&url, success, arrival, downloader.as_deref());
            }
        }
    }

    /// Extended BuildStarted fields → set attributes on root span.
    pub fn on_build_started_extended(
        &mut self,
        workspace_dir: Option<&str>,
        working_dir: Option<&str>,
        build_tool_version: Option<&str>,
        server_pid: Option<i64>,
        host: Option<&str>,
        user: Option<&str>,
    ) {
        let Some(cx) = &self.root_context else { return };
        if let Some(v) = workspace_dir {
            cx.span().set_attribute(KeyValue::new(BAZEL_WORKSPACE_DIR, v.to_string()));
        }
        if let Some(v) = working_dir {
            cx.span().set_attribute(KeyValue::new(BAZEL_WORKING_DIR, v.to_string()));
        }
        if let Some(v) = build_tool_version {
            cx.span().set_attribute(KeyValue::new(BAZEL_BUILD_TOOL_VERSION, v.to_string()));
        }
        if let Some(v) = server_pid {
            cx.span().set_attribute(KeyValue::new(BAZEL_SERVER_PID, v));
        }
        if let Some(v) = host {
            cx.span().set_attribute(KeyValue::new(BAZEL_HOST, v.to_string()));
        }
        if let Some(v) = user {
            cx.span().set_attribute(KeyValue::new(BAZEL_USER, v.to_string()));
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

    /// ActionMode → record the detected processing mode on the root span.
    pub fn on_action_mode(&mut self, mode: crate::state::ActionProcessingMode) {
        if let Some(cx) = &self.root_context {
            cx.span()
                .set_attribute(KeyValue::new(BAZEL_ACTION_MODE, mode.to_string()));
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

    /// PatternSkipped → add skipped patterns as span event on root.
    pub fn on_pattern_skipped(&mut self, patterns: &[String]) {
        if let Some(cx) = &self.root_context {
            if !patterns.is_empty() {
                cx.span().add_event(
                    "pattern_skipped",
                    vec![KeyValue::new(BAZEL_PATTERNS, patterns.join(", "))],
                );
            }
        }
    }

    /// UnconfiguredLabel / ConfiguredLabel with Aborted payload →
    /// record as a span under the "skipped" parent (root-cause failure).
    pub fn on_aborted_label(
        &mut self,
        label: &str,
        reason: Option<&str>,
        description: Option<&str>,
        event_time_nanos: Option<i64>,
    ) {
        // Lazily create the "skipped" parent span.
        if self.skipped_context.is_none() {
            if let Some(root) = &self.root_context {
                let builder = self
                    .tracer
                    .span_builder("skipped targets")
                    .with_kind(SpanKind::Internal);
                let span = self.tracer.build_with_context(builder, root);
                self.skipped_context = Some(Context::new().with_span(span));
            }
        }

        let parent = match &self.skipped_context {
            Some(cx) => cx,
            None => return,
        };

        let mut attrs = vec![
            KeyValue::new(BAZEL_TARGET_LABEL, label.to_string()),
            KeyValue::new(BAZEL_TARGET_SUCCESS, false),
        ];
        if let Some(r) = reason {
            attrs.push(KeyValue::new("bazel.target.abort_reason", r.to_string()));
        }
        if let Some(d) = description {
            attrs.push(KeyValue::new(
                "bazel.target.abort_description",
                d.to_string(),
            ));
        }

        let builder = self
            .tracer
            .span_builder(format!("target {label}"))
            .with_kind(SpanKind::Internal)
            .with_attributes(attrs);

        let mut span = self.tracer.build_with_context(builder, parent);
        span.set_status(Status::Error {
            description: std::borrow::Cow::Owned(
                description.unwrap_or("aborted").to_string(),
            ),
        });
        if let Some(nanos) = event_time_nanos {
            span.end_with_timestamp(nanos_to_system_time(nanos));
        } else {
            span.end();
        }

        debug!("Created aborted-label span for {label}");
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

    /// TargetConfigured → store metadata for deferred span creation.
    ///
    /// The actual span is created later in [`on_target_completed`] (or
    /// [`on_target_skipped`]) so we can choose the correct parent.
    /// If an action arrives before the target completes, the span is
    /// created lazily under root via [`ensure_target_span`].
    pub fn on_target_configured(
        &mut self,
        label: &str,
        kind: Option<&str>,
        tags: &[String],
        event_time_nanos: Option<i64>,
    ) {
        if self.root_context.is_none() {
            warn!("TargetConfigured before BuildStarted for {label}");
            return;
        }

        self.configured_targets.insert(
            label.to_string(),
            ConfiguredTarget {
                kind: kind.map(String::from),
                tags: tags.to_vec(),
                event_time_nanos,
            },
        );

        debug!("Stored configured target metadata for {label}");
    }

    /// Create a target span under a given parent context.
    /// When `start_nanos_override` is set (e.g. earliest action start), use it
    /// instead of configured event time so target duration reflects action timing.
    fn create_target_span(
        &self,
        label: &str,
        configured: &ConfiguredTarget,
        parent: &Context,
        start_nanos_override: Option<i64>,
    ) -> Context {
        let mut attrs = vec![KeyValue::new(BAZEL_TARGET_LABEL, label.to_string())];
        if let Some(k) = &configured.kind {
            attrs.push(KeyValue::new(BAZEL_TARGET_KIND, k.clone()));
        }
        if !configured.tags.is_empty() {
            attrs.push(KeyValue::new(
                BAZEL_TARGET_TAGS,
                configured.tags.join(", "),
            ));
        }

        let start_nanos = start_nanos_override.or(configured.event_time_nanos);
        let mut builder = self
            .tracer
            .span_builder(format!("target {label}"))
            .with_kind(SpanKind::Internal)
            .with_attributes(attrs);

        if let Some(nanos) = start_nanos {
            builder = builder.with_start_time(nanos_to_system_time(nanos));
        }

        let span = self.tracer.build_with_context(builder, parent);
        Context::new().with_span(span)
    }

    /// Ensure a target span exists in `target_contexts`, creating it
    /// lazily (under root) if we have `ConfiguredTarget` metadata, or a
    /// synthetic target from the action label when no TargetConfigured was seen.
    fn ensure_target_span(&mut self, label: &str) {
        if self.target_contexts.contains_key(label)
            || self.pending_output_resolutions.contains_key(label)
        {
            return;
        }
        let Some(root) = &self.root_context else {
            return;
        };
        if let Some(configured) = self.configured_targets.get(label) {
            let cx = self.create_target_span(label, configured, root, None);
            debug!("Lazily created target span for {label} (action needed parent)");
            self.target_contexts.insert(label.to_string(), cx);
        } else {
            // No TargetConfigured for this label; create a synthetic target from the action label.
            let cx = self.create_synthetic_target_span(label, root);
            debug!("Created synthetic target span for {label} (from action label)");
            self.target_contexts.insert(label.to_string(), cx);
        }
    }

    /// Create a target span with only the label (no kind/tags). Used when we see
    /// an action with a label but no prior TargetConfigured event.
    fn create_synthetic_target_span(&self, label: &str, parent: &Context) -> Context {
        let attrs = vec![KeyValue::new(BAZEL_TARGET_LABEL, label.to_string())];
        let builder = self
            .tracer
            .span_builder(format!("target {label}"))
            .with_kind(SpanKind::Internal)
            .with_attributes(attrs);
        let span = self.tracer.build_with_context(builder, parent);
        Context::new().with_span(span)
    }

    /// TargetCompleted → create (or reuse) the target span, set attributes,
    /// then either set output attributes and end (if file sets resolved), or
    /// keep the span open for later resolution in [`on_named_set`] / [`finish`].
    /// Target start/end use earliest action start and latest action end when
    /// available so duration reflects actual work.
    pub fn on_target_completed(
        &mut self,
        label: &str,
        success: bool,
        file_set_ids: &[String],
        tags: &[String],
        event_time_nanos: Option<i64>,
        earliest_action_start_nanos: Option<i64>,
        latest_action_end_nanos: Option<i64>,
        buffered_actions: &[BufferedAction],
    ) {
        let effective_end = latest_action_end_nanos.or(event_time_nanos);

        // Get or create the span under root.
        let cx = if let Some(cx) = self.target_contexts.remove(label) {
            cx
        } else if let Some(configured) = self.configured_targets.remove(label) {
            if let Some(root) = &self.root_context {
                let start_override = earliest_action_start_nanos;
                let cx = self.create_target_span(label, &configured, root, start_override);
                debug!("Created target span for {label} at completion time");
                cx
            } else {
                warn!("TargetCompleted before BuildStarted for {label}");
                return;
            }
        } else {
            warn!("TargetCompleted for unknown target {label}");
            return;
        };

        cx.span()
            .set_attribute(KeyValue::new(BAZEL_TARGET_SUCCESS, success));

        if !tags.is_empty() {
            cx.span()
                .set_attribute(KeyValue::new(BAZEL_TARGET_TAGS, tags.join(", ")));
        }

        let status = if success {
            Status::Ok
        } else {
            Status::Error {
                description: Cow::Borrowed("target failed"),
            }
        };
        cx.span().set_status(status);

        // Replay buffered actions as children of this target (target was created with action-based timing).
        if !buffered_actions.is_empty() {
            self.target_contexts.insert(label.to_string(), cx.clone());
            for act in buffered_actions {
                self.on_action_completed(
                    act.label.as_deref(),
                    act.mnemonic.as_deref(),
                    act.success,
                    act.exit_code,
                    act.primary_output.as_deref(),
                    act.configuration.as_deref(),
                    &act.command_line,
                    act.stdout_uri.as_deref(),
                    act.stderr_uri.as_deref(),
                    act.start_nanos,
                    act.end_nanos,
                );
            }
            self.target_contexts.remove(label);
        }

        if file_set_ids.is_empty() {
            if let Some(nanos) = effective_end {
                cx.span().end_with_timestamp(nanos_to_system_time(nanos));
            } else {
                cx.span().end();
            }
            debug!("Ended target span for {label} (success={success})");
            return;
        }

        let all_resolved =
            Self::all_sets_resolved(&self.named_set_cache, file_set_ids);

        if all_resolved {
            Self::set_output_attributes_and_end_target_span(
                &self.named_set_cache,
                &cx,
                file_set_ids,
                effective_end,
            );
            debug!("Ended target span for {label} with output attributes (success={success})");
        } else {
            debug!("Deferring target span end for {label} (waiting for named sets)");
            self.pending_output_resolutions.insert(
                label.to_string(),
                PendingOutputResolution {
                    parent_cx: cx,
                    file_set_ids: file_set_ids.to_vec(),
                    event_time_nanos: effective_end,
                },
            );
        }
    }

    /// TargetCompleted with `aborted` payload → create a brief span under
    /// the "skipped" parent and end it immediately.
    pub fn on_target_skipped(
        &mut self,
        label: &str,
        reason: Option<&str>,
        description: Option<&str>,
        event_time_nanos: Option<i64>,
    ) {
        // Consume the configured metadata (if any).
        let configured = self.configured_targets.remove(label);
        // Also consume any lazily-created span (shouldn't exist for skipped,
        // but clean up just in case).
        let _existing = self.target_contexts.remove(label);

        // Lazily create the "skipped" parent span.
        if self.skipped_context.is_none() {
            if let Some(root) = &self.root_context {
                let builder = self
                    .tracer
                    .span_builder("skipped targets")
                    .with_kind(SpanKind::Internal);
                let span = self.tracer.build_with_context(builder, root);
                self.skipped_context = Some(Context::new().with_span(span));
                debug!("Created 'skipped targets' parent span");
            }
        }

        let parent = match &self.skipped_context {
            Some(cx) => cx,
            None => return,
        };

        // Build the child span.
        let mut attrs = vec![
            KeyValue::new(BAZEL_TARGET_LABEL, label.to_string()),
            KeyValue::new(BAZEL_TARGET_SUCCESS, false),
        ];
        if let Some(configured) = &configured {
            if let Some(k) = &configured.kind {
                attrs.push(KeyValue::new(BAZEL_TARGET_KIND, k.clone()));
            }
        }
        if let Some(r) = reason {
            attrs.push(KeyValue::new("bazel.target.abort_reason", r.to_string()));
        }
        if let Some(d) = description {
            attrs.push(KeyValue::new(
                "bazel.target.abort_description",
                d.to_string(),
            ));
        }

        let mut builder = self
            .tracer
            .span_builder(format!("target {label}"))
            .with_kind(SpanKind::Internal)
            .with_attributes(attrs);

        // Use configured start time if available.
        if let Some(configured) = &configured {
            if let Some(nanos) = configured.event_time_nanos {
                builder = builder.with_start_time(nanos_to_system_time(nanos));
            }
        }

        let span = self.tracer.build_with_context(builder, parent);
        let cx = Context::new().with_span(span);

        cx.span().set_status(Status::Unset);
        if let Some(nanos) = event_time_nanos {
            cx.span().end_with_timestamp(nanos_to_system_time(nanos));
        } else {
            cx.span().end();
        }

        debug!("Created and ended skipped target span for {label}");
    }

    /// Set output file attributes on the target span and end it.
    fn set_output_attributes_and_end_target_span(
        cache: &HashMap<String, NamedSetEntry>,
        cx: &Context,
        file_set_ids: &[String],
        event_time_nanos: Option<i64>,
    ) {
        let all_files = Self::resolve_files(cache, file_set_ids);

        if !all_files.is_empty() {
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
                .set_attribute(KeyValue::new(BAZEL_NAMED_SET_FILE_COUNT, all_files.len() as i64));
            cx.span()
                .set_attribute(KeyValue::new(BAZEL_TARGET_OUTPUT_FILES, file_list));
        }

        if let Some(nanos) = event_time_nanos {
            cx.span().end_with_timestamp(nanos_to_system_time(nanos));
        } else {
            cx.span().end();
        }
    }

    // -----------------------------------------------------------------
    // NamedSet transitive helpers
    // -----------------------------------------------------------------

    /// Check whether all NamedSets reachable from `set_ids` (transitively)
    /// are present in the cache.
    fn all_sets_resolved(
        cache: &HashMap<String, NamedSetEntry>,
        set_ids: &[String],
    ) -> bool {
        let mut stack: Vec<&str> = set_ids.iter().map(String::as_str).collect();
        let mut visited = std::collections::HashSet::new();
        while let Some(id) = stack.pop() {
            if !visited.insert(id) {
                continue;
            }
            match cache.get(id) {
                Some(entry) => {
                    for child in &entry.child_set_ids {
                        stack.push(child.as_str());
                    }
                }
                None => return false,
            }
        }
        true
    }

    /// Recursively collect all files reachable from the given NamedSet IDs.
    fn resolve_files(
        cache: &HashMap<String, NamedSetEntry>,
        set_ids: &[String],
    ) -> Vec<String> {
        let mut files = Vec::new();
        let mut stack: Vec<&str> = set_ids.iter().map(String::as_str).collect();
        let mut visited = std::collections::HashSet::new();
        while let Some(id) = stack.pop() {
            if !visited.insert(id) {
                continue;
            }
            if let Some(entry) = cache.get(id) {
                files.extend(entry.files.iter().cloned());
                for child in &entry.child_set_ids {
                    stack.push(child.as_str());
                }
            }
        }
        files
    }

    // =====================================================================
    // Named set cache
    // =====================================================================

    /// Cache a NamedSet so it can be resolved when targets complete.
    ///
    /// A NamedSet can contain direct `files` AND references to other
    /// NamedSets (`child_set_ids`).  Both are stored so that
    /// [`resolve_files`] can recursively collect the transitive closure.
    ///
    /// After caching, any pending target spans whose output file sets are now
    /// fully resolved get output attributes set and are ended.
    pub fn on_named_set(&mut self, set_id: &str, files: Vec<String>, child_set_ids: &[String]) {
        self.named_set_cache.insert(
            set_id.to_string(),
            NamedSetEntry {
                files,
                child_set_ids: child_set_ids.to_vec(),
            },
        );

        let ready_labels: Vec<String> = self
            .pending_output_resolutions
            .iter()
            .filter(|(_, pending)| {
                Self::all_sets_resolved(&self.named_set_cache, &pending.file_set_ids)
            })
            .map(|(label, _)| label.clone())
            .collect();

        for label in ready_labels {
            if let Some(pending) = self.pending_output_resolutions.remove(&label) {
                debug!("Resolving deferred output attributes for target {label}");
                Self::set_output_attributes_and_end_target_span(
                    &self.named_set_cache,
                    &pending.parent_cx,
                    &pending.file_set_ids,
                    pending.event_time_nanos,
                );
            }
        }
    }

    // =====================================================================
    // Action events
    // =====================================================================

    /// ActionCompleted → create + immediately end `action {mnemonic} {label}`.
    ///
    /// In **lightweight** mode only failed actions reach this method.
    /// In **full** mode every action (success or failure) is mapped.
    ///
    /// When `start_time_nanos` / `end_time_nanos` are available (from the
    /// `ActionExecuted.start_time` / `end_time` proto fields) they are used
    /// for accurate span timing.
    #[allow(clippy::too_many_arguments)]
    pub fn on_action_completed(
        &mut self,
        label: Option<&str>,
        mnemonic: Option<&str>,
        success: bool,
        exit_code: Option<i32>,
        primary_output: Option<&str>,
        configuration: Option<&str>,
        command_line: &[String],
        stdout_path: Option<&str>,
        stderr_path: Option<&str>,
        start_time_nanos: Option<i64>,
        end_time_nanos: Option<i64>,
    ) {
        if let Some(l) = label {
            self.ensure_target_span(l);
        }

        let parent = label
            .and_then(|l| {
                self.target_contexts
                    .get(l)
                    .or_else(|| self.pending_output_resolutions.get(l).map(|p| &p.parent_cx))
            })
            .or(self.root_context.as_ref());

        let parent = match parent {
            Some(cx) => cx.clone(),
            None => {
                warn!("ActionCompleted with no parent context");
                return;
            }
        };

        let span_name = match (label, mnemonic) {
            (Some(l), Some(m)) => format!("action {m} {l}"),
            (Some(l), None) => format!("action {l}"),
            (None, Some(m)) => format!("action {m}"),
            _ => "action".to_string(),
        };

        let mut attrs = vec![KeyValue::new(BAZEL_ACTION_SUCCESS, success)];
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
            attrs.push(KeyValue::new(BAZEL_ACTION_LABEL, l.to_string()));
        }
        if let Some(cfg) = configuration {
            attrs.push(KeyValue::new(
                BAZEL_ACTION_CONFIGURATION,
                cfg.to_string(),
            ));
        }
        if !command_line.is_empty() {
            attrs.push(KeyValue::new(
                BAZEL_ACTION_COMMAND_LINE,
                command_line.join(" "),
            ));
        }
        if let Some(p) = stdout_path {
            attrs.push(KeyValue::new(BAZEL_ACTION_STDOUT, p.to_string()));
        }
        if let Some(p) = stderr_path {
            attrs.push(KeyValue::new(BAZEL_ACTION_STDERR, p.to_string()));
        }

        let mut builder = self
            .tracer
            .span_builder(span_name)
            .with_kind(SpanKind::Internal)
            .with_attributes(attrs);

        if let Some(nanos) = start_time_nanos {
            builder = builder.with_start_time(nanos_to_system_time(nanos));
        }

        let mut span = self.tracer.build_with_context(builder, &parent);

        if success {
            span.set_status(Status::Ok);
        } else {
            span.set_status(Status::Error {
                description: Cow::Borrowed("Action failed"),
            });
        }

        if let Some(nanos) = end_time_nanos {
            span.end_with_timestamp(nanos_to_system_time(nanos));
        } else {
            span.end();
        }

        debug!(
            "Created action span (success={success}) for {:?}",
            label
        );
    }

    // =====================================================================
    // Fetch events
    // =====================================================================

    /// Fetch → child span under root for external resource fetching.
    ///
    /// In gRPC mode, fetch events may arrive *before* the BuildStarted event
    /// (during Bazel module resolution).  These are buffered and replayed once
    /// the root span exists.
    pub fn on_fetch(&mut self, url: &str, success: bool, downloader: Option<&str>) {
        let arrival = SystemTime::now();
        if self.root_context.is_none() {
            debug!("Buffering fetch event (root span not yet created): {url}");
            self.pending_fetches.push((url.to_string(), success, arrival, downloader.map(String::from)));
            return;
        }
        self.emit_fetch_span(url, success, arrival, downloader);
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
    fn emit_fetch_span(&mut self, url: &str, success: bool, arrival: SystemTime, downloader: Option<&str>) {
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

    // =====================================================================
    // Test events
    // =====================================================================

    /// TestResult → child span under target span for a single test attempt.
    /// When `start_time_nanos` / `duration_nanos` are present, span uses accurate timing.
    pub fn on_test_result(
        &mut self,
        label: &str,
        status: Option<&str>,
        attempt: Option<i32>,
        run: Option<i32>,
        shard: Option<i32>,
        cached: Option<bool>,
        strategy: Option<&str>,
        start_time_nanos: Option<i64>,
        duration_nanos: Option<i64>,
    ) {
        self.ensure_target_span(label);

        let parent = self
            .target_contexts
            .get(label)
            .or_else(|| self.pending_output_resolutions.get(label).map(|p| &p.parent_cx))
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

        let mut builder = self
            .tracer
            .span_builder(span_name)
            .with_kind(SpanKind::Internal)
            .with_attributes(attrs);

        if let Some(nanos) = start_time_nanos {
            builder = builder.with_start_time(nanos_to_system_time(nanos));
        }

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

        if let (Some(start), Some(dur)) = (start_time_nanos, duration_nanos) {
            span.end_with_timestamp(nanos_to_system_time(start + dur));
        } else {
            span.end();
        }

        debug!("Created test result span for {label}");
    }

    /// TestSummary → span event on root (target span is already ended).
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
        if let Some(cx) = &self.root_context {
            let mut attrs = vec![KeyValue::new(BAZEL_TARGET_LABEL, label.to_string())];
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
            cx.span().add_event("test_summary", attrs);
            debug!("Added test summary event for {label}");
        }
    }

    /// TargetSummary → span event on root with overall build/test status for the target.
    pub fn on_target_summary(
        &mut self,
        label: &str,
        overall_build_success: Option<bool>,
        overall_test_status: Option<&str>,
    ) {
        if let Some(cx) = &self.root_context {
            let mut attrs = vec![KeyValue::new(BAZEL_TARGET_LABEL, label.to_string())];
            if let Some(s) = overall_build_success {
                attrs.push(KeyValue::new(BAZEL_TARGET_OVERALL_BUILD_SUCCESS, s));
            }
            if let Some(s) = overall_test_status {
                attrs.push(KeyValue::new(BAZEL_TARGET_OVERALL_TEST_STATUS, s.to_string()));
            }
            cx.span().add_event("target_summary", attrs);
            debug!("Added target summary event for {label}");
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

    /// BuildFinished → record exit code and finish time (root span is ended in [`finish`]).
    pub fn on_build_finished(&mut self, exit_code: Option<i32>, finish_time_nanos: Option<i64>) {
        self.exit_code = exit_code;
        self.finish_time_nanos = finish_time_nanos;

        if let Some(cx) = &self.root_context {
            if let Some(code) = exit_code {
                cx.span()
                    .set_attribute(KeyValue::new(BAZEL_EXIT_CODE, code as i64));
            }
        }
    }

    /// BuildMetrics → add metrics attributes to root span.
    /// Supports both legacy flat payload (actionsCreated/actionsExecuted) and full nested payload.
    pub fn on_build_metrics(&mut self, metrics: &serde_json::Value) {
        let Some(cx) = &self.root_context else {
            return;
        };
        let obj = match metrics.as_object() {
            Some(o) => o,
            None => return,
        };

        // ActionSummary: top-level (legacy) or under "actionSummary"
        let summary = obj.get("actionSummary").or(Some(metrics));
        if let Some(s) = summary.and_then(|v| v.as_object()) {
            if let Some(created) = s.get("actionsCreated").and_then(|v| v.as_i64()) {
                cx.span()
                    .set_attribute(KeyValue::new(BAZEL_METRICS_ACTIONS_CREATED, created));
            }
            if let Some(executed) = s.get("actionsExecuted").and_then(|v| v.as_i64()) {
                cx.span()
                    .set_attribute(KeyValue::new(BAZEL_METRICS_ACTIONS_EXECUTED, executed));
            }
            if let Some(action_data) = s.get("actionData").and_then(|v| v.as_array()) {
                let summary = summarize_action_data(action_data);
                cx.span()
                    .set_attribute(KeyValue::new(BAZEL_METRICS_ACTION_DATA, summary));
            }
            if let Some(acs) = s.get("actionCacheStatistics").and_then(|v| v.as_object()) {
                if let Some(hits) = acs.get("hits").and_then(|v| v.as_i64()) {
                    cx.span().set_attribute(KeyValue::new(BAZEL_METRICS_CACHE_HITS, hits));
                }
                if let Some(misses) = acs.get("misses").and_then(|v| v.as_i64()) {
                    cx.span().set_attribute(KeyValue::new(BAZEL_METRICS_CACHE_MISSES, misses));
                }
            }
        }

        if let Some(t) = obj.get("timingMetrics").and_then(|v| v.as_object()) {
            if let Some(v) = t.get("wallTimeInMs").and_then(|v| v.as_i64()) {
                cx.span().set_attribute(KeyValue::new(BAZEL_METRICS_WALL_TIME_MS, v));
            }
            if let Some(v) = t.get("cpuTimeInMs").and_then(|v| v.as_i64()) {
                cx.span().set_attribute(KeyValue::new(BAZEL_METRICS_CPU_TIME_MS, v));
            }
            if let Some(v) = t.get("analysisPhaseTimeInMs").and_then(|v| v.as_i64()) {
                cx.span().set_attribute(KeyValue::new(BAZEL_METRICS_ANALYSIS_PHASE_MS, v));
            }
            if let Some(v) = t.get("executionPhaseTimeInMs").and_then(|v| v.as_i64()) {
                cx.span().set_attribute(KeyValue::new(BAZEL_METRICS_EXECUTION_PHASE_MS, v));
            }
            if let Some(v) = t.get("criticalPathMs").and_then(|v| v.as_i64()) {
                cx.span().set_attribute(KeyValue::new(BAZEL_METRICS_CRITICAL_PATH_MS, v));
            }
        }

        if let Some(m) = obj.get("memoryMetrics").and_then(|v| v.as_object()) {
            if let Some(v) = m.get("usedHeapSizePostBuild").and_then(|v| v.as_i64()) {
                cx.span().set_attribute(KeyValue::new(BAZEL_METRICS_HEAP_POST_BUILD, v));
            }
            if let Some(v) = m.get("peakPostGcHeapSize").and_then(|v| v.as_i64()) {
                cx.span().set_attribute(KeyValue::new(BAZEL_METRICS_PEAK_HEAP_POST_GC, v));
            }
        }

        if let Some(tm) = obj.get("targetMetrics").and_then(|v| v.as_object()) {
            if let Some(v) = tm.get("targetsConfigured").and_then(|v| v.as_i64()) {
                cx.span().set_attribute(KeyValue::new(BAZEL_METRICS_TARGETS_CONFIGURED, v));
            }
        }

        if let Some(pm) = obj.get("packageMetrics").and_then(|v| v.as_object()) {
            if let Some(v) = pm.get("packagesLoaded").and_then(|v| v.as_i64()) {
                cx.span().set_attribute(KeyValue::new(BAZEL_METRICS_PACKAGES_LOADED, v));
            }
        }

        if let Some(am) = obj.get("artifactMetrics").and_then(|v| v.as_object()) {
            if let Some(f) = am.get("sourceArtifactsRead").and_then(|v| v.as_object()) {
                if let Some(v) = f.get("count").and_then(|v| v.as_i64()) {
                    cx.span()
                        .set_attribute(KeyValue::new(BAZEL_METRICS_SOURCE_ARTIFACTS_COUNT, v));
                }
            }
            if let Some(f) = am.get("outputArtifactsSeen").and_then(|v| v.as_object()) {
                if let Some(v) = f.get("count").and_then(|v| v.as_i64()) {
                    cx.span()
                        .set_attribute(KeyValue::new(BAZEL_METRICS_OUTPUT_ARTIFACTS_COUNT, v));
                }
            }
            if let Some(f) = am.get("outputArtifactsFromActionCache").and_then(|v| v.as_object()) {
                if let Some(v) = f.get("count").and_then(|v| v.as_i64()) {
                    cx.span().set_attribute(KeyValue::new(
                        BAZEL_METRICS_ACTION_CACHE_ARTIFACTS_COUNT,
                        v,
                    ));
                }
            }
        }

        if let Some(nm) = obj.get("networkMetrics").and_then(|v| v.as_object()) {
            if let Some(v) = nm.get("bytesSent").and_then(|v| v.as_u64()) {
                cx.span()
                    .set_attribute(KeyValue::new(BAZEL_METRICS_BYTES_SENT, v as i64));
            }
            if let Some(v) = nm.get("bytesRecv").and_then(|v| v.as_u64()) {
                cx.span()
                    .set_attribute(KeyValue::new(BAZEL_METRICS_BYTES_RECV, v as i64));
            }
        }

        if let Some(v) = summary
            .and_then(|s| s.as_object())
            .and_then(|s| s.get("runnerCount"))
        {
            if let Ok(s) = serde_json::to_string(v) {
                cx.span().set_attribute(KeyValue::new(BAZEL_METRICS_RUNNER_COUNT, s));
            }
        }

        if let Some(t) = obj.get("timingMetrics").and_then(|v| v.as_object()) {
            if let Some(v) = t.get("actionsExecutionStartInMs").and_then(|v| v.as_i64()) {
                cx.span().set_attribute(KeyValue::new(BAZEL_METRICS_ACTIONS_EXECUTION_START_MS, v));
            }
        }

        if let Some(cm) = obj.get("cumulativeMetrics").and_then(|v| v.as_object()) {
            if let Some(v) = cm.get("numAnalyses").and_then(|v| v.as_i64()) {
                cx.span().set_attribute(KeyValue::new(BAZEL_METRICS_CUMULATIVE_NUM_ANALYSES, v));
            }
            if let Some(v) = cm.get("numBuilds").and_then(|v| v.as_i64()) {
                cx.span().set_attribute(KeyValue::new(BAZEL_METRICS_CUMULATIVE_NUM_BUILDS, v));
            }
        }

        if let Some(am) = obj.get("artifactMetrics").and_then(|v| v.as_object()) {
            if let Some(f) = am.get("topLevelArtifacts").and_then(|v| v.as_object()) {
                if let Some(v) = f.get("count").and_then(|v| v.as_i64()) {
                    cx.span()
                        .set_attribute(KeyValue::new(BAZEL_METRICS_TOP_LEVEL_ARTIFACTS_COUNT, v));
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

        // Force-resolve any remaining pending target spans (set output attrs and end).
        let pending_labels: Vec<String> = self
            .pending_output_resolutions
            .keys()
            .cloned()
            .collect();
        for label in pending_labels {
            if let Some(pending) = self.pending_output_resolutions.remove(&label) {
                debug!("Force-ending target span for {label} at finish (with partial output data)");
                Self::set_output_attributes_and_end_target_span(
                    &self.named_set_cache,
                    &pending.parent_cx,
                    &pending.file_set_ids,
                    pending.event_time_nanos,
                );
            }
        }

        // End any remaining target spans (lazily created but never completed).
        let labels: Vec<String> = self.target_contexts.keys().cloned().collect();
        for label in labels {
            if let Some(cx) = self.target_contexts.remove(&label) {
                cx.span().set_status(Status::Unset);
                cx.span().end();
                warn!("Force-ended orphaned target span for {label}");
            }
        }

        // Warn about configured targets that never got a completed/aborted event.
        for label in self.configured_targets.keys() {
            warn!("Target {label} was configured but never completed or skipped");
        }
        self.configured_targets.clear();

        // End the `skipped targets` parent span.
        if let Some(cx) = self.skipped_context.take() {
            cx.span().set_status(Status::Unset);
            cx.span().end();
            debug!("Ended 'skipped targets' span");
        }

        // Flush accumulated progress text as an OTel log record correlated
        // with the trace (much friendlier for large build output than a span
        // event).  Falls back to a span event when no logger is available.
        {
            let stderr = std::mem::take(&mut self.progress_stderr);
            let stdout = std::mem::take(&mut self.progress_stdout);
            let has_content = !stderr.is_empty() || !stdout.is_empty();

            if has_content {
                if let (Some(logger), Some(cx)) = (&self.logger, &self.root_context) {
                    let span_ref = cx.span();
                    let span_cx = span_ref.span_context();

                    let mut record = logger.create_log_record();
                    record.set_trace_context(
                        span_cx.trace_id(),
                        span_cx.span_id(),
                        Some(span_cx.trace_flags()),
                    );
                    record.set_severity_number(Severity::Info);
                    record.set_severity_text("INFO");
                    record.set_event_name("build.log");

                    // Body = stderr (primary Bazel output); stdout as attribute
                    // when both are present.
                    match (!stderr.is_empty(), !stdout.is_empty()) {
                        (true, true) => {
                            record.set_body(AnyValue::String(stderr.into()));
                            record.add_attribute(
                                BAZEL_PROGRESS_STDOUT,
                                AnyValue::String(stdout.into()),
                            );
                        }
                        (true, false) => {
                            record.set_body(AnyValue::String(stderr.into()));
                        }
                        (false, true) => {
                            record.set_body(AnyValue::String(stdout.into()));
                        }
                        (false, false) => unreachable!("has_content guard"),
                    }

                    logger.emit(record);
                    debug!("Emitted build.log as OTel log record correlated with trace");
                } else if let Some(cx) = &self.root_context {
                    // Fallback: span event when no logger is available
                    let mut attrs = Vec::new();
                    if !stderr.is_empty() {
                        attrs.push(KeyValue::new(BAZEL_PROGRESS_STDERR, stderr));
                    }
                    if !stdout.is_empty() {
                        attrs.push(KeyValue::new(BAZEL_PROGRESS_STDOUT, stdout));
                    }
                    cx.span().add_event("build.log", attrs);
                    debug!("Added build.log as span event (no logger available)");
                }
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

            if let Some(nanos) = self.finish_time_nanos {
                cx.span().end_with_timestamp(nanos_to_system_time(nanos));
            } else {
                cx.span().end();
            }

            info!("Root span ended (exit_code={:?})", self.exit_code);
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

/// Compact summary of actionData: "Mnemonic(count), ..." sorted by count desc.
fn summarize_action_data(entries: &[serde_json::Value]) -> String {
    let mut by_mnemonic: HashMap<&str, i64> = HashMap::new();
    for entry in entries {
        let mnemonic = entry
            .get("mnemonic")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown");
        let count = entry
            .get("actionsExecuted")
            .and_then(|v| v.as_i64())
            .unwrap_or(0);
        *by_mnemonic.entry(mnemonic).or_default() += count;
    }
    let mut sorted: Vec<_> = by_mnemonic.into_iter().collect();
    sorted.sort_by(|a, b| b.1.cmp(&a.1));
    sorted
        .iter()
        .map(|(m, c)| format!("{m}({c})"))
        .collect::<Vec<_>>()
        .join(", ")
}
