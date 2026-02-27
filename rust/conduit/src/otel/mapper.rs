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

/// Holds data for a target whose `TargetCompleted` arrived before all
/// referenced `NamedSet` events.  Resolved lazily in [`OtelMapper::on_named_set`]
/// or force-resolved in [`OtelMapper::finish`].
struct PendingTargetCompletion {
    cx: Context,
    success: bool,
    file_set_ids: Vec<String>,
    end_time_nanos: Option<i64>,
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

    /// Targets whose `TargetCompleted` arrived but whose `NamedSet` data
    /// is not yet in the cache.  Keyed by target label.
    pending_completions: HashMap<String, PendingTargetCompletion>,

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
    /// Tuple: (url, success, wallclock arrival time).
    pending_fetches: Vec<(String, bool, SystemTime)>,
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
            pending_completions: HashMap::new(),
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

        let effective_start = event_time_nanos.or(start_time_nanos);
        if let Some(nanos) = effective_start {
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
    ///
    /// Returns the newly created [`Context`] wrapping the span.
    fn create_target_span(
        &self,
        label: &str,
        configured: &ConfiguredTarget,
        parent: &Context,
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

        let mut builder = self
            .tracer
            .span_builder(format!("target {label}"))
            .with_kind(SpanKind::Internal)
            .with_attributes(attrs);

        if let Some(nanos) = configured.event_time_nanos {
            builder = builder.with_start_time(nanos_to_system_time(nanos));
        }

        let span = self.tracer.build_with_context(builder, parent);
        Context::new().with_span(span)
    }

    /// Ensure a target span exists in `target_contexts`, creating it
    /// lazily (under root) if we have `ConfiguredTarget` metadata, or a
    /// synthetic target from the action label when no TargetConfigured was seen.
    fn ensure_target_span(&mut self, label: &str) {
        if self.target_contexts.contains_key(label) {
            return;
        }
        let Some(root) = &self.root_context else {
            return;
        };
        if let Some(configured) = self.configured_targets.get(label) {
            let cx = self.create_target_span(label, configured, root);
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
    /// resolve output files, and end the span.
    ///
    /// The span is created under **root** at this point because the target
    /// actually completed (i.e. was not skipped).  If an action already
    /// triggered lazy creation via [`ensure_target_span`], that span is reused.
    ///
    /// If all referenced `NamedSet` IDs are already in the cache the span is
    /// ended immediately.  Otherwise completion is deferred — the span stays
    /// open and is ended later when the missing `NamedSet` events arrive (see
    /// [`on_named_set`]) or at build end (see [`finish`]).
    pub fn on_target_completed(
        &mut self,
        label: &str,
        success: bool,
        file_set_ids: &[String],
        tags: &[String],
        event_time_nanos: Option<i64>,
    ) {
        // Get or create the span under root.
        let cx = if let Some(cx) = self.target_contexts.remove(label) {
            // Span already exists (lazily created by an action).
            cx
        } else if let Some(configured) = self.configured_targets.remove(label) {
            // First time — create under root.
            if let Some(root) = &self.root_context {
                let cx = self.create_target_span(label, &configured, root);
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

        // Merge tags from TargetCompleted (may overlap with Configured).
        if !tags.is_empty() {
            cx.span()
                .set_attribute(KeyValue::new(BAZEL_TARGET_TAGS, tags.join(", ")));
        }

        // Check whether all referenced file sets (transitively) are cached.
        let all_resolved = file_set_ids.is_empty()
            || Self::all_sets_resolved(&self.named_set_cache, file_set_ids);

        if all_resolved {
            // Resolve immediately.
            Self::apply_file_sets_and_end(
                &self.named_set_cache,
                &cx,
                label,
                success,
                file_set_ids,
                event_time_nanos,
            );
        } else {
            // Defer — keep the span open until the missing sets arrive.
            debug!("Deferring target completion for {label} (waiting for named sets)");
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

    /// Resolve file sets from cache and end the target span.
    ///
    /// File resolution is **recursive**: each NamedSet can reference child
    /// NamedSets, so we traverse the tree to collect every leaf file.
    fn apply_file_sets_and_end(
        cache: &HashMap<String, NamedSetEntry>,
        cx: &Context,
        label: &str,
        success: bool,
        file_set_ids: &[String],
        end_time_nanos: Option<i64>,
    ) {
        let all_files = Self::resolve_files(cache, file_set_ids);

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
    /// After caching, any pending target completions whose **full**
    /// transitive NamedSet tree is now cached are finalized.
    pub fn on_named_set(&mut self, set_id: &str, files: Vec<String>, child_set_ids: &[String]) {
        self.named_set_cache.insert(
            set_id.to_string(),
            NamedSetEntry {
                files,
                child_set_ids: child_set_ids.to_vec(),
            },
        );

        // Check if any pending target completions can now be resolved.
        let ready_labels: Vec<String> = self
            .pending_completions
            .iter()
            .filter(|(_, pending)| {
                Self::all_sets_resolved(&self.named_set_cache, &pending.file_set_ids)
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
        // Lazily create the target span if it hasn't been created yet.
        if let Some(l) = label {
            self.ensure_target_span(l);
        }

        // Parent: target span → pending completion → root.
        let parent = label
            .and_then(|l| {
                self.target_contexts
                    .get(l)
                    .or_else(|| self.pending_completions.get(l).map(|p| &p.cx))
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

        // Use action-level start_time if available (full mode).
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

        // Use action-level end_time if available (full mode).
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
            if let Some(action_data) = s.get("actionData") {
                if let Ok(s) = serde_json::to_string(action_data) {
                    cx.span().set_attribute(KeyValue::new(BAZEL_METRICS_ACTION_DATA, s));
                }
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
    UNIX_EPOCH + Duration::from_nanos(nanos as u64)
}
