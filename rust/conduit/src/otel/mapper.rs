// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

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
//! └── skipped targets (parent span grouping all skipped/aborted targets)
//!     └── target {label} (aborted by Bazel)
//!
//! Correlated OTel log records:
//!   - bazel.progress (one record per BEP `Progress` event, streamed as the
//!     build runs and correlated with the root span via trace_id/span_id;
//!     each record's stderr/stdout body is ANSI-stripped and capped at 1 MB).
//!     Without a `LoggerProvider`, all progress text accumulates in memory
//!     and is attached to the root span as a single `build.log` event in
//!     `finish`.
//!
//! Trace profiles (see [`crate::state::TracePolicy`]):
//!   - full: every action / spawn / target gets a span.
//!   - lightweight: only spans that carry signal survive -- errors, cacheable
//!     cache misses, and operations at or above the duration threshold. This
//!     is enforced at every choke point (BEP actions in the router, matched
//!     spawns in [`Self::apply_spawn_to_action`], orphan synthesis, and empty
//!     targets), so the compact exec log can't resurrect the dropped tree.
//!     Aggregate `BuildMetrics` stay on the root span regardless.
//! ```

use crate::otel::span::{Context, KeyValue, Logger, Span, SpanContext, SpanKind, Status, Tracer};
use std::borrow::Cow;
use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::path::PathBuf;
#[cfg(test)]
use std::time::{Duration, UNIX_EPOCH};

use spawn_proto::tools::protos::SpawnExec;
use tracing::{debug, info, warn};

use crate::exec_log::tailer::TailerHandle;

use super::attributes::*;
use super::redact::Redactor;
mod action_attrs;
mod actions;
mod test_results;
mod finalization;
mod lifecycle;
mod metrics;
mod progress;
mod test_attrs;
mod targets;
mod utils;

use self::action_attrs::{action_status, build_action_attrs, build_action_span_name};
use self::test_attrs::{build_test_attrs, build_test_span_name, test_status};
use self::utils::{
    append_progress_capped, detect_workspace_name, nanos_to_system_time,
    spawn_time_range, strip_ansi, wall_clock_now_nanos, COMMAND_LINE_CAP_BYTES,
    PROGRESS_CAP_BYTES,
};
#[cfg(test)]
use self::utils::tail_byte_offset;

pub use self::utils::{clamp_time_range, truncate_to_byte_limit};

/// Arguments for [`OtelMapper::on_action_completed`]. Owned, because the
/// lightweight profile holds successful actions until [`OtelMapper::finish`].
#[derive(Clone, Debug, Default)]
pub struct ActionCompletedEvent {
    pub label: Option<String>,
    pub mnemonic: Option<String>,
    pub success: bool,
    pub exit_code: Option<i32>,
    pub exit_code_name: Option<String>,
    pub primary_output: Option<String>,
    pub configuration: Option<String>,
    pub command_line: Vec<String>,
    pub stdout_path: Option<String>,
    pub stderr_path: Option<String>,
    pub start_time_nanos: Option<i64>,
    pub end_time_nanos: Option<i64>,
    pub cached: Option<bool>,
    pub runner: Option<String>,
}

impl ActionCompletedEvent {
    fn key(&self) -> ActionSpanKey {
        ActionSpanKey::new(
            self.label.as_deref(),
            self.mnemonic.as_deref(),
            self.primary_output.as_deref(),
        )
    }
}

/// A successful action competing for the lightweight span budget, ordered by
/// duration (untimed actions rank lowest) and then arrival.
struct BudgetedAction {
    duration_nanos: i64,
    seq: u64,
    event: ActionCompletedEvent,
}

impl BudgetedAction {
    fn rank(&self) -> (i64, u64) {
        (self.duration_nanos, self.seq)
    }
}

impl PartialEq for BudgetedAction {
    fn eq(&self, other: &Self) -> bool {
        self.rank() == other.rank()
    }
}

impl Eq for BudgetedAction {}

impl PartialOrd for BudgetedAction {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for BudgetedAction {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.rank().cmp(&other.rank())
    }
}

/// A `TargetCompleted` held until [`OtelMapper::finish`], so late tests and
/// budgeted actions still find the target open.
struct DeferredTarget {
    label: String,
    success: bool,
    tags: Vec<String>,
    event_time_nanos: Option<i64>,
}

/// Arguments for [`OtelMapper::on_test_result`].
pub struct TestResultEvent<'a> {
    pub label: &'a str,
    pub status: Option<&'a str>,
    pub attempt: Option<i32>,
    pub run: Option<i32>,
    pub shard: Option<i32>,
    pub cached: Option<bool>,
    pub strategy: Option<&'a str>,
    pub start_time_nanos: Option<i64>,
    pub duration_nanos: Option<i64>,
    pub hostname: Option<&'a str>,
    pub cached_remotely: Option<bool>,
}

/// Key for caching action span context for exec log enrichment.
/// Matches SpawnExec entries by target_label, mnemonic, and primary output.
#[derive(Clone, Debug, Hash, Eq, PartialEq)]
pub struct ActionSpanKey {
    pub target_label: String,
    pub mnemonic: String,
    pub primary_output: String,
}

/// Cached action span data for exec log enrichment.
/// Carries the SpanContext plus timing bounds so child spawn spans can be
/// clamped to the parent action's time range (required by Datadog's flame graph).
#[derive(Clone)]
pub struct ActionSpanInfo {
    pub span_context: SpanContext,
    pub start_nanos: Option<i64>,
    pub end_nanos: Option<i64>,
}

/// Live compact-exec-log tailer state.
///
/// Owned by [`OtelMapper`] while a compact log is being tailed. Dropping it
/// implicitly signals the tailer to shut down (via the `mpsc::Sender` drop),
/// but `finish()` does an orderly shutdown with a 2 s drain window first so
/// the post-`close()` final chunk + frame terminator make it through.
pub(crate) struct ExecLogState {
    /// Filesystem path of the compact log, retained for diagnostic logging
    /// in case the tailer aborts mid-build and we need to point a human at
    /// the file.
    #[allow(dead_code)]
    pub(crate) path: PathBuf,
    /// Handle to the blocking-thread tailer task plus its mpsc receiver.
    pub(crate) handle: TailerHandle,
}

/// Action span kept alive (`span: Some`) when the compact exec log is enabled
/// so that arriving [`SpawnExec`] entries can backfill spawn-derived attrs
/// onto it before it ends. When the compact log is disabled, the span is
/// ended in `on_action_completed` and `span` is `None`; the cache entry then
/// only carries the `SpanContext` + bounds that the (soon-to-be-deleted)
/// post-build [`crate::exec_log::enrich_trace`] still consumes.
///
/// End triggers when `span` is `Some`:
///   1. `TargetCompleted` for `target_label` — deterministic boundary, Bazel
///      guarantees no more `ActionExecuted` events for that target after.
///   2. `OtelMapper::finish` fallback — drains the post-`close()` tail of the
///      compact log first, then force-ends everything still open.
pub struct OpenAction {
    /// SpanContext + timing bounds; cloned cheaply for child-span parent
    /// linking. Stays valid even after `span` has been ended.
    pub info: ActionSpanInfo,
    /// Live span handle, `None` once ended. The OTel SDK seals attribute
    /// mutations on end so we drop the handle to make that misuse obvious
    /// (subsequent `set_attribute` calls would silently no-op).
    pub span: Option<Span>,
    /// Parent target label, used by `end_open_actions_for_target` to flush
    /// every still-open action belonging to a freshly-completed target.
    pub target_label: String,
    /// Count of [`SpawnExec`] entries seen for this action so far. The first
    /// spawn backfills curated attrs onto the action span; subsequent ones
    /// (retries, dynamic-exec races) only emit child spawn spans and bump
    /// this counter. Surfaced as `bazel.action.spawn.count` at end time.
    pub spawn_count: u32,
}

/// Extract (start_nanos, end_nanos) from a `SpawnExec` via its
/// `SpawnMetrics.start_time` + `total_time`. Returns `None` when timing
/// data is absent.
impl ActionSpanKey {
    pub fn new(
        target_label: Option<&str>,
        mnemonic: Option<&str>,
        primary_output: Option<&str>,
    ) -> Self {
        Self {
            target_label: target_label.unwrap_or("").to_string(),
            mnemonic: mnemonic.unwrap_or("").to_string(),
            primary_output: primary_output.unwrap_or("").to_string(),
        }
    }
}

/// Metadata captured at `TargetConfigured` time.
///
/// Span creation is **deferred** until `TargetCompleted` so the mapper can
/// decide whether the target belongs under the root span or the "skipped"
/// parent span.  If an action arrives before the target completes, the
/// target span is lazily created under root (see
/// [`OtelMapper::ensure_target_span`]).
#[derive(Clone)]
struct ConfiguredTarget {
    kind: Option<String>,
    tags: Vec<String>,
    event_time_nanos: Option<i64>,
    test_size: Option<String>,
}


/// Maps BEP events to OpenTelemetry spans.
pub struct OtelMapper {
    tracer: Tracer,

    /// Root span context (`bazel.invocation`).
    root_context: Option<Context>,

    /// Targets whose `TargetConfigured` arrived but whose span has **not**
    /// yet been created.  Span creation is deferred until `TargetCompleted`
    /// so we can choose the correct parent (root vs. "skipped").
    configured_targets: HashMap<String, ConfiguredTarget>,

    /// Open target spans (created either lazily by an action or at
    /// `TargetCompleted` time), keyed by target label.
    target_contexts: HashMap<String, Context>,

    /// Labels whose currently-open target context is synthetic
    /// (action/test-driven, no observed TargetConfigured lifecycle).
    synthetic_target_labels: HashSet<String>,

    /// Latest observed child end for each open target.
    target_child_end_nanos: HashMap<String, i64>,

    /// Start timestamp of each target span (real or synthetic), keyed by
    /// normalized label. Used to keep child action/spawn spans from starting
    /// before the parent target they hang under (Datadog containment).
    target_start_nanos: HashMap<String, i64>,

    /// Targets whose lifecycle has reached a terminal event
    /// (`TargetCompleted` or skipped/aborted). Used to avoid recreating
    /// long-lived target contexts from late action/test events.
    closed_targets: HashSet<String>,

    /// Single `skipped` parent span grouping all skipped/aborted targets.
    /// Created lazily on first skipped target, ended in [`finish`].
    skipped_context: Option<Context>,

    /// Cached configurations: config hash → mnemonic (e.g. `k8-fastbuild`).
    configurations: HashMap<String, String>,

    /// Cached command name from BuildStarted (for root span name).
    cached_command: Option<String>,

    /// Cached build patterns (for root span name).
    cached_patterns: Vec<String>,

    /// Tracks whether any action for a target was NOT a cache hit.
    target_has_non_cached_action: HashMap<String, bool>,

    /// Fallback buffers for the no-logger path: when no `LoggerProvider` is
    /// configured, progress text accumulates here and is attached to the root
    /// span as a single `build.log` event in [`finish`]. Each stream is capped
    /// at 1 MB (see [`PROGRESS_CAP_BYTES`]). Unused on the streaming path.
    progress_stderr: String,
    progress_stdout: String,

    /// Optional OTel logger. When present, every BEP `Progress` event is
    /// emitted as its own correlated log record (streaming) rather than
    /// buffered until the build ends.
    logger: Option<Logger>,

    /// Cached exit code from BuildFinished (root span ends in [`finish`]).
    exit_code: Option<i32>,

    /// Conduit stopped before BuildFinished arrived; root ends as an error.
    shut_down: bool,

    /// Cached finish timestamp (nanos since epoch) from BuildFinished.
    finish_time_nanos: Option<i64>,

    /// Root span start (nanos since epoch) used when creating the span.
    /// Used with BuildMetrics wallTimeInMs to derive correct end for cached builds.
    root_span_start_nanos: Option<i64>,

    /// Invocation start as reported by Bazel (not clamped like the root span).
    build_start_nanos: Option<i64>,

    /// Root span end derived from start + wallTimeInMs when BuildMetrics provides it.
    /// Takes precedence over finish_time_nanos so duration matches Bazel's wall time.
    root_span_end_from_wall_nanos: Option<i64>,


    /// True once a compact-log tailer has been wired in (whether via
    /// [`Self::on_exec_log_detected`] in prod or a test helper). Drives
    /// the deferred-end gate in [`Self::on_action_completed`]: when set,
    /// action spans stay open until `TargetCompleted` / `finish()` so
    /// arriving spawns can backfill attrs onto them; when clear, action
    /// spans end immediately as in the pre-streaming codepath.
    compact_streaming_active: bool,

    /// Live compact-exec-log tailer (spawned on `on_exec_log_detected`).
    /// `None` if the compact log isn't enabled — in that case action spans
    /// end immediately in `on_action_completed` and no spawn child spans
    /// or attribute backfill happens.
    exec_log_state: Option<ExecLogState>,

    /// Workspace directory from BuildStarted (for resolving relative exec log path).
    workspace_directory: Option<PathBuf>,

    /// Action spans, keyed by (target_label, mnemonic, primary_output). When
    /// the compact exec log is enabled, entries hold a live span that stays
    /// open until `TargetCompleted` or `finish()` forces it to end, allowing
    /// arriving `SpawnExec` entries to backfill spawn-derived attrs onto it.
    /// When the compact log is disabled, the span is ended immediately in
    /// `on_action_completed` and the cache entry is informational only.
    action_span_cache: HashMap<ActionSpanKey, OpenAction>,

    /// SpawnExecs that arrived before their parent action reached
    /// [`Self::action_span_cache`], indexed by their deterministic
    /// `(target_label, mnemonic, primary_output)` key.
    ///
    /// zstd-jni flushes ~128 KiB chunks asynchronously to BEP, so a chunk can
    /// deliver spawns whose `ActionExecuted` is still in flight on the stream.
    /// `on_action_completed` flushes only its own key; remaining entries are
    /// synthesised at [`Self::finish`].
    pending_spawns: HashMap<ActionSpanKey, Vec<SpawnExec>>,
    /// Other candidate keys of multi-output pending spawns -> the key they are
    /// filed under, so an action whose primary output isn't the first one
    /// still claims them.
    pending_spawn_aliases: HashMap<ActionSpanKey, ActionSpanKey>,

    /// Count of SpawnExec entries received from the tailer. Surfaced as
    /// `bazel.exec_log.spawns_received` on the `tailer_finished` root-span
    /// event so a trace consumer can see whether the tailer produced data
    /// without grep'ing conduit's logs.
    compact_spawns_received: u64,

    /// Trace emission policy, consulted at every span choke point (actions,
    /// spawns in [`Self::apply_spawn_to_action`], orphan synthesis, and target
    /// completion). `Full` until the router sets the CLI policy.
    trace_policy: crate::state::TracePolicy,

    /// Lightweight only: successful actions, min-heap by duration, capped at
    /// the policy's `max_action_spans`. Emitted at [`Self::finish`].
    action_budget: BinaryHeap<Reverse<BudgetedAction>>,
    action_seq: u64,
    /// Keys of actions the budget dropped, so their exec-log spawns aren't
    /// resurrected as orphans.
    budget_dropped: HashSet<ActionSpanKey>,
    /// Targets completed at [`Self::finish`]. Keyed by label: completions in
    /// several configurations share one span.
    deferred_targets: HashMap<String, DeferredTarget>,

    /// Spans dropped by the lightweight profile, surfaced on the root span at
    /// [`Self::finish`] so operators can see how much volume was cut.
    suppressed_actions: u64,
    suppressed_spawns: u64,
    suppressed_targets: u64,
    fetch_count: u64,
    fetch_failed: u64,

    /// Scrubs sensitive values out of command-line attributes before they
    /// hit the exporter. See [`crate::otel::redact`].
    redactor: Redactor,

    /// Per-message cap applied when parsing the execution log. Initialised
    /// from [`crate::exec_log::DEFAULT_EXECLOG_MAX_MESSAGE_BYTES`]; the CLI
    /// can override via `--exec-log-max-message-mib`.
    exec_log_max_message_bytes: usize,

    /// Total decompressed-bytes cap applied to the compact execution log.
    /// Initialised from
    /// [`crate::exec_log::DEFAULT_EXECLOG_MAX_DECOMPRESSED_BYTES`]; the CLI
    /// can override via `--exec-log-max-decompressed-mib`. Defends against
    /// a zstd zip-bomb whose payload count would OOM the in-memory
    /// `Vec<SpawnExec>` even after the per-message cap is enforced.
    exec_log_max_decompressed_bytes: usize,
}

impl OtelMapper {
    pub fn new(
        tracer: Tracer,
        logger: Option<Logger>,
    ) -> Self {
        Self {
            tracer,
            root_context: None,
            configured_targets: HashMap::new(),
            target_contexts: HashMap::new(),
            synthetic_target_labels: HashSet::new(),
            target_child_end_nanos: HashMap::new(),
            target_start_nanos: HashMap::new(),
            closed_targets: HashSet::new(),
            skipped_context: None,
            configurations: HashMap::new(),
            cached_command: None,
            cached_patterns: Vec::new(),
            target_has_non_cached_action: HashMap::new(),
            progress_stderr: String::new(),
            progress_stdout: String::new(),
            logger,
            exit_code: None,
            shut_down: false,
            finish_time_nanos: None,
            root_span_start_nanos: None,
            build_start_nanos: None,
            root_span_end_from_wall_nanos: None,
            compact_streaming_active: false,
            exec_log_state: None,
            action_span_cache: HashMap::new(),
            pending_spawns: HashMap::new(),
            pending_spawn_aliases: HashMap::new(),
            compact_spawns_received: 0,
            workspace_directory: None,
            trace_policy: crate::state::TracePolicy::new(
                crate::state::TraceProfile::Full,
                crate::state::DEFAULT_TRACE_MIN_DURATION_MS,
                crate::state::DEFAULT_MAX_ACTION_SPANS,
            ),
            action_budget: BinaryHeap::new(),
            action_seq: 0,
            budget_dropped: HashSet::new(),
            deferred_targets: HashMap::new(),
            suppressed_actions: 0,
            suppressed_spawns: 0,
            suppressed_targets: 0,
            fetch_count: 0,
            fetch_failed: 0,
            redactor: Redactor::default_enabled(),
            exec_log_max_message_bytes: crate::exec_log::DEFAULT_EXECLOG_MAX_MESSAGE_BYTES,
            exec_log_max_decompressed_bytes:
                crate::exec_log::DEFAULT_EXECLOG_MAX_DECOMPRESSED_BYTES,
        }
    }

    /// Override the default redactor (e.g. to disable scrubbing or to
    /// supply a custom name pattern list from the CLI).
    pub fn with_redactor(mut self, redactor: Redactor) -> Self {
        self.redactor = redactor;
        self
    }

    /// Override the per-message cap used when parsing the execution log.
    /// `0` is rejected silently (kept at default) -- the parser uses
    /// `Vec::resize` against this value, so an explicit floor avoids
    /// surprising allocations for misconfigured callers.
    pub fn with_exec_log_max_message_bytes(mut self, bytes: usize) -> Self {
        if bytes > 0 {
            self.exec_log_max_message_bytes = bytes;
        }
        self
    }

    /// Override the decompressed-bytes cap used when parsing the compact
    /// execution log. `0` is rejected silently (kept at default) for the
    /// same reason as [`Self::with_exec_log_max_message_bytes`]: a
    /// zero-budget reader would error immediately on the first byte.
    pub fn with_exec_log_max_decompressed_bytes(mut self, bytes: usize) -> Self {
        if bytes > 0 {
            self.exec_log_max_decompressed_bytes = bytes;
        }
        self
    }

    // =====================================================================
    // Private helpers
    // =====================================================================

    /// Clamp a `(start, end)` pair to the current invocation's window.
    ///
    /// Bazel preserves the original execution timestamps on cached
    /// `ActionExecuted` events and propagates them up to per-target metadata,
    /// so a build that replays a cache from weeks ago can produce spans whose
    /// `start_time` predates the invocation by days. Applying this at every
    /// site that hands BEP-supplied nanos to `with_start_time` /
    /// `end_with_timestamp` keeps every span within the root's window.
    fn clamp_to_invocation(
        &self,
        start: Option<i64>,
        end: Option<i64>,
    ) -> (Option<i64>, Option<i64>) {
        clamp_time_range(
            start,
            end,
            self.root_span_start_nanos,
            // Until the build finishes we don't know the root end. Cache hits
            // can carry timestamps outside this invocation's window; fall back
            // to wall-clock now as an upper bound since nothing can end in the
            // future.
            self.root_span_end_from_wall_nanos
                .or(self.finish_time_nanos)
                .or_else(wall_clock_now_nanos),
        )
    }

    /// Remember a target span's start so late children can be kept inside it.
    /// Overwrites: the value tracks the currently-open span for `label` (a
    /// synthetic span may be recreated after the original target closed).
    pub(super) fn record_target_start(&mut self, label: &str, start_nanos: Option<i64>) {
        if let Some(nanos) = start_nanos {
            self.target_start_nanos.insert(label.to_string(), nanos);
        }
    }

    /// Start of the target span a child hangs under, if known.
    pub(super) fn target_start_for(&self, label: &str) -> Option<i64> {
        self.target_start_nanos.get(label).copied()
    }

    /// End of the (synthetic) target span a child hangs under, if known.
    pub(super) fn target_end_for(&self, label: &str) -> Option<i64> {
        if !self.synthetic_target_labels.contains(label) {
            return None;
        }
        self.target_child_end_nanos.get(label).copied()
    }

    /// Set an attribute on the root span (no-op if root span not yet created).
    fn set_root_attr(&self, kv: KeyValue) {
        if let Some(cx) = &self.root_context {
            cx.span().set_attribute(kv);
        }
    }

    /// Add an event to the root span (no-op if root span not yet created).
    fn add_root_event(&self, name: &'static str, attrs: Vec<KeyValue>) {
        if let Some(cx) = &self.root_context {
            cx.span().add_event(name, attrs);
        }
    }

    /// Build a child span with the given attributes; does **not** end it.
    /// Callers either end it immediately (today's default for everything
    /// except deferred action spans) or hold the handle open and end it later.
    fn build_child_span(
        &self,
        parent: &Context,
        name: String,
        kind: SpanKind,
        attrs: Vec<KeyValue>,
        start_time: Option<i64>,
    ) -> Span {
        let mut builder = self
            .tracer
            .span_builder(name)
            .with_kind(kind)
            .with_attributes(attrs);
        if let Some(nanos) = start_time {
            builder = builder.with_start_time(nanos_to_system_time(nanos));
        }
        self.tracer.build_with_context(builder, parent)
    }

    /// Build a child span, set its status, and end it. Returns the finished
    /// span's `SpanContext` for cache/linking purposes.
    fn build_and_end_child_span(
        &self,
        parent: &Context,
        name: String,
        kind: SpanKind,
        attrs: Vec<KeyValue>,
        status: Status,
        start_time: Option<i64>,
        end_time: Option<i64>,
    ) -> SpanContext {
        let mut span = self.build_child_span(parent, name, kind, attrs, start_time);
        let span_cx = span.span_context().clone();
        span.set_status(status);
        if let Some(nanos) = end_time {
            span.end_with_timestamp(nanos_to_system_time(nanos));
        } else {
            span.end();
        }
        span_cx
    }

    /// Lazily create a group span (e.g. "skipped targets") under the root
    /// span. Returns the existing context if already created.
    fn ensure_group_span(
        slot: &mut Option<Context>,
        root_context: &Option<Context>,
        tracer: &Tracer,
        name: &'static str,
        start_nanos: Option<i64>,
    ) {
        if slot.is_some() {
            return;
        }
        if let Some(root_cx) = root_context {
            let mut builder = tracer
                .span_builder(name)
                .with_kind(SpanKind::Internal);
            // Anchor group spans at the invocation start. They are created
            // lazily on their first child, which can arrive late (e.g. the
            // first skipped target on an interrupted build); without an
            // explicit start the span would begin at "now" and a child whose
            // event time predates creation would fall outside the parent.
            if let Some(nanos) = start_nanos {
                builder = builder.with_start_time(nanos_to_system_time(nanos));
            }
            let span = tracer.build_with_context(builder, root_cx);
            *slot = Some(Context::new().with_span(span));
            debug!("Created '{name}' parent span");
        }
    }

    // =====================================================================
    // Progress events
    // =====================================================================

    /// Progress with stderr/stdout. With a logger configured, each event is
    /// emitted immediately as a correlated `bazel.progress` log record
    /// (streaming, capped at [`PROGRESS_CAP_BYTES`] per record). Without a
    /// logger, content accumulates in [`progress_stderr`]/[`progress_stdout`]
    /// and is attached as a single span event in [`finish`].
    pub fn on_progress(&mut self, stderr: Option<&str>, stdout: Option<&str>) {
        progress::handle_progress(self, stderr, stdout);
    }

    // =====================================================================
    // Build metadata
    // =====================================================================

    /// BuildMetadata → add as attributes on root span.
    pub fn on_build_metadata(&mut self, metadata: &crate::build_event_stream::BuildMetadata) {
        for (key, value) in &metadata.metadata {
            self.set_root_attr(KeyValue::new(format!("bazel.metadata.{key}"), value.clone()));
        }
    }

    // =====================================================================
    // Build finish / metrics
    // =====================================================================

    /// BuildFinished → record exit code and finish time (root span is ended in [`finish`]).
    pub fn on_build_finished(
        &mut self,
        exit_code: Option<i32>,
        finish_time_nanos: Option<i64>,
        exit_code_name: Option<&str>,
    ) {
        self.exit_code = exit_code;
        self.finish_time_nanos = finish_time_nanos;

        if let Some(code) = exit_code {
            self.set_root_attr(KeyValue::new(BAZEL_EXIT_CODE, code as i64));
        }
        if let Some(name) = exit_code_name {
            self.set_root_attr(KeyValue::new(BAZEL_EXIT_CODE_NAME, name.to_string()));
        }
    }

    /// Conduit is shutting down mid-build: mark the root so [`finish`] ends it
    /// as an error instead of a silently truncated trace.
    pub fn on_shutdown(&mut self) {
        if self.root_context.is_some() && self.exit_code.is_none() {
            self.shut_down = true;
            self.add_root_event("conduit.shutdown", Vec::new());
        }
    }

    /// BuildMetrics → add metrics attributes to root span.
    pub fn on_build_metrics(&mut self, metrics: &crate::build_event_stream::BuildMetrics) {
        metrics::apply_build_metrics(self, metrics);
    }

    // =====================================================================
    // State management
    // =====================================================================

    /// Reset all mapper state for a new build invocation.
    pub fn reset(&mut self) {
        if let Some(cx) = self.root_context.take() {
            cx.span().end();
        }
        if let Some(cx) = self.skipped_context.take() {
            cx.span().end();
        }
        for (_, cx) in self.target_contexts.drain() {
            cx.span().end();
        }
        self.configured_targets.clear();
        self.synthetic_target_labels.clear();
        self.target_child_end_nanos.clear();
        self.target_start_nanos.clear();
        self.closed_targets.clear();
        self.configurations.clear();
        self.cached_command = None;
        self.cached_patterns.clear();
        self.target_has_non_cached_action.clear();
        self.progress_stderr.clear();
        self.progress_stdout.clear();
        self.exit_code = None;
        self.shut_down = false;
        self.finish_time_nanos = None;
        self.root_span_start_nanos = None;
        self.build_start_nanos = None;
        self.root_span_end_from_wall_nanos = None;
        if let Some(state) = self.exec_log_state.take() {
            state.handle.shutdown();
        }
        self.compact_streaming_active = false;
        self.action_span_cache.clear();
        self.pending_spawns.clear();
        self.pending_spawn_aliases.clear();
        self.compact_spawns_received = 0;
        self.workspace_directory = None;
        self.action_budget.clear();
        self.action_seq = 0;
        self.budget_dropped.clear();
        self.deferred_targets.clear();
        self.suppressed_actions = 0;
        self.suppressed_spawns = 0;
        self.suppressed_targets = 0;
        self.fetch_count = 0;
        self.fetch_failed = 0;
    }

    /// Set the trace policy and mirror the profile onto the root span.
    pub fn set_trace_policy(&mut self, policy: crate::state::TracePolicy) {
        self.trace_policy = policy;
        self.set_root_attr(KeyValue::new(
            BAZEL_TRACE_PROFILE,
            policy.profile().as_str(),
        ));
    }

    // =====================================================================
    // Finalization
    // =====================================================================

    /// Signal shutdown, then drain the compact-log tailer channel with a
    /// bounded budget so the post-`close()` final chunk can still be consumed.
    ///
    /// Why a budget and not an unbounded await: the tailer is a separate
    /// blocking thread driving zstd decode. By the time `finish()` is
    /// called Bazel has already invoked `close()` on the spawn-log stream
    /// (it happens in `SpawnLogModule.afterCommand` before
    /// `BuildCompleteEvent` is published), so the final chunk + frame
    /// terminator are guaranteed to be on disk; the only question is
    /// whether the tailer has consumed and forwarded them yet. 2 s of
    /// slack is generous — typical decode of a final 128 KiB chunk +
    /// 200–600 entry burst takes <100 ms — but cheap insurance against
    /// transient slowness without holding the mapper for arbitrary time.
    fn drain_and_stop_tailer(&mut self) {
        finalization::drain_and_stop_tailer(self);
    }

    /// Build synthetic parent action spans for spawns left in
    /// [`Self::pending_spawns`] after `drain_and_stop_tailer`. These are
    /// spawns whose `ActionExecuted` never arrived on BEP — most commonly
    /// because the user passed `--nobuild_event_publish_all_actions` and
    /// the action was a non-failure. Group by
    /// `(target_label, mnemonic, primary_output)` to mirror the on-disk
    /// shape; emit child spawn spans under each synthetic parent and
    /// backfill the same curated attrs onto it.
    fn synthesise_orphan_actions(&mut self, budget: usize) {
        finalization::synthesise_orphan_actions(self, budget);
    }

    /// Finalize and end every action span still held open in
    /// [`Self::action_span_cache`]. Called from `finish()` after the tailer
    /// drain and orphan synthesis, so spawn counts reflect the complete
    /// compact-log stream.
    fn finalize_and_end_remaining_open_actions(&mut self) {
        finalization::finalize_and_end_remaining_open_actions(self);
    }

    /// End all remaining spans (call after last BEP event).
    pub fn finish(&mut self) {
        let effective_end = self.root_span_end_from_wall_nanos.or(self.finish_time_nanos);

        // Drain the compact-log tailer's post-`close()` final chunk before
        // sealing any spans. zstd-jni's 128 KiB input buffer means the last
        // batch of spawns only hits disk when Bazel runs `close()` on the
        // log stream — which happens before BuildCompleteEvent is emitted,
        // but the chunk + frame terminator may still be in flight through
        // the tailer's mpsc when we get here.
        self.drain_and_stop_tailer();

        // Budgeted and orphan actions go out before the (deferred) targets
        // complete, so they find their target open and only targets left
        // empty are suppressed.
        let kept = self.emit_budgeted_actions();

        // Anything still buffered after the drain never matched a BEP
        // ActionExecuted (e.g. `--build_event_publish_all_actions=false`).
        // Synthesise parent action spans for them.
        self.synthesise_orphan_actions(self.trace_policy.max_action_spans().saturating_sub(kept));

        let mut deferred: Vec<_> = std::mem::take(&mut self.deferred_targets).into_values().collect();
        deferred.sort_by(|a, b| (a.event_time_nanos, &a.label).cmp(&(b.event_time_nanos, &b.label)));
        for t in deferred {
            self.complete_target(&t.label, t.success, &t.tags, t.event_time_nanos);
        }

        // Finalise action-level spawn summaries only after the tailer has
        // fully drained, so late chunk arrivals still count.
        self.finalize_and_end_remaining_open_actions();

        // End any remaining target spans (lazily created but never completed).
        let labels: Vec<String> = self.target_contexts.keys().cloned().collect();
        for label in labels {
            if let Some(cx) = self.target_contexts.remove(&label) {
                cx.span().set_status(Status::Unset);
                let is_synthetic = self.synthetic_target_labels.remove(&label);
                let target_end = if is_synthetic {
                    self.target_child_end_nanos.remove(&label).or(effective_end)
                } else {
                    effective_end
                };
                if let Some(nanos) = target_end {
                    cx.span().end_with_timestamp(nanos_to_system_time(nanos));
                } else {
                    cx.span().end();
                }
                if is_synthetic {
                    debug!("Force-ended synthetic target span for {label}");
                } else {
                    warn!("Force-ended orphaned target span for {label}");
                }
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
            if let Some(nanos) = effective_end {
                cx.span().end_with_timestamp(nanos_to_system_time(nanos));
            } else {
                cx.span().end();
            }
            debug!("Ended 'skipped targets' span");
        }

        // No-logger fallback: progress was buffered, attach a single
        // `build.log` span event before closing the root. With a logger
        // configured, [`on_progress`] streams records and these buffers stay
        // empty.
        if self.logger.is_none() {
            let stderr = std::mem::take(&mut self.progress_stderr);
            let stdout = std::mem::take(&mut self.progress_stdout);
            if (!stderr.is_empty() || !stdout.is_empty()) && self.root_context.is_some() {
                let mut attrs = Vec::new();
                if !stderr.is_empty() {
                    attrs.push(KeyValue::new(BAZEL_PROGRESS_STDERR, stderr));
                }
                if !stdout.is_empty() {
                    attrs.push(KeyValue::new(BAZEL_PROGRESS_STDOUT, stdout));
                }
                if let Some(cx) = &self.root_context {
                    cx.span().add_event("build.log", attrs);
                }
            }
        }

        // Surface how much the lightweight profile trimmed (always set, even
        // when zero, so dashboards can rely on the attribute existing).
        self.set_root_attr(KeyValue::new(
            BAZEL_TRACE_SUPPRESSED_ACTIONS,
            self.suppressed_actions as i64,
        ));
        self.set_root_attr(KeyValue::new(
            BAZEL_TRACE_SUPPRESSED_SPAWNS,
            self.suppressed_spawns as i64,
        ));
        self.set_root_attr(KeyValue::new(
            BAZEL_TRACE_SUPPRESSED_TARGETS,
            self.suppressed_targets as i64,
        ));
        self.set_root_attr(KeyValue::new(BAZEL_FETCH_COUNT, self.fetch_count as i64));
        self.set_root_attr(KeyValue::new(BAZEL_FETCH_FAILED, self.fetch_failed as i64));

        // End root span with proper status + timestamp.
        if let Some(cx) = self.root_context.take() {
            let status = match self.exit_code {
                Some(0) => Status::Ok,
                Some(code) => Status::Error {
                    description: Cow::Owned(format!("exit code {code}")),
                },
                None if self.shut_down => Status::error("conduit shut down before the build finished"),
                None => Status::Unset,
            };
            cx.span().set_status(status);

            if let Some(nanos) = effective_end {
                cx.span().end_with_timestamp(nanos_to_system_time(nanos));
            } else {
                cx.span().end();
            }

            info!("Root span ended (exit_code={:?})", self.exit_code);
        }
    }
}

#[cfg(test)]
mod tests;

