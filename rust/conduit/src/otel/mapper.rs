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
//!   - build.log (accumulated progress stderr/stdout, each capped at 1 MB;
//!     emitted as a Log record correlated with the trace via trace_id/span_id —
//!     falls back to a span event when no LoggerProvider is configured)
//!
//! Action processing modes:
//!   - lightweight (default): only failed actions create spans
//!   - full (--build_event_publish_all_actions): every action gets a span
//!     with accurate start_time / end_time from the ActionExecuted event
//! ```

use std::borrow::Cow;
use std::collections::HashMap;
use std::path::PathBuf;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use opentelemetry::logs::{AnyValue, LogRecord as _, Logger as _, Severity};
use opentelemetry::trace::{Span, SpanContext, SpanKind, Status, TraceContextExt, Tracer};
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

/// Convert a bytestream URI to a display-friendly form: show the path part (e.g. after
/// the authority, such as `blobs/hash/size`) or leave as-is if no path or not bytestream.
fn bytestream_uri_to_display(uri: &str) -> Cow<'_, str> {
    const PREFIX: &str = "bytestream://";
    if !uri.starts_with(PREFIX) {
        return Cow::Borrowed(uri);
    }
    let after_prefix = &uri[PREFIX.len()..];
    if let Some(slash) = after_prefix.find('/') {
        let path = &after_prefix[slash + 1..];
        if !path.is_empty() {
            return Cow::Owned(path.to_string());
        }
    }
    Cow::Borrowed(uri)
}

/// Arguments for [`OtelMapper::on_action_completed`].
pub struct ActionCompletedEvent<'a> {
    pub label: Option<&'a str>,
    pub mnemonic: Option<&'a str>,
    pub success: bool,
    pub exit_code: Option<i32>,
    pub exit_code_name: Option<&'a str>,
    pub primary_output: Option<&'a str>,
    pub configuration: Option<&'a str>,
    pub command_line: &'a [String],
    pub stdout_path: Option<&'a str>,
    pub stderr_path: Option<&'a str>,
    pub start_time_nanos: Option<i64>,
    pub end_time_nanos: Option<i64>,
    pub cached: Option<bool>,
    pub hostname: Option<&'a str>,
    pub cached_remotely: Option<bool>,
    pub runner: Option<&'a str>,
}

impl<'a> ActionCompletedEvent<'a> {
    pub fn from_buffered(act: &'a BufferedAction) -> Self {
        Self {
            label: act.label.as_deref(),
            mnemonic: act.mnemonic.as_deref(),
            success: act.success,
            exit_code: act.exit_code,
            exit_code_name: act.exit_code_name.as_deref(),
            primary_output: act.primary_output.as_deref(),
            configuration: act.configuration.as_deref(),
            command_line: &act.command_line,
            stdout_path: act.stdout_uri.as_deref(),
            stderr_path: act.stderr_uri.as_deref(),
            start_time_nanos: act.start_nanos,
            end_time_nanos: act.end_nanos,
            cached: act.cached,
            hostname: act.hostname.as_deref(),
            cached_remotely: act.cached_remotely,
            runner: act.runner.as_deref(),
        }
    }
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

/// Clamp a (start, end) time range to fit within (bound_start, bound_end).
/// Ensures the result satisfies start <= end when both are present.
///
/// Heuristic: when the entire range predates `bound_start` (both start and end
/// are strictly before the invocation began), treat it as a cache hit replaying
/// original exec timestamps and collapse to zero duration at `bound_start`.
pub fn clamp_time_range(
    start: Option<i64>,
    end: Option<i64>,
    bound_start: Option<i64>,
    bound_end: Option<i64>,
) -> (Option<i64>, Option<i64>) {
    if let (Some(s), Some(e), Some(bs)) = (start, end, bound_start) {
        if s < bs && e <= bs {
            return (Some(bs), Some(bs));
        }
    }
    let clamped_start = match (start, bound_start) {
        (Some(s), Some(bs)) => Some(s.max(bs)),
        (s, _) => s,
    };
    let clamped_end = match (end, bound_end) {
        (Some(e), Some(be)) => Some(e.min(be)),
        (e, _) => e,
    };
    match (clamped_start, clamped_end) {
        (Some(s), Some(e)) if s > e => (Some(e), Some(e)),
        _ => (clamped_start, clamped_end),
    }
}

/// Normalize label for matching: strip leading `@` so BEP `@@repo//:t` and exec log `@repo//:t` match.
fn normalize_label(s: &str) -> &str {
    s.trim_start_matches('@')
}

/// Maximum size in bytes for each of progress_stderr and progress_stdout.
/// Each stream is capped at 1 MB; when appending would exceed the cap, the
/// existing buffer is truncated to keep the tail (recent output) and new
/// content is appended. Enforced in [`OtelMapper::on_progress`].
const PROGRESS_CAP_BYTES: usize = 1024 * 1024; // 1 MB
const COMMAND_LINE_CAP_BYTES: usize = 4096; // 4 KB

/// Byte length of the suffix added when we truncate from the front.
const PROGRESS_TRUNCATION_SUFFIX: &str = "\n...(truncated)\n";

/// Return the byte offset into `s` such that `s[offset..]` is the last at most
/// `max_tail_bytes` bytes of `s` at a valid UTF-8 character boundary.
fn tail_byte_offset(s: &str, max_tail_bytes: usize) -> usize {
    if s.len() <= max_tail_bytes {
        return 0;
    }
    let start = s.len() - max_tail_bytes;
    let mut i = start;
    while i < s.len() && !s.is_char_boundary(i) {
        i += 1;
    }
    i.min(s.len())
}

/// Truncate `s` to at most `max_bytes` bytes, stepping back to the nearest
/// UTF-8 boundary and appending `suffix`. Returns the original string when
/// it already fits.
pub fn truncate_to_byte_limit(s: &str, max_bytes: usize, suffix: &str) -> String {
    if s.len() <= max_bytes {
        return s.to_string();
    }
    let mut cut = max_bytes;
    while cut > 0 && !s.is_char_boundary(cut) {
        cut -= 1;
    }
    format!("{}{}", &s[..cut], suffix)
}

/// Append `new_content` to `buf` while keeping total size ≤ PROGRESS_CAP_BYTES.
/// When appending would exceed the cap, truncates the existing buffer to keep
/// the tail (recent output), adds a truncation marker, then appends the new content.
fn append_progress_capped(buf: &mut String, new_content: &str) {
    let suffix_len = PROGRESS_TRUNCATION_SUFFIX.len();
    let max_total = PROGRESS_CAP_BYTES;

    if buf.len() + new_content.len() <= max_total {
        buf.push_str(new_content);
        return;
    }

    // Need to drop from the front: keep last (max_total - new_content.len() - suffix_len) bytes of buf.
    let max_old_tail = max_total.saturating_sub(new_content.len()).saturating_sub(suffix_len);
    if max_old_tail > 0 {
        let offset = tail_byte_offset(buf, max_old_tail);
        let tail = buf[offset..].to_string();
        buf.clear();
        buf.push_str(&tail);
        buf.push_str(PROGRESS_TRUNCATION_SUFFIX);
    } else {
        buf.clear();
        // New content alone exceeds cap: keep only the tail of new_content.
        let keep_new = max_total.saturating_sub(suffix_len);
        if keep_new > 0 {
            let offset = tail_byte_offset(new_content, keep_new.min(new_content.len()));
            buf.push_str(PROGRESS_TRUNCATION_SUFFIX);
            buf.push_str(&new_content[offset..]);
        }
        return;
    }

    // Append new_content; if we're still over cap, trim from the end of new_content.
    let remaining = max_total.saturating_sub(buf.len());
    if new_content.len() <= remaining {
        buf.push_str(new_content);
    } else {
        let end = new_content.len() - remaining;
        let mut trim_start = end;
        while trim_start < new_content.len() && !new_content.is_char_boundary(trim_start) {
            trim_start += 1;
        }
        buf.push_str(&new_content[trim_start..]);
        buf.push_str("\n...(truncated)");
    }
}

impl ActionSpanKey {
    pub fn new(
        target_label: Option<&str>,
        mnemonic: Option<&str>,
        primary_output: Option<&str>,
    ) -> Self {
        Self {
            target_label: normalize_label(target_label.unwrap_or("")).to_string(),
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
struct ConfiguredTarget {
    kind: Option<String>,
    tags: Vec<String>,
    event_time_nanos: Option<i64>,
    test_size: Option<String>,
}

/// Target span kept open until output file sets are resolved so we can
/// set output attributes on it before ending.
struct PendingOutputResolution {
    parent_cx: Context,
    file_set_ids: Vec<String>,
    event_time_nanos: Option<i64>,
}

/// Test result data buffered when root span is not yet created (replay in on_build_started).
struct BufferedTestResult {
    label: String,
    status: Option<String>,
    attempt: Option<i32>,
    run: Option<i32>,
    shard: Option<i32>,
    cached: Option<bool>,
    strategy: Option<String>,
    start_time_nanos: Option<i64>,
    duration_nanos: Option<i64>,
    hostname: Option<String>,
    cached_remotely: Option<bool>,
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

    /// Single `external deps` parent span grouping targets from external repos.
    /// Created lazily on first external target, ended in [`finish`].
    external_deps_context: Option<Context>,

    /// Cached configurations: config hash → mnemonic (e.g. `k8-fastbuild`).
    configurations: HashMap<String, String>,

    /// Cached command name from BuildStarted (for root span name).
    cached_command: Option<String>,

    /// Cached build patterns (for root span name).
    cached_patterns: Vec<String>,

    /// Tracks how many child action spans each target has (for cached detection).
    target_action_counts: HashMap<String, u32>,
    /// Tracks whether any action for a target was NOT a cache hit.
    target_has_non_cached_action: HashMap<String, bool>,

    /// Accumulated stderr / stdout from all progress messages.
    /// Each is capped at 1 MB (see [`PROGRESS_CAP_BYTES`]); when the cap would
    /// be exceeded, the buffer is truncated to keep the tail and new content
    /// is appended. Flushed as an OTel log record in [`finish`].
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

    /// Root span start (nanos since epoch) used when creating the span.
    /// Used with BuildMetrics wallTimeInMs to derive correct end for cached builds.
    root_span_start_nanos: Option<i64>,

    /// Root span end derived from start + wallTimeInMs when BuildMetrics provides it.
    /// Takes precedence over finish_time_nanos so duration matches Bazel's wall time.
    root_span_end_from_wall_nanos: Option<i64>,

    /// Named set cache: set_id → entry with direct files + child set refs.
    /// Used to resolve output files (transitively) when target spans complete.
    named_set_cache: HashMap<String, NamedSetEntry>,

    /// Fetch events that arrived before the root span was created.
    /// Replayed once `on_build_started` fires.
    /// Tuple: (url, success, wallclock arrival time, downloader).
    pending_fetches: Vec<(String, bool, SystemTime, Option<String>)>,

    /// TestResult events that arrived before the root span was created.
    /// Replayed once `on_build_started` fires (BES can send test events before Started).
    pending_test_results: Vec<BufferedTestResult>,

    /// Path to execution log binary file (from --execution_log_binary_file=).
    /// Set in on_exec_log_detected, used in finish() for enrichment.
    exec_log_path: Option<PathBuf>,

    /// Workspace directory from BuildStarted (for resolving relative exec log path).
    workspace_directory: Option<PathBuf>,

    /// Cached action span info for exec log enrichment.
    /// Key: (target_label, mnemonic, primary_output). Used in finish() to attach
    /// spawn child spans to the correct action span, with timing for clamping.
    action_span_cache: HashMap<ActionSpanKey, ActionSpanInfo>,
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
            external_deps_context: None,
            configurations: HashMap::new(),
            cached_command: None,
            cached_patterns: Vec::new(),
            target_action_counts: HashMap::new(),
            target_has_non_cached_action: HashMap::new(),
            progress_stderr: String::new(),
            progress_stdout: String::new(),
            logger,
            exit_code: None,
            finish_time_nanos: None,
            root_span_start_nanos: None,
            root_span_end_from_wall_nanos: None,
            named_set_cache: HashMap::new(),
            pending_fetches: Vec::new(),
            pending_test_results: Vec::new(),
            exec_log_path: None,
            action_span_cache: HashMap::new(),
            workspace_directory: None,
        }
    }

    // =====================================================================
    // Private helpers
    // =====================================================================

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

    /// Build a child span with the given attributes, set its status, and end it.
    /// Returns the finished span's `SpanContext` for cache/linking purposes.
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
        let mut builder = self
            .tracer
            .span_builder(name)
            .with_kind(kind)
            .with_attributes(attrs);
        if let Some(nanos) = start_time {
            builder = builder.with_start_time(nanos_to_system_time(nanos));
        }
        let mut span = self.tracer.build_with_context(builder, parent);
        let span_cx = span.span_context().clone();
        span.set_status(status);
        if let Some(nanos) = end_time {
            span.end_with_timestamp(nanos_to_system_time(nanos));
        } else {
            span.end();
        }
        span_cx
    }

    /// Lazily create a group span (e.g. "skipped targets", "fetches", "external deps")
    /// under the root span. Returns the existing context if already created.
    fn ensure_group_span(
        slot: &mut Option<Context>,
        root_context: &Option<Context>,
        tracer: &opentelemetry_sdk::trace::Tracer,
        name: &'static str,
    ) {
        if slot.is_some() {
            return;
        }
        if let Some(root_cx) = root_context {
            let builder = tracer
                .span_builder(name)
                .with_kind(SpanKind::Internal);
            let span = tracer.build_with_context(builder, root_cx);
            *slot = Some(Context::new().with_span(span));
            debug!("Created '{name}' parent span");
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
        self.cached_command = Some(command.to_string());

        let trace_id = trace_context::uuid_to_trace_id(uuid);
        let parent_cx = trace_context::make_root_context(trace_id);

        let span_name = format!("bazel {command}");
        let mut builder = self
            .tracer
            .span_builder(span_name)
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
            self.root_span_start_nanos = Some(nanos);
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

        // Replay test results that arrived before the root span (e.g. BES stream order).
        if !self.pending_test_results.is_empty() {
            let buffered: Vec<_> = std::mem::take(&mut self.pending_test_results);
            info!("Replaying {} buffered test result events", buffered.len());
            for b in &buffered {
                self.on_test_result(&TestResultEvent {
                    label: &b.label,
                    status: b.status.as_deref(),
                    attempt: b.attempt,
                    run: b.run,
                    shard: b.shard,
                    cached: b.cached,
                    strategy: b.strategy.as_deref(),
                    start_time_nanos: b.start_time_nanos,
                    duration_nanos: b.duration_nanos,
                    hostname: b.hostname.as_deref(),
                    cached_remotely: b.cached_remotely,
                });
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
        if self.root_context.is_none() {
            return;
        }
        if let Some(v) = workspace_dir {
            self.set_root_attr(KeyValue::new(BAZEL_WORKSPACE_DIR, v.to_string()));
            if !v.is_empty() {
                self.workspace_directory = Some(PathBuf::from(v));
            }
        }
        if let Some(v) = working_dir {
            self.set_root_attr(KeyValue::new(BAZEL_WORKING_DIR, v.to_string()));
        }
        if let Some(v) = build_tool_version {
            self.set_root_attr(KeyValue::new(BAZEL_BUILD_TOOL_VERSION, v.to_string()));
        }
        if let Some(v) = server_pid {
            self.set_root_attr(KeyValue::new(BAZEL_SERVER_PID, v));
        }
        if let Some(v) = host {
            self.set_root_attr(KeyValue::new(BAZEL_WORKSPACE_HOST, v.to_string()));
        }
        if let Some(v) = user {
            self.set_root_attr(KeyValue::new(BAZEL_WORKSPACE_USER, v.to_string()));
        }
    }

    /// OptionsParsed → add attributes to root span.
    pub fn on_options_parsed(
        &mut self,
        startup_options: &[String],
        explicit_cmd_line: &[String],
    ) {
        if !startup_options.is_empty() {
            self.set_root_attr(KeyValue::new(BAZEL_STARTUP_OPTIONS, startup_options.join(" ")));
        }
        if !explicit_cmd_line.is_empty() {
            self.set_root_attr(KeyValue::new(BAZEL_EXPLICIT_CMD_LINE, explicit_cmd_line.join(" ")));
        }
    }

    /// OptionsParsed → extract tool_tag if present.
    pub fn on_tool_tag(&mut self, tool_tag: &str) {
        if !tool_tag.is_empty() {
            self.set_root_attr(KeyValue::new(BAZEL_TOOL_TAG, tool_tag.to_string()));
        }
    }

    /// ActionMode → record the detected processing mode on the root span.
    pub fn on_action_mode(&mut self, mode: crate::state::ActionProcessingMode) {
        self.set_root_attr(KeyValue::new(BAZEL_ACTION_MODE, mode.to_string()));
    }

    /// Execution log path detected (from --execution_log_binary_file=).
    /// Stored for use in finish() to enrich the trace with spawn data.
    pub fn on_exec_log_detected(&mut self, path: PathBuf) {
        self.exec_log_path = Some(path);
    }

    /// WorkspaceStatus → add workspace attributes to root span.
    /// BUILD_USER and BUILD_HOST are mapped to bazel.user/bazel.host directly
    /// (same semantic as the BuildStarted fields) to avoid duplication.
    pub fn on_workspace_status(&mut self, items: &HashMap<String, String>) {
        for (key, value) in items {
            let attr_key = match key.as_str() {
                "BUILD_USER" => BAZEL_WORKSPACE_USER.to_string(),
                "BUILD_HOST" => BAZEL_WORKSPACE_HOST.to_string(),
                _ => format!("bazel.workspace.{}", key.to_lowercase()),
            };
            self.set_root_attr(KeyValue::new(attr_key, value.clone()));
        }
    }

    /// UnstructuredCommandLine → add full command line as attribute.
    pub fn on_command_line(&mut self, args: &[String]) {
        if !args.is_empty() {
            self.set_root_attr(KeyValue::new(BAZEL_COMMAND_LINE, args.join(" ")));
        }
    }

    /// Pattern expanded → add build patterns as attribute and enrich root span name.
    pub fn on_pattern(&mut self, patterns: &[String]) {
        if !patterns.is_empty() {
            self.cached_patterns = patterns.to_vec();
            self.set_root_attr(KeyValue::new(BAZEL_PATTERNS, patterns.join(", ")));
            if let (Some(cmd), Some(cx)) = (&self.cached_command, &self.root_context) {
                cx.span().update_name(format!("bazel {} {}", cmd, patterns.join(" ")));
            }
        }
    }

    /// PatternSkipped → add skipped patterns as span event on root.
    pub fn on_pattern_skipped(&mut self, patterns: &[String]) {
        if !patterns.is_empty() {
            self.add_root_event(
                "pattern_skipped",
                vec![KeyValue::new(BAZEL_PATTERNS, patterns.join(", "))],
            );
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
        Self::ensure_group_span(
            &mut self.skipped_context,
            &self.root_context,
            &self.tracer,
            "skipped targets",
        );

        let Some(parent) = &self.skipped_context else { return };

        let short = shorten_label(label);
        let mut attrs = vec![
            KeyValue::new(BAZEL_TARGET_LABEL, label.to_string()),
            KeyValue::new(BAZEL_TARGET_LABEL_SHORT, short.to_string()),
            KeyValue::new(BAZEL_TARGET_SUCCESS, false),
        ];
        if let Some(r) = reason {
            attrs.push(KeyValue::new(BAZEL_TARGET_ABORT_REASON, r.to_string()));
        }
        if let Some(d) = description {
            attrs.push(KeyValue::new(BAZEL_TARGET_ABORT_DESCRIPTION, d.to_string()));
        }

        self.build_and_end_child_span(
            parent,
            format!("target {short}"),
            SpanKind::Internal,
            attrs,
            Status::Error {
                description: Cow::Owned(description.unwrap_or("aborted").to_string()),
            },
            None,
            event_time_nanos,
        );
        debug!("Created aborted-label span for {label}");
    }

    // =====================================================================
    // Configuration events
    // =====================================================================

    /// Configuration → span event on root span. Also caches mnemonic for action enrichment.
    pub fn on_configuration(
        &mut self,
        config_id: &str,
        mnemonic: Option<&str>,
        platform: Option<&str>,
    ) {
        if let Some(m) = mnemonic {
            self.configurations
                .insert(config_id.to_string(), m.to_string());
        }
        let mut attrs = vec![KeyValue::new(BAZEL_CONFIG_ID, config_id.to_string())];
        if let Some(m) = mnemonic {
            attrs.push(KeyValue::new(BAZEL_CONFIG_MNEMONIC, m.to_string()));
        }
        if let Some(p) = platform {
            attrs.push(KeyValue::new(BAZEL_CONFIG_PLATFORM, p.to_string()));
        }
        self.add_root_event("configuration", attrs);
        debug!("Added configuration event: {config_id}");
    }

    /// Configuration with cpu/is_tool → add to root span event.
    pub fn on_configuration_extended(
        &mut self,
        config_id: &str,
        cpu: Option<&str>,
        is_tool: Option<bool>,
    ) {
        let mut attrs = vec![KeyValue::new(BAZEL_CONFIG_ID, config_id.to_string())];
        if let Some(c) = cpu {
            attrs.push(KeyValue::new(BAZEL_CONFIG_CPU, c.to_string()));
        }
        if let Some(t) = is_tool {
            attrs.push(KeyValue::new(BAZEL_CONFIG_IS_TOOL, t));
        }
        self.add_root_event("configuration_detail", attrs);
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
        test_size: Option<&str>,
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
                test_size: test_size.filter(|s| !s.is_empty()).map(String::from),
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
        let short = shorten_label(label);
        let mut attrs = vec![
            KeyValue::new(BAZEL_TARGET_LABEL, label.to_string()),
            KeyValue::new(BAZEL_TARGET_LABEL_SHORT, short.to_string()),
        ];
        if let Some(k) = &configured.kind {
            attrs.push(KeyValue::new(BAZEL_TARGET_KIND, k.clone()));
        }
        if !configured.tags.is_empty() {
            attrs.push(KeyValue::new(
                BAZEL_TARGET_TAGS,
                configured.tags.join(", "),
            ));
        }
        if let Some(ts) = &configured.test_size {
            attrs.push(KeyValue::new(BAZEL_TARGET_TEST_SIZE, ts.clone()));
        }

        let start_nanos = start_nanos_override.or(configured.event_time_nanos);
        let mut builder = self
            .tracer
            .span_builder(format!("target {short}"))
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
    /// When `action_start_nanos` is set (e.g. from ActionExecuted), the new span's
    /// start time is set so target timing reflects the action.
    fn ensure_target_span(&mut self, label: &str, action_start_nanos: Option<i64>) {
        if self.target_contexts.contains_key(label)
            || self.pending_output_resolutions.contains_key(label)
        {
            return;
        }
        let parent = self.choose_parent_for_label(label);
        let Some(parent) = parent else { return };
        if let Some(configured) = self.configured_targets.get(label) {
            let cx = self.create_target_span(label, configured, &parent, action_start_nanos);
            debug!("Lazily created target span for {label} (action needed parent)");
            self.target_contexts.insert(label.to_string(), cx);
        } else {
            let cx = self.create_synthetic_target_span(label, &parent, action_start_nanos);
            debug!("Created synthetic target span for {label} (from action label)");
            self.target_contexts.insert(label.to_string(), cx);
        }
    }

    /// Returns true if a label looks like an external dependency.
    fn is_external_label(label: &str) -> bool {
        label.starts_with("@@") || label.contains("+_repo_rules+")
    }

    /// Choose the correct parent for a target: root for local targets,
    /// `external deps` group span for external dependencies.
    fn choose_parent_for_label(&mut self, label: &str) -> Option<Context> {
        if Self::is_external_label(label) {
            self.ensure_external_deps_span();
            self.external_deps_context.clone()
        } else {
            self.root_context.clone()
        }
    }

    /// Lazily create the `external deps` parent span.
    fn ensure_external_deps_span(&mut self) {
        Self::ensure_group_span(
            &mut self.external_deps_context,
            &self.root_context,
            &self.tracer,
            "external deps",
        );
    }

    /// Create a target span with only the label (no kind/tags). Used when we see
    /// an action with a label but no prior TargetConfigured event.
    fn create_synthetic_target_span(
        &self,
        label: &str,
        parent: &Context,
        start_nanos: Option<i64>,
    ) -> Context {
        let short = shorten_label(label);
        let attrs = vec![
            KeyValue::new(BAZEL_TARGET_LABEL, label.to_string()),
            KeyValue::new(BAZEL_TARGET_LABEL_SHORT, short.to_string()),
            KeyValue::new(BAZEL_TARGET_SYNTHETIC, true),
        ];
        let mut builder = self
            .tracer
            .span_builder(format!("target {short}"))
            .with_kind(SpanKind::Internal)
            .with_attributes(attrs);
        if let Some(nanos) = start_nanos {
            builder = builder.with_start_time(nanos_to_system_time(nanos));
        }
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

        // Get or create the span under the appropriate parent.
        let cx = if let Some(cx) = self.target_contexts.remove(label) {
            cx
        } else if let Some(configured) = self.configured_targets.remove(label) {
            let parent = self.choose_parent_for_label(label);
            if let Some(parent) = parent {
                let start_override = earliest_action_start_nanos;
                let cx = self.create_target_span(label, &configured, &parent, start_override);
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

        let is_cached = !self.target_has_non_cached_action.get(label).copied().unwrap_or(false);
        cx.span()
            .set_attribute(KeyValue::new(BAZEL_TARGET_CACHED, is_cached));

        if !tags.is_empty() {
            cx.span()
                .set_attribute(KeyValue::new(BAZEL_TARGET_TAGS, tags.join(", ")));
        }

        // Sub-ms target with no actions → mark trivial (e.g. alias, no-op).
        if buffered_actions.is_empty() {
            let duration_nanos = match (earliest_action_start_nanos, effective_end) {
                (Some(s), Some(e)) if e > s => e - s,
                _ => 0,
            };
            if duration_nanos < 1_000_000 {
                cx.span()
                    .set_attribute(KeyValue::new(BAZEL_TARGET_TRIVIAL, true));
            }
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
                self.on_action_completed(&ActionCompletedEvent::from_buffered(act));
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
        // End any open span from target_contexts to avoid leaks.
        if let Some(existing) = self.target_contexts.remove(label) {
            existing.span().set_status(Status::Unset);
            existing.span().end();
            debug!("Ended leaked target span for {label} on skip");
        }
        // End any open span from pending_output_resolutions (deferred end).
        if let Some(pending) = self.pending_output_resolutions.remove(label) {
            pending.parent_cx.span().set_status(Status::Unset);
            pending.parent_cx.span().end();
            debug!("Ended pending output resolution span for {label} on skip");
        }

        Self::ensure_group_span(
            &mut self.skipped_context,
            &self.root_context,
            &self.tracer,
            "skipped targets",
        );

        let parent = match &self.skipped_context {
            Some(cx) => cx,
            None => return,
        };

        let short = shorten_label(label);
        let mut attrs = vec![
            KeyValue::new(BAZEL_TARGET_LABEL, label.to_string()),
            KeyValue::new(BAZEL_TARGET_LABEL_SHORT, short.to_string()),
            KeyValue::new(BAZEL_TARGET_SUCCESS, false),
        ];
        if let Some(configured) = &configured {
            if let Some(k) = &configured.kind {
                attrs.push(KeyValue::new(BAZEL_TARGET_KIND, k.clone()));
            }
        }
        if let Some(r) = reason {
            attrs.push(KeyValue::new(BAZEL_TARGET_ABORT_REASON, r.to_string()));
        }
        if let Some(d) = description {
            attrs.push(KeyValue::new(BAZEL_TARGET_ABORT_DESCRIPTION, d.to_string()));
        }

        let instant = event_time_nanos
            .map(nanos_to_system_time)
            .unwrap_or_else(SystemTime::now);

        let builder = self
            .tracer
            .span_builder(format!("target {short}"))
            .with_kind(SpanKind::Internal)
            .with_attributes(attrs)
            .with_start_time(instant);

        let span = self.tracer.build_with_context(builder, parent);
        let cx = Context::new().with_span(span);

        cx.span().set_status(Status::Unset);
        cx.span().end_with_timestamp(instant);

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
            let display_files: Vec<String> = all_files
                .iter()
                .map(|s| bytestream_uri_to_display(s).into_owned())
                .collect();
            let file_list = if display_files.len() <= 20 {
                display_files.join(", ")
            } else {
                format!(
                    "{} ... and {} more",
                    display_files[..20].join(", "),
                    display_files.len() - 20
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
    pub fn on_action_completed(&mut self, ev: &ActionCompletedEvent<'_>) {
        if let Some(l) = ev.label {
            self.ensure_target_span(l, ev.start_time_nanos);
        }

        let parent = ev
            .label
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

        // Track action for cached-target detection.
        if let Some(l) = ev.label {
            *self.target_action_counts.entry(l.to_string()).or_insert(0) += 1;
            if ev.cached == Some(false) {
                self.target_has_non_cached_action.insert(l.to_string(), true);
            }
        }

        let span_name = match (ev.label, ev.mnemonic) {
            (Some(l), Some(m)) => format!("action {m} {}", shorten_label(l)),
            (Some(l), None) => format!("action {}", shorten_label(l)),
            (None, Some(m)) => format!("action {m}"),
            _ => "action".to_string(),
        };

        let mut attrs = vec![KeyValue::new(BAZEL_ACTION_SUCCESS, ev.success)];
        if let Some(m) = ev.mnemonic {
            attrs.push(KeyValue::new(BAZEL_ACTION_MNEMONIC, m.to_string()));
        }
        if let Some(code) = ev.exit_code {
            attrs.push(KeyValue::new(BAZEL_ACTION_EXIT_CODE, code as i64));
        }
        if let Some(name) = ev.exit_code_name.filter(|s| !s.is_empty()) {
            attrs.push(KeyValue::new(BAZEL_ACTION_EXIT_CODE_NAME, name.to_string()));
        }
        if let Some(c) = ev.cached {
            attrs.push(KeyValue::new(BAZEL_ACTION_CACHED, c));
        }
        if let Some(h) = ev.hostname.filter(|s| !s.is_empty()) {
            attrs.push(KeyValue::new(BAZEL_ACTION_HOSTNAME, h.to_string()));
        }
        if let Some(cr) = ev.cached_remotely {
            attrs.push(KeyValue::new(BAZEL_ACTION_CACHED_REMOTELY, cr));
        }
        if let Some(r) = ev.runner.filter(|s| !s.is_empty()) {
            attrs.push(KeyValue::new(BAZEL_ACTION_RUNNER, r.to_string()));
        }
        if let Some(output) = ev.primary_output {
            attrs.push(KeyValue::new(BAZEL_ACTION_PRIMARY_OUTPUT, output.to_string()));
        }
        if let Some(l) = ev.label {
            attrs.push(KeyValue::new(BAZEL_ACTION_LABEL, l.to_string()));
            attrs.push(KeyValue::new(BAZEL_ACTION_LABEL_SHORT, shorten_label(l).to_string()));
        }
        if let Some(cfg) = ev.configuration {
            let display_val = self.configurations.get(cfg).map_or_else(
                || cfg.to_string(),
                |s| s.clone(),
            );
            attrs.push(KeyValue::new(BAZEL_ACTION_CONFIGURATION, display_val));
        }
        if !ev.command_line.is_empty() {
            let joined = ev.command_line.join(" ");
            let capped = truncate_to_byte_limit(&joined, COMMAND_LINE_CAP_BYTES, "...(truncated)");
            attrs.push(KeyValue::new(BAZEL_ACTION_COMMAND_LINE, capped));
        }
        if let Some(p) = ev.stdout_path {
            attrs.push(KeyValue::new(BAZEL_ACTION_STDOUT, p.to_string()));
        }
        if let Some(p) = ev.stderr_path {
            attrs.push(KeyValue::new(BAZEL_ACTION_STDERR, p.to_string()));
        }

        let status = if ev.success {
            Status::Ok
        } else {
            Status::Error {
                description: Cow::Borrowed("Action failed"),
            }
        };

        // Clamp action timing to root span bounds so cached-action timestamps
        // (which reflect original remote execution time) don't escape the build.
        let (clamped_start, clamped_end) = clamp_time_range(
            ev.start_time_nanos,
            ev.end_time_nanos,
            self.root_span_start_nanos,
            self.root_span_end_from_wall_nanos.or(self.finish_time_nanos),
        );

        let span_cx = self.build_and_end_child_span(
            &parent,
            span_name,
            SpanKind::Internal,
            attrs,
            status,
            clamped_start,
            clamped_end,
        );

        let key = ActionSpanKey::new(ev.label, ev.mnemonic, ev.primary_output);
        self.action_span_cache.insert(key, ActionSpanInfo {
            span_context: span_cx,
            start_nanos: clamped_start,
            end_nanos: clamped_end,
        });

        debug!("Created action span (success={}) for {:?}", ev.success, ev.label);
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

        self.ensure_target_span(ev.label, None);

        let parent = self
            .target_contexts
            .get(ev.label)
            .or_else(|| self.pending_output_resolutions.get(ev.label).map(|p| &p.parent_cx))
            .or(self.root_context.as_ref());

        let parent = match parent {
            Some(cx) => cx.clone(),
            None => {
                warn!("TestResult with no parent context for {}", ev.label);
                return;
            }
        };

        let span_name = match (ev.attempt, ev.run, ev.shard) {
            (Some(a), Some(r), Some(s)) => format!("test {} attempt={a} run={r} shard={s}", ev.label),
            (Some(a), _, _) => format!("test {} attempt={a}", ev.label),
            _ => format!("test {}", ev.label),
        };

        let mut attrs = vec![
            KeyValue::new(BAZEL_TARGET_LABEL, ev.label.to_string()),
            KeyValue::new(BAZEL_TARGET_LABEL_SHORT, shorten_label(ev.label).to_string()),
        ];
        if let Some(s) = ev.status {
            attrs.push(KeyValue::new(BAZEL_TEST_STATUS, s.to_string()));
        }
        if let Some(a) = ev.attempt {
            attrs.push(KeyValue::new(BAZEL_TEST_ATTEMPT, a as i64));
        }
        if let Some(r) = ev.run {
            attrs.push(KeyValue::new(BAZEL_TEST_RUN, r as i64));
        }
        if let Some(s) = ev.shard {
            attrs.push(KeyValue::new(BAZEL_TEST_SHARD, s as i64));
        }
        if let Some(c) = ev.cached {
            attrs.push(KeyValue::new(BAZEL_TEST_CACHED, c));
        }
        if let Some(st) = ev.strategy {
            attrs.push(KeyValue::new(BAZEL_TEST_STRATEGY, st.to_string()));
        }
        if let Some(h) = ev.hostname.filter(|s| !s.is_empty()) {
            attrs.push(KeyValue::new(BAZEL_TEST_HOSTNAME, h.to_string()));
        }
        if let Some(cr) = ev.cached_remotely {
            attrs.push(KeyValue::new(BAZEL_TEST_CACHED_REMOTELY, cr));
        }

        let is_pass = ev.status.map(|s| s == "PASSED").unwrap_or(false);
        let status = if is_pass {
            Status::Ok
        } else {
            Status::Error {
                description: Cow::Owned(format!("test {}", ev.status.unwrap_or("UNKNOWN"))),
            }
        };

        let end_nanos = match (ev.start_time_nanos, ev.duration_nanos) {
            (Some(start), Some(dur)) => Some(start + dur),
            _ => None,
        };

        self.build_and_end_child_span(
            &parent,
            span_name,
            SpanKind::Internal,
            attrs,
            status,
            ev.start_time_nanos,
            end_nanos,
        );

        info!(label = ev.label, status = ?ev.status, "Created test result span");
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

    /// TargetSummary → span event on root with overall build/test status for the target.
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

    // =====================================================================
    // Progress events
    // =====================================================================

    /// Progress with stderr/stdout → text is buffered and flushed as a
    /// single `build.log` event on the root `bazel.invocation` span in
    /// [`finish`]. Each stream is capped at [`PROGRESS_CAP_BYTES`] (1 MB);
    /// when appending would exceed the cap, the existing buffer is truncated
    /// to keep the tail (recent output), then the new content is appended.
    pub fn on_progress(&mut self, stderr: Option<&str>, stdout: Option<&str>) {
        if let Some(err) = stderr {
            if !err.is_empty() {
                append_progress_capped(&mut self.progress_stderr, &strip_ansi(err));
            }
        }
        if let Some(out) = stdout {
            if !out.is_empty() {
                append_progress_capped(&mut self.progress_stdout, &strip_ansi(out));
            }
        }
    }

    // =====================================================================
    // Build metadata
    // =====================================================================

    /// BuildMetadata → add as attributes on root span.
    pub fn on_build_metadata(&mut self, metadata: &serde_json::Value) {
        if let Some(entries) = metadata.get("metadata").and_then(|m| m.as_object()) {
            for (key, value) in entries {
                if let Some(v) = value.as_str() {
                    self.set_root_attr(KeyValue::new(
                        format!("bazel.metadata.{key}"),
                        v.to_string(),
                    ));
                }
            }
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

    /// BuildMetrics → add metrics attributes to root span.
    /// Supports both legacy flat payload (actionsCreated/actionsExecuted) and full nested payload.
    pub fn on_build_metrics(&mut self, metrics: &serde_json::Value) {
        if self.root_context.is_none() {
            return;
        }
        let Some(obj) = metrics.as_object() else {
            return;
        };

        let summary = obj.get("actionSummary").or(Some(metrics));
        self.apply_action_summary_attrs(summary);
        self.apply_timing_metrics_attrs(obj);
        self.apply_memory_metrics_attrs(obj);
        self.apply_target_package_metrics_attrs(obj);
        self.apply_artifact_metrics_attrs(obj);
        self.apply_network_metrics_attrs(obj);
        self.apply_runner_count_attrs(summary);
        self.apply_cumulative_metrics_attrs(obj);
    }

    fn apply_action_summary_attrs(&self, summary: Option<&serde_json::Value>) {
        let Some(s) = summary.and_then(|v| v.as_object()) else { return };
        if let Some(v) = s.get("actionsCreated").and_then(|v| v.as_i64()) {
            self.set_root_attr(KeyValue::new(BAZEL_METRICS_ACTIONS_CREATED, v));
        }
        if let Some(v) = s.get("actionsExecuted").and_then(|v| v.as_i64()) {
            self.set_root_attr(KeyValue::new(BAZEL_METRICS_ACTIONS_EXECUTED, v));
        }
        if let Some(action_data) = s.get("actionData").and_then(|v| v.as_array()) {
            for entry in action_data {
                let mnemonic = entry.get("mnemonic").and_then(|v| v.as_str()).unwrap_or("unknown");
                let count = entry.get("actionsExecuted").and_then(|v| v.as_i64()).unwrap_or(0);
                self.set_root_attr(KeyValue::new(format!("bazel.metrics.actions.{mnemonic}"), count));
            }
        }
        if let Some(acs) = s.get("actionCacheStatistics").and_then(|v| v.as_object()) {
            if let Some(v) = acs.get("hits").and_then(|v| v.as_i64()) {
                self.set_root_attr(KeyValue::new(BAZEL_METRICS_CACHE_HITS, v));
            }
            if let Some(v) = acs.get("misses").and_then(|v| v.as_i64()) {
                self.set_root_attr(KeyValue::new(BAZEL_METRICS_CACHE_MISSES, v));
            }
        }
    }

    fn apply_timing_metrics_attrs(&mut self, obj: &serde_json::Map<String, serde_json::Value>) {
        let Some(t) = obj.get("timingMetrics").and_then(|v| v.as_object()) else { return };
        if let Some(v) = t.get("wallTimeInMs").and_then(|v| v.as_i64()) {
            self.set_root_attr(KeyValue::new(BAZEL_METRICS_WALL_TIME_MS, v));
            // Derive root span end from start + wall time for correct duration on cached builds.
            if let Some(start) = self.root_span_start_nanos {
                self.root_span_end_from_wall_nanos = Some(start + v.saturating_mul(1_000_000));
            }
        }
        let timing_attrs: &[(&str, &str)] = &[
            ("cpuTimeInMs", BAZEL_METRICS_CPU_TIME_MS),
            ("analysisPhaseTimeInMs", BAZEL_METRICS_ANALYSIS_PHASE_MS),
            ("executionPhaseTimeInMs", BAZEL_METRICS_EXECUTION_PHASE_MS),
            ("criticalPathMs", BAZEL_METRICS_CRITICAL_PATH_MS),
            ("actionsExecutionStartInMs", BAZEL_METRICS_ACTIONS_EXECUTION_START_MS),
        ];
        for &(json_key, attr_key) in timing_attrs {
            if let Some(v) = t.get(json_key).and_then(|v| v.as_i64()) {
                self.set_root_attr(KeyValue::new(attr_key, v));
            }
        }
    }

    fn apply_memory_metrics_attrs(&self, obj: &serde_json::Map<String, serde_json::Value>) {
        let Some(m) = obj.get("memoryMetrics").and_then(|v| v.as_object()) else { return };
        if let Some(v) = m.get("usedHeapSizePostBuild").and_then(|v| v.as_i64()) {
            self.set_root_attr(KeyValue::new(BAZEL_METRICS_HEAP_POST_BUILD, v));
        }
        if let Some(v) = m.get("peakPostGcHeapSize").and_then(|v| v.as_i64()) {
            self.set_root_attr(KeyValue::new(BAZEL_METRICS_PEAK_HEAP_POST_GC, v));
        }
    }

    fn apply_target_package_metrics_attrs(&self, obj: &serde_json::Map<String, serde_json::Value>) {
        if let Some(v) = obj.get("targetMetrics")
            .and_then(|v| v.get("targetsConfigured"))
            .and_then(|v| v.as_i64())
        {
            self.set_root_attr(KeyValue::new(BAZEL_METRICS_TARGETS_CONFIGURED, v));
        }
        if let Some(v) = obj.get("packageMetrics")
            .and_then(|v| v.get("packagesLoaded"))
            .and_then(|v| v.as_i64())
        {
            self.set_root_attr(KeyValue::new(BAZEL_METRICS_PACKAGES_LOADED, v));
        }
    }

    fn apply_artifact_metrics_attrs(&self, obj: &serde_json::Map<String, serde_json::Value>) {
        let Some(am) = obj.get("artifactMetrics").and_then(|v| v.as_object()) else { return };
        let artifact_attrs: &[(&str, &str)] = &[
            ("sourceArtifactsRead", BAZEL_METRICS_SOURCE_ARTIFACTS_COUNT),
            ("outputArtifactsSeen", BAZEL_METRICS_OUTPUT_ARTIFACTS_COUNT),
            ("outputArtifactsFromActionCache", BAZEL_METRICS_ACTION_CACHE_ARTIFACTS_COUNT),
            ("topLevelArtifacts", BAZEL_METRICS_TOP_LEVEL_ARTIFACTS_COUNT),
        ];
        for &(json_key, attr_key) in artifact_attrs {
            if let Some(v) = am.get(json_key).and_then(|v| v.get("count")).and_then(|v| v.as_i64()) {
                self.set_root_attr(KeyValue::new(attr_key, v));
            }
        }
    }

    fn apply_network_metrics_attrs(&self, obj: &serde_json::Map<String, serde_json::Value>) {
        let Some(nm) = obj.get("networkMetrics").and_then(|v| v.as_object()) else { return };
        if let Some(v) = nm.get("bytesSent").and_then(|v| v.as_u64()) {
            self.set_root_attr(KeyValue::new(BAZEL_METRICS_BYTES_SENT, v as i64));
        }
        if let Some(v) = nm.get("bytesRecv").and_then(|v| v.as_u64()) {
            self.set_root_attr(KeyValue::new(BAZEL_METRICS_BYTES_RECV, v as i64));
        }
    }

    fn apply_runner_count_attrs(&self, summary: Option<&serde_json::Value>) {
        let Some(runners) = summary
            .and_then(|s| s.as_object())
            .and_then(|s| s.get("runnerCount"))
            .and_then(|v| v.as_array())
        else {
            return;
        };
        for r in runners {
            let name = r.get("name").and_then(|v| v.as_str()).unwrap_or("unknown");
            let count = r.get("count").and_then(|v| v.as_i64()).unwrap_or(0);
            self.set_root_attr(KeyValue::new(format!("bazel.metrics.runner.{name}"), count));
        }
    }

    fn apply_cumulative_metrics_attrs(&self, obj: &serde_json::Map<String, serde_json::Value>) {
        let Some(cm) = obj.get("cumulativeMetrics").and_then(|v| v.as_object()) else { return };
        if let Some(v) = cm.get("numAnalyses").and_then(|v| v.as_i64()) {
            self.set_root_attr(KeyValue::new(BAZEL_METRICS_CUMULATIVE_NUM_ANALYSES, v));
        }
        if let Some(v) = cm.get("numBuilds").and_then(|v| v.as_i64()) {
            self.set_root_attr(KeyValue::new(BAZEL_METRICS_CUMULATIVE_NUM_BUILDS, v));
        }
    }

    // =====================================================================
    // Build tool logs (critical path)
    // =====================================================================

    /// BuildToolLogs → extract critical path info, set root attribute for URI, emit span event.
    pub fn on_build_tool_logs(&mut self, logs: &serde_json::Value) {
        if let Some(entries) = logs.get("log").and_then(|v| v.as_array()) {
            for entry in entries {
                let name = entry.get("name").and_then(|v| v.as_str()).unwrap_or("");
                if name == "critical path" || name.contains("critical") {
                    let uri = entry.get("uri").and_then(|v| v.as_str()).unwrap_or("");
                    if !uri.is_empty() {
                        self.set_root_attr(KeyValue::new(
                            BAZEL_CRITICAL_PATH_LOG_URI,
                            uri.to_string(),
                        ));
                    }
                    self.add_root_event(
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

    // =====================================================================
    // State management
    // =====================================================================

    /// Reset all mapper state for a new build invocation.
    pub fn reset(&mut self) {
        if let Some(cx) = self.root_context.take() {
            cx.span().end();
        }
        if let Some(cx) = self.fetches_context.take() {
            cx.span().end();
        }
        if let Some(cx) = self.skipped_context.take() {
            cx.span().end();
        }
        if let Some(cx) = self.external_deps_context.take() {
            cx.span().end();
        }
        for (_, cx) in self.target_contexts.drain() {
            cx.span().end();
        }
        for (_, p) in self.pending_output_resolutions.drain() {
            p.parent_cx.span().end();
        }
        self.configured_targets.clear();
        self.configurations.clear();
        self.cached_command = None;
        self.cached_patterns.clear();
        self.target_action_counts.clear();
        self.target_has_non_cached_action.clear();
        self.progress_stderr.clear();
        self.progress_stdout.clear();
        self.exit_code = None;
        self.finish_time_nanos = None;
        self.root_span_start_nanos = None;
        self.root_span_end_from_wall_nanos = None;
        self.named_set_cache.clear();
        self.pending_fetches.clear();
        self.pending_test_results.clear();
        self.exec_log_path = None;
        self.action_span_cache.clear();
        self.workspace_directory = None;
    }

    /// Create synthetic target spans and replay actions for orphaned buffers
    /// (transitive deps with actions but no TargetCompleted event).
    pub fn drain_orphaned_actions(
        &mut self,
        label: &str,
        earliest: Option<i64>,
        latest: Option<i64>,
        actions: &[BufferedAction],
    ) {
        if actions.is_empty() {
            return;
        }
        let parent = self.choose_parent_for_label(label);
        let Some(parent) = parent else { return };

        let root_end = self.root_span_end_from_wall_nanos.or(self.finish_time_nanos);
        let (clamped_start, clamped_end) =
            clamp_time_range(earliest, latest, self.root_span_start_nanos, root_end);

        let short = shorten_label(label);
        let attrs = vec![
            KeyValue::new(BAZEL_TARGET_LABEL, label.to_string()),
            KeyValue::new(BAZEL_TARGET_LABEL_SHORT, short.to_string()),
            KeyValue::new(BAZEL_TARGET_SYNTHETIC, true),
        ];
        let mut builder = self
            .tracer
            .span_builder(format!("target {short}"))
            .with_kind(SpanKind::Internal)
            .with_attributes(attrs);

        if let Some(nanos) = clamped_start {
            builder = builder.with_start_time(nanos_to_system_time(nanos));
        }

        let span = self.tracer.build_with_context(builder, &parent);
        let cx = Context::new().with_span(span);

        self.target_contexts.insert(label.to_string(), cx.clone());
        for act in actions {
            self.on_action_completed(&ActionCompletedEvent::from_buffered(act));
        }
        let cx = self.target_contexts.remove(label).unwrap_or(cx);

        if let Some(nanos) = clamped_end {
            cx.span().end_with_timestamp(nanos_to_system_time(nanos));
        } else {
            cx.span().end();
        }
        debug!("Created synthetic target span for orphaned transitive dep {label} ({} actions)", actions.len());
    }

    // =====================================================================
    // Finalization
    // =====================================================================

    /// End all remaining spans (call after last BEP event).
    pub fn finish(&mut self) {
        // Enrich trace with execution log data if --execution_log_binary_file was set.
        if let Some(ref path) = self.exec_log_path {
            let resolved = if path.is_relative() {
                self.workspace_directory
                    .as_ref()
                    .map(|ws| ws.join(path))
                    .unwrap_or_else(|| path.clone())
            } else {
                path.clone()
            };
            let root_cx = self
                .root_context
                .as_ref()
                .map(|c| c.span().span_context().clone());
            let root_end = self.root_span_end_from_wall_nanos.or(self.finish_time_nanos);
            crate::exec_log::enrich_trace(
                &resolved,
                &self.action_span_cache,
                &self.tracer,
                root_cx.as_ref(),
                self.root_span_start_nanos,
                root_end,
            );
        }

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

        // End the `external deps` parent span.
        if let Some(cx) = self.external_deps_context.take() {
            cx.span().set_status(Status::Ok);
            cx.span().end();
            debug!("Ended 'external deps' span");
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
                    debug_assert!(has_content, "expected non-empty stderr or stdout");
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
                        (false, false) => {}
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

            let end_nanos = self.root_span_end_from_wall_nanos.or(self.finish_time_nanos);
            if let Some(nanos) = end_nanos {
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

#[cfg(test)]
mod tests {
    use super::*;

    // ---- Helper function tests ----

    #[test]
    fn strip_ansi_no_escapes() {
        assert_eq!(strip_ansi("hello world"), "hello world");
    }

    #[test]
    fn strip_ansi_csi_sequence() {
        assert_eq!(strip_ansi("\x1b[31mred\x1b[0m"), "red");
    }

    #[test]
    fn strip_ansi_osc_sequence() {
        assert_eq!(strip_ansi("before\x1b]0;title\x07after"), "beforeafter");
    }

    #[test]
    fn strip_ansi_empty() {
        assert_eq!(strip_ansi(""), "");
    }

    #[test]
    fn bytestream_uri_to_display_non_bytestream() {
        assert_eq!(bytestream_uri_to_display("file:///tmp/out"), "file:///tmp/out");
    }

    #[test]
    fn bytestream_uri_to_display_strips_authority() {
        let uri = "bytestream://remote.example.com/blobs/abc123/42";
        assert_eq!(bytestream_uri_to_display(uri), "blobs/abc123/42");
    }

    #[test]
    fn tail_byte_offset_short_string() {
        assert_eq!(tail_byte_offset("hello", 10), 0);
    }

    #[test]
    fn tail_byte_offset_exact() {
        assert_eq!(tail_byte_offset("hello", 5), 0);
    }

    #[test]
    fn tail_byte_offset_truncates() {
        assert_eq!(tail_byte_offset("hello world", 5), 6);
    }

    #[test]
    fn append_progress_capped_within_limit() {
        let mut buf = String::from("hello ");
        append_progress_capped(&mut buf, "world");
        assert_eq!(buf, "hello world");
    }

    #[test]
    fn normalize_label_strips_at() {
        assert_eq!(normalize_label("@@repo//:t"), "repo//:t");
        assert_eq!(normalize_label("@repo//:t"), "repo//:t");
        assert_eq!(normalize_label("//pkg:t"), "//pkg:t");
    }

    #[test]
    fn nanos_to_system_time_positive() {
        let t = nanos_to_system_time(1_000_000_000);
        assert_eq!(t, UNIX_EPOCH + Duration::from_secs(1));
    }

    #[test]
    fn nanos_to_system_time_negative() {
        assert_eq!(nanos_to_system_time(-1), UNIX_EPOCH);
    }

    // ---- Mapper lifecycle tests ----

    fn test_mapper() -> OtelMapper {
        use opentelemetry::trace::TracerProvider;
        let tp = opentelemetry_sdk::trace::TracerProvider::builder().build();
        let tracer = tp.tracer("test");
        OtelMapper::new(tracer, None)
    }

    #[test]
    fn root_span_lifecycle() {
        let mut mapper = test_mapper();
        assert!(mapper.root_context.is_none());

        mapper.on_build_started("test-uuid", "build", Some(1_000_000_000), None);
        assert!(mapper.root_context.is_some());
        assert_eq!(mapper.cached_command.as_deref(), Some("build"));

        mapper.on_build_finished(Some(0), Some(2_000_000_000), None);
        assert_eq!(mapper.exit_code, Some(0));

        mapper.finish();
        assert!(mapper.root_context.is_none());
    }

    #[test]
    fn pattern_enriches_root_name() {
        let mut mapper = test_mapper();
        mapper.on_build_started("uuid-1", "build", Some(1_000_000_000), None);
        mapper.on_pattern(&["//...".to_string()]);
        assert_eq!(mapper.cached_patterns, vec!["//..."]);
    }

    #[test]
    fn reset_clears_state() {
        let mut mapper = test_mapper();
        mapper.on_build_started("uuid-1", "build", Some(1_000_000_000), None);
        mapper.on_build_finished(Some(0), None, None);
        mapper.reset();
        assert!(mapper.root_context.is_none());
        assert!(mapper.exit_code.is_none());
        assert!(mapper.cached_command.is_none());
    }

    #[test]
    fn clamp_time_range_no_bounds() {
        let (s, e) = clamp_time_range(Some(10), Some(20), None, None);
        assert_eq!(s, Some(10));
        assert_eq!(e, Some(20));
    }

    #[test]
    fn clamp_time_range_clamps_start() {
        let (s, e) = clamp_time_range(Some(5), Some(20), Some(10), None);
        assert_eq!(s, Some(10));
        assert_eq!(e, Some(20));
    }

    #[test]
    fn clamp_time_range_clamps_end() {
        let (s, e) = clamp_time_range(Some(10), Some(30), None, Some(20));
        assert_eq!(s, Some(10));
        assert_eq!(e, Some(20));
    }

    #[test]
    fn clamp_time_range_both_bounds() {
        // Spawn starts before parent and ends after parent
        let (s, e) = clamp_time_range(Some(5), Some(30), Some(10), Some(20));
        assert_eq!(s, Some(10));
        assert_eq!(e, Some(20));
    }

    #[test]
    fn clamp_time_range_inverted_collapses() {
        // After clamping, start > end → collapse to (end, end)
        let (s, e) = clamp_time_range(Some(25), Some(8), Some(10), Some(20));
        assert_eq!(s, Some(8));
        assert_eq!(e, Some(8));
    }

    #[test]
    fn clamp_time_range_within_bounds_unchanged() {
        let (s, e) = clamp_time_range(Some(12), Some(18), Some(10), Some(20));
        assert_eq!(s, Some(12));
        assert_eq!(e, Some(18));
    }

    #[test]
    fn clamp_time_range_cached_collapses_to_zero() {
        // Both endpoints predate the invocation → cached replay, zero duration at bound_start.
        let (s, e) = clamp_time_range(Some(2), Some(8), Some(10), Some(20));
        assert_eq!(s, Some(10));
        assert_eq!(e, Some(10));
    }

    #[test]
    fn clamp_time_range_cached_end_equal_bound_start() {
        // end exactly at bound_start still counts as fully-before.
        let (s, e) = clamp_time_range(Some(2), Some(10), Some(10), Some(20));
        assert_eq!(s, Some(10));
        assert_eq!(e, Some(10));
    }

    #[test]
    fn truncate_to_byte_limit_short_string_unchanged() {
        let out = truncate_to_byte_limit("hello", 10, "...");
        assert_eq!(out, "hello");
    }

    #[test]
    fn truncate_to_byte_limit_ascii_truncates() {
        let out = truncate_to_byte_limit("0123456789abcdef", 5, "...");
        assert_eq!(out, "01234...");
    }

    #[test]
    fn truncate_to_byte_limit_steps_back_to_char_boundary() {
        // 'é' is 2 bytes; cutting at 5 lands mid-char, must step back to 4.
        let out = truncate_to_byte_limit("café café", 5, "...");
        assert_eq!(out, "café...");
    }

    #[test]
    fn action_completed_event_from_buffered() {
        let act = BufferedAction {
            label: Some("//pkg:t".to_string()),
            mnemonic: Some("Javac".to_string()),
            success: true,
            exit_code: Some(0),
            exit_code_name: None,
            primary_output: Some("out.jar".to_string()),
            configuration: None,
            command_line: vec!["javac".to_string(), "Main.java".to_string()],
            stdout_uri: None,
            stderr_uri: None,
            start_nanos: Some(100),
            end_nanos: Some(200),
            cached: Some(false),
            hostname: None,
            cached_remotely: None,
            runner: None,
        };
        let ev = ActionCompletedEvent::from_buffered(&act);
        assert_eq!(ev.label, Some("//pkg:t"));
        assert_eq!(ev.mnemonic, Some("Javac"));
        assert!(ev.success);
        assert_eq!(ev.command_line.len(), 2);
    }
}

