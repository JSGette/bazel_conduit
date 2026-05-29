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
//!   - bazel.progress (one record per BEP `Progress` event, streamed as the
//!     build runs and correlated with the root span via trace_id/span_id;
//!     each record's stderr/stdout body is ANSI-stripped and capped at 1 MB).
//!     Without a `LoggerProvider`, all progress text accumulates in memory
//!     and is attached to the root span as a single `build.log` event in
//!     `finish`.
//!
//! Action processing modes:
//!   - lightweight (default): only failed actions create spans
//!   - full (--build_event_publish_all_actions): every action gets a span
//!     with accurate start_time / end_time from the ActionExecuted event
//! ```

use std::borrow::Cow;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use opentelemetry::logs::{AnyValue, LogRecord as _, Logger as _, Severity};
use opentelemetry::trace::{Span, SpanContext, SpanKind, Status, TraceContextExt, Tracer};
use opentelemetry::{Context, KeyValue};
use spawn_proto::tools::protos::SpawnExec;
use tracing::{debug, info, warn};

use crate::exec_log::tailer::TailerHandle;

use super::attributes::*;
use super::redact::Redactor;
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
    pub span: Option<opentelemetry_sdk::trace::Span>,
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
fn spawn_time_range(s: &SpawnExec) -> Option<(i64, i64)> {
    let m = s.metrics.as_ref()?;
    let start = m.start_time.as_ref()?;
    let total = m.total_time.as_ref()?;
    let start_nanos = start.seconds * 1_000_000_000 + i64::from(start.nanos);
    let total_nanos = total.seconds * 1_000_000_000 + i64::from(total.nanos);
    Some((start_nanos, start_nanos + total_nanos))
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

    /// Fallback buffers for the no-logger path: when no `LoggerProvider` is
    /// configured, progress text accumulates here and is attached to the root
    /// span as a single `build.log` event in [`finish`]. Each stream is capped
    /// at 1 MB (see [`PROGRESS_CAP_BYTES`]). Unused on the streaming path.
    progress_stderr: String,
    progress_stdout: String,

    /// Optional OTel logger. When present, every BEP `Progress` event is
    /// emitted as its own correlated log record (streaming) rather than
    /// buffered until the build ends.
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

    /// Relative compact-log path observed before `workspace_directory` was
    /// known. Retried from `on_build_started_extended`.
    pending_exec_log_path: Option<PathBuf>,

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

    /// Count of SpawnExec entries received from the tailer. Surfaced as
    /// `bazel.exec_log.spawns_received` on the `tailer_finished` root-span
    /// event so a trace consumer can see whether the tailer produced data
    /// without grep'ing conduit's logs.
    compact_spawns_received: u64,

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
        tracer: opentelemetry_sdk::trace::Tracer,
        logger: Option<opentelemetry_sdk::logs::Logger>,
    ) -> Self {
        Self {
            tracer,
            root_context: None,
            configured_targets: HashMap::new(),
            target_contexts: HashMap::new(),
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
            compact_streaming_active: false,
            exec_log_state: None,
            pending_exec_log_path: None,
            action_span_cache: HashMap::new(),
            pending_spawns: HashMap::new(),
            compact_spawns_received: 0,
            workspace_directory: None,
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
            self.root_span_end_from_wall_nanos.or(self.finish_time_nanos),
        )
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
    ) -> opentelemetry_sdk::trace::Span {
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
                let path = PathBuf::from(v);
                if let Some(name) = detect_workspace_name(&path) {
                    self.set_root_attr(KeyValue::new(VCS_REPOSITORY_NAME, name));
                }
                self.workspace_directory = Some(path);
            }
        }
        // Retry a previously skipped relative exec-log path once workspace is known.
        if let Some(path) = self.pending_exec_log_path.take() {
            self.on_exec_log_detected(path);
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
            let scrubbed = self.redactor.scrub_args(startup_options);
            self.set_root_attr(KeyValue::new(BAZEL_STARTUP_OPTIONS, scrubbed.join(" ")));
        }
        if !explicit_cmd_line.is_empty() {
            let scrubbed = self.redactor.scrub_args(explicit_cmd_line);
            self.set_root_attr(KeyValue::new(BAZEL_EXPLICIT_CMD_LINE, scrubbed.join(" ")));
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

    /// Compact-exec-log path detected (typically from BEP `OptionsParsed`).
    /// Spawns the tail-follow worker; arriving [`SpawnExec`] entries are
    /// drained at every BEP-event boundary via [`Self::pump_compact_spawns`].
    ///
    /// Bazel resolves `--execution_log_compact_file=<rel>` against the client
    /// cwd (i.e. the workspace), not against conduit's cwd. We replicate that
    /// here so the tailer opens the same file Bazel writes.
    pub fn on_exec_log_detected(&mut self, path: PathBuf) {
        if self.compact_streaming_active {
            warn!("on_exec_log_detected called twice; ignoring second");
            return;
        }
        let resolved = if path.is_absolute() {
            path
        } else {
            match self.workspace_directory.as_ref() {
                Some(ws) => ws.join(&path),
                None => {
                    self.pending_exec_log_path = Some(path.clone());
                    warn!(
                        path = %path.display(),
                        "Compact exec log path is relative but workspace_directory is not set yet; skipping tailer"
                    );
                    self.add_root_event(
                        "bazel.exec_log.tailer_skipped",
                        vec![
                            KeyValue::new("path", path.display().to_string()),
                            KeyValue::new("reason", "workspace_directory_not_set"),
                        ],
                    );
                    return;
                }
            }
        };
        self.start_compact_tailer(resolved);
    }

    fn start_compact_tailer(&mut self, path: PathBuf) {
        let handle = crate::exec_log::tailer::spawn(
            path.clone(),
            self.exec_log_max_message_bytes,
            self.exec_log_max_decompressed_bytes,
        );
        info!(path = %path.display(), "Started compact exec log tailer");
        self.add_root_event(
            "bazel.exec_log.tailer_started",
            vec![KeyValue::new("path", path.display().to_string())],
        );
        self.exec_log_state = Some(ExecLogState { path, handle });
        self.compact_streaming_active = true;
    }

    /// `--execution_log_binary_file` / `--execution_log_json_file` observed
    /// in BEP `OptionsParsed`. Neither format is supported by conduit's
    /// live streaming pipeline (no dedup table, no zstd framing); record a
    /// root-span event so users can spot the misconfiguration in their
    /// trace UI without grepping the conduit log.
    pub fn on_unsupported_exec_log_flag(&mut self, flags: &str) {
        self.add_root_event(
            "bazel.exec_log.unsupported_format",
            vec![
                KeyValue::new("flags", flags.to_string()),
                KeyValue::new(
                    "supported",
                    "--execution_log_compact_file".to_string(),
                ),
            ],
        );
    }

    /// WorkspaceStatus → add workspace attributes to root span.
    /// BUILD_USER and BUILD_HOST are mapped to bazel.user/bazel.host directly
    /// (same semantic as the BuildStarted fields) to avoid duplication.
    ///
    /// `STABLE_*` keys produced by `--workspace_status_command` routinely
    /// carry CI-injected secrets (`STABLE_CI_JOB_TOKEN`, `STABLE_GIT_TOKEN`,
    /// etc.), so each value is run through the redactor's name-based
    /// scrubber before it lands on the root span.
    pub fn on_workspace_status(&mut self, items: &HashMap<String, String>) {
        for (key, value) in items {
            let attr_key = match key.as_str() {
                "BUILD_USER" => BAZEL_WORKSPACE_USER.to_string(),
                "BUILD_HOST" => BAZEL_WORKSPACE_HOST.to_string(),
                _ => format!("bazel.workspace.{}", key.to_lowercase()),
            };
            let scrubbed = self.redactor.scrub_value_by_name(key, value).into_owned();
            self.set_root_attr(KeyValue::new(attr_key, scrubbed));
        }
    }

    /// UnstructuredCommandLine → add full command line as attribute.
    pub fn on_command_line(&mut self, args: &[String]) {
        if !args.is_empty() {
            let scrubbed = self.redactor.scrub_args(args);
            self.set_root_attr(KeyValue::new(BAZEL_COMMAND_LINE, scrubbed.join(" ")));
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

        let raw_start = start_nanos_override.or(configured.event_time_nanos);
        let (start_nanos, _) = self.clamp_to_invocation(raw_start, None);
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
        if self.target_contexts.contains_key(label) {
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
        let (clamped_start, _) = self.clamp_to_invocation(start_nanos, None);
        if let Some(nanos) = clamped_start {
            builder = builder.with_start_time(nanos_to_system_time(nanos));
        }
        let span = self.tracer.build_with_context(builder, parent);
        Context::new().with_span(span)
    }

    /// TargetCompleted → create (or reuse) the target span, set attributes,
    /// resolve output file sets from whatever's currently in the
    /// `NamedSet` cache (`resolve_files` skips missing IDs), and end the
    /// span synchronously.
    pub fn on_target_completed(
        &mut self,
        label: &str,
        success: bool,
        file_set_ids: &[String],
        tags: &[String],
        event_time_nanos: Option<i64>,
    ) {
        // Drain any spawns that flushed between the last ActionExecuted and
        // this TargetCompleted; they need to land on the action span before
        // `end_open_actions_for_target` seals it.
        self.pump_compact_spawns();

        // Clamp end-of-target to the invocation window. Cached targets re-emit
        // their original action timestamps, so without this a span ending at
        // event_time will run from the cached `start` (weeks ago) to today.
        let (_, effective_end) = self.clamp_to_invocation(None, event_time_nanos);

        // Get or create the span under the appropriate parent.
        let cx = if let Some(cx) = self.target_contexts.remove(label) {
            cx
        } else if let Some(configured) = self.configured_targets.remove(label) {
            let parent = self.choose_parent_for_label(label);
            if let Some(parent) = parent {
                let cx = self.create_target_span(label, &configured, &parent, None);
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

        let status = if success {
            Status::Ok
        } else {
            Status::Error {
                description: Cow::Borrowed("target failed"),
            }
        };
        cx.span().set_status(status);

        // End any action spans we held open for this target. Deterministic
        // boundary — Bazel guarantees no further ActionExecuted events for
        // this target. Anything still open without a matching SpawnExec at
        // this point picks up `bazel.action.spawn.missing=true`.
        self.end_open_actions_for_target(label, effective_end);

        Self::set_output_attributes_and_end_target_span(
            &self.named_set_cache,
            &cx,
            file_set_ids,
            effective_end,
        );
        debug!("Ended target span for {label} (success={success})");
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

        let (_, clamped_event) = self.clamp_to_invocation(None, event_time_nanos);
        let instant = clamped_event
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

    /// Recursively collect all files reachable from the given NamedSet IDs.
    /// IDs missing from the cache are silently skipped.
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

    /// Cache a NamedSet so its files can be resolved when targets complete.
    ///
    /// A NamedSet can contain direct `files` AND references to other
    /// NamedSets (`child_set_ids`); both are stored so [`resolve_files`]
    /// can collect the transitive closure.
    pub fn on_named_set(&mut self, set_id: &str, files: Vec<String>, child_set_ids: &[String]) {
        self.named_set_cache.insert(
            set_id.to_string(),
            NamedSetEntry {
                files,
                child_set_ids: child_set_ids.to_vec(),
            },
        );
    }

    // =====================================================================
    // Action events
    // =====================================================================

    /// ActionCompleted → create `action {mnemonic} {label}` span. With the
    /// compact exec log enabled the span is kept open so arriving spawns can
    /// backfill attrs onto it; without it the span ends here as today.
    ///
    /// In **lightweight** mode only failed actions reach this method.
    /// In **full** mode every action (success or failure) is mapped.
    ///
    /// When `start_time_nanos` / `end_time_nanos` are available (from the
    /// `ActionExecuted.start_time` / `end_time` proto fields) they are used
    /// for accurate span timing.
    pub fn on_action_completed(&mut self, ev: &ActionCompletedEvent<'_>) {
        // Drain whatever the tailer has produced so far. Cheap; runs the
        // backfill against actions that already landed in the cache.
        self.pump_compact_spawns();

        if let Some(l) = ev.label {
            self.ensure_target_span(l, ev.start_time_nanos);
        }

        let parent = ev
            .label
            .and_then(|l| self.target_contexts.get(l))
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
            let scrubbed = self.redactor.scrub_args(ev.command_line);
            let joined = scrubbed.join(" ");
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

        let key = ActionSpanKey::new(ev.label, ev.mnemonic, ev.primary_output);
        let target_label = ev.label.map(str::to_string).unwrap_or_default();
        let defer_end = self.compact_streaming_active;

        let mut span =
            self.build_child_span(&parent, span_name, SpanKind::Internal, attrs, clamped_start);
        span.set_status(status);
        let span_context = span.span_context().clone();

        let info = ActionSpanInfo {
            span_context,
            start_nanos: clamped_start,
            end_nanos: clamped_end,
        };

        let open = if defer_end {
            OpenAction {
                info,
                span: Some(span),
                target_label,
                spawn_count: 0,
            }
        } else {
            if let Some(nanos) = clamped_end {
                span.end_with_timestamp(nanos_to_system_time(nanos));
            } else {
                span.end();
            }
            OpenAction {
                info,
                span: None,
                target_label,
                spawn_count: 0,
            }
        };
        self.action_span_cache.insert(key.clone(), open);

        // Re-check pending spawns for any spawn that the just-inserted action
        // is the parent of. Order: a spawn that arrived first via the tailer,
        // then its ActionExecuted via BEP — flush the buffered spawn now so
        // it backfills attrs on the freshly-built span.
        if defer_end {
            self.flush_pending_spawns_for_action(&key);
        }

        debug!(
            success = ev.success,
            label = ?ev.label,
            defer_end,
            "Recorded action span",
        );
    }

    /// Drain everything the compact-log tailer has produced through the mpsc.
    /// Each [`SpawnExec`] either finds its parent action in
    /// [`Self::action_span_cache`] (backfill + emit child span) or lands in
    /// [`Self::pending_spawns`] to be retried by
    /// [`Self::flush_pending_spawns_for_action`] when the parent action
    /// arrives, or finally synthesised at [`Self::finish`].
    ///
    /// Non-blocking: called from BEP event handlers, never blocks the gRPC
    /// receive loop. The tailer's mpsc buffer absorbs bursts.
    pub(super) fn pump_compact_spawns(&mut self) {
        let mut drained = Vec::new();
        if let Some(state) = self.exec_log_state.as_mut() {
            while let Ok(spawn) = state.handle.rx.try_recv() {
                drained.push(spawn);
            }
        }
        for spawn in drained {
            self.on_compact_spawn(spawn);
        }
    }

    /// One [`SpawnExec`] from the tailer: match it against an open action,
    /// emit the child spawn span, and (on the action's first spawn only)
    /// backfill curated attrs onto the parent. Unmatched spawns are queued by
    /// action key for targeted flush once their ActionExecuted arrives.
    pub(crate) fn on_compact_spawn(&mut self, spawn: SpawnExec) {
        self.compact_spawns_received = self.compact_spawns_received.saturating_add(1);
        // Exact-match path. Fuzzy matching (output-dir / suffix) is
        // deliberately not run live — multi-spawn actions confuse fuzzy
        // matching, and we'd rather emit a child span under a synthesised
        // parent at finish() than misattribute attrs onto a sibling action.
        let key = crate::exec_log::spawn_exec_candidate_keys(&spawn)
            .into_iter()
            .find(|k| self.action_span_cache.contains_key(k));
        let Some(key) = key else {
            self.push_pending_spawn(spawn);
            return;
        };
        self.apply_spawn_to_action(&key, spawn);
    }

    /// Apply a single spawn to a known-present action cache entry: bump the
    /// counter, backfill attrs on the first one, emit the child span.
    fn apply_spawn_to_action(&mut self, key: &ActionSpanKey, spawn: SpawnExec) {
        let (parent_info, is_first) = {
            let action = self
                .action_span_cache
                .get_mut(key)
                .expect("apply_spawn_to_action called with key not in cache");
            let is_first = action.spawn_count == 0;
            if is_first {
                if let Some(span) = action.span.as_mut() {
                    crate::exec_log::apply_spawn_attrs_to_action(span, &spawn);
                }
            }
            action.spawn_count = action.spawn_count.saturating_add(1);
            (action.info.clone(), is_first)
        };
        debug!(
            label = %key.target_label,
            mnemonic = %key.mnemonic,
            first = is_first,
            "Applied compact spawn to action span",
        );
        crate::exec_log::emit_spawn_span(&self.tracer, &parent_info, &spawn, &self.redactor);
    }

    fn push_pending_spawn(&mut self, spawn: SpawnExec) {
        let key = crate::exec_log::spawn_exec_candidate_keys(&spawn)
            .into_iter()
            .next()
            .unwrap_or_else(|| {
                ActionSpanKey::new(
                    Some(spawn.target_label.as_str()),
                    Some(spawn.mnemonic.as_str()),
                    Some(""),
                )
            });
        self.pending_spawns.entry(key).or_default().push(spawn);
    }

    /// Move buffered spawns for `key` from [`Self::pending_spawns`] into the
    /// action cache once ActionExecuted has arrived.
    fn flush_pending_spawns_for_action(&mut self, key: &ActionSpanKey) {
        let Some(matched) = self.pending_spawns.remove(key) else {
            return;
        };
        for spawn in matched {
            self.apply_spawn_to_action(key, spawn);
        }
    }

    /// Record an end timestamp hint for every still-open action span belonging
    /// to `target_label`. The action spans stay open until `finish()` so late
    /// tailer arrivals can still backfill attrs before finalisation.
    ///
    /// `fallback_end_nanos` is used when the action's own clamped end is
    /// `None` (an action completed without timing data) — typically the
    /// target's effective end time.
    pub(super) fn end_open_actions_for_target(
        &mut self,
        target_label: &str,
        fallback_end_nanos: Option<i64>,
    ) {
        let normalized = normalize_label(target_label);
        for action in self.action_span_cache.values_mut() {
            if action.span.is_some()
                && normalize_label(&action.target_label) == normalized
                && action.info.end_nanos.is_none()
            {
                action.info.end_nanos = fallback_end_nanos;
            }
        }
    }

    fn finalize_action_summary_attrs(span: &mut opentelemetry_sdk::trace::Span, spawn_count: u32) {
        span.set_attribute(KeyValue::new(BAZEL_ACTION_SPAWN_COUNT, i64::from(spawn_count)));
        if spawn_count == 0 {
            span.set_attribute(KeyValue::new(BAZEL_ACTION_SPAWN_MISSING, true));
        }
    }

    /// Finalize and end the in-place action span for `key`. No-op if the span
    /// has already been ended (e.g. compact log disabled).
    fn finalize_and_end_action_entry(
        &mut self,
        key: &ActionSpanKey,
        fallback_end_nanos: Option<i64>,
    ) {
        let Some(action) = self.action_span_cache.get_mut(key) else {
            return;
        };
        let Some(mut span) = action.span.take() else {
            return;
        };
        Self::finalize_action_summary_attrs(&mut span, action.spawn_count);
        let end_nanos = action.info.end_nanos.or(fallback_end_nanos);
        if let Some(nanos) = end_nanos {
            span.end_with_timestamp(nanos_to_system_time(nanos));
        } else {
            span.end();
        }
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

        let raw_end = match (ev.start_time_nanos, ev.duration_nanos) {
            (Some(start), Some(dur)) => Some(start + dur),
            _ => None,
        };
        // Cached test results carry the original execution timestamps; clamp
        // so they don't escape the current invocation's window.
        let (start_nanos, end_nanos) =
            self.clamp_to_invocation(ev.start_time_nanos, raw_end);

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

    /// Progress with stderr/stdout. With a logger configured, each event is
    /// emitted immediately as a correlated `bazel.progress` log record
    /// (streaming, capped at [`PROGRESS_CAP_BYTES`] per record). Without a
    /// logger, content accumulates in [`progress_stderr`]/[`progress_stdout`]
    /// and is attached as a single span event in [`finish`].
    pub fn on_progress(&mut self, stderr: Option<&str>, stdout: Option<&str>) {
        let stderr = stderr.unwrap_or("");
        let stdout = stdout.unwrap_or("");
        if stderr.is_empty() && stdout.is_empty() {
            return;
        }

        if self.logger.is_some() && self.root_context.is_some() {
            self.emit_progress_log(stderr, stdout);
            return;
        }

        if !stderr.is_empty() {
            let cleaned = self.redactor.scrub_text(&strip_ansi(stderr)).into_owned();
            append_progress_capped(&mut self.progress_stderr, &cleaned);
        }
        if !stdout.is_empty() {
            let cleaned = self.redactor.scrub_text(&strip_ansi(stdout)).into_owned();
            append_progress_capped(&mut self.progress_stdout, &cleaned);
        }
    }

    /// Emit one `bazel.progress` log record correlated with the root span.
    /// Body is stderr (Bazel's primary output stream); stdout, when present,
    /// rides as an attribute. Each stream is ANSI-stripped and capped per
    /// record so a single noisy progress message can't blow up the exporter.
    fn emit_progress_log(&self, stderr: &str, stdout: &str) {
        let Some(logger) = self.logger.as_ref() else { return };
        let Some(cx) = self.root_context.as_ref() else { return };
        let span_cx = cx.span().span_context().clone();

        let stderr = if stderr.is_empty() {
            String::new()
        } else {
            let scrubbed = self.redactor.scrub_text(&strip_ansi(stderr)).into_owned();
            truncate_to_byte_limit(&scrubbed, PROGRESS_CAP_BYTES, "...(truncated)")
        };
        let stdout = if stdout.is_empty() {
            String::new()
        } else {
            let scrubbed = self.redactor.scrub_text(&strip_ansi(stdout)).into_owned();
            truncate_to_byte_limit(&scrubbed, PROGRESS_CAP_BYTES, "...(truncated)")
        };

        let mut record = logger.create_log_record();
        record.set_trace_context(
            span_cx.trace_id(),
            span_cx.span_id(),
            Some(span_cx.trace_flags()),
        );
        record.set_severity_number(Severity::Info);
        record.set_severity_text("INFO");
        record.add_attribute("bazel.log.kind", AnyValue::String("progress".into()));

        let stderr_len = stderr.len();
        let stdout_len = stdout.len();
        match (!stderr.is_empty(), !stdout.is_empty()) {
            (true, true) => {
                record.set_body(AnyValue::String(stderr.into()));
                record.add_attribute(BAZEL_PROGRESS_STDOUT, AnyValue::String(stdout.into()));
            }
            (true, false) => record.set_body(AnyValue::String(stderr.into())),
            (false, true) => record.set_body(AnyValue::String(stdout.into())),
            (false, false) => return,
        }

        logger.emit(record);
        debug!(
            trace_id = %span_cx.trace_id(),
            stderr_len,
            stdout_len,
            "Streamed bazel.progress log record"
        );
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
        if let Some(state) = self.exec_log_state.take() {
            state.handle.shutdown();
        }
        self.pending_exec_log_path = None;
        self.compact_streaming_active = false;
        self.action_span_cache.clear();
        self.pending_spawns.clear();
        self.compact_spawns_received = 0;
        self.workspace_directory = None;
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
        let Some(mut state) = self.exec_log_state.take() else {
            return;
        };
        state.handle.shutdown();
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        let mut drained = Vec::new();
        loop {
            let mut got_any = false;
            while let Ok(spawn) = state.handle.rx.try_recv() {
                drained.push(spawn);
                got_any = true;
            }
            // Exit once the worker has exited and we've observed an empty poll,
            // or once the bounded budget is exhausted.
            if std::time::Instant::now() >= deadline {
                break;
            }
            if state.handle.is_finished() && !got_any {
                break;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        // Poll for worker exit before reading the error slot. The blocking
        // thread may still be in a short sleep window; wait briefly for a
        // clean terminal state, then do one final non-blocking drain.
        let worker_exit_deadline = std::time::Instant::now() + Duration::from_millis(300);
        while !state.handle.is_finished()
            && std::time::Instant::now() < worker_exit_deadline
        {
            std::thread::sleep(Duration::from_millis(20));
        }
        while let Ok(spawn) = state.handle.rx.try_recv() {
            drained.push(spawn);
        }
        debug!(
            count = drained.len(),
            "Drained compact exec log tailer at finish()",
        );
        for spawn in drained {
            self.on_compact_spawn(spawn);
        }
        // Worker error (if any) is only readable now — the shutdown sleep
        // above gives the blocking task time to populate the slot before
        // we read it here.
        let worker_error = state.handle.take_error();
        let mut attrs = vec![KeyValue::new(
            "spawns_received",
            self.compact_spawns_received as i64,
        )];
        if let Some(err) = worker_error {
            attrs.push(KeyValue::new("status", "failed"));
            attrs.push(KeyValue::new("error", err));
        } else {
            attrs.push(KeyValue::new("status", "ok"));
        }
        self.add_root_event("bazel.exec_log.tailer_finished", attrs);
    }

    /// Build synthetic parent action spans for spawns left in
    /// [`Self::pending_spawns`] after `drain_and_stop_tailer`. These are
    /// spawns whose `ActionExecuted` never arrived on BEP — most commonly
    /// because the user passed `--nobuild_event_publish_all_actions` and
    /// the action was a non-failure. Group by
    /// `(target_label, mnemonic, primary_output)` to mirror the on-disk
    /// shape; emit child spawn spans under each synthetic parent and
    /// backfill the same curated attrs onto it.
    fn synthesise_orphan_actions(&mut self) {
        if self.pending_spawns.is_empty() {
            return;
        }
        let mut groups: HashMap<ActionSpanKey, Vec<SpawnExec>> = HashMap::new();
        for (key, spawns) in self.pending_spawns.drain() {
            groups.entry(key).or_default().extend(spawns);
        }
        let group_count = groups.len();
        for (key, spawns) in groups {
            self.synthesise_one_orphan_action(key, spawns);
        }
        debug!(
            groups = group_count,
            "Synthesised orphan action spans from unmatched compact-log spawns",
        );
    }

    fn synthesise_one_orphan_action(&mut self, key: ActionSpanKey, spawns: Vec<SpawnExec>) {
        // Parent picks target if BEP told us about it, else root. We don't
        // create a target span on the fly — a target that never appeared in
        // BEP is itself an orphan, beyond the scope of compact-log backfill.
        let parent = self
            .target_contexts
            .get(&key.target_label)
            .cloned()
            .or_else(|| self.root_context.clone());
        let Some(parent) = parent else {
            warn!(
                label = %key.target_label,
                mnemonic = %key.mnemonic,
                "Cannot synthesise orphan action: no parent context (root span missing?)",
            );
            return;
        };

        // Bounds: earliest start to latest end across the group's spawns.
        // `SpawnMetrics.start_time` + `total_time` gives an absolute end.
        let (raw_start, raw_end) = spawns
            .iter()
            .filter_map(spawn_time_range)
            .fold((None::<i64>, None::<i64>), |(s, e), (ns, ne)| {
                (
                    Some(s.map_or(ns, |v| v.min(ns))),
                    Some(e.map_or(ne, |v| v.max(ne))),
                )
            });
        let (clamped_start, clamped_end) = self.clamp_to_invocation(raw_start, raw_end);

        let span_name = if key.mnemonic.is_empty() {
            format!("action {} (synth)", shorten_label(&key.target_label))
        } else {
            format!(
                "action {} {} (synth)",
                key.mnemonic,
                shorten_label(&key.target_label)
            )
        };

        let mut attrs = vec![
            KeyValue::new(BAZEL_ACTION_SUCCESS, true),
            KeyValue::new(BAZEL_TARGET_SYNTHETIC, true),
        ];
        if !key.mnemonic.is_empty() {
            attrs.push(KeyValue::new(BAZEL_ACTION_MNEMONIC, key.mnemonic.clone()));
        }
        if !key.target_label.is_empty() {
            attrs.push(KeyValue::new(BAZEL_ACTION_LABEL, key.target_label.clone()));
            attrs.push(KeyValue::new(
                BAZEL_ACTION_LABEL_SHORT,
                shorten_label(&key.target_label).to_string(),
            ));
        }
        if !key.primary_output.is_empty() {
            attrs.push(KeyValue::new(
                BAZEL_ACTION_PRIMARY_OUTPUT,
                key.primary_output.clone(),
            ));
        }

        let mut span =
            self.build_child_span(&parent, span_name, SpanKind::Internal, attrs, clamped_start);
        let span_context = span.span_context().clone();
        if let Some(first) = spawns.first() {
            crate::exec_log::apply_spawn_attrs_to_action(&mut span, first);
        }
        span.set_attribute(KeyValue::new(
            BAZEL_ACTION_SPAWN_COUNT,
            i64::try_from(spawns.len()).unwrap_or(i64::MAX),
        ));

        let info = ActionSpanInfo {
            span_context,
            start_nanos: clamped_start,
            end_nanos: clamped_end,
        };
        for spawn in &spawns {
            crate::exec_log::emit_spawn_span(&self.tracer, &info, spawn, &self.redactor);
        }

        if let Some(nanos) = clamped_end {
            span.end_with_timestamp(nanos_to_system_time(nanos));
        } else {
            span.end();
        }
    }

    /// Finalize and end every action span still held open in
    /// [`Self::action_span_cache`]. Called from `finish()` after the tailer
    /// drain and orphan synthesis, so `spawn.missing` decisions are based on
    /// the complete compact-log stream.
    fn finalize_and_end_remaining_open_actions(&mut self) {
        let fallback = self.root_span_end_from_wall_nanos.or(self.finish_time_nanos);
        let keys: Vec<ActionSpanKey> = self
            .action_span_cache
            .iter()
            .filter(|(_, action)| action.span.is_some())
            .map(|(k, _)| k.clone())
            .collect();
        for key in keys {
            self.finalize_and_end_action_entry(&key, fallback);
        }
    }

    /// End all remaining spans (call after last BEP event).
    pub fn finish(&mut self) {
        // Drain the compact-log tailer's post-`close()` final chunk before
        // sealing any spans. zstd-jni's 128 KiB input buffer means the last
        // batch of spawns only hits disk when Bazel runs `close()` on the
        // log stream — which happens before BuildCompleteEvent is emitted,
        // but the chunk + frame terminator may still be in flight through
        // the tailer's mpsc when we get here.
        self.drain_and_stop_tailer();

        // Anything still buffered after the drain never matched a BEP
        // ActionExecuted (e.g. `--build_event_publish_all_actions=false`).
        // Synthesise parent action spans for them.
        self.synthesise_orphan_actions();

        // Finalise action-level spawn summaries only after the tailer has
        // fully drained, so late chunk arrivals cannot create false
        // `bazel.action.spawn.missing=true` on parent action spans.
        self.finalize_and_end_remaining_open_actions();

        // End the `fetches` parent span.
        if let Some(cx) = self.fetches_context.take() {
            cx.span().set_status(Status::Ok);
            cx.span().end();
            debug!("Ended fetches span");
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

/// Resolve a human-readable name for the Bazel workspace producing this
/// trace. Used to populate [`VCS_REPOSITORY_NAME`] on the root span.
///
/// Lookup order (first non-empty wins):
///   1. `MODULE.bazel` → `module(name = "...")` (bzlmod, default since 7.0).
///   2. `WORKSPACE.bazel` / `WORKSPACE` → `workspace(name = "...")` (legacy).
///   3. Basename of `workspace_dir` (e.g. `/repos/bazel_conduit` → `bazel_conduit`).
///
/// I/O happens once per build at `BuildStarted` — both candidate files are
/// small (typically <2 KiB) and read synchronously. Failures fall through
/// to the next source rather than propagating, since this attribute is a
/// hint rather than load-bearing.
fn detect_workspace_name(workspace_dir: &Path) -> Option<String> {
    if let Some(name) = read_starlark_name(&workspace_dir.join("MODULE.bazel"), "module") {
        return Some(name);
    }
    for candidate in ["WORKSPACE.bazel", "WORKSPACE"] {
        if let Some(name) = read_starlark_name(&workspace_dir.join(candidate), "workspace") {
            return Some(name);
        }
    }
    let basename = workspace_dir.file_name()?.to_str()?;
    if basename.is_empty() {
        None
    } else {
        Some(basename.to_string())
    }
}

/// Pull `name = "<value>"` out of the first `<func>(...)` block in a
/// MODULE.bazel / WORKSPACE.bazel file. Substring-based and deliberately
/// dumb — Starlark string escaping, commented-out blocks, or unusual
/// formatting (e.g. `module (\n name="x"\n)`) trips it. That's fine for
/// the canonical idioms emitted by `bazel new`, `bazel mod tidy`, and
/// every real-world MODULE.bazel I've seen; degenerate inputs fall
/// through to the basename in `detect_workspace_name`.
fn read_starlark_name(path: &Path, func: &str) -> Option<String> {
    let content = std::fs::read_to_string(path).ok()?;
    let needle = format!("{func}(");
    let start = content.find(&needle)?;
    let after = &content[start + needle.len()..];
    // Bound the search to the matching `)` so we don't grab `name=` out
    // of a later `bazel_dep(name = "...")`.
    let mut depth = 1i32;
    let mut end = after.len();
    for (i, c) in after.char_indices() {
        match c {
            '(' => depth += 1,
            ')' => {
                depth -= 1;
                if depth == 0 {
                    end = i;
                    break;
                }
            }
            _ => {}
        }
    }
    let body = &after[..end];
    let key_pos = body.find("name")?;
    let rest = body[key_pos + 4..].trim_start();
    let after_eq = rest.strip_prefix('=')?.trim_start();
    let after_quote = after_eq.strip_prefix('"')?;
    let close = after_quote.find('"')?;
    let name = &after_quote[..close];
    if name.is_empty() {
        None
    } else {
        Some(name.to_string())
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
    fn detect_workspace_name_reads_module_bazel() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("MODULE.bazel"),
            "module(\n    name = \"my_repo\",\n    version = \"0.1.0\",\n)\n\nbazel_dep(name = \"rules_rust\", version = \"0.40\")\n",
        )
        .unwrap();
        assert_eq!(
            detect_workspace_name(dir.path()).as_deref(),
            Some("my_repo"),
        );
    }

    #[test]
    fn detect_workspace_name_falls_back_to_workspace_file() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("WORKSPACE"),
            "workspace(name = \"legacy_repo\")\n",
        )
        .unwrap();
        assert_eq!(
            detect_workspace_name(dir.path()).as_deref(),
            Some("legacy_repo"),
        );
    }

    #[test]
    fn detect_workspace_name_falls_back_to_basename() {
        let parent = tempfile::tempdir().unwrap();
        let dir = parent.path().join("just_a_dir");
        std::fs::create_dir(&dir).unwrap();
        // No MODULE.bazel or WORKSPACE -> directory basename.
        assert_eq!(
            detect_workspace_name(&dir).as_deref(),
            Some("just_a_dir"),
        );
    }

    #[test]
    fn detect_workspace_name_ignores_bazel_dep_blocks() {
        // The regex-free parser must consume only the first `module(...)`
        // block, not pick up `name=` from a later `bazel_dep(...)`.
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("MODULE.bazel"),
            "module(\n    name = \"the_one\",\n)\n\nbazel_dep(name = \"not_this\", version = \"1.0\")\n",
        )
        .unwrap();
        assert_eq!(
            detect_workspace_name(dir.path()).as_deref(),
            Some("the_one"),
        );
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

    // ---- on_exec_log_detected path resolution ----

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn on_exec_log_detected_resolves_relative_against_workspace() {
        let ws = tempfile::tempdir().unwrap();
        let mut mapper = test_mapper();
        mapper.on_build_started("uuid", "test", Some(1_000_000_000), None);
        mapper.on_build_started_extended(
            ws.path().to_str(),
            None,
            None,
            None,
            None,
            None,
        );
        mapper.on_exec_log_detected(PathBuf::from("exec.log"));

        let state = mapper.exec_log_state.as_ref().expect("tailer should start");
        assert_eq!(state.path, ws.path().join("exec.log"));
        assert!(mapper.compact_streaming_active);

        mapper.drain_and_stop_tailer();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn on_exec_log_detected_passes_absolute_path_through() {
        let ws = tempfile::tempdir().unwrap();
        let absolute = ws.path().join("custom_exec.log");
        let mut mapper = test_mapper();
        mapper.on_build_started("uuid", "test", Some(1_000_000_000), None);
        mapper.on_build_started_extended(
            ws.path().to_str(),
            None,
            None,
            None,
            None,
            None,
        );
        mapper.on_exec_log_detected(absolute.clone());

        let state = mapper.exec_log_state.as_ref().expect("tailer should start");
        assert_eq!(state.path, absolute);

        mapper.drain_and_stop_tailer();
    }

    #[test]
    fn on_exec_log_detected_skips_when_workspace_unknown() {
        let mut mapper = test_mapper();
        mapper.on_build_started("uuid", "test", Some(1_000_000_000), None);
        mapper.on_exec_log_detected(PathBuf::from("exec.log"));
        assert!(mapper.exec_log_state.is_none());
        assert!(!mapper.compact_streaming_active);
        assert_eq!(mapper.pending_exec_log_path, Some(PathBuf::from("exec.log")));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn on_build_started_extended_retries_pending_relative_exec_log() {
        let ws = tempfile::tempdir().unwrap();
        let mut mapper = test_mapper();
        mapper.on_build_started("uuid", "test", Some(1_000_000_000), None);
        mapper.on_exec_log_detected(PathBuf::from("exec.log"));
        assert!(mapper.exec_log_state.is_none());
        assert_eq!(mapper.pending_exec_log_path, Some(PathBuf::from("exec.log")));

        mapper.on_build_started_extended(
            ws.path().to_str(),
            None,
            None,
            None,
            None,
            None,
        );

        let state = mapper.exec_log_state.as_ref().expect("tailer should start after workspace arrives");
        assert_eq!(state.path, ws.path().join("exec.log"));
        assert!(mapper.pending_exec_log_path.is_none());
        mapper.drain_and_stop_tailer();
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
    fn clamp_to_invocation_ignores_unbounded() {
        let mapper = test_mapper();
        let (s, e) = mapper.clamp_to_invocation(Some(10), Some(20));
        assert_eq!(s, Some(10));
        assert_eq!(e, Some(20));
    }

    #[test]
    fn clamp_to_invocation_uses_root_span_window() {
        let mut mapper = test_mapper();
        mapper.root_span_start_nanos = Some(1_000_000_000);
        mapper.root_span_end_from_wall_nanos = Some(2_000_000_000);
        // Cached-action style: start 5 weeks ago, end at invocation event_time.
        let (s, e) = mapper.clamp_to_invocation(Some(1), Some(1_500_000_000));
        assert_eq!(s, Some(1_000_000_000));
        assert_eq!(e, Some(1_500_000_000));
    }

    #[test]
    fn clamp_to_invocation_falls_back_to_finish_time() {
        let mut mapper = test_mapper();
        mapper.root_span_start_nanos = Some(1_000_000_000);
        mapper.finish_time_nanos = Some(2_000_000_000);
        let (s, e) = mapper.clamp_to_invocation(Some(500_000_000), Some(3_000_000_000));
        assert_eq!(s, Some(1_000_000_000));
        assert_eq!(e, Some(2_000_000_000));
    }

    // =====================================================================
    // Compact exec log streaming / enrichment fixtures
    //
    // Each test instruments the mapper with a `CollectingProcessor` so we can
    // inspect every span the pipeline emits. The tailer thread is bypassed —
    // tests inject `SpawnExec` records directly via `on_compact_spawn` and
    // flip `compact_streaming_active` to mimic the deferred-end mode. The
    // tailer's own machinery (file open, blocking-EOF read, zstd decode) is
    // covered by `crate::exec_log::tailer::tests`.
    // =====================================================================

    use opentelemetry::trace::SpanId;
    use opentelemetry_sdk::export::trace::{ExportResult, SpanData, SpanExporter};
    use spawn_proto::tools::protos::{File as ProtoFile, SpawnExec, SpawnMetrics};
    use std::sync::{Arc, Mutex};

    // `SpanExporter::export` returns `futures_util::future::BoxFuture<'static, _>`.
    // We don't depend on `futures-util` directly, so spell the equivalent
    // type out by hand; it's `Pin<Box<dyn Future<Output = T> + Send>>` with
    // the implicit `'static` bound that `Box<dyn Trait>` carries.
    type BoxFuture<T> = std::pin::Pin<Box<dyn std::future::Future<Output = T> + Send>>;

    #[derive(Debug, Default, Clone)]
    struct Collector(Arc<Mutex<Vec<SpanData>>>);

    impl Collector {
        fn snapshot(&self) -> Vec<SpanData> {
            self.0.lock().unwrap().clone()
        }
    }

    impl SpanExporter for Collector {
        fn export(&mut self, batch: Vec<SpanData>) -> BoxFuture<ExportResult> {
            self.0.lock().unwrap().extend(batch);
            Box::pin(async { Ok(()) })
        }
    }

    fn mapper_with_collector() -> (OtelMapper, Collector) {
        use opentelemetry::trace::TracerProvider;
        let collector = Collector::default();
        // SimpleSpanProcessor exports synchronously on each span.end(), so we
        // see attributes immediately on inspection without needing flush().
        let processor = opentelemetry_sdk::trace::SimpleSpanProcessor::new(Box::new(
            collector.clone(),
        ));
        let tp = opentelemetry_sdk::trace::TracerProvider::builder()
            .with_span_processor(processor)
            .build();
        let tracer = tp.tracer("test");
        let mut mapper = OtelMapper::new(tracer, None);
        // Bring the root span up; downstream spans need it as their grand-parent.
        mapper.on_build_started("test-uuid", "build", Some(1_000_000_000), None);
        (mapper, collector)
    }

    fn spawn_exec_with_metrics(
        target: &str,
        mnemonic: &str,
        output: &str,
        runner: &str,
        cache_hit: bool,
    ) -> SpawnExec {
        SpawnExec {
            target_label: target.to_string(),
            mnemonic: mnemonic.to_string(),
            runner: runner.to_string(),
            cache_hit,
            remote_cacheable: true,
            listed_outputs: vec![output.to_string()],
            actual_outputs: vec![ProtoFile {
                path: output.to_string(),
                ..Default::default()
            }],
            metrics: Some(SpawnMetrics {
                start_time: Some(spawn_proto::timestamp_proto::google::protobuf::Timestamp {
                    seconds: 1,
                    nanos: 100_000_000,
                }),
                total_time: Some(spawn_proto::duration_proto::google::protobuf::Duration {
                    seconds: 0,
                    nanos: 400_000_000,
                }),
                execution_wall_time: Some(
                    spawn_proto::duration_proto::google::protobuf::Duration {
                        seconds: 0,
                        nanos: 300_000_000,
                    },
                ),
                queue_time: Some(spawn_proto::duration_proto::google::protobuf::Duration {
                    seconds: 0,
                    nanos: 50_000_000,
                }),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    fn action_event_for<'a>(
        label: &'a str,
        mnemonic: &'a str,
        output: &'a str,
    ) -> ActionCompletedEvent<'a> {
        ActionCompletedEvent {
            label: Some(label),
            mnemonic: Some(mnemonic),
            success: true,
            exit_code: Some(0),
            exit_code_name: None,
            primary_output: Some(output),
            configuration: None,
            command_line: &[],
            stdout_path: None,
            stderr_path: None,
            start_time_nanos: Some(1_000_000_000),
            end_time_nanos: Some(1_500_000_000),
            cached: Some(false),
            hostname: None,
            cached_remotely: None,
            runner: None,
        }
    }

    /// Find the first span matching `pred` in the collector's output.
    fn find_span(spans: &[SpanData], pred: impl Fn(&SpanData) -> bool) -> Option<SpanData> {
        spans.iter().find(|s| pred(s)).cloned()
    }

    /// Read a single attribute (returns the rendered display string).
    fn attr_value<'a>(span: &'a SpanData, key: &str) -> Option<String> {
        span.attributes
            .iter()
            .find(|kv| kv.key.as_str() == key)
            .map(|kv| kv.value.to_string())
    }

    /// Scenario 1: BEP action + a single matching SpawnExec → action span
    /// carries runner / cache_hit / SpawnMetrics, plus exactly one child
    /// spawn span with `bazel.spawn.*` attrs.
    #[test]
    fn cache_hit_single_spawn_backfills_action_span() {
        let (mut mapper, collector) = mapper_with_collector();
        mapper.compact_streaming_active = true;

        mapper.on_action_completed(&action_event_for(
            "//pkg:foo",
            "CppCompile",
            "bazel-out/foo.o",
        ));
        mapper.on_compact_spawn(spawn_exec_with_metrics(
            "//pkg:foo",
            "CppCompile",
            "bazel-out/foo.o",
            "remote cache hit",
            true,
        ));
        // Close out the action span (TargetCompleted boundary in real life).
        mapper.end_open_actions_for_target("//pkg:foo", Some(2_000_000_000));
        mapper.finish();

        let spans = collector.snapshot();
        let action = find_span(&spans, |s| s.name.starts_with("action CppCompile"))
            .expect("action span emitted");
        assert_eq!(
            attr_value(&action, BAZEL_ACTION_SPAWN_RUNNER).as_deref(),
            Some("remote cache hit"),
        );
        assert_eq!(
            attr_value(&action, BAZEL_ACTION_SPAWN_CACHE_HIT).as_deref(),
            Some("true"),
        );
        assert_eq!(
            attr_value(&action, BAZEL_ACTION_SPAWN_COUNT).as_deref(),
            Some("1"),
        );
        // exec_wall = 300 ms.
        assert_eq!(
            attr_value(&action, BAZEL_ACTION_SPAWN_EXEC_WALL_TIME_MS).as_deref(),
            Some("300"),
        );
        // `spawn.missing` should not be set.
        assert!(attr_value(&action, BAZEL_ACTION_SPAWN_MISSING).is_none());

        let child_spawns: Vec<_> = spans
            .iter()
            .filter(|s| s.name.starts_with("spawn CppCompile"))
            .collect();
        assert_eq!(child_spawns.len(), 1, "exactly one child spawn span");
    }

    /// Scenario 2: BEP action + TWO matching SpawnExecs (retry). The action
    /// span carries attrs from the FIRST spawn only, `spawn.count == 2`,
    /// and two child spawn spans are emitted.
    #[test]
    fn two_attempt_retry_backfills_first_only() {
        let (mut mapper, collector) = mapper_with_collector();
        mapper.compact_streaming_active = true;

        mapper.on_action_completed(&action_event_for(
            "//pkg:bar",
            "GoCompile",
            "bazel-out/bar.a",
        ));
        // First attempt: remote, cache miss.
        mapper.on_compact_spawn(spawn_exec_with_metrics(
            "//pkg:bar",
            "GoCompile",
            "bazel-out/bar.a",
            "remote",
            false,
        ));
        // Second attempt: local fallback. Should NOT overwrite the action's
        // runner attribute.
        mapper.on_compact_spawn(spawn_exec_with_metrics(
            "//pkg:bar",
            "GoCompile",
            "bazel-out/bar.a",
            "linux-sandbox",
            false,
        ));
        mapper.end_open_actions_for_target("//pkg:bar", Some(2_000_000_000));
        mapper.finish();

        let spans = collector.snapshot();
        let action = find_span(&spans, |s| s.name.starts_with("action GoCompile"))
            .expect("action span emitted");
        // First spawn wins for the action-level backfill.
        assert_eq!(
            attr_value(&action, BAZEL_ACTION_SPAWN_RUNNER).as_deref(),
            Some("remote"),
        );
        assert_eq!(
            attr_value(&action, BAZEL_ACTION_SPAWN_COUNT).as_deref(),
            Some("2"),
        );

        let child_spawns: Vec<_> = spans
            .iter()
            .filter(|s| s.name.starts_with("spawn GoCompile"))
            .collect();
        assert_eq!(child_spawns.len(), 2, "one child span per attempt");
    }

    /// Spawn arrives before ActionExecuted and is buffered. Once the action
    /// lands, buffered spawns must flush onto that action (no orphan synthesis).
    #[test]
    fn buffered_spawn_flushes_when_action_arrives() {
        let (mut mapper, collector) = mapper_with_collector();
        mapper.compact_streaming_active = true;

        mapper.on_compact_spawn(spawn_exec_with_metrics(
            "//pkg:pre",
            "RustCompile",
            "bazel-out/pre.rlib",
            "remote",
            false,
        ));
        mapper.on_action_completed(&action_event_for(
            "//pkg:pre",
            "RustCompile",
            "bazel-out/pre.rlib",
        ));
        mapper.finish();

        let spans = collector.snapshot();
        let action = find_span(&spans, |s| s.name.starts_with("action RustCompile"))
            .expect("action span emitted");
        assert_eq!(
            attr_value(&action, BAZEL_ACTION_SPAWN_COUNT).as_deref(),
            Some("1"),
        );
        assert!(attr_value(&action, BAZEL_ACTION_SPAWN_MISSING).is_none());
        let child_count = spans
            .iter()
            .filter(|s| s.name.starts_with("spawn RustCompile"))
            .count();
        assert_eq!(child_count, 1);
    }

    /// Scenario 3: SpawnExec arrives but the BEP `ActionExecuted` never
    /// does (most commonly `--build_event_publish_all_actions=false` on a
    /// successful action). `finish()` synthesises a parent action span,
    /// backfills attrs, and emits the child spawn under it.
    #[test]
    fn orphan_spawn_synthesises_parent_action() {
        let (mut mapper, collector) = mapper_with_collector();
        mapper.compact_streaming_active = true;

        // Spawn comes through but no matching `on_action_completed` fired
        // for this target/mnemonic/output triple.
        mapper.on_compact_spawn(spawn_exec_with_metrics(
            "//pkg:orphan",
            "JavaCompile",
            "bazel-out/orphan.jar",
            "remote cache hit",
            true,
        ));
        mapper.finish();

        let spans = collector.snapshot();
        let synth_action = find_span(&spans, |s| s.name.contains("JavaCompile") && s.name.contains("(synth)"))
            .expect("synthesised orphan action span emitted");
        assert_eq!(
            attr_value(&synth_action, BAZEL_TARGET_SYNTHETIC).as_deref(),
            Some("true"),
        );
        assert_eq!(
            attr_value(&synth_action, BAZEL_ACTION_SPAWN_RUNNER).as_deref(),
            Some("remote cache hit"),
        );
        assert_eq!(
            attr_value(&synth_action, BAZEL_ACTION_SPAWN_CACHE_HIT).as_deref(),
            Some("true"),
        );
        assert_eq!(
            attr_value(&synth_action, BAZEL_ACTION_SPAWN_COUNT).as_deref(),
            Some("1"),
        );

        let child_count = spans
            .iter()
            .filter(|s| s.name.starts_with("spawn JavaCompile"))
            .count();
        assert_eq!(child_count, 1);
    }

    /// Scenario 4: BEP action arrives but **no** compact-log spawn matches
    /// it (e.g. action-cache hit invisible to the spawn log). At `finish()`
    /// the still-open action gets ended with
    /// `bazel.action.spawn.missing = true` and `bazel.action.spawn.count = 0`.
    #[test]
    fn bep_only_action_marked_spawn_missing() {
        let (mut mapper, collector) = mapper_with_collector();
        mapper.compact_streaming_active = true;

        mapper.on_action_completed(&action_event_for(
            "//pkg:bep_only",
            "GenRule",
            "bazel-out/bep_only.txt",
        ));
        // No on_compact_spawn call: the spawn log produced nothing for this
        // action.
        mapper.finish();

        let spans = collector.snapshot();
        let action = find_span(&spans, |s| s.name.starts_with("action GenRule"))
            .expect("action span emitted");
        assert_eq!(
            attr_value(&action, BAZEL_ACTION_SPAWN_COUNT).as_deref(),
            Some("0"),
        );
        assert_eq!(
            attr_value(&action, BAZEL_ACTION_SPAWN_MISSING).as_deref(),
            Some("true"),
        );

        let any_spawn = spans.iter().any(|s| s.name.starts_with("spawn "));
        assert!(!any_spawn, "no child spawn spans expected");
    }

    /// Spawn arrives after TargetCompleted marked the action boundary but
    /// before finish-time finalization. Parent action must not carry
    /// `spawn.missing=true`.
    #[test]
    fn late_spawn_before_finish_clears_missing() {
        let (mut mapper, collector) = mapper_with_collector();
        mapper.compact_streaming_active = true;

        mapper.on_action_completed(&action_event_for(
            "//pkg:late",
            "CppCompile",
            "bazel-out/late.o",
        ));
        mapper.end_open_actions_for_target("//pkg:late", Some(2_000_000_000));
        mapper.on_compact_spawn(spawn_exec_with_metrics(
            "//pkg:late",
            "CppCompile",
            "bazel-out/late.o",
            "linux-sandbox",
            false,
        ));
        mapper.finish();

        let spans = collector.snapshot();
        let action = find_span(&spans, |s| s.name.starts_with("action CppCompile"))
            .expect("action span emitted");
        assert_eq!(
            attr_value(&action, BAZEL_ACTION_SPAWN_COUNT).as_deref(),
            Some("1"),
        );
        assert!(attr_value(&action, BAZEL_ACTION_SPAWN_MISSING).is_none());
    }

    /// Find a root-span event by name. Returns the event's attribute map as
    /// a flat (key, display_value) Vec.
    fn root_event<'a>(spans: &'a [SpanData], event_name: &str) -> Option<Vec<(String, String)>> {
        let root = spans.iter().find(|s| s.parent_span_id == SpanId::INVALID)?;
        let event = root.events.iter().find(|e| e.name == event_name)?;
        Some(
            event
                .attributes
                .iter()
                .map(|kv| (kv.key.to_string(), kv.value.to_string()))
                .collect(),
        )
    }

    /// Missing-file scenario: the tailer aborts in `open_with_backoff` once
    /// the mapper signals shutdown. We exercise that path because it's the
    /// real-world "user pointed at the wrong file / wrote to a read-only
    /// dir" failure mode. Asserts the started event carries the resolved
    /// path, and the finished event reports `status=failed` with a non-empty
    /// error string. Happy-path (`status=ok`) coverage is in the integration
    /// suite where a real Bazel build writes a valid log; reproducing a
    /// valid zstd frame in a unit test would be more brittle than useful.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn exec_log_finished_event_reports_open_failure() {
        let ws = tempfile::tempdir().unwrap();
        let absolute = ws.path().join("exec.log");

        let (mut mapper, collector) = mapper_with_collector();
        mapper.on_build_started_extended(
            ws.path().to_str(),
            None,
            None,
            None,
            None,
            None,
        );
        mapper.on_exec_log_detected(absolute.clone());
        mapper.finish();

        let spans = collector.snapshot();
        let started = root_event(&spans, "bazel.exec_log.tailer_started")
            .expect("tailer_started event on root");
        assert!(
            started.iter().any(|(k, v)| k == "path" && v == &absolute.display().to_string()),
            "tailer_started should carry resolved path, got {started:?}",
        );

        let finished = root_event(&spans, "bazel.exec_log.tailer_finished")
            .expect("tailer_finished event on root");
        assert!(
            finished.iter().any(|(k, v)| k == "status" && v == "failed"),
            "expected status=failed when the log file never appears, got {finished:?}",
        );
        assert!(
            finished
                .iter()
                .any(|(k, v)| k == "error" && !v.is_empty()),
            "expected non-empty error string, got {finished:?}",
        );
        assert!(
            finished.iter().any(|(k, v)| k == "spawns_received" && v == "0"),
            "expected spawns_received=0, got {finished:?}",
        );
    }

    #[test]
    fn exec_log_emits_skipped_event_when_workspace_missing() {
        let (mut mapper, collector) = mapper_with_collector();
        mapper.on_exec_log_detected(PathBuf::from("exec.log"));
        mapper.finish();

        let spans = collector.snapshot();
        let skipped = root_event(&spans, "bazel.exec_log.tailer_skipped")
            .expect("tailer_skipped event on root");
        assert!(
            skipped.iter().any(|(k, v)| k == "reason" && v == "workspace_directory_not_set"),
            "expected reason attribute, got {skipped:?}",
        );
        // No tailer was spawned, so no finished event.
        assert!(
            root_event(&spans, "bazel.exec_log.tailer_finished").is_none(),
            "tailer_finished must not appear when the tailer was skipped",
        );
    }
}

