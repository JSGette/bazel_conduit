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
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use opentelemetry::trace::{Span, SpanContext, SpanKind, Status, TraceContextExt, Tracer};
use opentelemetry::{Context, KeyValue};
use spawn_proto::tools::protos::SpawnExec;
use tracing::{debug, info, warn};

use crate::exec_log::tailer::TailerHandle;

use super::attributes::*;
use super::redact::Redactor;
use super::trace_context;

mod action_attrs;
mod actions;
mod fetch_tests;
mod finalization;
mod lifecycle;
mod metadata_handlers;
mod metrics;
mod progress;
mod test_attrs;
mod targets;

use self::action_attrs::{action_status, build_action_attrs, build_action_span_name};
use self::test_attrs::{build_test_attrs, build_test_span_name, test_status};

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

    /// Labels whose currently-open target context is synthetic
    /// (action/test-driven, no observed TargetConfigured lifecycle).
    synthetic_target_labels: HashSet<String>,

    /// Targets whose lifecycle has reached a terminal event
    /// (`TargetCompleted` or skipped/aborted). Used to avoid recreating
    /// long-lived target contexts from late action/test events.
    closed_targets: HashSet<String>,

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
            synthetic_target_labels: HashSet::new(),
            closed_targets: HashSet::new(),
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
    pub fn on_build_metadata(&mut self, metadata: &serde_json::Value) {
        metadata_handlers::apply_build_metadata(self, metadata);
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
        metrics::apply_build_metrics(self, metrics);
    }

    // =====================================================================
    // Build tool logs (critical path)
    // =====================================================================

    /// BuildToolLogs → extract critical path info, set root attribute for URI, emit span event.
    pub fn on_build_tool_logs(&mut self, logs: &serde_json::Value) {
        metadata_handlers::apply_build_tool_logs(self, logs);
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
        self.synthetic_target_labels.clear();
        self.closed_targets.clear();
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
    fn synthesise_orphan_actions(&mut self) {
        finalization::synthesise_orphan_actions(self);
    }

    /// Finalize and end every action span still held open in
    /// [`Self::action_span_cache`]. Called from `finish()` after the tailer
    /// drain and orphan synthesis, so `spawn.missing` decisions are based on
    /// the complete compact-log stream.
    fn finalize_and_end_remaining_open_actions(&mut self) {
        finalization::finalize_and_end_remaining_open_actions(self);
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
                if self.synthetic_target_labels.remove(&label) {
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
mod tests;

