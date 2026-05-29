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
mod finalization;
mod metadata_handlers;
mod metrics;
mod progress;
mod test_attrs;

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
        self.closed_targets.remove(label);

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

    /// Resolve the parent context to use for action/test child spans.
    ///
    /// Lifecycle targets (`TargetConfigured` seen and not yet terminal) are
    /// kept in `target_contexts`. Synthetic targets are also persisted so
    /// action/test children share a stable target parent in exporters.
    fn target_parent_context(
        &mut self,
        label: &str,
        start_nanos: Option<i64>,
    ) -> Option<Context> {
        if let Some(existing) = self.target_contexts.get(label) {
            return Some(existing.clone());
        }
        let parent = self.choose_parent_for_label(label)?;
        if self.closed_targets.contains(label) {
            let cx = self.create_synthetic_target_span(label, &parent, start_nanos);
            self.synthetic_target_labels.insert(label.to_string());
            self.target_contexts.insert(label.to_string(), cx.clone());
            return Some(cx);
        }
        if let Some(configured) = self.configured_targets.get(label) {
            let cx = self.create_target_span(label, configured, &parent, start_nanos);
            debug!("Lazily created target span for {label} (action needed parent)");
            self.target_contexts.insert(label.to_string(), cx.clone());
            return Some(cx);
        }
        let cx = self.create_synthetic_target_span(label, &parent, start_nanos);
        self.synthetic_target_labels.insert(label.to_string());
        self.target_contexts.insert(label.to_string(), cx.clone());
        Some(cx)
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
        // Consume configured metadata up front regardless of whether the span
        // was already created lazily from an action/test event.
        let configured = self.configured_targets.remove(label);

        // Get or create the span under the appropriate parent.
        let cx = if let Some(cx) = self.target_contexts.remove(label) {
            cx
        } else if let Some(configured) = configured {
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
        self.synthetic_target_labels.remove(label);
        self.closed_targets.insert(label.to_string());
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
        self.synthetic_target_labels.remove(label);
        self.closed_targets.insert(label.to_string());

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

        let parent = match ev.label {
            Some(label) => self
                .target_parent_context(label, ev.start_time_nanos)
                .or_else(|| self.root_context.clone()),
            None => self.root_context.clone(),
        };
        let Some(parent) = parent else {
            warn!("ActionCompleted with no parent context");
            return;
        };

        // Track action for cached-target detection.
        if let Some(l) = ev.label {
            *self.target_action_counts.entry(l.to_string()).or_insert(0) += 1;
            if ev.cached == Some(false) {
                self.target_has_non_cached_action.insert(l.to_string(), true);
            }
        }

        let span_name = build_action_span_name(ev);
        let attrs = build_action_attrs(ev, &self.configurations, &self.redactor);
        let status = action_status(ev.success);

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

