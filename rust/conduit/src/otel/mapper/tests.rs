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

    let state = mapper
        .exec_log_state
        .as_ref()
        .expect("tailer should start after workspace arrives");
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
fn target_completed_clears_configured_after_lazy_target_span_creation() {
    let mut mapper = test_mapper();
    mapper.on_build_started("uuid-1", "build", Some(1_000_000_000), None);

    let tags = vec!["manual".to_string()];
    mapper.on_target_configured(
        "//pkg:lazy",
        Some("cc_library"),
        &tags,
        Some(1_100_000_000),
        None,
    );
    assert!(mapper.configured_targets.contains_key("//pkg:lazy"));

    // Action arrival lazily creates target context before TargetCompleted.
    mapper.on_action_completed(&action_event_for(
        "//pkg:lazy",
        "CppCompile",
        "bazel-out/lazy.o",
    ));
    assert!(mapper.target_contexts.contains_key("//pkg:lazy"));

    mapper.on_target_completed("//pkg:lazy", true, &[], &[], Some(1_500_000_000));

    assert!(
        !mapper.configured_targets.contains_key("//pkg:lazy"),
        "configured entry must be cleared on completion",
    );
    assert!(
        !mapper.target_contexts.contains_key("//pkg:lazy"),
        "target context must be closed on completion",
    );
}

#[test]
fn action_only_target_creates_persistent_synthetic_parent_context() {
    let mut mapper = test_mapper();
    mapper.on_build_started("uuid-1", "build", Some(1_000_000_000), None);

    let label = "@@repo//pkg:external";
    mapper.on_action_completed(&action_event_for(
        label,
        "GoCompile",
        "bazel-out/external.a",
    ));

    assert!(
        mapper.target_contexts.contains_key(label),
        "synthetic action-only target should be kept as a shared parent",
    );
    assert!(mapper.synthetic_target_labels.contains(label));
}

#[test]
fn action_after_target_completion_reopens_as_synthetic_parent() {
    let mut mapper = test_mapper();
    mapper.on_build_started("uuid-1", "build", Some(1_000_000_000), None);

    mapper.on_target_configured("//pkg:late", Some("cc_library"), &[], Some(1_100_000_000), None);
    mapper.on_target_completed("//pkg:late", true, &[], &[], Some(1_200_000_000));
    assert!(mapper.closed_targets.contains("//pkg:late"));

    mapper.on_action_completed(&action_event_for(
        "//pkg:late",
        "CppCompile",
        "bazel-out/late.o",
    ));

    assert!(
        mapper.target_contexts.contains_key("//pkg:late"),
        "late action should use a synthetic target parent for linkage",
    );
    assert!(mapper.synthetic_target_labels.contains("//pkg:late"));
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
    let synth_action = find_span(&spans, |s| {
        s.name.contains("JavaCompile") && s.name.contains("(synth)")
    })
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
    let started =
        root_event(&spans, "bazel.exec_log.tailer_started").expect("tailer_started event on root");
    assert!(
        started
            .iter()
            .any(|(k, v)| k == "path" && v == &absolute.display().to_string()),
        "tailer_started should carry resolved path, got {started:?}",
    );

    let finished = root_event(&spans, "bazel.exec_log.tailer_finished")
        .expect("tailer_finished event on root");
    assert!(
        finished.iter().any(|(k, v)| k == "status" && v == "failed"),
        "expected status=failed when the log file never appears, got {finished:?}",
    );
    assert!(
        finished.iter().any(|(k, v)| k == "error" && !v.is_empty()),
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
    let skipped =
        root_event(&spans, "bazel.exec_log.tailer_skipped").expect("tailer_skipped event on root");
    assert!(
        skipped
            .iter()
            .any(|(k, v)| k == "reason" && v == "workspace_directory_not_set"),
        "expected reason attribute, got {skipped:?}",
    );
    // No tailer was spawned, so no finished event.
    assert!(
        root_event(&spans, "bazel.exec_log.tailer_finished").is_none(),
        "tailer_finished must not appear when the tailer was skipped",
    );
}
