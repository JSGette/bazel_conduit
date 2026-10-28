// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

use super::*;
use crate::state::{TracePolicy, TraceProfile};

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
}

// ---- Mapper lifecycle tests ----

fn test_mapper() -> OtelMapper {
    OtelMapper::new(Tracer::default(), None)
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
fn shutdown_marks_only_unfinished_invocations() {
    let mut mapper = test_mapper();
    mapper.on_shutdown();
    assert!(!mapper.shut_down, "no root span yet");

    mapper.on_build_started("uuid-1", "build", Some(1_000_000_000), None);
    mapper.on_shutdown();
    assert!(mapper.shut_down);

    mapper.reset();
    mapper.on_build_started("uuid-2", "build", Some(1_000_000_000), None);
    mapper.on_build_finished(Some(0), Some(2_000_000_000), None);
    mapper.on_shutdown();
    assert!(!mapper.shut_down, "finished build keeps its real status");
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
    mapper.on_action_completed(action_event_for(
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
    mapper.on_action_completed(action_event_for(
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

    mapper.on_action_completed(action_event_for(
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
fn clamp_time_range_cached_preserves_duration_at_bound_start() {
    let (s, e) = clamp_time_range(Some(2), Some(8), Some(10), Some(20));
    assert_eq!(s, Some(10));
    assert_eq!(e, Some(16));
}

#[test]
fn clamp_time_range_cached_end_equal_bound_start() {
    let (s, e) = clamp_time_range(Some(2), Some(10), Some(10), Some(20));
    assert_eq!(s, Some(10));
    assert_eq!(e, Some(18));
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

use crate::otel::span::{MemorySpans, Sink, SpanData, SpanId, Tracer};
use spawn_proto::tools::protos::{File as ProtoFile, SpawnExec, SpawnMetrics};

fn mapper_with_collector() -> (OtelMapper, MemorySpans) {
    let (sink, collector) = Sink::memory();
    let mut mapper = OtelMapper::new(Tracer::new(sink), None);
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

fn action_event_for(label: &str, mnemonic: &str, output: &str) -> ActionCompletedEvent {
    ActionCompletedEvent {
        label: Some(label.into()),
        mnemonic: Some(mnemonic.into()),
        success: true,
        exit_code: Some(0),
        primary_output: Some(output.into()),
        start_time_nanos: Some(1_000_000_000),
        end_time_nanos: Some(1_500_000_000),
        cached: Some(false),
        ..Default::default()
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
        .find(|kv| kv.key == key)
        .map(|kv| kv.value.to_string())
}

/// Scenario 1: BEP action + a single matching SpawnExec → action span
/// carries runner / cache_hit / SpawnMetrics, plus exactly one child
/// spawn span with `bazel.spawn.*` attrs.
#[test]
fn cache_hit_single_spawn_backfills_action_span() {
    let (mut mapper, collector) = mapper_with_collector();
    mapper.compact_streaming_active = true;

    mapper.on_action_completed(action_event_for(
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

    mapper.on_action_completed(action_event_for(
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
    mapper.on_action_completed(action_event_for(
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

    mapper.on_action_completed(action_event_for(
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

    mapper.on_action_completed(action_event_for(
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

#[test]
fn cached_spawn_chain_is_nested_inside_root_window() {
    let (mut mapper, collector) = mapper_with_collector();
    let invocation_start = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as i64;
    mapper.root_span_start_nanos = Some(invocation_start);
    mapper.root_span_end_from_wall_nanos = Some(invocation_start + 2_000_000_000);
    mapper.compact_streaming_active = true;

    mapper.on_action_completed(action_event_for(
        "//pkg:cached",
        "GoCompilePkg",
        "bazel-out/cached.a",
    ));
    mapper.on_compact_spawn(spawn_exec_with_metrics(
        "//pkg:cached",
        "GoCompilePkg",
        "bazel-out/cached.a",
        "remote cache hit",
        true,
    ));
    mapper.finish();

    let spans = collector.snapshot();
    let root = find_span(&spans, |s| s.parent_span_id == SpanId::INVALID).expect("root span");
    let target = find_span(&spans, |s| s.name == "target //pkg:cached").expect("target span");
    let action =
        find_span(&spans, |s| s.name.starts_with("action GoCompilePkg")).expect("action span");
    let spawn =
        find_span(&spans, |s| s.name.starts_with("spawn GoCompilePkg")).expect("spawn span");

    assert_eq!(target.parent_span_id, root.span_context.span_id());
    assert_eq!(action.parent_span_id, target.span_context.span_id());
    assert_eq!(spawn.parent_span_id, action.span_context.span_id());

    assert!(root.start_time <= target.start_time);
    assert!(target.start_time <= action.start_time);
    assert!(action.start_time <= spawn.start_time);
    assert!(spawn.end_time <= action.end_time);
    assert!(action.end_time <= target.end_time);
    assert!(target.end_time <= root.end_time);
    assert_eq!(target.end_time, action.end_time);
    assert!(action.start_time < action.end_time);
    assert!(spawn.start_time < spawn.end_time);
}

#[test]
fn instantaneous_action_does_not_stretch_synthetic_target_to_build_end() {
    let (mut mapper, collector) = mapper_with_collector();
    let invocation_start = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as i64;
    mapper.root_span_start_nanos = Some(invocation_start);
    mapper.root_span_end_from_wall_nanos = Some(invocation_start + 360_000_000_000);

    let mut action = action_event_for(
        "@@repo//pkg:cached",
        "GoCompilePkg",
        "bazel-out/cached.a",
    );
    action.end_time_nanos = action.start_time_nanos;
    mapper.on_action_completed(action);
    mapper.finish();

    let spans = collector.snapshot();
    let target = find_span(&spans, |s| s.name.contains("target repo//pkg:cached"))
        .expect("synthetic target span");
    let action =
        find_span(&spans, |s| s.name.starts_with("action GoCompilePkg")).expect("action span");
    let root = find_span(&spans, |s| s.parent_span_id == SpanId::INVALID).expect("root span");

    assert_eq!(target.start_time, target.end_time);
    assert_eq!(target.start_time, action.start_time);
    assert_eq!(target.end_time, action.end_time);
    assert!(target.end_time < root.end_time);
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

// =====================================================================
// Trace profile: lightweight suppression.
//
// These assert the fix for the compact-log bypass -- lightweight must
// stop resurrecting the successful cache-hit action/spawn tree -- while
// keeping errors, cache misses, and slow work. The default policy is
// Full, so all the tests above still exercise the complete hierarchy.
// =====================================================================

fn lightweight(mapper: &mut OtelMapper) {
    mapper.set_trace_policy(TracePolicy::new(TraceProfile::Lightweight, 500, 200));
}

/// A spawn the lightweight filter keeps on its own: a cacheable cache miss.
fn missed_spawn(target: &str, output: &str) -> SpawnExec {
    SpawnExec {
        cacheable: true,
        ..spawn_exec_with_metrics(target, "CppCompile", output, "local", false)
    }
}

fn timed_action(label: &str, success: bool, millis: i64) -> ActionCompletedEvent {
    ActionCompletedEvent {
        success,
        end_time_nanos: Some(1_000_000_000 + millis * 1_000_000),
        ..action_event_for(label, "Genrule", &format!("bazel-out/{}", &label[2..]))
    }
}

fn action_names(spans: &[SpanData]) -> Vec<String> {
    let mut names: Vec<String> = spans
        .iter()
        .filter(|s| s.name.starts_with("action "))
        .map(|s| s.name.to_string())
        .collect();
    names.sort();
    names
}

/// Successful actions compete for the budget (the slowest win, untimed rank
/// lowest); failures bypass it. Dropped actions are counted on the root.
#[test]
fn lightweight_budget_keeps_slowest_successes_and_all_failures() {
    let (mut mapper, collector) = mapper_with_collector();
    mapper.set_trace_policy(TracePolicy::new(TraceProfile::Lightweight, 500, 2));

    mapper.on_action_completed(timed_action("//:a", true, 30));
    mapper.on_action_completed(timed_action("//:b", true, 10));
    mapper.on_action_completed(timed_action("//:c", false, 1));
    mapper.on_action_completed(timed_action("//:d", true, 20));
    mapper.on_action_completed(ActionCompletedEvent {
        end_time_nanos: None,
        ..timed_action("//:e", true, 0)
    });
    mapper.finish();

    let spans = collector.snapshot();
    assert_eq!(
        action_names(&spans),
        ["action Genrule //:a", "action Genrule //:c", "action Genrule //:d"],
    );
    let root = find_span(&spans, |s| s.parent_span_id == SpanId::INVALID).expect("root span");
    assert_eq!(
        attr_value(&root, BAZEL_TRACE_SUPPRESSED_ACTIONS).as_deref(),
        Some("2"),
    );
}

/// A successful target completes after the budget settles: it survives
/// (with its configured kind) when one of its actions was kept, and is
/// suppressed as empty when all of them were dropped.
#[test]
fn lightweight_budget_decides_successful_targets() {
    let (mut mapper, collector) = mapper_with_collector();
    mapper.set_trace_policy(TracePolicy::new(TraceProfile::Lightweight, 500, 1));

    for (label, millis) in [("//:slow", 50), ("//:fast", 5)] {
        mapper.on_target_configured(label, Some("genrule"), &[], Some(1_000_000_000), None);
        mapper.on_action_completed(timed_action(label, true, millis));
        mapper.on_target_completed(label, true, &[], &[], Some(2_000_000_000));
    }
    mapper.finish();

    let spans = collector.snapshot();
    let target = find_span(&spans, |s| s.name == "target //:slow").expect("kept target");
    assert_eq!(attr_value(&target, BAZEL_TARGET_KIND).as_deref(), Some("genrule"));
    let action = find_span(&spans, |s| s.name == "action Genrule //:slow").expect("kept action");
    assert_eq!(action.parent_span_id, target.span_context.span_id());
    assert!(find_span(&spans, |s| s.name == "target //:fast").is_none());
    let root = find_span(&spans, |s| s.parent_span_id == SpanId::INVALID).expect("root span");
    assert_eq!(
        attr_value(&root, BAZEL_TRACE_SUPPRESSED_TARGETS).as_deref(),
        Some("1"),
    );
}

/// A failed target is also held, so a kept successful action of it lands
/// under the real target span rather than a synthetic duplicate.
#[test]
fn lightweight_budget_parents_kept_action_under_failed_target() {
    let (mut mapper, collector) = mapper_with_collector();
    lightweight(&mut mapper);

    mapper.on_target_configured("//:t", Some("genrule"), &[], Some(1_000_000_000), None);
    mapper.on_action_completed(timed_action("//:t", true, 50));
    mapper.on_target_completed("//:t", false, &[], &[], Some(2_000_000_000));
    mapper.finish();

    let spans = collector.snapshot();
    let targets: Vec<&SpanData> = spans.iter().filter(|s| s.name == "target //:t").collect();
    assert_eq!(targets.len(), 1);
    assert_eq!(attr_value(targets[0], BAZEL_TARGET_KIND).as_deref(), Some("genrule"));
    let action = find_span(&spans, |s| s.name == "action Genrule //:t").expect("kept action");
    assert_eq!(action.parent_span_id, targets[0].span_context.span_id());
}

/// A kept orphan spawn (no BEP action) also lands under its real, deferred
/// target, which is then not counted as an empty suppressed target.
#[test]
fn lightweight_orphan_action_keeps_deferred_target() {
    let (mut mapper, collector) = mapper_with_collector();
    mapper.compact_streaming_active = true;
    lightweight(&mut mapper);

    mapper.on_target_configured("//pkg:boom", Some("cc_library"), &[], Some(1_000_000_000), None);
    mapper.on_target_completed("//pkg:boom", true, &[], &[], Some(2_000_000_000));
    let mut spawn =
        spawn_exec_with_metrics("//pkg:boom", "CppCompile", "bazel-out/boom.o", "local", false);
    spawn.exit_code = 1;
    mapper.on_compact_spawn(spawn);
    mapper.finish();

    let spans = collector.snapshot();
    let target = find_span(&spans, |s| s.name == "target //pkg:boom").expect("real target");
    assert_eq!(attr_value(&target, BAZEL_TARGET_KIND).as_deref(), Some("cc_library"));
    let action = find_span(&spans, |s| s.name.contains("CppCompile") && s.name.contains("(synth)"))
        .expect("synthetic orphan action");
    assert_eq!(action.parent_span_id, target.span_context.span_id());
    let root = find_span(&spans, |s| s.parent_span_id == SpanId::INVALID).expect("root span");
    assert_eq!(
        attr_value(&root, BAZEL_TRACE_SUPPRESSED_TARGETS).as_deref(),
        Some("0"),
    );
}

/// A deferred action claims its pending spawn even when its primary output
/// is not the spawn's first listed output.
#[test]
fn lightweight_deferred_action_claims_multi_output_spawn() {
    let (mut mapper, collector) = mapper_with_collector();
    mapper.compact_streaming_active = true;
    lightweight(&mut mapper);

    let mut spawn =
        spawn_exec_with_metrics("//pkg:gen", "Genrule", "bazel-out/a.h", "local", false);
    spawn.listed_outputs.push("bazel-out/b.cc".into());
    mapper.on_compact_spawn(spawn);
    mapper.on_action_completed(action_event_for("//pkg:gen", "Genrule", "bazel-out/b.cc"));
    mapper.finish();

    let spans = collector.snapshot();
    assert!(find_span(&spans, |s| s.name.contains("(synth)")).is_none());
    let action = find_span(&spans, |s| s.name.starts_with("action Genrule")).expect("action");
    assert_eq!(
        attr_value(&action, BAZEL_ACTION_SPAWN_CACHE_HIT).as_deref(),
        Some("false"),
    );
}

/// Successful orphan actions share what is left of the budget after BEP
/// actions; orphans with a failed attempt bypass it.
#[test]
fn lightweight_budget_caps_successful_orphan_actions() {
    let (mut mapper, collector) = mapper_with_collector();
    mapper.compact_streaming_active = true;
    mapper.set_trace_policy(TracePolicy::new(TraceProfile::Lightweight, 500, 2));

    mapper.on_action_completed(timed_action("//:bep", true, 50));
    mapper.on_compact_spawn(missed_spawn("//pkg:x", "bazel-out/x.o"));
    mapper.on_compact_spawn(missed_spawn("//pkg:y", "bazel-out/y.o"));
    let mut failed = missed_spawn("//pkg:z", "bazel-out/z.o");
    failed.exit_code = 1;
    mapper.on_compact_spawn(failed);
    mapper.finish();

    let spans = collector.snapshot();
    let synth: Vec<String> = action_names(&spans)
        .into_iter()
        .filter(|n| n.ends_with("(synth)"))
        .collect();
    assert_eq!(synth.len(), 2, "one successful orphan plus the failed one: {synth:?}");
    assert!(synth.iter().any(|n| n.contains(":z")));
    let root = find_span(&spans, |s| s.parent_span_id == SpanId::INVALID).expect("root span");
    assert_eq!(
        attr_value(&root, BAZEL_TRACE_SUPPRESSED_ACTIONS).as_deref(),
        Some("1"),
    );
}

/// Exec-log spawns of a budget-dropped action are not resurrected as a
/// synthetic orphan action, even when they would pass the spawn filter.
#[test]
fn lightweight_budget_dropped_action_spawns_are_not_synthesised() {
    let (mut mapper, collector) = mapper_with_collector();
    mapper.compact_streaming_active = true;
    mapper.set_trace_policy(TracePolicy::new(TraceProfile::Lightweight, 500, 1));

    mapper.on_action_completed(timed_action("//:slow", true, 50));
    mapper.on_action_completed(timed_action("//:fast", true, 5));
    mapper.on_compact_spawn(SpawnExec {
        mnemonic: "Genrule".into(),
        ..missed_spawn("//:fast", "bazel-out/:fast")
    });
    mapper.finish();

    let spans = collector.snapshot();
    assert_eq!(action_names(&spans), ["action Genrule //:slow"]);
    assert!(!spans.iter().any(|s| s.name.starts_with("spawn ")));
    let root = find_span(&spans, |s| s.parent_span_id == SpanId::INVALID).expect("root span");
    assert_eq!(
        attr_value(&root, BAZEL_TRACE_SUPPRESSED_SPAWNS).as_deref(),
        Some("1"),
    );
    assert_eq!(
        attr_value(&root, BAZEL_TRACE_SUPPRESSED_ACTIONS).as_deref(),
        Some("1"),
        "the dropped action is not counted again as a capped orphan",
    );
}

/// A successful, fast, cache-hit orphan spawn is dropped: no synthetic
/// action, no spawn span, and the drop is counted on the root span.
#[test]
fn lightweight_drops_orphan_cache_hit_spawn() {
    let (mut mapper, collector) = mapper_with_collector();
    mapper.compact_streaming_active = true;
    lightweight(&mut mapper);

    mapper.on_compact_spawn(spawn_exec_with_metrics(
        "//pkg:orphan",
        "JavaCompile",
        "bazel-out/orphan.jar",
        "remote cache hit",
        true,
    ));
    mapper.finish();

    let spans = collector.snapshot();
    assert!(
        find_span(&spans, |s| s.name.contains("(synth)")).is_none(),
        "no synthetic action for a dropped orphan spawn",
    );
    assert!(
        !spans.iter().any(|s| s.name.starts_with("spawn JavaCompile")),
        "no child spawn span",
    );
    let root = find_span(&spans, |s| s.parent_span_id == SpanId::INVALID).expect("root span");
    assert_eq!(
        attr_value(&root, BAZEL_TRACE_SUPPRESSED_SPAWNS).as_deref(),
        Some("1"),
    );
}

/// A failing orphan spawn survives lightweight: the synthetic parent
/// action and its child spawn are both emitted.
#[test]
fn lightweight_keeps_failed_orphan_spawn() {
    let (mut mapper, collector) = mapper_with_collector();
    mapper.compact_streaming_active = true;
    lightweight(&mut mapper);

    let mut spawn =
        spawn_exec_with_metrics("//pkg:boom", "CppCompile", "bazel-out/boom.o", "local", false);
    spawn.exit_code = 1;
    spawn.status = "NON_ZERO_EXIT".to_string();
    mapper.on_compact_spawn(spawn);
    mapper.finish();

    let spans = collector.snapshot();
    assert!(
        find_span(&spans, |s| s.name.contains("CppCompile") && s.name.contains("(synth)"))
            .is_some(),
        "failing orphan spawn keeps its synthetic parent action",
    );
    assert_eq!(
        spans
            .iter()
            .filter(|s| s.name.starts_with("spawn CppCompile"))
            .count(),
        1,
    );
}

/// A matched spawn under a (retained) action still backfills action-level
/// attrs, but a fast cache-hit child spawn span is dropped in lightweight.
#[test]
fn lightweight_drops_matched_cache_hit_child_spawn() {
    let (mut mapper, collector) = mapper_with_collector();
    mapper.compact_streaming_active = true;
    lightweight(&mut mapper);

    mapper.on_action_completed(action_event_for(
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
    mapper.end_open_actions_for_target("//pkg:foo", Some(2_000_000_000));
    mapper.finish();

    let spans = collector.snapshot();
    let action = find_span(&spans, |s| s.name.starts_with("action CppCompile"))
        .expect("action span still emitted");
    // Backfill still happened from the (only) spawn.
    assert_eq!(
        attr_value(&action, BAZEL_ACTION_SPAWN_CACHE_HIT).as_deref(),
        Some("true"),
    );
    // But the child spawn span itself was suppressed.
    assert!(
        !spans.iter().any(|s| s.name.starts_with("spawn CppCompile")),
        "fast cache-hit child spawn dropped in lightweight",
    );
}

/// An empty successful target (no retained child span) is dropped in
/// lightweight and counted on the root span.
#[test]
fn lightweight_suppresses_empty_successful_target() {
    let (mut mapper, collector) = mapper_with_collector();
    lightweight(&mut mapper);

    mapper.on_target_configured("//pkg:lib", Some("rust_library"), &[], Some(1_000_000_000), None);
    mapper.on_target_completed("//pkg:lib", true, &[], &[], Some(1_500_000_000));
    mapper.finish();

    let spans = collector.snapshot();
    assert!(
        find_span(&spans, |s| s.name == "target //pkg:lib").is_none(),
        "empty successful target dropped in lightweight",
    );
    let root = find_span(&spans, |s| s.parent_span_id == SpanId::INVALID).expect("root span");
    assert_eq!(
        attr_value(&root, BAZEL_TRACE_SUPPRESSED_TARGETS).as_deref(),
        Some("1"),
    );
}

/// A failed target always survives, even with no child spans.
#[test]
fn lightweight_keeps_failed_target() {
    let (mut mapper, collector) = mapper_with_collector();
    lightweight(&mut mapper);

    mapper.on_target_configured("//pkg:lib", Some("rust_library"), &[], Some(1_000_000_000), None);
    mapper.on_target_completed("//pkg:lib", false, &[], &[], Some(1_500_000_000));
    mapper.finish();

    let spans = collector.snapshot();
    assert!(
        find_span(&spans, |s| s.name == "target //pkg:lib").is_some(),
        "failed target kept in lightweight",
    );
}

#[test]
fn build_metrics_set_root_attrs_and_root_contains_finish() {
    use crate::build_event_stream::build_metrics::{
        action_summary::RunnerCount, ActionSummary, TimingMetrics,
    };
    use crate::build_event_stream::BuildMetrics;

    let (mut mapper, collector) = mapper_with_collector();
    mapper.on_build_finished(Some(0), Some(5_000_000_000), None);
    mapper.on_build_metrics(&BuildMetrics {
        action_summary: Some(ActionSummary {
            actions_created: 7,
            runner_count: vec![RunnerCount {
                name: "remote".into(),
                count: 3,
                ..Default::default()
            }],
            ..Default::default()
        }),
        // Wall time ends at 3s, before BuildFinished at 5s.
        timing_metrics: Some(TimingMetrics {
            wall_time_in_ms: 2_000,
            ..Default::default()
        }),
        ..Default::default()
    });
    mapper.finish();

    let spans = collector.snapshot();
    let root = find_span(&spans, |s| s.parent_span_id == SpanId::INVALID)
        .expect("root span");
    assert_eq!(attr_value(&root, BAZEL_METRICS_ACTIONS_CREATED).as_deref(), Some("7"));
    assert_eq!(attr_value(&root, BAZEL_METRICS_WALL_TIME_MS).as_deref(), Some("2000"));
    assert_eq!(attr_value(&root, "bazel.metrics.runner.remote").as_deref(), Some("3"));
    assert_eq!(root.end_time, super::nanos_to_system_time(5_000_000_000));
}
