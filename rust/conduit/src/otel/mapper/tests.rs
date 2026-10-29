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
fn repository_name_from_url_forms() {
    for url in [
        "https://github.com/DataDog/datadog-agent",
        "https://github.com/DataDog/datadog-agent.git",
        "https://github.com/DataDog/datadog-agent/",
        "git@github.com:DataDog/datadog-agent.git",
        "datadog-agent",
    ] {
        assert_eq!(repository_name_from_url(url).as_deref(), Some("datadog-agent"), "{url}");
    }
    assert_eq!(repository_name_from_url(""), None);
}

#[test]
fn strip_url_credentials_drops_userinfo() {
    assert_eq!(
        strip_url_credentials("https://gitlab-ci-token:s3cr3t@gitlab.example.com/g/p.git"),
        "https://gitlab.example.com/g/p.git",
    );
    assert_eq!(
        strip_url_credentials("https://github.com/o/r@v1"),
        "https://github.com/o/r@v1",
    );
    assert_eq!(
        strip_url_credentials("git@github.com:o/r.git"),
        "git@github.com:o/r.git",
    );
}

#[test]
fn build_metadata_maps_repository_identity() {
    let (mut mapper, collector) = mapper_with_collector();
    mapper.on_build_started_extended(Some("/repos/datadog-agent-nogo"), None, None, None, None);
    let root_cx = mapper.root_context.clone().unwrap();
    let mut child = mapper.build_child_span(&root_cx, "child".into(), SpanKind::Internal, vec![], None);
    mapper.on_build_metadata(&crate::build_event_stream::BuildMetadata {
        metadata: HashMap::from([
            ("REPO_URL".into(), "https://user:tok@github.com/DataDog/datadog-agent.git".into()),
            ("COMMIT_SHA".into(), "abc123".into()),
            ("BRANCH".into(), String::new()),
            ("OWNER".into(), "me".into()),
        ]),
    });
    child.end();
    mapper.finish();

    let spans = collector.snapshot();
    let root = find_span(&spans, |s| s.parent_span_id == SpanId::INVALID).expect("root span");
    let url = "https://github.com/DataDog/datadog-agent.git";
    assert_eq!(attr_value(&root, VCS_REPOSITORY_NAME).as_deref(), Some("datadog-agent"));
    assert_eq!(attr_value(&root, VCS_REPOSITORY_URL).as_deref(), Some(url));
    assert_eq!(attr_value(&root, GIT_REPOSITORY_URL).as_deref(), Some(url));
    assert_eq!(attr_value(&root, VCS_REF_HEAD_REVISION).as_deref(), Some("abc123"));
    assert_eq!(attr_value(&root, GIT_COMMIT_SHA).as_deref(), Some("abc123"));
    assert_eq!(attr_value(&root, GIT_BRANCH), None);
    assert_eq!(attr_value(&root, "bazel.metadata.OWNER").as_deref(), Some("me"));
    assert!(
        !root.attributes.iter().any(|kv| kv.key.starts_with("bazel.metadata.REPO")),
        "mapped keys are not duplicated: {:?}",
        root.attributes,
    );
    let child = find_span(&spans, |s| s.name == "child").expect("child span");
    assert_eq!(attr_value(&child, VCS_REPOSITORY_NAME).as_deref(), Some("datadog-agent"));
    assert_eq!(attr_value(&child, GIT_REPOSITORY_URL).as_deref(), Some(url));
    assert_eq!(attr_value(&child, GIT_COMMIT_SHA).as_deref(), Some("abc123"));
    assert_eq!(attr_value(&child, "bazel.metadata.OWNER"), None);
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
    mapper.on_build_started_extended(ws.path().to_str(), None, None, None, None);
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
    mapper.on_build_started_extended(ws.path().to_str(), None, None, None, None);
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
fn reset_drops_repository_identity() {
    let (mut mapper, collector) = mapper_with_collector();
    mapper.on_build_metadata(&crate::build_event_stream::BuildMetadata {
        metadata: HashMap::from([("COMMIT_SHA".into(), "abc123".into())]),
    });
    mapper.reset();
    mapper.on_build_started("uuid-2", "build", Some(1_000_000_000), None);
    mapper.finish();

    let spans = collector.snapshot();
    let roots: Vec<_> = spans.iter().filter(|s| s.parent_span_id == SpanId::INVALID).collect();
    assert_eq!(roots.len(), 2);
    assert_eq!(attr_value(roots[0], GIT_COMMIT_SHA).as_deref(), Some("abc123"));
    assert_eq!(attr_value(roots[1], GIT_COMMIT_SHA), None);
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

    mapper.on_target_completed("//pkg:lazy", true, &[], Some(1_500_000_000));
    mapper.finish();

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

/// Fetches only count on the root, and external-repo targets hang directly
/// off the root: neither produces a group span.
#[test]
fn fetches_are_root_counters_and_external_targets_parent_to_root() {
    let (mut mapper, collector) = mapper_with_collector();
    mapper.on_fetch(true);
    mapper.on_fetch(false);
    mapper.on_fetch(true);
    mapper.on_action_completed(action_event_for(
        "@@repo//pkg:ext",
        "GoCompile",
        "bazel-out/ext.a",
    ));
    mapper.finish();

    let spans = collector.snapshot();
    let root = find_span(&spans, |s| s.parent_span_id == SpanId::INVALID).expect("root span");
    assert_eq!(attr_value(&root, BAZEL_FETCH_COUNT).as_deref(), Some("3"));
    assert_eq!(attr_value(&root, BAZEL_FETCH_FAILED).as_deref(), Some("1"));
    let target = find_span(&spans, |s| s.name.starts_with("target ")).expect("target span");
    assert_eq!(target.parent_span_id, root.span_context.span_id());
    assert_eq!(attr_value(&target, BAZEL_TARGET_SYNTHETIC).as_deref(), Some("true"));
    assert_eq!(spans.len(), 3, "root, target and action only: {spans:#?}");
}

#[test]
fn untimed_late_action_stays_inside_its_target() {
    let (mut mapper, collector) = mapper_with_collector();
    mapper.on_target_configured("//pkg:u", Some("genrule"), &[], Some(1_100_000_000), None);
    mapper.on_target_completed("//pkg:u", true, &[], Some(1_200_000_000));
    mapper.on_action_completed(ActionCompletedEvent {
        start_time_nanos: None,
        end_time_nanos: None,
        ..action_event_for("//pkg:u", "Genrule", "bazel-out/u.txt")
    });
    mapper.finish();

    let spans = collector.snapshot();
    let target = find_span(&spans, |s| s.name == "target //pkg:u").expect("target span");
    let action = find_span(&spans, |s| s.name.starts_with("action ")).expect("action span");
    assert_eq!(action.parent_span_id, target.span_context.span_id());
    assert!(target.end_time >= action.end_time, "{target:#?}\n{action:#?}");
}

#[test]
fn repeated_target_completions_merge_into_one_span() {
    let (mut mapper, collector) = mapper_with_collector();
    mapper.on_target_configured("//pkg:m", Some("cc_library"), &[], Some(1_100_000_000), None);
    mapper.on_target_completed("//pkg:m", false, &[], Some(1_300_000_000));
    mapper.on_target_completed("//pkg:m", true, &[], Some(1_200_000_000));
    mapper.finish();

    let spans = collector.snapshot();
    let targets: Vec<_> = spans.iter().filter(|s| s.name == "target //pkg:m").collect();
    assert_eq!(targets.len(), 1);
    assert_eq!(attr_value(targets[0], BAZEL_TARGET_SUCCESS).as_deref(), Some("false"));
    assert_eq!(targets[0].end_time, nanos_to_system_time(1_300_000_000));
}

#[test]
fn test_summary_for_skipped_target_adds_no_span() {
    let (mut mapper, collector) = mapper_with_collector();
    mapper.on_target_configured("//pkg:s", Some("sh_test"), &[], Some(1_100_000_000), None);
    mapper.on_target_skipped("//pkg:s", Some("ANALYSIS_FAILURE"), None, Some(1_200_000_000));
    mapper.on_test_summary("//pkg:s", Some("FAILED_TO_BUILD"), Some(0), None, None, None, None);
    mapper.finish();

    let spans = collector.snapshot();
    let targets: Vec<_> = spans.iter().filter(|s| s.name.ends_with("//pkg:s")).collect();
    assert_eq!(targets.len(), 1, "only the skipped span: {targets:#?}");
    assert_eq!(attr_value(targets[0], BAZEL_TARGET_SYNTHETIC), None);
}

/// Events after `TargetCompleted` (late actions, tests and their summary)
/// land on the real target span, which completes at finish.
#[test]
fn events_after_target_completed_land_on_the_real_target() {
    let (mut mapper, collector) = mapper_with_collector();

    mapper.on_target_configured("//pkg:t", Some("cc_test"), &[], Some(1_100_000_000), None);
    mapper.on_target_completed("//pkg:t", true, &[], Some(1_200_000_000));
    mapper.on_action_completed(action_event_for("//pkg:t", "CppCompile", "bazel-out/t.o"));
    mapper.on_test_result(&TestResultEvent {
        label: "//pkg:t",
        status: Some("PASSED"),
        attempt: Some(1),
        run: None,
        shard: None,
        cached: Some(false),
        strategy: None,
        start_time_nanos: Some(1_300_000_000),
        duration_nanos: Some(100_000_000),
        hostname: None,
        cached_remotely: Some(true),
    });
    mapper.on_test_summary("//pkg:t", Some("PASSED"), Some(1), Some(1), Some(1), Some(1), Some(1));
    mapper.finish();

    let spans = collector.snapshot();
    let targets: Vec<&SpanData> = spans.iter().filter(|s| s.name == "target //pkg:t").collect();
    assert_eq!(targets.len(), 1, "no synthetic duplicate");
    let target = targets[0];
    assert_eq!(attr_value(target, BAZEL_TARGET_KIND).as_deref(), Some("cc_test"));
    assert_eq!(attr_value(target, BAZEL_TARGET_SYNTHETIC), None);
    assert_eq!(attr_value(target, BAZEL_TEST_OVERALL_STATUS).as_deref(), Some("PASSED"));
    assert_eq!(attr_value(target, BAZEL_TEST_TOTAL_NUM_CACHED).as_deref(), Some("1"));
    for child in spans.iter().filter(|s| s.name.starts_with("action ") || s.name.starts_with("test ")) {
        assert_eq!(child.parent_span_id, target.span_context.span_id(), "{}", child.name);
    }
    let test = find_span(&spans, |s| s.name.starts_with("test ")).expect("test span");
    assert_eq!(attr_value(&test, BAZEL_TEST_CACHE).as_deref(), Some("remote"));
    assert!(target.end_time >= test.end_time, "target must contain the later test");
    let root = find_span(&spans, |s| s.parent_span_id == SpanId::INVALID).expect("root span");
    assert!(root.events.is_empty(), "no root events: {:?}", root.events);
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

    let child_spawns: Vec<_> = spans
        .iter()
        .filter(|s| s.name.starts_with("spawn CppCompile"))
        .collect();
    assert_eq!(child_spawns.len(), 1, "exactly one child spawn span");
    let spawn = child_spawns[0];
    assert_eq!(attr_value(spawn, BAZEL_SPAWN_RUNNER).as_deref(), Some("remote cache hit"));
    assert_eq!(attr_value(spawn, BAZEL_SPAWN_QUEUE_TIME_MS).as_deref(), Some("50"));
    // Remote-cacheable is the norm, so only the exception is recorded; the
    // label, mnemonic and outputs already live on the parent action.
    for key in [
        BAZEL_SPAWN_REMOTE_CACHEABLE,
        "bazel.spawn.target_label",
        "bazel.spawn.mnemonic",
        "bazel.spawn.primary_output",
        "bazel.spawn.listed_outputs",
    ] {
        assert_eq!(attr_value(spawn, key), None, "{key}");
    }
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
/// the still-open action gets ended with `bazel.action.spawn.count = 0`.
#[test]
fn bep_only_action_reports_zero_spawns() {
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

    let any_spawn = spans.iter().any(|s| s.name.starts_with("spawn "));
    assert!(!any_spawn, "no child spawn spans expected");
}

/// Spawn arrives after TargetCompleted marked the action boundary but
/// before finish-time finalization; it still counts on the parent action.
#[test]
fn late_spawn_before_finish_is_counted() {
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

fn root_events(spans: &[SpanData]) -> Vec<String> {
    let root = spans.iter().find(|s| s.parent_span_id == SpanId::INVALID).expect("root span");
    root.events.iter().map(|e| e.name.to_string()).collect()
}

/// Missing-file scenario: the tailer aborts in `open_with_backoff` once
/// the mapper signals shutdown. We exercise that path because it's the
/// real-world "user pointed at the wrong file / wrote to a read-only
/// dir" failure mode. Asserts the worker error reaches finish and no root
/// events are emitted. Happy-path coverage is in the integration
/// suite where a real Bazel build writes a valid log; reproducing a
/// valid zstd frame in a unit test would be more brittle than useful.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn exec_log_tailer_open_failure_is_reported() {
    let ws = tempfile::tempdir().unwrap();

    let (mut mapper, collector) = mapper_with_collector();
    mapper.on_build_started_extended(ws.path().to_str(), None, None, None, None);
    mapper.on_exec_log_detected(ws.path().join("exec.log"));
    match mapper.drain_and_stop_tailer() {
        Some(Err(error)) => assert!(!error.is_empty()),
        other => panic!("expected a tailer error, got {other:?}"),
    }
    assert_eq!(mapper.compact_spawns_received, 0);
    mapper.finish();
    assert!(root_events(&collector.snapshot()).is_empty());
}

#[test]
fn exec_log_tailer_is_skipped_without_workspace() {
    let (mut mapper, collector) = mapper_with_collector();
    mapper.on_exec_log_detected(PathBuf::from("exec.log"));
    assert!(!mapper.compact_streaming_active);
    assert_eq!(mapper.drain_and_stop_tailer(), None, "no tailer was started");
    mapper.finish();
    assert!(root_events(&collector.snapshot()).is_empty());
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
        mapper.on_target_completed(label, true, &[], Some(2_000_000_000));
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
    mapper.on_target_completed("//:t", false, &[], Some(2_000_000_000));
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
    mapper.on_target_completed("//pkg:boom", true, &[], Some(2_000_000_000));
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
    assert_eq!(attr_value(&action, BAZEL_ACTION_SUCCESS).as_deref(), Some("false"));
    assert!(matches!(action.status, Status::Error { .. }));
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
    mapper.on_target_completed("//pkg:lib", true, &[], Some(1_500_000_000));
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
    mapper.on_target_completed("//pkg:lib", false, &[], Some(1_500_000_000));
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
        action_summary::RunnerCount,
        artifact_metrics::FilesMetric,
        ActionSummary, ArtifactMetrics, CumulativeMetrics, MemoryMetrics, TimingMetrics,
    };
    use crate::build_event_stream::BuildMetrics;

    let (mut mapper, collector) = mapper_with_collector();
    mapper.on_build_finished(Some(0), Some(5_000_000_000), None);
    let mut metrics = BuildMetrics {
        action_summary: Some(ActionSummary {
            actions_created: 7,
            runner_count: vec![RunnerCount {
                name: "remote cache hit".into(),
                count: 3,
                ..Default::default()
            }],
            ..Default::default()
        }),
        memory_metrics: Some(MemoryMetrics {
            used_heap_size_post_build: 1,
            peak_post_gc_heap_size: 9,
            ..Default::default()
        }),
        artifact_metrics: Some(ArtifactMetrics {
            source_artifacts_read: Some(FilesMetric::default()),
            ..Default::default()
        }),
        cumulative_metrics: Some(CumulativeMetrics::default()),
        // Wall time ends at 3s, before BuildFinished at 5s.
        timing_metrics: Some(TimingMetrics {
            wall_time_in_ms: 2_000,
            ..Default::default()
        }),
        ..Default::default()
    };
    let acs = metrics
        .action_summary
        .as_mut()
        .unwrap()
        .action_cache_statistics
        .get_or_insert_with(Default::default);
    acs.hits = 4;
    acs.misses = 2;
    mapper.on_build_metrics(&metrics);
    mapper.finish();

    let spans = collector.snapshot();
    let root = find_span(&spans, |s| s.parent_span_id == SpanId::INVALID)
        .expect("root span");
    assert_eq!(attr_value(&root, BAZEL_METRICS_ACTIONS_CREATED).as_deref(), Some("7"));
    assert_eq!(attr_value(&root, BAZEL_METRICS_WALL_TIME_MS).as_deref(), Some("2000"));
    assert_eq!(attr_value(&root, "bazel.metrics.runner.remote_cache_hit").as_deref(), Some("3"));
    assert_eq!(attr_value(&root, BAZEL_METRICS_ACTION_CACHE_HITS).as_deref(), Some("4"));
    assert_eq!(attr_value(&root, BAZEL_METRICS_ACTION_CACHE_MISSES).as_deref(), Some("2"));
    assert_eq!(attr_value(&root, BAZEL_METRICS_PEAK_HEAP_POST_GC).as_deref(), Some("9"));
    for dropped in [
        "bazel.metrics.heap_post_build",
        "bazel.metrics.source_artifacts_count",
        "bazel.metrics.cumulative_num_builds",
        "bazel.metrics.actions_execution_start_ms",
    ] {
        assert_eq!(attr_value(&root, dropped), None, "{dropped} should not be emitted");
    }
    assert_eq!(root.end_time, super::nanos_to_system_time(5_000_000_000));
}

// =====================================================================
// Progress logs (`--logs`). Fixtures are Bazel 9 console chunks.
// =====================================================================

use super::progress::{error_record_summaries, LOG_RECORD_CAP_BYTES, MAX_LOG_RECORDS};
use crate::otel::span::{Logger, Severity, Status};

#[test]
fn error_logs_keep_warnings_and_drop_progress_noise() {
    let stderr = "Computing main repo mapping: \nLoading: 0 packages loaded\n\
        WARNING: /tmp/ws/BUILD.bazel:2:8: target '//:old' is deprecated: use //:new instead\n\
        Analyzing: 4 targets (5 packages loaded, 6 targets configured)\n\
        [4 / 4] no actions running\nINFO: Analyzed 4 targets (81 packages loaded).\n";
    assert_eq!(
        error_record_summaries(&[(stderr, "")]),
        vec![(
            Severity::Warn,
            "WARNING: /tmp/ws/BUILD.bazel:2:8: target '//:old' is deprecated: use //:new instead".to_string(),
        )],
    );
}

#[test]
fn error_logs_keep_failed_action_stderr_with_its_error() {
    let stderr = "ERROR: /tmp/ws/BUILD.bazel:14:8: Executing genrule //:broken failed: (Exit 1)\n\n\
        Use --sandbox_debug to see verbose messages from the sandbox\n\
        compiler: error: something bad\n    at detail line\nplain trailing line\n\
        INFO: Elapsed time: 11.999s, Critical Path: 0.47s\n\
        ERROR: Build did NOT complete successfully\nFAILED: \n";
    let records = error_record_summaries(&[(stderr, "")]);
    assert_eq!(records.len(), 2, "{records:#?}");
    assert_eq!(records[0].0, Severity::Error);
    assert!(records[0].1.starts_with("ERROR: /tmp/ws/BUILD.bazel:14:8"));
    assert!(records[0].1.ends_with("compiler: error: something bad\n    at detail line\nplain trailing line"));
    assert_eq!(records[1], (Severity::Error, "ERROR: Build did NOT complete successfully".to_string()));
}

#[test]
fn error_logs_keep_failed_test_line_and_test_output() {
    let stderr = "FAIL: //:failing_test (Exit 1) (see /tmp/ws/testlogs/failing_test/test.log)\n\
        INFO: From Testing //:failing_test:\n";
    let stdout = "==================== Test output for //:failing_test:\ntest says hello\n\
        assertion failed: 1 != 2\n\
        ================================================================================\n\
        //:failing_test                                                 FAILED in 0.4s\n";
    let records = error_record_summaries(&[(stderr, stdout)]);
    assert_eq!(records.len(), 2, "{records:#?}");
    assert_eq!(
        records[0],
        (Severity::Error, "FAIL: //:failing_test (Exit 1) (see /tmp/ws/testlogs/failing_test/test.log)".to_string()),
    );
    assert_eq!(records[1].0, Severity::Error);
    assert!(records[1].1.contains("assertion failed: 1 != 2"));
    assert!(!records[1].1.contains("FAILED in"), "summary table is not test output");
}

/// Progress events carry console deltas: a block and even a line can be split
/// across events, and the last block only ends at finish.
#[test]
fn error_logs_survive_progress_chunk_boundaries() {
    let records = error_record_summaries(&[
        ("ERROR: //:broken failed: (Exit 1)\n", ""),
        ("compiler: error: something bad\nWARN", "==================== Test output for //:t:\nline 1\n"),
        ("ING: deprecated\nINFO: done\nERROR: tail", "line 2\n====================\n"),
        (" block\nstill tail", ""),
    ]);
    assert_eq!(
        records,
        vec![
            (Severity::Error, "ERROR: //:broken failed: (Exit 1)\ncompiler: error: something bad".to_string()),
            (Severity::Warn, "WARNING: deprecated".to_string()),
            (Severity::Error, "ERROR: tail block\nstill tail".to_string()),
            (Severity::Error, "==================== Test output for //:t:\nline 1\nline 2\n====================".to_string()),
        ],
    );
}

#[test]
fn error_logs_keep_test_output_written_to_stderr() {
    let stderr = "FAIL: //:t (Exit 1)\n==================== Test output for //:t:\n\
        ERROR: from the test itself\n\
        ================================================================================\n\
        INFO: Build completed\n";
    assert_eq!(
        error_record_summaries(&[(stderr, "")]),
        vec![
            (Severity::Error, "FAIL: //:t (Exit 1)".to_string()),
            (
                Severity::Error,
                "==================== Test output for //:t:\nERROR: from the test itself\n\
                 ================================================================================"
                    .to_string(),
            ),
        ],
    );
}

const TEST_OUTPUT_END: &str = "================================================================================\n";

fn summarize(mapper: &mut OtelMapper, label: &str, status: &str) {
    mapper.on_test_summary(label, Some(status), Some(2), Some(1), Some(1), Some(2), Some(0));
}

/// `--test_output=all` prints passing tests too; only failed ones are errors.
#[test]
fn error_logs_drop_passing_test_output() {
    let (mut mapper, _) = mapper_with_logger(LogMode::Errors);
    for shard in ["shard 1 of 2", "shard 2 of 2"] {
        let out = format!("==================== Test output for //:ok ({shard}):\nall good\n{TEST_OUTPUT_END}");
        mapper.on_progress(Some("INFO: From Testing //:ok:\n"), Some(&out));
    }
    summarize(&mut mapper, "//:ok", "PASSED");
    assert_eq!(mapper.log_records, 0);

    for shard in ["shard 1 of 2", "shard 2 of 2"] {
        let out = format!("==================== Test output for //:flaky ({shard}):\nboom\n{TEST_OUTPUT_END}");
        mapper.on_progress(None, Some(&out));
    }
    assert_eq!(mapper.log_records, 0, "held until the TestSummary");
    summarize(&mut mapper, "//:flaky", "FLAKY");
    assert_eq!(mapper.log_records, 2);

    let late = format!("==================== Test output for //:flaky:\nboom\n{TEST_OUTPUT_END}");
    mapper.on_progress(None, Some(&late));
    assert_eq!(mapper.log_records, 3, "status already known");

    summarize(&mut mapper, "//:flaky", "PASSED");
    mapper.on_progress(None, Some(&late));
    assert_eq!(mapper.log_records, 4, "another configuration passing keeps the failure");

    mapper.finish();
    assert_eq!(mapper.log_records, 4);
}

#[test]
fn held_test_output_is_capped() {
    let (mut mapper, collector) = mapper_with_logger(LogMode::Errors);
    for i in 0..MAX_LOG_RECORDS + 3 {
        let out = format!("==================== Test output for //:t{i}:\nboom\n{TEST_OUTPUT_END}");
        mapper.on_progress(None, Some(&out));
    }
    assert_eq!(mapper.error_logs.pending_blocks, MAX_LOG_RECORDS as usize);
    summarize(&mut mapper, "//:t0", "FAILED");
    assert_eq!(mapper.error_logs.pending_blocks, MAX_LOG_RECORDS as usize - 1);
    summarize(&mut mapper, &format!("//:t{MAX_LOG_RECORDS}"), "PASSED");
    assert_eq!(mapper.error_logs.dropped, 0, "a passing label's output is not suppressed");
    mapper.finish();
    assert_eq!(mapper.log_records, MAX_LOG_RECORDS);

    let spans = collector.snapshot();
    let root = find_span(&spans, |s| s.parent_span_id == SpanId::INVALID).expect("root span");
    assert_eq!(attr_value(&root, BAZEL_TRACE_SUPPRESSED_LOGS).as_deref(), Some("2"));
}

#[test]
fn open_error_block_is_emitted_at_finish() {
    let (mut mapper, _) = mapper_with_logger(LogMode::Errors);
    mapper.on_progress(Some("ERROR: boom\naction output\n"), None);
    assert_eq!(mapper.log_records, 0, "block may continue in the next event");
    mapper.finish();
    assert_eq!(mapper.log_records, 1);
}

fn mapper_with_logger(mode: LogMode) -> (OtelMapper, MemorySpans) {
    let (sink, collector) = Sink::memory();
    let mut mapper =
        OtelMapper::new(Tracer::new(sink), Some(Logger::default())).with_log_mode(mode);
    mapper.on_build_started("test-uuid", "build", Some(1_000_000_000), None);
    (mapper, collector)
}

#[test]
fn log_records_are_capped_per_invocation() {
    let (mut mapper, collector) = mapper_with_logger(LogMode::Errors);
    for _ in 0..MAX_LOG_RECORDS + 3 {
        mapper.on_progress(Some("ERROR: boom\n"), None);
    }
    mapper.finish();
    assert_eq!(mapper.log_records, MAX_LOG_RECORDS);

    let spans = collector.snapshot();
    let root = find_span(&spans, |s| s.parent_span_id == SpanId::INVALID).expect("root span");
    assert_eq!(attr_value(&root, BAZEL_TRACE_SUPPRESSED_LOGS).as_deref(), Some("3"));
}

#[test]
fn log_modes_all_and_none() {
    let (mut all, _) = mapper_with_logger(LogMode::All);
    all.on_progress(Some("INFO: noise\n"), Some("stdout"));
    all.on_progress(None, Some("stdout only"));
    assert_eq!(all.log_records, 3, "one record per non-empty stream");

    let (mut none, _) = mapper_with_logger(LogMode::None);
    none.on_progress(Some("ERROR: boom\n"), None);
    assert_eq!((none.log_records, none.suppressed_logs), (0, 0));
}

#[test]
fn log_record_body_is_capped() {
    let long = format!("ERROR: {}", "x".repeat(2 * LOG_RECORD_CAP_BYTES));
    let capped = super::progress::cap(&long);
    assert!(capped.len() <= LOG_RECORD_CAP_BYTES);
    assert!(capped.ends_with("...(truncated)"));
}
