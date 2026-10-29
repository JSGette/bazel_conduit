// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

//! OTel attribute key constants for BEP → OTel mapping.
//!
//! Follows the naming convention `bazel.<component>.<field>`.

// Trace-level (from BuildStarted / invocation metadata)
pub const BAZEL_INVOCATION_ID: &str = "bazel.invocation_id";
pub const BAZEL_COMMAND: &str = "bazel.command";
pub const BAZEL_PATTERNS: &str = "bazel.patterns";
pub const BAZEL_EXIT_CODE: &str = "bazel.exit_code";
pub const BAZEL_EXIT_CODE_NAME: &str = "bazel.exit_code_name";

// Trace emission policy (set on root span from the TracePolicy).
pub const BAZEL_TRACE_PROFILE: &str = "bazel.trace.profile";
// Counts of spans dropped by the lightweight profile, for volume visibility.
pub const BAZEL_TRACE_SUPPRESSED_ACTIONS: &str = "bazel.trace.suppressed_actions";
pub const BAZEL_TRACE_SUPPRESSED_SPAWNS: &str = "bazel.trace.suppressed_spawns";
/// Log records over the per-invocation cap.
pub const BAZEL_TRACE_SUPPRESSED_LOGS: &str = "bazel.trace.suppressed_logs";
pub const BAZEL_TRACE_SUPPRESSED_TARGETS: &str = "bazel.trace.suppressed_targets";
pub const BAZEL_EXPLICIT_CMD_LINE: &str = "bazel.explicit_cmd_line";
pub const BAZEL_TOOL_TAG: &str = "bazel.tool_tag";

// Workspace status
pub const BAZEL_WORKSPACE_USER: &str = "bazel.workspace.user";
pub const BAZEL_WORKSPACE_HOST: &str = "bazel.workspace.host";

// Target span
pub const BAZEL_TARGET_LABEL: &str = "bazel.target.label";
pub const BAZEL_TARGET_KIND: &str = "bazel.target.kind";
pub const BAZEL_TARGET_SUCCESS: &str = "bazel.target.success";
pub const BAZEL_TARGET_TAGS: &str = "bazel.target.tags";
pub const BAZEL_TARGET_ABORT_REASON: &str = "bazel.target.abort_reason";
pub const BAZEL_TARGET_ABORT_DESCRIPTION: &str = "bazel.target.abort_description";
pub const BAZEL_TARGET_CACHED: &str = "bazel.target.cached";
pub const BAZEL_TARGET_SYNTHETIC: &str = "bazel.target.synthetic";
pub const BAZEL_TARGET_TEST_SIZE: &str = "bazel.target.test_size";

// Action span
pub const BAZEL_ACTION_MNEMONIC: &str = "bazel.action.mnemonic";
pub const BAZEL_ACTION_EXIT_CODE: &str = "bazel.action.exit_code";
pub const BAZEL_ACTION_EXIT_CODE_NAME: &str = "bazel.action.exit_code_name";
pub const BAZEL_ACTION_CACHE_HIT: &str = "bazel.action.cache_hit";
pub const BAZEL_ACTION_RUNNER: &str = "bazel.action.runner";
pub const BAZEL_ACTION_PRIMARY_OUTPUT: &str = "bazel.action.primary_output";
pub const BAZEL_ACTION_SUCCESS: &str = "bazel.action.success";
pub const BAZEL_ACTION_COMMAND_LINE: &str = "bazel.action.command_line";
pub const BAZEL_ACTION_STDERR: &str = "bazel.action.stderr";
pub const BAZEL_ACTION_CONFIGURATION: &str = "bazel.action.configuration";

// Action span: spawn-derived backfill from compact exec log.
// The first SpawnExec for an action backfills `runner` / `cache_hit` /
// SpawnMetrics onto the parent action span, mirroring the per-spawn
// `bazel.spawn.*` keys. Multi-spawn actions (retries, dynamic-exec races)
// only bump `bazel.action.spawn.count`; their attrs land on the per-spawn
// child spans. A count of 0 at `finish()` means no SpawnExec matched (e.g.
// action-cache-only hits).
pub const BAZEL_ACTION_SPAWN_COUNT: &str = "bazel.action.spawn.count";
pub const BAZEL_ACTION_SPAWN_RUNNER: &str = "bazel.action.spawn.runner";
pub const BAZEL_ACTION_SPAWN_CACHE_HIT: &str = "bazel.action.spawn.cache_hit";
pub const BAZEL_ACTION_SPAWN_REMOTE_CACHEABLE: &str = "bazel.action.spawn.remote_cacheable";
pub const BAZEL_ACTION_SPAWN_DIGEST: &str = "bazel.action.spawn.digest";
pub const BAZEL_ACTION_SPAWN_EXEC_WALL_TIME_MS: &str = "bazel.action.spawn.execution_wall_time_ms";
pub const BAZEL_ACTION_SPAWN_QUEUE_TIME_MS: &str = "bazel.action.spawn.queue_time_ms";
pub const BAZEL_ACTION_SPAWN_FETCH_TIME_MS: &str = "bazel.action.spawn.fetch_time_ms";
pub const BAZEL_ACTION_SPAWN_NETWORK_TIME_MS: &str = "bazel.action.spawn.network_time_ms";
pub const BAZEL_ACTION_SPAWN_UPLOAD_TIME_MS: &str = "bazel.action.spawn.upload_time_ms";
pub const BAZEL_ACTION_SPAWN_SETUP_TIME_MS: &str = "bazel.action.spawn.setup_time_ms";
pub const BAZEL_ACTION_SPAWN_PROCESS_OUTPUTS_TIME_MS: &str =
    "bazel.action.spawn.process_outputs_time_ms";

// Spawn span (exec log enrichment)
pub const BAZEL_SPAWN_RUNNER: &str = "bazel.spawn.runner";
pub const BAZEL_SPAWN_CACHE_HIT: &str = "bazel.spawn.cache_hit";
pub const BAZEL_SPAWN_REMOTE_CACHEABLE: &str = "bazel.spawn.remote_cacheable";
pub const BAZEL_SPAWN_EXIT_CODE: &str = "bazel.spawn.exit_code";
pub const BAZEL_SPAWN_INPUT_BYTES: &str = "bazel.spawn.input_bytes";
pub const BAZEL_SPAWN_INPUT_FILES: &str = "bazel.spawn.input_files";
pub const BAZEL_SPAWN_COMMAND: &str = "bazel.spawn.command";
pub const BAZEL_SPAWN_EXEC_WALL_TIME_MS: &str = "bazel.spawn.execution_wall_time_ms";
pub const BAZEL_SPAWN_QUEUE_TIME_MS: &str = "bazel.spawn.queue_time_ms";
pub const BAZEL_SPAWN_NETWORK_TIME_MS: &str = "bazel.spawn.network_time_ms";
pub const BAZEL_SPAWN_SETUP_TIME_MS: &str = "bazel.spawn.setup_time_ms";
pub const BAZEL_SPAWN_FETCH_TIME_MS: &str = "bazel.spawn.fetch_time_ms";
pub const BAZEL_SPAWN_UPLOAD_TIME_MS: &str = "bazel.spawn.upload_time_ms";
pub const BAZEL_SPAWN_PROCESS_OUTPUTS_TIME_MS: &str = "bazel.spawn.process_outputs_time_ms";
pub const BAZEL_SPAWN_RETRY_TIME_MS: &str = "bazel.spawn.retry_time_ms";

// BuildStarted extended (use BAZEL_WORKSPACE_HOST/USER for host/user to avoid duplication)
pub const BAZEL_WORKSPACE_DIR: &str = "bazel.workspace_directory";
pub const BAZEL_WORKING_DIR: &str = "bazel.working_directory";
pub const BAZEL_BUILD_TOOL_VERSION: &str = "bazel.build_tool_version";

// VCS / repository identity on every span. The name comes from
// `module(name = ...)` in MODULE.bazel, else the basename of
// `workspace_directory`; a `--build_metadata=REPO_URL=...` overrides it.
// URL, commit and branch come from `--build_metadata=REPO_URL|COMMIT_SHA|BRANCH`
// and are emitted under both the OTel `vcs.*` and the Datadog `git.*` names.
pub const VCS_REPOSITORY_NAME: &str = "vcs.repository.name";
pub const VCS_REPOSITORY_URL: &str = "vcs.repository.url.full";
pub const VCS_REF_HEAD_REVISION: &str = "vcs.ref.head.revision";
pub const VCS_REF_HEAD_NAME: &str = "vcs.ref.head.name";
pub const GIT_REPOSITORY_URL: &str = "git.repository_url";
pub const GIT_COMMIT_SHA: &str = "git.commit.sha";
pub const GIT_BRANCH: &str = "git.branch";

// Fetch counters (root span)
pub const BAZEL_FETCH_COUNT: &str = "bazel.fetch.count";
pub const BAZEL_FETCH_FAILED: &str = "bazel.fetch.failed";

// Test span
pub const BAZEL_TEST_STATUS: &str = "bazel.test.status";
pub const BAZEL_TEST_ATTEMPT: &str = "bazel.test.attempt";
pub const BAZEL_TEST_RUN: &str = "bazel.test.run";
pub const BAZEL_TEST_SHARD: &str = "bazel.test.shard";
pub const BAZEL_TEST_CACHE: &str = "bazel.test.cache";
pub const BAZEL_TEST_STRATEGY: &str = "bazel.test.strategy";
pub const BAZEL_TEST_OVERALL_STATUS: &str = "bazel.test.overall_status";
pub const BAZEL_TEST_TOTAL_RUN_COUNT: &str = "bazel.test.total_run_count";

// Build metrics
pub const BAZEL_METRICS_ACTIONS_CREATED: &str = "bazel.metrics.actions_created";
pub const BAZEL_METRICS_ACTIONS_EXECUTED: &str = "bazel.metrics.actions_executed";
pub const BAZEL_METRICS_WALL_TIME_MS: &str = "bazel.metrics.wall_time_ms";
pub const BAZEL_METRICS_CPU_TIME_MS: &str = "bazel.metrics.cpu_time_ms";
pub const BAZEL_METRICS_ANALYSIS_PHASE_MS: &str = "bazel.metrics.analysis_phase_ms";
pub const BAZEL_METRICS_EXECUTION_PHASE_MS: &str = "bazel.metrics.execution_phase_ms";
pub const BAZEL_METRICS_CRITICAL_PATH_MS: &str = "bazel.metrics.critical_path_ms";
pub const BAZEL_METRICS_PEAK_HEAP_POST_GC: &str = "bazel.metrics.peak_heap_post_gc";
pub const BAZEL_METRICS_TARGETS_CONFIGURED: &str = "bazel.metrics.targets_configured";
pub const BAZEL_METRICS_PACKAGES_LOADED: &str = "bazel.metrics.packages_loaded";
pub const BAZEL_METRICS_ACTION_CACHE_HITS: &str = "bazel.metrics.action_cache_hits";
pub const BAZEL_METRICS_ACTION_CACHE_MISSES: &str = "bazel.metrics.action_cache_misses";
pub const BAZEL_METRICS_BYTES_SENT: &str = "bazel.metrics.bytes_sent";
pub const BAZEL_METRICS_BYTES_RECV: &str = "bazel.metrics.bytes_recv";


// Test extended
pub const BAZEL_TEST_RUN_COUNT: &str = "bazel.test.run_count";
pub const BAZEL_TEST_ATTEMPT_COUNT: &str = "bazel.test.attempt_count";
pub const BAZEL_TEST_SHARD_COUNT: &str = "bazel.test.shard_count";
pub const BAZEL_TEST_TOTAL_NUM_CACHED: &str = "bazel.test.total_num_cached";
pub const BAZEL_TEST_HOSTNAME: &str = "bazel.test.hostname";

// -----------------------------------------------------------------------------
// Label helpers (for readable span names and attributes)
// -----------------------------------------------------------------------------

/// Strip Bzlmod canonical repository prefixes from a Bazel label for readable
/// span names and label attributes.
///
/// - `@@//path/to:target` (main repo canonical) → `//path/to:target`
/// - `@repository_name//path:target` (external repo) → `//path:target`
///
/// Full label remains available on spans via attributes (e.g. `bazel.target.label`).
pub fn shorten_label(label: &str) -> &str {
    if label.starts_with("@@") {
        return &label[2..];
    }
    if label.starts_with('@') {
        if let Some(idx) = label.find("//") {
            return &label[idx..];
        }
    }
    label
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shorten_label_main_repo_canonical() {
        assert_eq!(shorten_label("@@//path/to:target"), "//path/to:target");
    }

    #[test]
    fn shorten_label_external_repo() {
        assert_eq!(shorten_label("@rules_rust//rust:defs.bzl"), "//rust:defs.bzl");
    }

    #[test]
    fn shorten_label_double_at_external() {
        assert_eq!(
            shorten_label("@@rules_rust+0.40.0//rust:defs.bzl"),
            "rules_rust+0.40.0//rust:defs.bzl"
        );
    }

    #[test]
    fn shorten_label_no_prefix() {
        assert_eq!(shorten_label("//path/to:target"), "//path/to:target");
    }

    #[test]
    fn shorten_label_empty() {
        assert_eq!(shorten_label(""), "");
    }

    #[test]
    fn shorten_label_at_no_double_slash() {
        assert_eq!(shorten_label("@repo"), "@repo");
    }

    #[test]
    fn shorten_label_short_label() {
        assert_eq!(shorten_label("//:main"), "//:main");
    }
}
