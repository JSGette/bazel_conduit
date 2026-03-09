//! OTel attribute key constants for BEP → OTel mapping.
//!
//! Follows the naming convention `bazel.<component>.<field>`.

// Trace-level (from BuildStarted / invocation metadata)
pub const BAZEL_INVOCATION_ID: &str = "bazel.invocation_id";
pub const BAZEL_COMMAND: &str = "bazel.command";
pub const BAZEL_COMMAND_LINE: &str = "bazel.command_line";
pub const BAZEL_PATTERNS: &str = "bazel.patterns";
pub const BAZEL_EXIT_CODE: &str = "bazel.exit_code";
pub const BAZEL_ACTION_MODE: &str = "bazel.action_mode";

// Workspace status
pub const BAZEL_WORKSPACE_USER: &str = "bazel.workspace.user";
pub const BAZEL_WORKSPACE_HOST: &str = "bazel.workspace.host";

// Configuration
pub const BAZEL_CONFIG_MNEMONIC: &str = "bazel.config.mnemonic";
pub const BAZEL_CONFIG_PLATFORM: &str = "bazel.config.platform";
pub const BAZEL_CONFIG_ID: &str = "bazel.config.id";

// Target span
pub const BAZEL_TARGET_LABEL: &str = "bazel.target.label";
pub const BAZEL_TARGET_KIND: &str = "bazel.target.kind";
pub const BAZEL_TARGET_SUCCESS: &str = "bazel.target.success";
pub const BAZEL_TARGET_TAGS: &str = "bazel.target.tags";
pub const BAZEL_TARGET_OUTPUT_COUNT: &str = "bazel.target.output_count";
pub const BAZEL_TARGET_OUTPUT_FILES: &str = "bazel.target.output_files";

// Action span
pub const BAZEL_ACTION_MNEMONIC: &str = "bazel.action.mnemonic";
pub const BAZEL_ACTION_EXIT_CODE: &str = "bazel.action.exit_code";
pub const BAZEL_ACTION_PRIMARY_OUTPUT: &str = "bazel.action.primary_output";
pub const BAZEL_ACTION_SUCCESS: &str = "bazel.action.success";
pub const BAZEL_ACTION_COMMAND_LINE: &str = "bazel.action.command_line";
pub const BAZEL_ACTION_STDOUT: &str = "bazel.action.stdout";
pub const BAZEL_ACTION_STDERR: &str = "bazel.action.stderr";
pub const BAZEL_ACTION_LABEL: &str = "bazel.action.label";
pub const BAZEL_ACTION_CONFIGURATION: &str = "bazel.action.configuration";
// BuildStarted extended
pub const BAZEL_WORKSPACE_DIR: &str = "bazel.workspace_directory";
pub const BAZEL_WORKING_DIR: &str = "bazel.working_directory";
pub const BAZEL_BUILD_TOOL_VERSION: &str = "bazel.build_tool_version";
pub const BAZEL_SERVER_PID: &str = "bazel.server_pid";
pub const BAZEL_HOST: &str = "bazel.host";
pub const BAZEL_USER: &str = "bazel.user";

// Fetch span
pub const BAZEL_FETCH_URL: &str = "bazel.fetch.url";
pub const BAZEL_FETCH_SUCCESS: &str = "bazel.fetch.success";
pub const BAZEL_FETCH_DOWNLOADER: &str = "bazel.fetch.downloader";

// Test span
pub const BAZEL_TEST_STATUS: &str = "bazel.test.status";
pub const BAZEL_TEST_ATTEMPT: &str = "bazel.test.attempt";
pub const BAZEL_TEST_RUN: &str = "bazel.test.run";
pub const BAZEL_TEST_SHARD: &str = "bazel.test.shard";
pub const BAZEL_TEST_CACHED: &str = "bazel.test.cached_locally";
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
pub const BAZEL_METRICS_HEAP_POST_BUILD: &str = "bazel.metrics.heap_post_build";
pub const BAZEL_METRICS_PEAK_HEAP_POST_GC: &str = "bazel.metrics.peak_heap_post_gc";
pub const BAZEL_METRICS_TARGETS_CONFIGURED: &str = "bazel.metrics.targets_configured";
pub const BAZEL_METRICS_PACKAGES_LOADED: &str = "bazel.metrics.packages_loaded";
pub const BAZEL_METRICS_CACHE_HITS: &str = "bazel.metrics.cache_hits";
pub const BAZEL_METRICS_CACHE_MISSES: &str = "bazel.metrics.cache_misses";
pub const BAZEL_METRICS_SOURCE_ARTIFACTS_COUNT: &str = "bazel.metrics.source_artifacts_count";
pub const BAZEL_METRICS_OUTPUT_ARTIFACTS_COUNT: &str = "bazel.metrics.output_artifacts_count";
pub const BAZEL_METRICS_ACTION_CACHE_ARTIFACTS_COUNT: &str =
    "bazel.metrics.action_cache_artifacts_count";
pub const BAZEL_METRICS_BYTES_SENT: &str = "bazel.metrics.bytes_sent";
pub const BAZEL_METRICS_BYTES_RECV: &str = "bazel.metrics.bytes_recv";
pub const BAZEL_METRICS_ACTION_DATA: &str = "bazel.metrics.action_data";

pub const BAZEL_METRICS_ACTIONS_EXECUTION_START_MS: &str =
    "bazel.metrics.actions_execution_start_ms";
pub const BAZEL_METRICS_RUNNER_COUNT: &str = "bazel.metrics.runner_count";
pub const BAZEL_METRICS_TOP_LEVEL_ARTIFACTS_COUNT: &str =
    "bazel.metrics.top_level_artifacts_count";
pub const BAZEL_METRICS_CUMULATIVE_NUM_ANALYSES: &str = "bazel.metrics.cumulative_num_analyses";
pub const BAZEL_METRICS_CUMULATIVE_NUM_BUILDS: &str = "bazel.metrics.cumulative_num_builds";

// Test extended
pub const BAZEL_TEST_RUN_COUNT: &str = "bazel.test.run_count";
pub const BAZEL_TEST_ATTEMPT_COUNT: &str = "bazel.test.attempt_count";
pub const BAZEL_TEST_SHARD_COUNT: &str = "bazel.test.shard_count";
pub const BAZEL_TEST_TOTAL_NUM_CACHED: &str = "bazel.test.total_num_cached";

// Target summary (span event)
pub const BAZEL_TARGET_OVERALL_BUILD_SUCCESS: &str = "bazel.target.overall_build_success";
pub const BAZEL_TARGET_OVERALL_TEST_STATUS: &str = "bazel.target.overall_test_status";

// Named set (span event)
pub const BAZEL_NAMED_SET_ID: &str = "bazel.named_set.id";
pub const BAZEL_NAMED_SET_FILE_COUNT: &str = "bazel.named_set.file_count";

// Progress / build log span events
pub const BAZEL_PROGRESS_STDERR: &str = "bazel.progress.stderr";
pub const BAZEL_PROGRESS_STDOUT: &str = "bazel.progress.stdout";
