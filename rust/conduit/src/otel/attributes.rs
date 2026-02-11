//! OTel attribute key constants for BEP → OTel mapping.
//!
//! Follows the naming convention `bazel.<component>.<field>`.

// Trace-level (from BuildStarted / invocation metadata)
pub const BAZEL_INVOCATION_ID: &str = "bazel.invocation_id";
pub const BAZEL_COMMAND: &str = "bazel.command";
pub const BAZEL_COMMAND_LINE: &str = "bazel.command_line";
pub const BAZEL_PATTERNS: &str = "bazel.patterns";
pub const BAZEL_EXIT_CODE: &str = "bazel.exit_code";

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

// Fetch span
pub const BAZEL_FETCH_URL: &str = "bazel.fetch.url";
pub const BAZEL_FETCH_SUCCESS: &str = "bazel.fetch.success";

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

// Named set (span event)
pub const BAZEL_NAMED_SET_ID: &str = "bazel.named_set.id";
pub const BAZEL_NAMED_SET_FILE_COUNT: &str = "bazel.named_set.file_count";

// Progress / build log span events
pub const BAZEL_PROGRESS_STDERR: &str = "bazel.progress.stderr";
pub const BAZEL_PROGRESS_STDOUT: &str = "bazel.progress.stdout";
