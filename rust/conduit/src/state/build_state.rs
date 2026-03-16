//! Build state tracking
//!
//! Maintains state throughout a build invocation.

use super::ActionProcessingMode;
use dashmap::DashMap;
use std::collections::HashMap;
use std::path::PathBuf;

/// Tracked state for a single target
#[derive(Debug, Clone)]
pub struct TargetState {
    pub label: String,
    pub kind: Option<String>,
    pub start_time: Option<String>,
    pub end_time: Option<String>,
    pub success: Option<bool>,
    pub output_file_sets: Vec<String>,
}

/// Tracked state for a single action
#[derive(Debug, Clone)]
pub struct ActionState {
    pub label: Option<String>,
    pub mnemonic: Option<String>,
    pub primary_output: Option<String>,
    pub success: bool,
    pub exit_code: Option<i32>,
}

/// Action data buffered until target completion so target span timing can use
/// earliest action start and latest action end.
#[derive(Debug, Clone)]
pub struct BufferedAction {
    pub label: Option<String>,
    pub mnemonic: Option<String>,
    pub success: bool,
    pub exit_code: Option<i32>,
    pub exit_code_name: Option<String>,
    pub primary_output: Option<String>,
    pub configuration: Option<String>,
    pub command_line: Vec<String>,
    pub stdout_uri: Option<String>,
    pub stderr_uri: Option<String>,
    pub start_nanos: Option<i64>,
    pub end_nanos: Option<i64>,
    pub cached: Option<bool>,
    pub hostname: Option<String>,
    pub cached_remotely: Option<bool>,
    pub runner: Option<String>,
}

/// Per-target min/max action timings and buffered actions for OTEL replay.
#[derive(Debug, Default)]
pub struct TargetActionBuffer {
    pub earliest_start_nanos: Option<i64>,
    pub latest_end_nanos: Option<i64>,
    pub actions: Vec<BufferedAction>,
}

impl TargetActionBuffer {
    fn update_timing(&mut self, start_nanos: i64, end_nanos: i64) {
        self.earliest_start_nanos = Some(
            self.earliest_start_nanos
                .map_or(start_nanos, |x| x.min(start_nanos)),
        );
        self.latest_end_nanos =
            Some(self.latest_end_nanos.map_or(end_nanos, |x| x.max(end_nanos)));
    }
}

/// Build state tracker
#[derive(Debug)]
pub struct BuildState {
    // Invocation metadata
    invocation_id: Option<String>,
    command: Option<String>,
    command_args: Vec<String>,
    start_time_millis: Option<i64>,
    end_time_millis: Option<i64>,
    exit_code: Option<i32>,
    finished: bool,

    // Options
    startup_options: Vec<String>,
    action_mode: ActionProcessingMode,

    // Workspace
    workspace_status: HashMap<String, String>,
    patterns: Vec<String>,

    // Targets (label -> TargetState)
    targets: DashMap<String, TargetState>,

    // Named sets cache (set_id -> files)
    named_sets: DashMap<String, Vec<String>>,

    // Actions (failed in lightweight mode, all in full mode)
    actions: Vec<ActionState>,

    // Per-target action timing and buffered actions (for OTEL target span timing)
    target_action_buffers: DashMap<String, TargetActionBuffer>,

    // Build metrics (stored as JSON for now)
    build_metrics: Option<serde_json::Value>,

    // Execution log path (from --execution_log_binary_file=)
    exec_log_path: Option<PathBuf>,
}

impl BuildState {
    /// Create new build state
    pub fn new() -> Self {
        Self {
            invocation_id: None,
            command: None,
            command_args: Vec::new(),
            start_time_millis: None,
            end_time_millis: None,
            exit_code: None,
            finished: false,
            startup_options: Vec::new(),
            action_mode: ActionProcessingMode::default(),
            workspace_status: HashMap::new(),
            patterns: Vec::new(),
            targets: DashMap::new(),
            named_sets: DashMap::new(),
            actions: Vec::new(),
            target_action_buffers: DashMap::new(),
            build_metrics: None,
            exec_log_path: None,
        }
    }

    // =========================================================================
    // Invocation metadata
    // =========================================================================

    pub fn set_invocation_id(&mut self, id: String) {
        self.invocation_id = Some(id);
    }

    pub fn invocation_id(&self) -> Option<&str> {
        self.invocation_id.as_deref()
    }

    pub fn set_command(&mut self, command: String) {
        self.command = Some(command);
    }

    pub fn command(&self) -> Option<&str> {
        self.command.as_deref()
    }

    pub fn set_command_args(&mut self, args: Vec<String>) {
        self.command_args = args;
    }

    pub fn command_args(&self) -> &[String] {
        &self.command_args
    }

    pub fn set_start_time_millis(&mut self, millis: i64) {
        self.start_time_millis = Some(millis);
    }

    pub fn start_time_millis(&self) -> Option<i64> {
        self.start_time_millis
    }

    pub fn set_end_time_millis(&mut self, millis: i64) {
        self.end_time_millis = Some(millis);
    }

    pub fn end_time_millis(&self) -> Option<i64> {
        self.end_time_millis
    }

    pub fn set_exit_code(&mut self, code: i32) {
        self.exit_code = Some(code);
    }

    pub fn exit_code(&self) -> Option<i32> {
        self.exit_code
    }

    pub fn mark_finished(&mut self) {
        self.finished = true;
    }

    pub fn is_finished(&self) -> bool {
        self.finished
    }

    // =========================================================================
    // Options
    // =========================================================================

    pub fn set_startup_options(&mut self, options: Vec<String>) {
        self.startup_options = options;
    }

    pub fn startup_options(&self) -> &[String] {
        &self.startup_options
    }

    pub fn set_action_mode(&mut self, mode: ActionProcessingMode) {
        self.action_mode = mode;
    }

    pub fn action_mode(&self) -> ActionProcessingMode {
        self.action_mode
    }

    pub fn set_exec_log_path(&mut self, path: Option<PathBuf>) {
        self.exec_log_path = path;
    }

    pub fn exec_log_path(&self) -> Option<&PathBuf> {
        self.exec_log_path.as_ref()
    }

    // =========================================================================
    // Workspace
    // =========================================================================

    pub fn add_workspace_status(&mut self, key: String, value: String) {
        self.workspace_status.insert(key, value);
    }

    pub fn workspace_status(&self) -> &HashMap<String, String> {
        &self.workspace_status
    }

    pub fn set_patterns(&mut self, patterns: Vec<String>) {
        self.patterns = patterns;
    }

    pub fn patterns(&self) -> &[String] {
        &self.patterns
    }

    // =========================================================================
    // Targets
    // =========================================================================

    pub fn start_target(
        &self,
        label: String,
        kind: Option<String>,
        start_time: Option<String>,
    ) {
        self.targets.insert(
            label.clone(),
            TargetState {
                label,
                kind,
                start_time,
                end_time: None,
                success: None,
                output_file_sets: Vec::new(),
            },
        );
    }

    pub fn complete_target(
        &self,
        label: String,
        success: bool,
        file_sets: Vec<String>,
        end_time: Option<String>,
    ) {
        if let Some(mut target) = self.targets.get_mut(&label) {
            target.success = Some(success);
            target.end_time = end_time;
            target.output_file_sets = file_sets;
        }
    }

    pub fn targets(&self) -> Vec<TargetState> {
        self.targets.iter().map(|r| r.value().clone()).collect()
    }

    pub fn targets_count(&self) -> usize {
        self.targets.len()
    }

    // =========================================================================
    // Named sets (output file cache)
    // =========================================================================

    pub fn cache_named_set(&self, id: String, files: Vec<String>) {
        self.named_sets.insert(id, files);
    }

    pub fn get_named_set(&self, id: &str) -> Option<Vec<String>> {
        self.named_sets.get(id).map(|r| r.value().clone())
    }

    // =========================================================================
    // Actions
    // =========================================================================

    pub fn record_action(
        &mut self,
        label: Option<String>,
        mnemonic: Option<String>,
        primary_output: Option<String>,
        success: bool,
        exit_code: Option<i32>,
    ) {
        self.actions.push(ActionState {
            label,
            mnemonic,
            primary_output,
            success,
            exit_code,
        });
    }

    pub fn actions(&self) -> &[ActionState] {
        &self.actions
    }

    pub fn actions_count(&self) -> usize {
        self.actions.len()
    }

    /// Buffer action for OTEL and update per-target earliest/latest timings.
    /// Used so the target span can be created at completion with start =
    /// min(action starts) and end = max(action ends).
    #[allow(clippy::too_many_arguments)]
    pub fn record_and_buffer_action(
        &self,
        label: String,
        mnemonic: Option<String>,
        primary_output: Option<String>,
        success: bool,
        exit_code: Option<i32>,
        exit_code_name: Option<String>,
        configuration: Option<String>,
        command_line: Vec<String>,
        stdout_uri: Option<String>,
        stderr_uri: Option<String>,
        start_nanos: Option<i64>,
        end_nanos: Option<i64>,
        cached: Option<bool>,
        hostname: Option<String>,
        cached_remotely: Option<bool>,
        runner: Option<String>,
    ) {
        let mut buf = self.target_action_buffers.entry(label.clone()).or_default();
        buf.actions.push(BufferedAction {
            label: Some(label),
            mnemonic,
            success,
            exit_code,
            exit_code_name,
            primary_output,
            configuration,
            command_line,
            stdout_uri,
            stderr_uri,
            start_nanos,
            end_nanos,
            cached,
            hostname,
            cached_remotely,
            runner,
        });
        if let (Some(s), Some(e)) = (start_nanos, end_nanos) {
            buf.update_timing(s, e);
        }
    }

    /// Remove and return the action buffer for a target, if any.
    /// Returns (earliest_start_nanos, latest_end_nanos, buffered_actions).
    pub fn take_target_action_buffer(
        &self,
        label: &str,
    ) -> Option<(Option<i64>, Option<i64>, Vec<BufferedAction>)> {
        self.target_action_buffers.remove(label).map(|(_, buf)| {
            (
                buf.earliest_start_nanos,
                buf.latest_end_nanos,
                buf.actions,
            )
        })
    }

    /// Drain ALL remaining action buffers (unconsumed transitive deps).
    /// Returns a vec of (label, earliest, latest, actions) tuples.
    pub fn drain_remaining_action_buffers(
        &self,
    ) -> Vec<(String, Option<i64>, Option<i64>, Vec<BufferedAction>)> {
        let keys: Vec<String> = self
            .target_action_buffers
            .iter()
            .map(|r| r.key().clone())
            .collect();
        let mut result = Vec::new();
        for key in keys {
            if let Some((label, buf)) = self.target_action_buffers.remove(&key) {
                result.push((
                    label,
                    buf.earliest_start_nanos,
                    buf.latest_end_nanos,
                    buf.actions,
                ));
            }
        }
        result
    }

    // =========================================================================
    // Build metrics
    // =========================================================================

    pub fn set_build_metrics(&mut self, metrics: serde_json::Value) {
        self.build_metrics = Some(metrics);
    }

    pub fn build_metrics(&self) -> Option<&serde_json::Value> {
        self.build_metrics.as_ref()
    }

    // =========================================================================
    // Summary
    // =========================================================================

    /// Get a summary of the build state
    pub fn summary(&self) -> BuildSummary {
        let successful_targets = self
            .targets
            .iter()
            .filter(|t| t.success == Some(true))
            .count();
        let failed_targets = self
            .targets
            .iter()
            .filter(|t| t.success == Some(false))
            .count();
        let failed_actions = self.actions.iter().filter(|a| !a.success).count();

        BuildSummary {
            invocation_id: self.invocation_id.clone(),
            command: self.command.clone(),
            exit_code: self.exit_code,
            total_targets: self.targets.len(),
            successful_targets,
            failed_targets,
            total_actions: self.actions.len(),
            failed_actions,
            action_mode: self.action_mode,
        }
    }

    /// Reset all state for a new build invocation.
    pub fn reset(&mut self) {
        self.invocation_id = None;
        self.command = None;
        self.command_args.clear();
        self.start_time_millis = None;
        self.end_time_millis = None;
        self.exit_code = None;
        self.finished = false;
        self.startup_options.clear();
        self.action_mode = ActionProcessingMode::default();
        self.workspace_status.clear();
        self.patterns.clear();
        self.targets.clear();
        self.named_sets.clear();
        self.actions.clear();
        self.target_action_buffers.clear();
        self.build_metrics = None;
        self.exec_log_path = None;
    }
}

impl Default for BuildState {
    fn default() -> Self {
        Self::new()
    }
}

/// Summary of build state
#[derive(Debug, Clone)]
pub struct BuildSummary {
    pub invocation_id: Option<String>,
    pub command: Option<String>,
    pub exit_code: Option<i32>,
    pub total_targets: usize,
    pub successful_targets: usize,
    pub failed_targets: usize,
    pub total_actions: usize,
    pub failed_actions: usize,
    pub action_mode: ActionProcessingMode,
}

impl std::fmt::Display for BuildSummary {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Build Summary:")?;
        if let Some(id) = &self.invocation_id {
            writeln!(f, "  Invocation ID: {}", id)?;
        }
        if let Some(cmd) = &self.command {
            writeln!(f, "  Command: {}", cmd)?;
        }
        if let Some(code) = self.exit_code {
            writeln!(f, "  Exit Code: {}", code)?;
        }
        writeln!(f, "  Targets: {} total, {} successful, {} failed",
            self.total_targets, self.successful_targets, self.failed_targets)?;
        writeln!(f, "  Actions: {} total, {} failed",
            self.total_actions, self.failed_actions)?;
        writeln!(f, "  Action Mode: {}", self.action_mode)?;
        Ok(())
    }
}
