//! Build state tracking
//!
//! Maintains state throughout a build invocation.

use super::ActionProcessingMode;
use dashmap::DashMap;
use std::collections::HashMap;

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

/// Configuration state
#[derive(Debug, Clone)]
pub struct ConfigurationState {
    pub id: String,
    pub mnemonic: Option<String>,
    pub platform: Option<String>,
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

    // Configurations (config_id -> ConfigurationState)
    configurations: DashMap<String, ConfigurationState>,

    // Targets (label -> TargetState)
    targets: DashMap<String, TargetState>,

    // Named sets cache (set_id -> files)
    named_sets: DashMap<String, Vec<String>>,

    // Failed actions
    failed_actions: Vec<ActionState>,

    // Build metrics (stored as JSON for now)
    build_metrics: Option<serde_json::Value>,
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
            configurations: DashMap::new(),
            targets: DashMap::new(),
            named_sets: DashMap::new(),
            failed_actions: Vec::new(),
            build_metrics: None,
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
    // Configurations
    // =========================================================================

    pub fn add_configuration(
        &self,
        id: String,
        mnemonic: Option<String>,
        platform: Option<String>,
    ) {
        self.configurations.insert(
            id.clone(),
            ConfigurationState {
                id,
                mnemonic,
                platform,
            },
        );
    }

    pub fn get_configuration(&self, id: &str) -> Option<ConfigurationState> {
        self.configurations.get(id).map(|r| r.value().clone())
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

    pub fn get_target(&self, label: &str) -> Option<TargetState> {
        self.targets.get(label).map(|r| r.value().clone())
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

    /// Resolve file set IDs to actual file names
    pub fn resolve_output_files(&self, file_set_ids: &[String]) -> Vec<String> {
        file_set_ids
            .iter()
            .filter_map(|id| self.get_named_set(id))
            .flatten()
            .collect()
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
        self.failed_actions.push(ActionState {
            label,
            mnemonic,
            primary_output,
            success,
            exit_code,
        });
    }

    pub fn failed_actions(&self) -> &[ActionState] {
        &self.failed_actions
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

        BuildSummary {
            invocation_id: self.invocation_id.clone(),
            command: self.command.clone(),
            exit_code: self.exit_code,
            total_targets: self.targets.len(),
            successful_targets,
            failed_targets,
            failed_actions: self.failed_actions.len(),
            action_mode: self.action_mode,
        }
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
        writeln!(f, "  Failed Actions: {}", self.failed_actions)?;
        writeln!(f, "  Action Mode: {}", self.action_mode)?;
        Ok(())
    }
}
