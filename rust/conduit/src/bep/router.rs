//! BEP event router
//!
//! Routes BEP events to appropriate handlers based on event type.

use super::decoder::BepJsonEvent;
use crate::state::{ActionProcessingMode, BuildState};
use thiserror::Error;
use tracing::{debug, info, trace, warn};

/// Errors that can occur during event routing
#[derive(Error, Debug)]
pub enum RouterError {
    #[error("Missing event ID")]
    MissingEventId,

    #[error("Unknown event type: {0}")]
    UnknownEventType(String),

    #[error("Handler error: {0}")]
    HandlerError(String),
}

/// Event router that processes BEP events
pub struct EventRouter {
    state: BuildState,
}

impl EventRouter {
    /// Create a new event router
    pub fn new() -> Self {
        Self {
            state: BuildState::new(),
        }
    }

    /// Get a reference to the build state
    pub fn state(&self) -> &BuildState {
        &self.state
    }

    /// Get a mutable reference to the build state
    pub fn state_mut(&mut self) -> &mut BuildState {
        &mut self.state
    }

    /// Route a BEP JSON event to the appropriate handler
    pub fn route(&mut self, event: &BepJsonEvent) -> Result<(), RouterError> {
        let event_type = event
            .event_type()
            .ok_or(RouterError::MissingEventId)?;

        trace!(event_type, "Routing BEP event");

        match event_type {
            // Lifecycle events
            "started" => self.handle_started(event),
            "unstructuredCommandLine" => self.handle_unstructured_command_line(event),
            "optionsParsed" => self.handle_options_parsed(event),
            "workspaceStatus" => self.handle_workspace_status(event),
            "configuration" => self.handle_configuration(event),
            "buildFinished" => self.handle_build_finished(event),
            "buildMetrics" => self.handle_build_metrics(event),

            // Build events
            "pattern" => self.handle_pattern(event),
            "targetConfigured" => self.handle_target_configured(event),
            "targetCompleted" => self.handle_target_completed(event),
            "namedSet" => self.handle_named_set(event),
            "actionCompleted" => self.handle_action_completed(event),

            // Test events
            "testResult" => self.handle_test_result(event),
            "testSummary" => self.handle_test_summary(event),

            // Ignored events
            "progress" => self.handle_progress(event),
            "structuredCommandLine" => Ok(()), // Redundant with optionsParsed
            "convenienceSymlinksIdentified" => Ok(()), // Not useful for tracing
            "buildMetadata" => Ok(()), // Usually empty
            "fetch" => Ok(()), // External resource fetch
            "workspaceInfo" => Ok(()), // Workspace info

            // Unknown events
            other => {
                debug!(event_type = other, "Ignoring unknown event type");
                Ok(())
            }
        }
    }

    // =========================================================================
    // Lifecycle event handlers
    // =========================================================================

    fn handle_started(&mut self, event: &BepJsonEvent) -> Result<(), RouterError> {
        let payload = event.get_payload("started");

        if let Some(started) = payload {
            let uuid = started.get("uuid").and_then(|v| v.as_str());
            let command = started.get("command").and_then(|v| v.as_str());
            let start_time_millis = started.get("startTimeMillis").and_then(|v| v.as_i64());

            info!(
                uuid = uuid,
                command = command,
                "Build started"
            );

            if let Some(uuid) = uuid {
                self.state.set_invocation_id(uuid.to_string());
            }
            if let Some(cmd) = command {
                self.state.set_command(cmd.to_string());
            }
            if let Some(millis) = start_time_millis {
                self.state.set_start_time_millis(millis);
            }
        }

        Ok(())
    }

    fn handle_unstructured_command_line(&mut self, event: &BepJsonEvent) -> Result<(), RouterError> {
        if let Some(payload) = event.get_payload("unstructuredCommandLine") {
            if let Some(args) = payload.get("args").and_then(|v| v.as_array()) {
                let args: Vec<String> = args
                    .iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect();
                debug!(args_count = args.len(), "Unstructured command line");
                self.state.set_command_args(args);
            }
        }
        Ok(())
    }

    fn handle_options_parsed(&mut self, event: &BepJsonEvent) -> Result<(), RouterError> {
        if let Some(payload) = event.get_payload("optionsParsed") {
            // Detect action processing mode from options
            let cmd_line = payload.get("cmdLine").and_then(|v| v.as_array());
            let explicit_cmd_line = payload.get("explicitCmdLine").and_then(|v| v.as_array());

            let all_options: Vec<&str> = cmd_line
                .into_iter()
                .chain(explicit_cmd_line)
                .flatten()
                .filter_map(|v| v.as_str())
                .collect();

            // Check for --build_event_publish_all_actions flag
            let has_all_actions = all_options
                .iter()
                .any(|opt| opt.contains("build_event_publish_all_actions"));

            if has_all_actions {
                info!("Detected --build_event_publish_all_actions flag");
                self.state.set_action_mode(ActionProcessingMode::Full);
            } else {
                debug!("Using lightweight action processing (failed actions only)");
                self.state.set_action_mode(ActionProcessingMode::Lightweight);
            }

            // Extract startup options
            if let Some(startup) = payload.get("startupOptions").and_then(|v| v.as_array()) {
                let opts: Vec<String> = startup
                    .iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect();
                self.state.set_startup_options(opts);
            }
        }
        Ok(())
    }

    fn handle_workspace_status(&mut self, event: &BepJsonEvent) -> Result<(), RouterError> {
        if let Some(payload) = event.get_payload("workspaceStatus") {
            if let Some(items) = payload.get("item").and_then(|v| v.as_array()) {
                for item in items {
                    let key = item.get("key").and_then(|v| v.as_str());
                    let value = item.get("value").and_then(|v| v.as_str());
                    if let (Some(k), Some(v)) = (key, value) {
                        self.state.add_workspace_status(k.to_string(), v.to_string());
                    }
                }
            }
        }
        Ok(())
    }

    fn handle_configuration(&mut self, event: &BepJsonEvent) -> Result<(), RouterError> {
        if let Some(config_id) = event.bazel_event.get("id")
            .and_then(|id| id.get("configuration"))
            .and_then(|c| c.get("id"))
            .and_then(|v| v.as_str())
        {
            if let Some(payload) = event.get_payload("configuration") {
                let mnemonic = payload.get("mnemonic").and_then(|v| v.as_str());
                let platform = payload.get("platformName").and_then(|v| v.as_str());

                debug!(
                    config_id,
                    mnemonic,
                    platform,
                    "Configuration"
                );

                self.state.add_configuration(
                    config_id.to_string(),
                    mnemonic.map(String::from),
                    platform.map(String::from),
                );
            }
        }
        Ok(())
    }

    fn handle_build_finished(&mut self, event: &BepJsonEvent) -> Result<(), RouterError> {
        if let Some(payload) = event.get_payload("finished") {
            let exit_code = payload
                .get("exitCode")
                .and_then(|ec| ec.get("code"))
                .and_then(|v| v.as_i64())
                .map(|v| v as i32);
            let finish_time_millis = payload.get("finishTimeMillis").and_then(|v| v.as_i64());

            info!(
                exit_code,
                "Build finished"
            );

            if let Some(code) = exit_code {
                self.state.set_exit_code(code);
            }
            if let Some(millis) = finish_time_millis {
                self.state.set_end_time_millis(millis);
            }
            self.state.mark_finished();
        }
        Ok(())
    }

    fn handle_build_metrics(&mut self, event: &BepJsonEvent) -> Result<(), RouterError> {
        if let Some(payload) = event.get_payload("buildMetrics") {
            debug!("Build metrics received");
            // Store metrics for later processing
            self.state.set_build_metrics(payload.clone());
        }
        Ok(())
    }

    // =========================================================================
    // Build event handlers
    // =========================================================================

    fn handle_pattern(&mut self, event: &BepJsonEvent) -> Result<(), RouterError> {
        if let Some(_payload) = event.get_payload("expanded") {
            // Pattern expanded contains the list of configured targets
            let patterns: Vec<String> = event
                .bazel_event
                .get("id")
                .and_then(|id| id.get("pattern"))
                .and_then(|p| p.get("pattern"))
                .and_then(|v| v.as_array())
                .map(|arr| arr.iter().filter_map(|v| v.as_str().map(String::from)).collect())
                .unwrap_or_default();

            debug!(patterns = ?patterns, "Pattern expanded");
            self.state.set_patterns(patterns);
        }
        Ok(())
    }

    fn handle_target_configured(&mut self, event: &BepJsonEvent) -> Result<(), RouterError> {
        let target_id = event
            .bazel_event
            .get("id")
            .and_then(|id| id.get("targetConfigured"))
            .and_then(|tc| tc.get("label"))
            .and_then(|v| v.as_str());

        if let Some(label) = target_id {
            let target_kind = event
                .get_payload("configured")
                .and_then(|c| c.get("targetKind"))
                .and_then(|v| v.as_str());

            debug!(
                label,
                target_kind,
                "Target configured"
            );

            self.state.start_target(
                label.to_string(),
                target_kind.map(String::from),
                event.event_time.clone(),
            );
        }
        Ok(())
    }

    fn handle_target_completed(&mut self, event: &BepJsonEvent) -> Result<(), RouterError> {
        let target_id = event
            .bazel_event
            .get("id")
            .and_then(|id| id.get("targetCompleted"))
            .and_then(|tc| tc.get("label"))
            .and_then(|v| v.as_str());

        if let Some(label) = target_id {
            let success = event
                .get_payload("completed")
                .and_then(|c| c.get("success"))
                .and_then(|v| v.as_bool())
                .unwrap_or(false);

            // Get output files from fileSets references
            let file_sets: Vec<String> = event
                .get_payload("completed")
                .and_then(|c| c.get("outputGroup"))
                .and_then(|og| og.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|og| og.get("fileSets"))
                        .filter_map(|fs| fs.as_array())
                        .flatten()
                        .filter_map(|fs| fs.get("id"))
                        .filter_map(|v| v.as_str().map(String::from))
                        .collect()
                })
                .unwrap_or_default();

            debug!(
                label,
                success,
                file_sets = ?file_sets,
                "Target completed"
            );

            self.state.complete_target(
                label.to_string(),
                success,
                file_sets,
                event.event_time.clone(),
            );
        }
        Ok(())
    }

    fn handle_named_set(&mut self, event: &BepJsonEvent) -> Result<(), RouterError> {
        let set_id = event
            .bazel_event
            .get("id")
            .and_then(|id| id.get("namedSet"))
            .and_then(|ns| ns.get("id"))
            .and_then(|v| v.as_str());

        if let Some(id) = set_id {
            let files: Vec<String> = event
                .get_payload("namedSetOfFiles")
                .and_then(|ns| ns.get("files"))
                .and_then(|f| f.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|f| f.get("name").or(f.get("uri")))
                        .filter_map(|v| v.as_str().map(String::from))
                        .collect()
                })
                .unwrap_or_default();

            trace!(
                set_id = id,
                files_count = files.len(),
                "Named set"
            );

            self.state.cache_named_set(id.to_string(), files);
        }
        Ok(())
    }

    fn handle_action_completed(&mut self, event: &BepJsonEvent) -> Result<(), RouterError> {
        let action_id = event.bazel_event.get("id").and_then(|id| id.get("actionCompleted"));

        if let Some(action_id) = action_id {
            let label = action_id.get("label").and_then(|v| v.as_str());
            let primary_output = action_id.get("primaryOutput").and_then(|v| v.as_str());

            let payload = event.get_payload("action");
            let success = payload
                .and_then(|a| a.get("success"))
                .and_then(|v| v.as_bool())
                .unwrap_or(false);
            let mnemonic = payload
                .and_then(|a| a.get("type"))
                .and_then(|v| v.as_str());
            let exit_code = payload
                .and_then(|a| a.get("exitCode"))
                .and_then(|v| v.as_i64())
                .map(|v| v as i32);

            // Check if we should process this action based on mode
            match self.state.action_mode() {
                ActionProcessingMode::Lightweight => {
                    if success {
                        trace!(
                            label,
                            mnemonic,
                            "Skipping successful action (lightweight mode)"
                        );
                        return Ok(());
                    }
                }
                ActionProcessingMode::Full => {
                    // Full mode not yet implemented
                    warn!("Full action processing not yet implemented");
                    unimplemented!("Full action span processing requires future implementation");
                }
            }

            // Process failed action
            debug!(
                label,
                mnemonic,
                exit_code,
                primary_output,
                "Action failed"
            );

            self.state.record_action(
                label.map(String::from),
                mnemonic.map(String::from),
                primary_output.map(String::from),
                success,
                exit_code,
            );
        }
        Ok(())
    }

    // =========================================================================
    // Test event handlers
    // =========================================================================

    fn handle_test_result(&mut self, _event: &BepJsonEvent) -> Result<(), RouterError> {
        // Test results - deferred for now
        debug!("Test result received (handling deferred)");
        Ok(())
    }

    fn handle_test_summary(&mut self, _event: &BepJsonEvent) -> Result<(), RouterError> {
        // Test summary - deferred for now
        debug!("Test summary received (handling deferred)");
        Ok(())
    }

    // =========================================================================
    // Progress events (mostly ignored)
    // =========================================================================

    fn handle_progress(&mut self, event: &BepJsonEvent) -> Result<(), RouterError> {
        // Progress events are mostly empty, ignore unless there's content
        if let Some(payload) = event.get_payload("progress") {
            let stdout = payload.get("stdout").and_then(|v| v.as_str());
            let stderr = payload.get("stderr").and_then(|v| v.as_str());

            if stdout.is_some() || stderr.is_some() {
                trace!("Progress event with output");
            }
        }
        Ok(())
    }
}

impl Default for EventRouter {
    fn default() -> Self {
        Self::new()
    }
}
