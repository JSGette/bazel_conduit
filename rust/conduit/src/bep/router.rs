//! BEP event router
//!
//! Routes BEP events to appropriate handlers based on event type.

use super::decoder::BepJsonEvent;
use crate::otel::OtelMapper;
use crate::state::{ActionProcessingMode, BuildState};
use thiserror::Error;
use tracing::{debug, info, trace};

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
    mapper: Option<OtelMapper>,
}

impl EventRouter {
    /// Create a new event router
    pub fn new() -> Self {
        Self {
            state: BuildState::new(),
            mapper: None,
        }
    }

    /// Attach an OTel mapper for span generation.
    pub fn with_mapper(mut self, mapper: OtelMapper) -> Self {
        self.mapper = Some(mapper);
        self
    }

    /// Get a reference to the build state
    pub fn state(&self) -> &BuildState {
        &self.state
    }

    /// Get a mutable reference to the build state
    pub fn state_mut(&mut self) -> &mut BuildState {
        &mut self.state
    }

    /// Finalize: end any remaining open spans and flush.
    pub fn finish(&mut self) {
        if let Some(mapper) = &mut self.mapper {
            mapper.finish();
        }
    }

    /// Route a BEP JSON event to the appropriate handler
    pub fn route(&mut self, event: &BepJsonEvent) -> Result<(), RouterError> {
        let event_type = event.event_type().ok_or(RouterError::MissingEventId)?;

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
            "fetch" => self.handle_fetch(event),

            // Test events
            "testResult" => self.handle_test_result(event),
            "testSummary" => self.handle_test_summary(event),
            "targetSummary" => self.handle_target_summary(event),

            // Progress events (may have stderr/stdout content)
            "progress" => self.handle_progress(event),

            // Metadata
            "buildMetadata" => self.handle_build_metadata(event),

            // Skipped patterns
            "patternSkipped" => self.handle_pattern_skipped(event),

            // Root-cause failure events (payload is always Aborted)
            "unconfiguredLabel" => self.handle_aborted_label(event, "unconfiguredLabel"),
            "configuredLabel" => self.handle_aborted_label(event, "configuredLabel"),

            // Live test progress (test URI while running)
            "testProgress" => Ok(()),

            // ExecRequest for `bazel run`
            "execRequest" => Ok(()),

            // Ignored events (no OTel value)
            "structuredCommandLine" => Ok(()), // Redundant with optionsParsed
            "convenienceSymlinksIdentified" => Ok(()), // Local filesystem detail
            "workspaceInfo" => Ok(()), // Rarely populated
            "buildToolLogs" => Ok(()), // BES-level, not BEP-level

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
            // Prefer the proto Timestamp field; do NOT fall back to the
            // deprecated start_time_millis — it can hold a stale Bazel-server
            // start time on long-lived daemons, producing wildly wrong spans.
            let start_time_nanos = started
                .get("startTimeNanos")
                .and_then(|v| v.as_i64());
            let event_time_nanos = event.event_time_nanos;

            info!(
                uuid = uuid,
                command = command,
                start_time_nanos,
                event_time_nanos,
                start_time_millis,
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

            // Extended fields from BuildStarted
            let workspace_dir = started.get("workspaceDirectory").and_then(|v| v.as_str());
            let working_dir = started.get("workingDirectory").and_then(|v| v.as_str());
            let build_tool_version = started.get("buildToolVersion").and_then(|v| v.as_str());
            let server_pid = started.get("serverPid").and_then(|v| v.as_i64());
            let host = started.get("host").and_then(|v| v.as_str());
            let user = started.get("user").and_then(|v| v.as_str());

            if let Some(mapper) = &mut self.mapper {
                mapper.on_build_started(
                    uuid.unwrap_or("unknown"),
                    command.unwrap_or("unknown"),
                    start_time_nanos,
                    event_time_nanos,
                );
                mapper.on_build_started_extended(
                    workspace_dir,
                    working_dir,
                    build_tool_version,
                    server_pid,
                    host,
                    user,
                );
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
                self.state.set_command_args(args.clone());

                // OTel: add full command line as attribute
                if let Some(mapper) = &mut self.mapper {
                    mapper.on_command_line(&args);
                }
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
                info!("Detected --build_event_publish_all_actions → full action processing");
                self.state.set_action_mode(ActionProcessingMode::Full);
            } else {
                info!("Lightweight action processing (failed actions only)");
                self.state
                    .set_action_mode(ActionProcessingMode::Lightweight);
            }

            // OTel: record action mode on root span
            if let Some(mapper) = &mut self.mapper {
                mapper.on_action_mode(self.state.action_mode());
            }

            // Extract startup options
            let startup_opts: Vec<String> = payload
                .get("startupOptions")
                .and_then(|v| v.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|v| v.as_str().map(String::from))
                        .collect()
                })
                .unwrap_or_default();
            self.state.set_startup_options(startup_opts.clone());

            let explicit: Vec<String> = payload
                .get("explicitCmdLine")
                .and_then(|v| v.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|v| v.as_str().map(String::from))
                        .collect()
                })
                .unwrap_or_default();

            // OTel
            if let Some(mapper) = &mut self.mapper {
                mapper.on_options_parsed(&startup_opts, &explicit);
            }
        }
        Ok(())
    }

    fn handle_workspace_status(&mut self, event: &BepJsonEvent) -> Result<(), RouterError> {
        if let Some(payload) = event.get_payload("workspaceStatus") {
            let mut items_map = std::collections::HashMap::new();
            if let Some(items) = payload.get("item").and_then(|v| v.as_array()) {
                for item in items {
                    let key = item.get("key").and_then(|v| v.as_str());
                    let value = item.get("value").and_then(|v| v.as_str());
                    if let (Some(k), Some(v)) = (key, value) {
                        self.state
                            .add_workspace_status(k.to_string(), v.to_string());
                        items_map.insert(k.to_string(), v.to_string());
                    }
                }
            }

            // OTel
            if !items_map.is_empty() {
                if let Some(mapper) = &mut self.mapper {
                    mapper.on_workspace_status(&items_map);
                }
            }
        }
        Ok(())
    }

    fn handle_configuration(&mut self, event: &BepJsonEvent) -> Result<(), RouterError> {
        if let Some(config_id) = event
            .id
            .get("configuration")
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

                // OTel: add configuration as span event on root
                if let Some(mapper) = &mut self.mapper {
                    mapper.on_configuration(config_id, mnemonic, platform);
                }
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
            let finish_time_nanos = payload
                .get("finishTimeNanos")
                .and_then(|v| v.as_i64())
                .or_else(|| finish_time_millis.map(|ms| ms * 1_000_000));

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

            if let Some(mapper) = &mut self.mapper {
                mapper.on_build_finished(exit_code, finish_time_nanos);
            }
        }
        Ok(())
    }

    fn handle_build_metrics(&mut self, event: &BepJsonEvent) -> Result<(), RouterError> {
        if let Some(payload) = event.get_payload("buildMetrics") {
            debug!("Build metrics received");
            // Store metrics for later processing
            self.state.set_build_metrics(payload.clone());

            // OTel: add metrics attributes to root span
            if let Some(mapper) = &mut self.mapper {
                mapper.on_build_metrics(payload);
            }
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
                .id
                .get("pattern")
                .and_then(|p| p.get("pattern"))
                .and_then(|v| v.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|v| v.as_str().map(String::from))
                        .collect()
                })
                .unwrap_or_default();

            debug!(patterns = ?patterns, "Pattern expanded");
            self.state.set_patterns(patterns.clone());

            // OTel: add build patterns as attribute
            if let Some(mapper) = &mut self.mapper {
                mapper.on_pattern(&patterns);
            }
        }
        Ok(())
    }

    fn handle_target_configured(&mut self, event: &BepJsonEvent) -> Result<(), RouterError> {
        let target_id = event
            .id
            .get("targetConfigured")
            .and_then(|tc| tc.get("label"))
            .and_then(|v| v.as_str());

        if let Some(label) = target_id {
            let payload = event.get_payload("configured");
            let target_kind = payload
                .and_then(|c| c.get("targetKind"))
                .and_then(|v| v.as_str());

            let tags: Vec<String> = payload
                .and_then(|c| c.get("tag"))
                .and_then(|v| v.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|v| v.as_str().map(String::from))
                        .collect()
                })
                .unwrap_or_default();

            debug!(
                label,
                target_kind,
                tags = ?tags,
                "Target configured"
            );

            self.state.start_target(
                label.to_string(),
                target_kind.map(String::from),
                None,
            );

            // OTel: create target span with BES event_time as start
            if let Some(mapper) = &mut self.mapper {
                mapper.on_target_configured(
                    label,
                    target_kind,
                    &tags,
                    event.event_time_nanos,
                );
            }
        }
        Ok(())
    }

    fn handle_target_completed(&mut self, event: &BepJsonEvent) -> Result<(), RouterError> {
        let target_id = event
            .id
            .get("targetCompleted")
            .and_then(|tc| tc.get("label"))
            .and_then(|v| v.as_str());

        if let Some(label) = target_id {
            // Check if this is an aborted/skipped target first.
            let aborted = event.payload.get("aborted");
            if let Some(abort_info) = aborted {
                let reason = abort_info.get("reason").and_then(|v| v.as_str());
                let description = abort_info.get("description").and_then(|v| v.as_str());

                debug!(
                    label,
                    reason,
                    description,
                    "Target skipped/aborted"
                );

                self.state.complete_target(
                    label.to_string(),
                    false,
                    Vec::new(),
                    None,
                );

                if let Some(mapper) = &mut self.mapper {
                    mapper.on_target_skipped(
                        label,
                        reason,
                        description,
                        event.event_time_nanos,
                    );
                }
                return Ok(());
            }

            let payload = event.get_payload("completed");

            // In proto3 JSON, `true` is the default for bool and is often
            // omitted from the wire.  Bazel only sets `success: false`
            // explicitly when a target genuinely fails.
            let success = payload
                .and_then(|c| c.get("success"))
                .and_then(|v| v.as_bool())
                .unwrap_or(true);

            // Get output files from fileSets references
            let file_sets: Vec<String> = payload
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

            // Tags from the BUILD rule
            let tags: Vec<String> = payload
                .and_then(|c| c.get("tag"))
                .and_then(|v| v.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|v| v.as_str().map(String::from))
                        .collect()
                })
                .unwrap_or_default();

            debug!(
                label,
                success,
                file_sets = ?file_sets,
                tags = ?tags,
                "Target completed"
            );

            self.state.complete_target(
                label.to_string(),
                success,
                file_sets.clone(),
                None,
            );

            // OTel: end target span with resolved file sets + BES event_time
            if let Some(mapper) = &mut self.mapper {
                mapper.on_target_completed(
                    label,
                    success,
                    &file_sets,
                    &tags,
                    event.event_time_nanos,
                );
            }
        }
        Ok(())
    }

    fn handle_named_set(&mut self, event: &BepJsonEvent) -> Result<(), RouterError> {
        let set_id = event
            .id
            .get("namedSet")
            .and_then(|ns| ns.get("id"))
            .and_then(|v| v.as_str());

        if let Some(id) = set_id {
            let payload = event.get_payload("namedSetOfFiles");

            // Direct files in this set.
            let files: Vec<String> = payload
                .and_then(|ns| ns.get("files"))
                .and_then(|f| f.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|f| f.get("name").or(f.get("uri")))
                        .filter_map(|v| v.as_str().map(String::from))
                        .collect()
                })
                .unwrap_or_default();

            // Child NamedSet references (transitive).
            let child_set_ids: Vec<String> = payload
                .and_then(|ns| ns.get("fileSets"))
                .and_then(|fs| fs.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|fs| fs.get("id"))
                        .filter_map(|v| v.as_str().map(String::from))
                        .collect()
                })
                .unwrap_or_default();

            trace!(
                set_id = id,
                files_count = files.len(),
                child_sets = child_set_ids.len(),
                "Named set"
            );

            self.state.cache_named_set(id.to_string(), files.clone());

            // OTel: cache in mapper for later resolution during targetCompleted
            if let Some(mapper) = &mut self.mapper {
                mapper.on_named_set(id, files, &child_set_ids);
            }
        }
        Ok(())
    }

    fn handle_action_completed(&mut self, event: &BepJsonEvent) -> Result<(), RouterError> {
        let action_id = event.id.get("actionCompleted");

        if let Some(action_id) = action_id {
            let label = action_id.get("label").and_then(|v| v.as_str());
            let primary_output = action_id.get("primaryOutput").and_then(|v| v.as_str());
            let configuration = action_id
                .get("configuration")
                .and_then(|c| c.get("id"))
                .and_then(|v| v.as_str());

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

            // Full-mode fields (may be absent in lightweight mode)
            let command_line: Vec<String> = payload
                .and_then(|a| a.get("commandLine"))
                .and_then(|v| v.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|v| v.as_str().map(String::from))
                        .collect()
                })
                .unwrap_or_default();
            let stdout_path = payload
                .and_then(|a| a.get("stdout"))
                .and_then(|v| v.as_str());
            let stderr_path = payload
                .and_then(|a| a.get("stderr"))
                .and_then(|v| v.as_str());
            let start_time_nanos = payload
                .and_then(|a| a.get("startTimeNanos"))
                .and_then(|v| v.as_i64());
            let end_time_nanos = payload
                .and_then(|a| a.get("endTimeNanos"))
                .and_then(|v| v.as_i64());

            // SpawnExec fields (runner, cache, I/O)
            let runner = payload
                .and_then(|a| a.get("runner"))
                .and_then(|v| v.as_str());
            let cache_hit = payload
                .and_then(|a| a.get("cacheHit"))
                .and_then(|v| v.as_bool());
            let remotable = payload
                .and_then(|a| a.get("remotable"))
                .and_then(|v| v.as_bool());
            let remote_cacheable = payload
                .and_then(|a| a.get("remoteCacheable"))
                .and_then(|v| v.as_bool());
            let inputs = payload
                .and_then(|a| a.get("inputs"))
                .and_then(|v| v.as_array());
            let actual_outputs = payload
                .and_then(|a| a.get("actualOutputs"))
                .and_then(|v| v.as_array());
            let listed_outputs: Vec<String> = payload
                .and_then(|a| a.get("listedOutputs"))
                .and_then(|v| v.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|v| v.as_str().map(String::from))
                        .collect()
                })
                .unwrap_or_default();

            // Check if we should process this action based on mode
            let should_process = self.state.action_mode().should_create_span(success);
            if !should_process {
                trace!(
                    label,
                    mnemonic,
                    "Skipping successful action (lightweight mode)"
                );
                return Ok(());
            }

            debug!(
                label,
                mnemonic,
                success,
                exit_code,
                primary_output,
                "Action completed"
            );

            self.state.record_action(
                label.map(String::from),
                mnemonic.map(String::from),
                primary_output.map(String::from),
                success,
                exit_code,
            );

            // OTel: create action span
            if let Some(mapper) = &mut self.mapper {
                let input_paths: Vec<String> = inputs
                    .map(|arr| {
                        arr.iter()
                            .filter_map(|v| v.get("path").and_then(|p| p.as_str()).map(String::from))
                            .collect()
                    })
                    .unwrap_or_default();
                let output_paths: Vec<String> = actual_outputs
                    .map(|arr| {
                        arr.iter()
                            .filter_map(|v| v.get("path").and_then(|p| p.as_str()).map(String::from))
                            .collect()
                    })
                    .unwrap_or_default();

                mapper.on_action_completed(
                    label,
                    mnemonic,
                    success,
                    exit_code,
                    primary_output,
                    configuration,
                    &command_line,
                    stdout_path,
                    stderr_path,
                    start_time_nanos,
                    end_time_nanos,
                    runner,
                    cache_hit,
                    remotable,
                    remote_cacheable,
                    &input_paths,
                    &listed_outputs,
                    &output_paths,
                );
            }
        }
        Ok(())
    }

    fn handle_fetch(&mut self, event: &BepJsonEvent) -> Result<(), RouterError> {
        let fetch_id = event.id.get("fetch");
        let url = fetch_id
            .and_then(|f| f.get("url"))
            .and_then(|v| v.as_str());
        let downloader = fetch_id
            .and_then(|f| f.get("downloader"))
            .and_then(|v| v.as_str());

        if let Some(url) = url {
            let success = event
                .get_payload("fetch")
                .and_then(|f| f.get("success"))
                .and_then(|v| v.as_bool())
                .unwrap_or(false);

            debug!(url, success, downloader, "Fetch event");

            if let Some(mapper) = &mut self.mapper {
                mapper.on_fetch(url, success, downloader);
            }
        }
        Ok(())
    }

    // =========================================================================
    // Test event handlers
    // =========================================================================

    fn handle_test_result(&mut self, event: &BepJsonEvent) -> Result<(), RouterError> {
        let label = event
            .id
            .get("testResult")
            .and_then(|tr| tr.get("label"))
            .and_then(|v| v.as_str());

        if let Some(label) = label {
            let payload = event.get_payload("testResult");

            let status = payload
                .and_then(|p| p.get("status"))
                .and_then(|v| v.as_str());
            let cached = payload
                .and_then(|p| p.get("cachedLocally"))
                .and_then(|v| v.as_bool());
            let strategy = payload
                .and_then(|p| p.get("executionInfo"))
                .and_then(|ei| ei.get("strategy"))
                .and_then(|v| v.as_str());

            // Test attempt info from the event ID
            let attempt = event
                .id
                .get("testResult")
                .and_then(|tr| tr.get("attempt"))
                .and_then(|v| v.as_i64())
                .map(|v| v as i32);
            let run = event
                .id
                .get("testResult")
                .and_then(|tr| tr.get("run"))
                .and_then(|v| v.as_i64())
                .map(|v| v as i32);
            let shard = event
                .id
                .get("testResult")
                .and_then(|tr| tr.get("shard"))
                .and_then(|v| v.as_i64())
                .map(|v| v as i32);

            let start_time_nanos = payload
                .and_then(|p| p.get("testAttemptStartNanos"))
                .and_then(|v| v.as_i64());
            let duration_nanos = payload
                .and_then(|p| p.get("testAttemptDurationNanos"))
                .and_then(|v| v.as_i64());

            debug!(
                label,
                status,
                attempt,
                cached,
                "Test result"
            );

            if let Some(mapper) = &mut self.mapper {
                mapper.on_test_result(
                    label,
                    status,
                    attempt,
                    run,
                    shard,
                    cached,
                    strategy,
                    start_time_nanos,
                    duration_nanos,
                );
            }
        }
        Ok(())
    }

    fn handle_target_summary(&mut self, event: &BepJsonEvent) -> Result<(), RouterError> {
        let label = event
            .id
            .get("targetSummary")
            .and_then(|ts| ts.get("label"))
            .and_then(|v| v.as_str());

        if let Some(label) = label {
            let payload = event.get_payload("targetSummary");
            let overall_build_success = payload
                .and_then(|p| p.get("overallBuildSuccess"))
                .and_then(|v| v.as_bool());
            let overall_test_status = payload
                .and_then(|p| p.get("overallTestStatus"))
                .and_then(|v| v.as_str());

            debug!(
                label,
                overall_build_success,
                overall_test_status,
                "Target summary"
            );

            if let Some(mapper) = &mut self.mapper {
                mapper.on_target_summary(label, overall_build_success, overall_test_status);
            }
        }
        Ok(())
    }

    fn handle_test_summary(&mut self, event: &BepJsonEvent) -> Result<(), RouterError> {
        let label = event
            .id
            .get("testSummary")
            .and_then(|ts| ts.get("label"))
            .and_then(|v| v.as_str());

        if let Some(label) = label {
            let payload = event.get_payload("testSummary");

            let overall_status = payload
                .and_then(|p| p.get("overallStatus"))
                .and_then(|v| v.as_str());
            let total_run_count = payload
                .and_then(|p| p.get("totalRunCount"))
                .and_then(|v| v.as_i64())
                .map(|v| v as i32);
            let run_count = payload
                .and_then(|p| p.get("runCount"))
                .and_then(|v| v.as_i64())
                .map(|v| v as i32);
            let attempt_count = payload
                .and_then(|p| p.get("attemptCount"))
                .and_then(|v| v.as_i64())
                .map(|v| v as i32);
            let shard_count = payload
                .and_then(|p| p.get("shardCount"))
                .and_then(|v| v.as_i64())
                .map(|v| v as i32);
            let total_num_cached = payload
                .and_then(|p| p.get("totalNumCached"))
                .and_then(|v| v.as_i64())
                .map(|v| v as i32);

            debug!(
                label,
                overall_status,
                total_run_count,
                "Test summary"
            );

            if let Some(mapper) = &mut self.mapper {
                mapper.on_test_summary(
                    label,
                    overall_status,
                    total_run_count,
                    run_count,
                    attempt_count,
                    shard_count,
                    total_num_cached,
                );
            }
        }
        Ok(())
    }

    // =========================================================================
    // Progress events
    // =========================================================================

    fn handle_progress(&mut self, event: &BepJsonEvent) -> Result<(), RouterError> {
        if let Some(payload) = event.get_payload("progress") {
            let stdout = payload.get("stdout").and_then(|v| v.as_str());
            let stderr = payload.get("stderr").and_then(|v| v.as_str());

            let has_content = stdout.map(|s| !s.is_empty()).unwrap_or(false)
                || stderr.map(|s| !s.is_empty()).unwrap_or(false);

            if has_content {
                trace!("Progress event with output");

                // OTel: add progress as span event on root
                if let Some(mapper) = &mut self.mapper {
                    mapper.on_progress(stderr, stdout);
                }
            }
        }
        Ok(())
    }

    // =========================================================================
    // Metadata
    // =========================================================================

    fn handle_pattern_skipped(&mut self, event: &BepJsonEvent) -> Result<(), RouterError> {
        let patterns: Vec<String> = event
            .id
            .get("patternSkipped")
            .and_then(|p| p.get("pattern"))
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();

        debug!(patterns = ?patterns, "Pattern skipped");

        if let Some(mapper) = &mut self.mapper {
            mapper.on_pattern_skipped(&patterns);
        }
        Ok(())
    }

    /// Handle unconfiguredLabel / configuredLabel events whose payload is Aborted.
    fn handle_aborted_label(
        &mut self,
        event: &BepJsonEvent,
        id_key: &str,
    ) -> Result<(), RouterError> {
        let label = event
            .id
            .get(id_key)
            .and_then(|o| o.get("label"))
            .and_then(|v| v.as_str());

        let aborted = event.payload.get("aborted");
        let reason = aborted
            .and_then(|a| a.get("reason"))
            .and_then(|v| v.as_str());
        let description = aborted
            .and_then(|a| a.get("description"))
            .and_then(|v| v.as_str());

        debug!(
            label,
            reason,
            description,
            id_key,
            "Aborted label"
        );

        if let Some(mapper) = &mut self.mapper {
            if let Some(label) = label {
                mapper.on_aborted_label(label, reason, description, event.event_time_nanos);
            }
        }
        Ok(())
    }

    fn handle_build_metadata(&mut self, event: &BepJsonEvent) -> Result<(), RouterError> {
        if let Some(payload) = event.get_payload("buildMetadata") {
            debug!("Build metadata received");

            // OTel: add metadata as attributes on root
            if let Some(mapper) = &mut self.mapper {
                mapper.on_build_metadata(payload);
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
