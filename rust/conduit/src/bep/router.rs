//! BEP event router
//!
//! Dispatches incoming Bazel Build Event Protocol (BEP) events to the
//! [`OtelMapper`] for span creation and to [`BuildState`] for bookkeeping.
//!
//! Supports two input paths:
//! - **Protobuf** (gRPC BES): [`route_build_event`] receives decoded
//!   `BuildEvent` protos from the gRPC server and maps them directly.
//! - **JSON** (file / stdin): [`route`] receives [`BepJsonEvent`] objects
//!   parsed from newline-delimited JSON BEP streams.
//!
//! The router owns deferred `TracerProvider` initialization so invocation-
//! scoped resource attributes (invocation_id, command, workspace) are available
//! on every span.

use super::decoder::BepJsonEvent;
use crate::build_event_stream::build_event::Payload as BepPayload;
use crate::build_event_stream::build_event_id::Id as BepId;
use crate::build_event_stream::BuildEvent;
use crate::build_event_stream::ActionExecuted;
use crate::otel::{
    build_invocation_resource, init_tracer_provider_with_resource, ActionCompletedEvent,
    ExportConfig, OtelMapper, Redactor, TestResultEvent,
};
use crate::state::{ActionProcessingMode, BuildState};
use opentelemetry::logs::LoggerProvider as _;
use opentelemetry::trace::TracerProvider;
use prost::Message;
use spawn_proto::tools::protos::SpawnExec;
use std::path::PathBuf;
use thiserror::Error;
use tracing::{debug, info, trace};

const EXEC_LOG_BINARY_FILE_PREFIX: &str = "--execution_log_binary_file=";

fn extract_exec_log_binary_path(options: &[&str]) -> Option<PathBuf> {
    options
        .iter()
        .find_map(|opt| opt.strip_prefix(EXEC_LOG_BINARY_FILE_PREFIX))
        .filter(|s| !s.is_empty())
        .map(PathBuf::from)
}

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
    /// When set, tracer and mapper are created on first Started with invocation resource.
    export_config: Option<ExportConfig>,
    logger_provider: Option<opentelemetry_sdk::logs::LoggerProvider>,
    tracer_provider: Option<opentelemetry_sdk::trace::TracerProvider>,
    /// Redactor applied to command-line attributes when the mapper is
    /// created lazily on first Started.
    redactor: Redactor,
}

impl EventRouter {
    /// Create a new event router
    pub fn new() -> Self {
        Self {
            state: BuildState::new(),
            mapper: None,
            export_config: None,
            logger_provider: None,
            tracer_provider: None,
            redactor: Redactor::default_enabled(),
        }
    }

    /// Use OTel export with invocation-scoped Resource (mapper created on first Started).
    pub fn with_export(
        mut self,
        config: ExportConfig,
        logger_provider: opentelemetry_sdk::logs::LoggerProvider,
    ) -> Self {
        self.export_config = Some(config);
        self.logger_provider = Some(logger_provider);
        self
    }

    /// Override the default command-line redactor.
    pub fn with_redactor(mut self, redactor: Redactor) -> Self {
        self.redactor = redactor.clone();
        if let Some(mapper) = self.mapper.take() {
            self.mapper = Some(mapper.with_redactor(redactor));
        }
        self
    }

    /// Attach an OTel mapper for span generation (legacy; use with_export for Resource attributes).
    pub fn with_mapper(mut self, mapper: OtelMapper) -> Self {
        self.mapper = Some(mapper.with_redactor(self.redactor.clone()));
        self
    }

    /// (Re)build tracer + mapper for the current Started so Resource carries the
    /// active invocation_id / command / workspace. In `--serve` mode the router
    /// outlives a single invocation, and Resource is baked into the
    /// TracerProvider at construction; without a rebuild every later
    /// invocation would export under the *first* invocation's Resource.
    fn ensure_mapper_for_started(
        &mut self,
        invocation_id: &str,
        command: &str,
        workspace_dir: Option<&str>,
    ) {
        let Some(config) = self.export_config.as_ref() else {
            return;
        };
        // Drop the previous provider so its batched spans flush under the old
        // Resource and the new one starts clean. The previous mapper has
        // already been ended via `finish()` at end-of-stream by the gRPC
        // worker; this is just the Resource swap.
        if let Some(old) = self.tracer_provider.take() {
            if let Err(e) = old.shutdown() {
                tracing::warn!(error = ?e, "Previous TracerProvider shutdown reported error");
            }
        }
        self.mapper = None;

        let resource = build_invocation_resource(invocation_id, command, workspace_dir);
        let Ok(Some(provider)) = init_tracer_provider_with_resource(config, resource) else {
            return;
        };
        let logger = self
            .logger_provider
            .as_ref()
            .map(|lp| lp.logger("conduit"));
        let mapper = OtelMapper::new(provider.tracer("conduit"), logger)
            .with_redactor(self.redactor.clone());
        self.tracer_provider = Some(provider);
        self.mapper = Some(mapper);
    }

    /// Get a reference to the build state
    pub fn state(&self) -> &BuildState {
        &self.state
    }

    /// Get a mutable reference to the build state
    pub fn state_mut(&mut self) -> &mut BuildState {
        &mut self.state
    }

    /// Finalize: drain remaining action buffers, end all open spans, and
    /// force-flush the LoggerProvider.
    ///
    /// We intentionally do NOT call `tracer_provider.force_flush()` here.
    /// `BatchSpanProcessor::force_flush` (with `runtime::Tokio`) is a sync
    /// `block_on` that strangles the runtime when invoked from inside a
    /// `tokio::spawn` task: the export and timeout-timer tasks share the
    /// same workers, so a non-empty queue (large build) deadlocks.
    /// Instead we rely on the BatchSpanProcessor's `scheduled_delay`
    /// (configured to 1 s in `build_tracer_provider`) to drain naturally.
    pub fn finish(&mut self) {
        if let Some(mapper) = &mut self.mapper {
            let remaining = self.state.drain_remaining_action_buffers();
            for (label, earliest, latest, actions) in remaining {
                mapper.drain_orphaned_actions(&label, earliest, latest, &actions);
            }
            mapper.finish();
        }
        if let Some(lp) = &self.logger_provider {
            for res in lp.force_flush() {
                if let Err(e) = res {
                    tracing::warn!(error = ?e, "LoggerProvider force_flush reported error");
                }
            }
        }
    }

    /// Shutdown tracer and logger providers (call after finish when using with_export).
    pub fn shutdown_providers(&mut self) -> anyhow::Result<()> {
        self.finish();
        if let Some(p) = self.tracer_provider.take() {
            p.shutdown()?;
        }
        if let Some(lp) = self.logger_provider.take() {
            lp.shutdown()?;
        }
        Ok(())
    }
}

impl EventRouter {
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

            "buildToolLogs" => self.handle_build_tool_logs(event),

            // Ignored events (no OTel value)
            "structuredCommandLine" => Ok(()), // Redundant with optionsParsed
            "convenienceSymlinksIdentified" => Ok(()), // Local filesystem detail
            "workspaceInfo" => Ok(()), // Rarely populated

            // Unknown events
            other => {
                debug!(event_type = other, "Ignoring unknown event type");
                Ok(())
            }
        }
    }

    /// Route a BEP BuildEvent from the gRPC path (no JSON round-trip).
    pub fn route_build_event(
        &mut self,
        event: &BuildEvent,
        event_time_nanos: Option<i64>,
    ) -> Result<(), RouterError> {
        let id = match event.id.as_ref().and_then(|i| i.id.as_ref()) {
            Some(id) => id,
            None => return Err(RouterError::MissingEventId),
        };

        let event_type = bep_event_type_name(id);
        match event_type {
            "TestResult" | "TestSummary" | "TargetConfigured" | "TargetCompleted" | "Started" | "BuildFinished" => {
                info!(event_type, "BEP event received");
            }
            _ => {
                trace!(event_type, "BEP event received");
            }
        }

        match id {
            BepId::Started(_) => {
                self.state.reset();
                if let Some(mapper) = &mut self.mapper {
                    mapper.reset();
                }
                if let Some(BepPayload::Started(s)) = &event.payload {
                    self.ensure_mapper_for_started(
                        if s.uuid.is_empty() { "unknown" } else { &s.uuid },
                        if s.command.is_empty() { "unknown" } else { &s.command },
                        Some(s.workspace_directory.as_str()).filter(|d| !d.is_empty()),
                    );
                    let start_time_nanos = s
                        .start_time
                        .as_ref()
                        .map(|ts| ts.seconds * 1_000_000_000 + i64::from(ts.nanos));
                    if !s.uuid.is_empty() {
                        self.state.set_invocation_id(s.uuid.clone());
                    }
                    if !s.command.is_empty() {
                        self.state.set_command(s.command.clone());
                    }
                    #[allow(deprecated)]
                    if s.start_time_millis != 0 {
                        self.state.set_start_time_millis(s.start_time_millis);
                    }
                    if let Some(mapper) = &mut self.mapper {
                        mapper.on_build_started(
                            if s.uuid.is_empty() { "unknown" } else { &s.uuid },
                            if s.command.is_empty() { "unknown" } else { &s.command },
                            start_time_nanos,
                            event_time_nanos,
                        );
                        mapper.on_build_started_extended(
                            Some(s.workspace_directory.as_str()),
                            Some(s.working_directory.as_str()),
                            Some(s.build_tool_version.as_str()),
                            if s.server_pid != 0 {
                                Some(s.server_pid)
                            } else {
                                None
                            },
                            Some(s.host.as_str()),
                            Some(s.user.as_str()),
                        );
                    }
                }
                Ok(())
            }
            BepId::UnstructuredCommandLine(_) => {
                if let Some(BepPayload::UnstructuredCommandLine(u)) = &event.payload {
                    let args: Vec<String> = u.args.clone();
                    if !args.is_empty() {
                        self.state.set_command_args(args.clone());
                        if let Some(mapper) = &mut self.mapper {
                            mapper.on_command_line(&args);
                        }
                    }
                }
                Ok(())
            }
            BepId::OptionsParsed(_) => {
                if let Some(BepPayload::OptionsParsed(o)) = &event.payload {
                    let all_options: Vec<&str> = o
                        .cmd_line
                        .iter()
                        .chain(&o.explicit_cmd_line)
                        .map(String::as_str)
                        .collect();
                    let has_all_actions = has_publish_all_actions_flag(&all_options);
                    self.state.set_action_mode(if has_all_actions {
                        ActionProcessingMode::Full
                    } else {
                        ActionProcessingMode::Lightweight
                    });
                    if let Some(mapper) = &mut self.mapper {
                        mapper.on_action_mode(self.state.action_mode());
                    }
                    if let Some(path) = extract_exec_log_binary_path(&all_options) {
                        self.state.set_exec_log_path(Some(path.clone()));
                        if let Some(mapper) = &mut self.mapper {
                            mapper.on_exec_log_detected(path);
                        }
                    }
                    self.state.set_startup_options(o.startup_options.clone());
                    let explicit = o.explicit_cmd_line.clone();
                    if let Some(mapper) = &mut self.mapper {
                        mapper.on_options_parsed(&o.startup_options, &explicit);
                        if !o.tool_tag.is_empty() {
                            mapper.on_tool_tag(&o.tool_tag);
                        }
                    }
                }
                Ok(())
            }
            BepId::WorkspaceStatus(_) => {
                if let Some(BepPayload::WorkspaceStatus(ws)) = &event.payload {
                    let mut items = std::collections::HashMap::new();
                    for i in &ws.item {
                        if !i.key.is_empty() {
                            items.insert(i.key.clone(), i.value.clone());
                            self.state.add_workspace_status(i.key.clone(), i.value.clone());
                        }
                    }
                    if !items.is_empty() {
                        if let Some(mapper) = &mut self.mapper {
                            mapper.on_workspace_status(&items);
                        }
                    }
                }
                Ok(())
            }
            BepId::Configuration(c) => {
                if let Some(BepPayload::Configuration(cfg)) = &event.payload {
                    let id = c.id.clone();
                    if let Some(mapper) = &mut self.mapper {
                        mapper.on_configuration(
                            &id,
                            Some(cfg.mnemonic.as_str()),
                            Some(cfg.platform_name.as_str()),
                        );
                        mapper.on_configuration_extended(
                            &id,
                            if cfg.cpu.is_empty() { None } else { Some(cfg.cpu.as_str()) },
                            Some(cfg.is_tool),
                        );
                    }
                }
                Ok(())
            }
            BepId::BuildFinished(_) => {
                if let Some(BepPayload::Finished(f)) = &event.payload {
                    let exit_code = f.exit_code.as_ref().map(|ec| ec.code);
                    let exit_code_name = f.exit_code.as_ref().map(|ec| ec.name.as_str());
                    let finish_time_nanos = f
                        .finish_time
                        .as_ref()
                        .map(|ts| ts.seconds * 1_000_000_000 + i64::from(ts.nanos))
                        .or_else(|| {
                            if f.finish_time_millis != 0 {
                                Some(f.finish_time_millis * 1_000_000)
                            } else {
                                None
                            }
                        });
                    if let Some(code) = exit_code {
                        self.state.set_exit_code(code);
                    }
                    #[allow(deprecated)]
                    if f.finish_time_millis != 0 {
                        self.state.set_end_time_millis(f.finish_time_millis);
                    }
                    self.state.mark_finished();
                    if let Some(mapper) = &mut self.mapper {
                        mapper.on_build_finished(exit_code, finish_time_nanos, exit_code_name);
                    }
                }
                Ok(())
            }
            BepId::BuildMetrics(_) => {
                if let Some(BepPayload::BuildMetrics(m)) = &event.payload {
                    let json = crate::grpc::server::build_metrics_to_json(m);
                    self.state.set_build_metrics(json.clone());
                    if let Some(mapper) = &mut self.mapper {
                        mapper.on_build_metrics(&json);
                    }
                }
                Ok(())
            }
            BepId::Pattern(p) => {
                if let Some(BepPayload::Expanded(_)) = &event.payload {
                    let patterns = p.pattern.clone();
                    self.state.set_patterns(patterns.clone());
                    if let Some(mapper) = &mut self.mapper {
                        mapper.on_pattern(&patterns);
                    }
                }
                Ok(())
            }
            BepId::TargetConfigured(t) => {
                if let Some(BepPayload::Configured(c)) = &event.payload {
                    let label = t.label.clone();
                    let kind = c.target_kind.clone();
                    let tags = c.tag.clone();
                    let test_size = match c.test_size {
                        1 => Some("SMALL"),
                        2 => Some("MEDIUM"),
                        3 => Some("LARGE"),
                        4 => Some("ENORMOUS"),
                        _ => None,
                    };
                    self.state.start_target(label.clone(), Some(kind.clone()), None);
                    if let Some(mapper) = &mut self.mapper {
                        mapper.on_target_configured(
                            &label,
                            Some(kind.as_str()),
                            &tags,
                            event_time_nanos,
                            test_size,
                        );
                    }
                }
                Ok(())
            }
            BepId::TargetCompleted(t) => {
                let label = t.label.clone();
                if let Some(BepPayload::Aborted(a)) = &event.payload {
                    let reason = crate::grpc::server::aborted_reason_to_str(a.reason());
                    self.state.complete_target(label.clone(), false, Vec::new(), None);
                    if let Some(mapper) = &mut self.mapper {
                        mapper.on_target_skipped(
                            &label,
                            Some(reason),
                            Some(a.description.as_str()),
                            event_time_nanos,
                        );
                    }
                    return Ok(());
                }
                if let Some(BepPayload::Completed(c)) = &event.payload {
                    let file_sets: Vec<String> = c
                        .output_group
                        .iter()
                        .flat_map(|og| og.file_sets.iter())
                        .map(|fs| fs.id.clone())
                        .collect();
                    let tags = c.tag.clone();
                    self.state.complete_target(
                        label.clone(),
                        c.success,
                        file_sets.clone(),
                        None,
                    );
                    if let Some(mapper) = &mut self.mapper {
                        let (earliest, latest, buffered) = self
                            .state
                            .take_target_action_buffer(&label)
                            .unwrap_or((None, None, vec![]));
                        mapper.on_target_completed(
                            &label,
                            c.success,
                            &file_sets,
                            &tags,
                            event_time_nanos,
                            earliest,
                            latest,
                            &buffered,
                        );
                    }
                }
                Ok(())
            }
            BepId::NamedSet(n) => {
                if let Some(BepPayload::NamedSetOfFiles(ns)) = &event.payload {
                    let id = n.id.clone();
                    let files: Vec<String> = ns
                        .files
                        .iter()
                        .filter_map(|f| {
                            crate::grpc::server::bep_file_uri(f)
                                .or_else(|| if f.name.is_empty() { None } else { Some(f.name.clone()) })
                        })
                        .collect();
                    let child_set_ids: Vec<String> =
                        ns.file_sets.iter().map(|fs| fs.id.clone()).collect();
                    self.state.cache_named_set(id.clone(), files.clone());
                    if let Some(mapper) = &mut self.mapper {
                        mapper.on_named_set(&id, files, &child_set_ids);
                    }
                }
                Ok(())
            }
            BepId::ActionCompleted(a) => {
                if let Some(BepPayload::Action(act)) = &event.payload {
                    let label = a.label.clone();
                    let primary_output = a.primary_output.clone();
                    let configuration = a
                        .configuration
                        .as_ref()
                        .map(|c| c.id.clone());
                    let should_process = self
                        .state
                        .action_mode()
                        .should_create_span(act.success);
                    if !should_process {
                        return Ok(());
                    }
                    self.state.record_action(
                        Some(label.clone()),
                        Some(act.r#type.clone()),
                        Some(primary_output.clone()),
                        act.success,
                        Some(act.exit_code),
                    );
                    if let Some(mapper) = &mut self.mapper {
                        let stdout_uri = act
                            .stdout
                            .as_ref()
                            .and_then(crate::grpc::server::bep_file_uri);
                        let stderr_uri = act
                            .stderr
                            .as_ref()
                            .and_then(crate::grpc::server::bep_file_uri);
                        let start_nanos = act.start_time.as_ref().map(|ts| {
                            ts.seconds * 1_000_000_000 + i64::from(ts.nanos)
                        });
                        let end_nanos = act.end_time.as_ref().map(|ts| {
                            ts.seconds * 1_000_000_000 + i64::from(ts.nanos)
                        });
                        let (cached_from_spawn, runner_from_spawn) =
                            extract_spawn_exec_from_action(act);
                        mapper.on_action_completed(&ActionCompletedEvent {
                            label: Some(&label),
                            mnemonic: Some(&act.r#type),
                            success: act.success,
                            exit_code: Some(act.exit_code),
                            exit_code_name: None,
                            primary_output: Some(&primary_output),
                            configuration: configuration.as_deref(),
                            command_line: &act.command_line,
                            stdout_path: stdout_uri.as_deref(),
                            stderr_path: stderr_uri.as_deref(),
                            start_time_nanos: start_nanos,
                            end_time_nanos: end_nanos,
                            cached: cached_from_spawn,
                            hostname: None,
                            cached_remotely: None,
                            runner: runner_from_spawn.as_deref(),
                        });
                    }
                }
                Ok(())
            }
            BepId::Fetch(f) => {
                if let Some(BepPayload::Fetch(p)) = &event.payload {
                    let url = f.url.clone();
                    let downloader = match f.downloader() {
                        crate::build_event_stream::build_event_id::fetch_id::Downloader::Http => {
                            "HTTP"
                        }
                        crate::build_event_stream::build_event_id::fetch_id::Downloader::Grpc => {
                            "GRPC"
                        }
                        _ => "UNKNOWN",
                    };
                    if let Some(mapper) = &mut self.mapper {
                        mapper.on_fetch(&url, p.success, Some(downloader));
                    }
                }
                Ok(())
            }
            BepId::Progress(_) => {
                if let Some(BepPayload::Progress(pr)) = &event.payload {
                    if let Some(mapper) = &mut self.mapper {
                        mapper.on_progress(
                            Some(pr.stderr.as_str()),
                            Some(pr.stdout.as_str()),
                        );
                    }
                }
                Ok(())
            }
            BepId::BuildMetadata(_) => {
                if let Some(BepPayload::BuildMetadata(bm)) = &event.payload {
                    let json = crate::grpc::server::build_metadata_to_json(bm);
                    if let Some(mapper) = &mut self.mapper {
                        mapper.on_build_metadata(&json);
                    }
                }
                Ok(())
            }
            BepId::PatternSkipped(ps) => {
                let patterns = ps.pattern.clone();
                if let Some(mapper) = &mut self.mapper {
                    mapper.on_pattern_skipped(&patterns);
                }
                Ok(())
            }
            BepId::UnconfiguredLabel(t) => {
                let label = t.label.clone();
                if let Some(BepPayload::Aborted(a)) = &event.payload {
                    let reason = crate::grpc::server::aborted_reason_to_str(a.reason());
                    if let Some(mapper) = &mut self.mapper {
                        mapper.on_target_skipped(
                            &label,
                            Some(reason),
                            Some(a.description.as_str()),
                            event_time_nanos,
                        );
                    }
                }
                Ok(())
            }
            BepId::ConfiguredLabel(t) => {
                let label = t.label.clone();
                if let Some(BepPayload::Aborted(a)) = &event.payload {
                    let reason = crate::grpc::server::aborted_reason_to_str(a.reason());
                    if let Some(mapper) = &mut self.mapper {
                        mapper.on_target_skipped(
                            &label,
                            Some(reason),
                            Some(a.description.as_str()),
                            event_time_nanos,
                        );
                    }
                }
                Ok(())
            }
            BepId::TestResult(tr) => {
                let label = tr.label.clone();
                if let Some(BepPayload::TestResult(pr)) = &event.payload {
                    info!(
                        label = %label,
                        status = ?crate::grpc::server::test_status_to_str(pr.status),
                        attempt = tr.attempt,
                        "Received TestResult (BEP)"
                    );
                    let ei = pr.execution_info.as_ref();
                    let strategy: Option<&str> = ei.map(|e| e.strategy.as_str());
                    let hostname: Option<&str> = ei
                        .map(|e| e.hostname.as_str())
                        .filter(|h| !h.is_empty());
                    let cached_remotely: Option<bool> = ei.map(|e| e.cached_remotely);
                    let start_time_nanos = pr.test_attempt_start.as_ref()
                        .map(|ts| ts.seconds * 1_000_000_000 + i64::from(ts.nanos));
                    let duration_nanos = pr.test_attempt_duration.as_ref()
                        .map(|d| d.seconds * 1_000_000_000 + i64::from(d.nanos));
                    if let Some(mapper) = &mut self.mapper {
                        mapper.on_test_result(&TestResultEvent {
                            label: &label,
                            status: Some(crate::grpc::server::test_status_to_str(pr.status)),
                            attempt: Some(tr.attempt),
                            run: Some(tr.run),
                            shard: Some(tr.shard),
                            cached: Some(pr.cached_locally),
                            strategy,
                            start_time_nanos,
                            duration_nanos,
                            hostname,
                            cached_remotely,
                        });
                    }
                } else {
                    debug!(
                        label = %label,
                        "TestResult id received but payload is not TestResult (payload variant mismatch)"
                    );
                }
                Ok(())
            }
            BepId::TestSummary(ts) => {
                if let Some(BepPayload::TestSummary(ps)) = &event.payload {
                    let label = ts.label.clone();
                    if let Some(mapper) = &mut self.mapper {
                        mapper.on_test_summary(
                            &label,
                            Some(crate::grpc::server::test_status_to_str(ps.overall_status)),
                            Some(ps.total_run_count),
                            Some(ps.run_count),
                            Some(ps.attempt_count),
                            Some(ps.shard_count),
                            Some(ps.total_num_cached),
                        );
                    }
                }
                Ok(())
            }
            BepId::TargetSummary(ts) => {
                if let Some(BepPayload::TargetSummary(ps)) = &event.payload {
                    let label = ts.label.clone();
                    if let Some(mapper) = &mut self.mapper {
                        mapper.on_target_summary(
                            &label,
                            Some(ps.overall_build_success),
                            Some(crate::grpc::server::test_status_to_str(
                                ps.overall_test_status,
                            )),
                        );
                    }
                }
                Ok(())
            }
            BepId::BuildToolLogs(_) => {
                if let Some(BepPayload::BuildToolLogs(btl)) = &event.payload {
                    if let Some(mapper) = &mut self.mapper {
                        let logs: Vec<serde_json::Value> = btl
                            .log
                            .iter()
                            .map(|f| {
                                serde_json::json!({
                                    "name": f.name,
                                    "uri": crate::grpc::server::bep_file_uri(f),
                                })
                            })
                            .collect();
                        let json = serde_json::json!({"log": logs});
                        mapper.on_build_tool_logs(&json);
                    }
                }
                Ok(())
            }
            BepId::StructuredCommandLine(_)
            | BepId::TestProgress(_)
            | BepId::ExecRequest(_)
            | BepId::Workspace(_)
            | BepId::ConvenienceSymlinksIdentified(_)
            | BepId::Unknown(_) => Ok(()),
        }
    }

    // =========================================================================
    // Lifecycle event handlers
    // =========================================================================

    fn handle_started(&mut self, event: &BepJsonEvent) -> Result<(), RouterError> {
        self.state.reset();
        if let Some(mapper) = &mut self.mapper {
            mapper.reset();
        }

        let payload = event.get_payload("started");

        if let Some(started) = payload {
            let uuid = started.get("uuid").and_then(|v| v.as_str());
            let command = started.get("command").and_then(|v| v.as_str());
            let workspace_dir = started.get("workspaceDirectory").and_then(|v| v.as_str());
            self.ensure_mapper_for_started(
                uuid.unwrap_or("unknown"),
                command.unwrap_or("unknown"),
                workspace_dir,
            );
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

            let has_all_actions = has_publish_all_actions_flag(&all_options);

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

            if let Some(path) = extract_exec_log_binary_path(&all_options) {
                self.state.set_exec_log_path(Some(path.clone()));
                if let Some(mapper) = &mut self.mapper {
                    mapper.on_exec_log_detected(path);
                }
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

            if let Some(mapper) = &mut self.mapper {
                mapper.on_options_parsed(&startup_opts, &explicit);

                let tool_tag = payload
                    .get("toolTag")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                if !tool_tag.is_empty() {
                    mapper.on_tool_tag(tool_tag);
                }
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

                if let Some(mapper) = &mut self.mapper {
                    mapper.on_configuration(config_id, mnemonic, platform);

                    let cpu = payload.get("cpu").and_then(|v| v.as_str());
                    let is_tool = payload.get("isTool").and_then(|v| v.as_bool());
                    mapper.on_configuration_extended(config_id, cpu, is_tool);
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
            let exit_code_name = payload
                .get("exitCode")
                .and_then(|ec| ec.get("name"))
                .and_then(|v| v.as_str());
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
                mapper.on_build_finished(exit_code, finish_time_nanos, exit_code_name);
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

            let test_size = payload
                .and_then(|c| c.get("testSize"))
                .and_then(|v| v.as_str())
                .filter(|s| !s.is_empty());

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

            if let Some(mapper) = &mut self.mapper {
                mapper.on_target_configured(
                    label,
                    target_kind,
                    &tags,
                    event.event_time_nanos,
                    test_size,
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

            // OTel: end target span with resolved file sets + action-based timing when available
            if let Some(mapper) = &mut self.mapper {
                let (earliest, latest, buffered) = self
                    .state
                    .take_target_action_buffer(label)
                    .unwrap_or((None, None, vec![]));
                mapper.on_target_completed(
                    label,
                    success,
                    &file_sets,
                    &tags,
                    event.event_time_nanos,
                    earliest,
                    latest,
                    &buffered,
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

            let exit_code_name = payload
                .and_then(|a| a.get("failureDetail"))
                .and_then(|f| f.get("message"))
                .and_then(|v| v.as_str())
                .filter(|s| !s.is_empty());

            if let Some(mapper) = &mut self.mapper {
                mapper.on_action_completed(&ActionCompletedEvent {
                    label,
                    mnemonic,
                    success,
                    exit_code,
                    exit_code_name,
                    primary_output,
                    configuration,
                    command_line: &command_line,
                    stdout_path,
                    stderr_path,
                    start_time_nanos,
                    end_time_nanos,
                    cached: None,
                    hostname: None,
                    cached_remotely: None,
                    runner: None,
                });
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
            let ei = payload.and_then(|p| p.get("executionInfo"));
            let strategy = ei.and_then(|e| e.get("strategy")).and_then(|v| v.as_str());
            let hostname = ei
                .and_then(|e| e.get("hostname"))
                .and_then(|v| v.as_str())
                .filter(|h| !h.is_empty());
            let cached_remotely = ei
                .and_then(|e| e.get("cachedRemotely"))
                .and_then(|v| v.as_bool());

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
                mapper.on_test_result(&TestResultEvent {
                    label,
                    status,
                    attempt,
                    run,
                    shard,
                    cached,
                    strategy,
                    start_time_nanos,
                    duration_nanos,
                    hostname,
                    cached_remotely,
                });
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

    fn handle_build_tool_logs(&mut self, event: &BepJsonEvent) -> Result<(), RouterError> {
        if let Some(payload) = event.get_payload("buildToolLogs") {
            debug!("Build tool logs received");
            if let Some(mapper) = &mut self.mapper {
                mapper.on_build_tool_logs(payload);
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

impl Drop for EventRouter {
    fn drop(&mut self) {
        self.finish();
        let _ = self.tracer_provider.take().map(|p| p.shutdown());
        let _ = self.logger_provider.take().map(|lp| lp.shutdown());
    }
}

/// Check for --build_event_publish_all_actions with exact matching.
/// Explicitly rejects --nobuild_event_publish_all_actions* and =false/=0 to avoid false positives.
fn has_publish_all_actions_flag(options: &[&str]) -> bool {
    let mut seen_on = false;
    for opt in options {
        if opt.starts_with("--nobuild_event_publish_all_actions")
            || *opt == "--build_event_publish_all_actions=false"
            || *opt == "--build_event_publish_all_actions=0"
        {
            return false;
        }
        if *opt == "--build_event_publish_all_actions"
            || *opt == "--build_event_publish_all_actions=true"
            || *opt == "--build_event_publish_all_actions=1"
        {
            seen_on = true;
        }
    }
    seen_on
}

/// Return a short name for the BEP event type (for diagnostic logging).
fn bep_event_type_name(id: &crate::build_event_stream::build_event_id::Id) -> &'static str {
    use crate::build_event_stream::build_event_id::Id;
    match id {
        Id::Started(_) => "Started",
        Id::UnstructuredCommandLine(_) => "UnstructuredCommandLine",
        Id::StructuredCommandLine(_) => "StructuredCommandLine",
        Id::OptionsParsed(_) => "OptionsParsed",
        Id::WorkspaceStatus(_) => "WorkspaceStatus",
        Id::Configuration(_) => "Configuration",
        Id::Pattern(_) => "Pattern",
        Id::PatternSkipped(_) => "PatternSkipped",
        Id::TargetConfigured(_) => "TargetConfigured",
        Id::TargetCompleted(_) => "TargetCompleted",
        Id::ActionCompleted(_) => "ActionCompleted",
        Id::NamedSet(_) => "NamedSet",
        Id::TestResult(_) => "TestResult",
        Id::TestSummary(_) => "TestSummary",
        Id::BuildFinished(_) => "BuildFinished",
        Id::BuildMetrics(_) => "BuildMetrics",
        Id::TargetSummary(_) => "TargetSummary",
        Id::Progress(_) => "Progress",
        Id::Fetch(_) => "Fetch",
        Id::Workspace(_) => "Workspace",
        Id::BuildToolLogs(_) => "BuildToolLogs",
        Id::BuildMetadata(_) => "BuildMetadata",
        Id::ConvenienceSymlinksIdentified(_) => "ConvenienceSymlinksIdentified",
        Id::UnconfiguredLabel(_) => "UnconfiguredLabel",
        Id::ConfiguredLabel(_) => "ConfiguredLabel",
        Id::TestProgress(_) => "TestProgress",
        Id::ExecRequest(_) => "ExecRequest",
        Id::Unknown(_) => "Unknown",
    }
}

/// Extract SpawnExec from ActionExecuted.strategy_details (first successful decode).
/// Returns (cached, runner) for use on action spans.
fn extract_spawn_exec_from_action(act: &ActionExecuted) -> (Option<bool>, Option<String>) {
    for any in &act.strategy_details {
        if let Ok(exec) = SpawnExec::decode(any.value.as_ref()) {
            let runner = if exec.runner.is_empty() {
                None
            } else {
                Some(exec.runner.clone())
            };
            return (Some(exec.cache_hit), runner);
        }
    }
    (None, None)
}
