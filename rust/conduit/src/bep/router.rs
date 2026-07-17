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
//! Per-invocation attributes (`bazel.invocation_id`, `bazel.command`,
//! `bazel.workspace.directory`) are set on the root span by [`OtelMapper`];
//! the [`TracerProvider`] / [`LoggerProvider`] it uses are owned by `main`
//! and live for the whole conduit process.

use super::decoder::BepJsonEvent;
use crate::build_event_stream::build_event::Payload as BepPayload;
use crate::build_event_stream::build_event_id::Id as BepId;
use crate::build_event_stream::BuildEvent;
use crate::otel::{ActionCompletedEvent, OtelMapper, Redactor, TestResultEvent};
use crate::state::{ActionProcessingMode, BuildState};
use std::path::PathBuf;
use thiserror::Error;
use tracing::{debug, info, trace, warn};

const EXEC_LOG_BINARY_PREFIX: &str = "--execution_log_binary_file=";
const EXEC_LOG_COMPACT_PREFIX: &str = "--execution_log_compact_file=";
const EXEC_LOG_COMPACT_EXPERIMENTAL_PREFIX: &str = "--experimental_execution_log_compact_file=";
const EXEC_LOG_JSON_PREFIX: &str = "--execution_log_json_file=";

/// Detect the **compact** execution log path from a flat options list.
///
/// Only `--execution_log_compact_file=` and its `--experimental_*` alias are
/// supported: live tailing needs the dedup-table format. The binary and JSON
/// formats produce a warning on the root span (via the mapper) but no log is
/// consumed; users who hit this should switch to the compact flag.
fn extract_exec_log(options: &[&str], mapper: Option<&mut OtelMapper>) -> Option<PathBuf> {
    let mut compact: Option<PathBuf> = None;
    let mut unsupported: Vec<(&'static str, String)> = Vec::new();
    for opt in options {
        if let Some(p) = opt
            .strip_prefix(EXEC_LOG_COMPACT_PREFIX)
            .or_else(|| opt.strip_prefix(EXEC_LOG_COMPACT_EXPERIMENTAL_PREFIX))
        {
            if !p.is_empty() && compact.is_none() {
                compact = Some(PathBuf::from(p));
            }
        } else if let Some(p) = opt.strip_prefix(EXEC_LOG_BINARY_PREFIX) {
            if !p.is_empty() {
                unsupported.push(("--execution_log_binary_file", p.to_string()));
            }
        } else if let Some(p) = opt.strip_prefix(EXEC_LOG_JSON_PREFIX) {
            if !p.is_empty() {
                unsupported.push(("--execution_log_json_file", p.to_string()));
            }
        }
    }

    if !unsupported.is_empty() {
        for (flag, path) in &unsupported {
            warn!(
                flag = %flag,
                path = %path,
                "Unsupported execution log format; only --execution_log_compact_file enables conduit's spawn enrichment",
            );
        }
        if let Some(mapper) = mapper {
            let flags = unsupported
                .iter()
                .map(|(f, _)| f.to_string())
                .collect::<Vec<_>>()
                .join(", ");
            mapper.on_unsupported_exec_log_flag(&flags);
        }
    }
    compact
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

struct StartedEventArgs {
    uuid: String,
    command: String,
    start_time_nanos: Option<i64>,
    start_time_millis: Option<i64>,
    event_time_nanos: Option<i64>,
    workspace_dir: Option<String>,
    working_dir: Option<String>,
    build_tool_version: Option<String>,
    server_pid: Option<i64>,
    host: Option<String>,
    user: Option<String>,
}

struct OptionsParsedArgs {
    cmd_line: Vec<String>,
    explicit_cmd_line: Vec<String>,
    startup_options: Vec<String>,
    tool_tag: Option<String>,
}

struct ActionCompletedArgs {
    label: Option<String>,
    primary_output: Option<String>,
    configuration: Option<String>,
    success: bool,
    mnemonic: Option<String>,
    exit_code: Option<i32>,
    exit_code_name: Option<String>,
    command_line: Vec<String>,
    stdout_path: Option<String>,
    stderr_path: Option<String>,
    start_time_nanos: Option<i64>,
    end_time_nanos: Option<i64>,
}

/// Event router that processes BEP events.
///
/// Holds an [`OtelMapper`] keyed to a long-lived `Tracer`/`Logger` pair;
/// each `Started` event resets the mapper's per-invocation state without
/// rebuilding any provider.
pub struct EventRouter {
    state: BuildState,
    mapper: Option<OtelMapper>,
    redactor: Redactor,
    exec_log_max_message_bytes: usize,
    exec_log_max_decompressed_bytes: usize,
}

impl EventRouter {
    /// Create a new event router
    pub fn new() -> Self {
        Self {
            state: BuildState::new(),
            mapper: None,
            redactor: Redactor::default_enabled(),
            exec_log_max_message_bytes: crate::exec_log::DEFAULT_EXECLOG_MAX_MESSAGE_BYTES,
            exec_log_max_decompressed_bytes:
                crate::exec_log::DEFAULT_EXECLOG_MAX_DECOMPRESSED_BYTES,
        }
    }

    /// Attach OTel export. The router holds [`Tracer`]/[`Logger`] clones for
    /// the lifetime of the process; the underlying providers are owned by
    /// `main`. Per-invocation attributes are set on the root span (not on
    /// `Resource`).
    pub fn with_export(
        mut self,
        tracer: opentelemetry_sdk::trace::Tracer,
        logger: opentelemetry_sdk::logs::Logger,
    ) -> Self {
        self.mapper = Some(
            OtelMapper::new(tracer, Some(logger))
                .with_redactor(self.redactor.clone())
                .with_exec_log_max_message_bytes(self.exec_log_max_message_bytes)
                .with_exec_log_max_decompressed_bytes(self.exec_log_max_decompressed_bytes),
        );
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

    /// Override the per-message cap used when parsing the execution log.
    /// Stored on the router so a later `with_export()` call can propagate
    /// it to a freshly-built [`OtelMapper`].
    pub fn with_exec_log_max_message_bytes(mut self, bytes: usize) -> Self {
        self.exec_log_max_message_bytes = bytes;
        if let Some(mapper) = self.mapper.take() {
            self.mapper = Some(mapper.with_exec_log_max_message_bytes(bytes));
        }
        self
    }

    /// Override the decompressed-bytes cap used when parsing the compact
    /// execution log. Same propagation pattern as
    /// [`Self::with_exec_log_max_message_bytes`].
    pub fn with_exec_log_max_decompressed_bytes(mut self, bytes: usize) -> Self {
        self.exec_log_max_decompressed_bytes = bytes;
        if let Some(mapper) = self.mapper.take() {
            self.mapper = Some(mapper.with_exec_log_max_decompressed_bytes(bytes));
        }
        self
    }

    /// Attach an OTel mapper directly (used by tests; production wires the
    /// mapper through [`Self::with_export`]).
    pub fn with_mapper(mut self, mapper: OtelMapper) -> Self {
        self.mapper = Some(mapper.with_redactor(self.redactor.clone()));
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

    /// Apply compact execution-log records produced since the last pump.
    pub(crate) fn pump_exec_log(&mut self) {
        if let Some(mapper) = &mut self.mapper {
            mapper.pump_compact_spawns();
        }
    }

    /// Finalize: end all open spans on the current invocation.
    ///
    /// We intentionally do NOT call `force_flush()` on either provider here.
    /// Both `BatchSpanProcessor::force_flush` and `BatchLogProcessor::force_flush`
    /// (with `runtime::Tokio`) are sync `block_on`s against their worker
    /// channels. Calling them from inside a `tokio::spawn`'d task (the route
    /// worker) deadlocks the runtime when the queue is non-empty: the export
    /// and timeout-timer tasks share workers with us, so the worker we just
    /// blocked is the one that would have driven the response back.
    /// Instead we rely on the periodic `scheduled_delay` ticker (200 ms,
    /// see [`crate::otel::trace_context`]) to drain.
    pub fn finish(&mut self) {
        if let Some(mapper) = &mut self.mapper {
            mapper.finish();
        }
    }
}

impl EventRouter {
    fn apply_started_event(&mut self, args: StartedEventArgs) {
        let carry_exec_log_path = if self.state.invocation_id().is_none() {
            self.state.exec_log_path().cloned()
        } else {
            None
        };
        self.state.reset();
        if let Some(mapper) = &mut self.mapper {
            mapper.reset();
        }
        if let Some(path) = carry_exec_log_path {
            self.state.set_exec_log_path(Some(path));
        }

        self.state.set_invocation_id(args.uuid.clone());
        self.state.set_command(args.command.clone());
        if let Some(millis) = args.start_time_millis {
            self.state.set_start_time_millis(millis);
        }

        if let Some(mapper) = &mut self.mapper {
            mapper.on_build_started(
                &args.uuid,
                &args.command,
                args.start_time_nanos,
                args.event_time_nanos,
            );
            mapper.on_build_started_extended(
                args.workspace_dir.as_deref(),
                args.working_dir.as_deref(),
                args.build_tool_version.as_deref(),
                args.server_pid,
                args.host.as_deref(),
                args.user.as_deref(),
            );
        }
        self.retry_pending_exec_log_on_started();
    }

    fn retry_pending_exec_log_on_started(&mut self) {
        let Some(path) = self.state.exec_log_path().cloned() else {
            return;
        };
        // Absolute paths are already actionable from OptionsParsed; retry only
        // relative paths that may have been skipped while workspace was unknown.
        if !path.is_relative() {
            return;
        }
        if let Some(mapper) = &mut self.mapper {
            mapper.on_exec_log_detected(path);
        }
    }

    fn apply_options_parsed_event(&mut self, args: OptionsParsedArgs) {
        let all_options: Vec<&str> = args
            .cmd_line
            .iter()
            .chain(&args.explicit_cmd_line)
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

        if let Some(path) = extract_exec_log(&all_options, self.mapper.as_mut()) {
            self.state.set_exec_log_path(Some(path.clone()));
            if let Some(mapper) = &mut self.mapper {
                mapper.on_exec_log_detected(path);
            }
        }

        self.state.set_startup_options(args.startup_options.clone());
        if let Some(mapper) = &mut self.mapper {
            mapper.on_options_parsed(&args.startup_options, &args.explicit_cmd_line);
            if let Some(tool_tag) = args.tool_tag.filter(|s| !s.is_empty()) {
                mapper.on_tool_tag(&tool_tag);
            }
        }
    }

    fn apply_action_completed_event(&mut self, args: ActionCompletedArgs) {
        let should_process = self
            .state
            .action_mode()
            .should_create_span(args.success);
        if !should_process {
            return;
        }

        self.state.record_action(
            args.label.clone(),
            args.mnemonic.clone(),
            args.primary_output.clone(),
            args.success,
            args.exit_code,
        );

        if let Some(mapper) = &mut self.mapper {
            mapper.on_action_completed(&ActionCompletedEvent {
                label: args.label.as_deref(),
                mnemonic: args.mnemonic.as_deref(),
                success: args.success,
                exit_code: args.exit_code,
                exit_code_name: args.exit_code_name.as_deref(),
                primary_output: args.primary_output.as_deref(),
                configuration: args.configuration.as_deref(),
                command_line: &args.command_line,
                stdout_path: args.stdout_path.as_deref(),
                stderr_path: args.stderr_path.as_deref(),
                start_time_nanos: args.start_time_nanos,
                end_time_nanos: args.end_time_nanos,
                cached: None,
                hostname: None,
                cached_remotely: None,
                runner: None,
            });
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
                if let Some(BepPayload::Started(s)) = &event.payload {
                    let start_time_nanos = s
                        .start_time
                        .as_ref()
                        .map(|ts| ts.seconds * 1_000_000_000 + i64::from(ts.nanos));
                    #[allow(deprecated)]
                    let start_time_millis = (s.start_time_millis != 0).then_some(s.start_time_millis);
                    self.apply_started_event(StartedEventArgs {
                        uuid: if s.uuid.is_empty() {
                            "unknown".to_string()
                        } else {
                            s.uuid.clone()
                        },
                        command: if s.command.is_empty() {
                            "unknown".to_string()
                        } else {
                            s.command.clone()
                        },
                        start_time_nanos,
                        start_time_millis,
                        event_time_nanos,
                        workspace_dir: (!s.workspace_directory.is_empty())
                            .then_some(s.workspace_directory.clone()),
                        working_dir: (!s.working_directory.is_empty())
                            .then_some(s.working_directory.clone()),
                        build_tool_version: (!s.build_tool_version.is_empty())
                            .then_some(s.build_tool_version.clone()),
                        server_pid: (s.server_pid != 0).then_some(s.server_pid),
                        host: (!s.host.is_empty()).then_some(s.host.clone()),
                        user: (!s.user.is_empty()).then_some(s.user.clone()),
                    });
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
                    self.apply_options_parsed_event(OptionsParsedArgs {
                        cmd_line: o.cmd_line.clone(),
                        explicit_cmd_line: o.explicit_cmd_line.clone(),
                        startup_options: o.startup_options.clone(),
                        tool_tag: (!o.tool_tag.is_empty()).then_some(o.tool_tag.clone()),
                    });
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
                            // Bazel pre-7.0 only set the deprecated `finish_time_millis`.
                            #[allow(deprecated)]
                            let ms = f.finish_time_millis;
                            (ms != 0).then_some(ms * 1_000_000)
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
                        mapper.on_target_completed(
                            &label,
                            c.success,
                            &file_sets,
                            &tags,
                            event_time_nanos,
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
                    let configuration = a
                        .configuration
                        .as_ref()
                        .map(|c| c.id.clone());
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
                    self.apply_action_completed_event(ActionCompletedArgs {
                        label: Some(a.label.clone()),
                        primary_output: Some(a.primary_output.clone()),
                        configuration,
                        success: act.success,
                        mnemonic: Some(act.r#type.clone()),
                        exit_code: Some(act.exit_code),
                        exit_code_name: None,
                        command_line: act.command_line.clone(),
                        stdout_path: stdout_uri,
                        stderr_path: stderr_uri,
                        start_time_nanos: start_nanos,
                        end_time_nanos: end_nanos,
                    });
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

            // Extended fields from BuildStarted
            let workspace_dir = started.get("workspaceDirectory").and_then(|v| v.as_str());
            let working_dir = started.get("workingDirectory").and_then(|v| v.as_str());
            let build_tool_version = started.get("buildToolVersion").and_then(|v| v.as_str());
            let server_pid = started.get("serverPid").and_then(|v| v.as_i64());
            let host = started.get("host").and_then(|v| v.as_str());
            let user = started.get("user").and_then(|v| v.as_str());

            self.apply_started_event(StartedEventArgs {
                uuid: uuid.unwrap_or("unknown").to_string(),
                command: command.unwrap_or("unknown").to_string(),
                start_time_nanos,
                start_time_millis,
                event_time_nanos,
                workspace_dir: workspace_dir.map(str::to_string),
                working_dir: working_dir.map(str::to_string),
                build_tool_version: build_tool_version.map(str::to_string),
                server_pid,
                host: host.map(str::to_string),
                user: user.map(str::to_string),
            });
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
            let startup_opts: Vec<String> = payload
                .get("startupOptions")
                .and_then(|v| v.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|v| v.as_str().map(String::from))
                        .collect()
                })
                .unwrap_or_default();

            let explicit: Vec<String> = payload
                .get("explicitCmdLine")
                .and_then(|v| v.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|v| v.as_str().map(String::from))
                        .collect()
                })
                .unwrap_or_default();

            let cmd_line: Vec<String> = payload
                .get("cmdLine")
                .and_then(|v| v.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|v| v.as_str().map(String::from))
                        .collect()
                })
                .unwrap_or_default();
            let tool_tag = payload
                .get("toolTag")
                .and_then(|v| v.as_str())
                .map(str::to_string);

            self.apply_options_parsed_event(OptionsParsedArgs {
                cmd_line,
                explicit_cmd_line: explicit,
                startup_options: startup_opts,
                tool_tag,
            });
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

            // OTel: end target span with resolved file sets.
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

            debug!(
                label,
                mnemonic,
                success,
                exit_code,
                primary_output,
                "Action completed"
            );

            let exit_code_name = payload
                .and_then(|a| a.get("failureDetail"))
                .and_then(|f| f.get("message"))
                .and_then(|v| v.as_str())
                .filter(|s| !s.is_empty());
            self.apply_action_completed_event(ActionCompletedArgs {
                label: label.map(String::from),
                primary_output: primary_output.map(String::from),
                configuration: configuration.map(String::from),
                success,
                mnemonic: mnemonic.map(String::from),
                exit_code,
                exit_code_name: exit_code_name.map(String::from),
                command_line,
                stdout_path: stdout_path.map(String::from),
                stderr_path: stderr_path.map(String::from),
                start_time_nanos,
                end_time_nanos,
            });
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
