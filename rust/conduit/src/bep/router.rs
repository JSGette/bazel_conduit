// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

//! BEP event router
//!
//! Dispatches incoming Bazel Build Event Protocol (BEP) events to the
//! [`OtelMapper`] for span creation and keeps a per-invocation [`Summary`].
//!
//! [`EventRouter::route_build_event`] receives decoded `BuildEvent` protos,
//! from the gRPC server or from a binary BEP file replay.
//!
//! Per-invocation attributes (`bazel.invocation_id`, `bazel.command`,
//! `bazel.workspace.directory`) are set on the root span by [`OtelMapper`];
//! the [`TracerProvider`] / [`LoggerProvider`] it uses are owned by `main`
//! and live for the whole conduit process.

use crate::build_event_stream::build_event::Payload as BepPayload;
use crate::build_event_stream::build_event_id::Id as BepId;
use crate::build_event_stream::BuildEvent;
use crate::otel::{ActionCompletedEvent, OtelMapper, Redactor, TestResultEvent};
use crate::state::{TracePolicy, TraceProfile, DEFAULT_TRACE_MIN_DURATION_MS};
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
    cached: Option<bool>,
    runner: Option<String>,
}

/// Per-invocation counts, logged when an invocation finalizes.
#[derive(Debug, Default, Clone)]
pub struct Summary {
    pub invocation_id: Option<String>,
    pub command: Option<String>,
    pub exit_code: Option<i32>,
    pub targets: usize,
    pub failed_targets: usize,
    /// Actions kept by the trace policy.
    pub actions: usize,
    pub failed_actions: usize,
    pub exec_log: Option<PathBuf>,
}

impl std::fmt::Display for Summary {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "invocation {} ({}): exit {}, {} targets ({} failed), {} actions ({} failed)",
            self.invocation_id.as_deref().unwrap_or("-"),
            self.command.as_deref().unwrap_or("-"),
            self.exit_code.map_or("-".to_string(), |c| c.to_string()),
            self.targets,
            self.failed_targets,
            self.actions,
            self.failed_actions,
        )
    }
}

/// Event router that processes BEP events.
///
/// Holds an [`OtelMapper`] keyed to a long-lived `Tracer`/`Logger` pair;
/// each `Started` event resets the mapper's per-invocation state without
/// rebuilding any provider.
pub struct EventRouter {
    summary: Summary,
    trace_policy: TracePolicy,
    mapper: Option<OtelMapper>,
    redactor: Redactor,
    exec_log_max_message_bytes: usize,
    exec_log_max_decompressed_bytes: usize,
    /// Trace profile requested on the CLI. `Auto` (default) resolves against
    /// Bazel's `--build_event_publish_all_actions` at `OptionsParsed`.
    requested_profile: TraceProfile,
    /// Minimum operation duration (ms) kept by the lightweight profile.
    trace_min_duration_ms: i64,
    /// Tail Bazel's compact exec log when the build writes one (`--exec-log`).
    exec_log: bool,
    /// Latest wall time seen in BEP payloads; stands in for the BES event
    /// time when replaying a file, which carries none.
    bep_clock_nanos: Option<i64>,
}

impl EventRouter {
    /// Create a new event router
    pub fn new() -> Self {
        Self {
            summary: Summary::default(),
            trace_policy: TracePolicy::default(),
            mapper: None,
            redactor: Redactor::default_enabled(),
            exec_log_max_message_bytes: crate::exec_log::DEFAULT_EXECLOG_MAX_MESSAGE_BYTES,
            exec_log_max_decompressed_bytes:
                crate::exec_log::DEFAULT_EXECLOG_MAX_DECOMPRESSED_BYTES,
            requested_profile: TraceProfile::default(),
            trace_min_duration_ms: DEFAULT_TRACE_MIN_DURATION_MS,
            exec_log: false,
            bep_clock_nanos: None,
        }
    }

    /// Enable exec-log enrichment. Off by default: a build that writes an
    /// exec log for other reasons must not suddenly emit spawn spans.
    pub fn with_exec_log(mut self, enabled: bool) -> Self {
        self.exec_log = enabled;
        self
    }

    /// Set the requested trace profile and lightweight duration threshold.
    /// Resolution against the Bazel flag happens per-invocation at
    /// `OptionsParsed`.
    pub fn with_trace_profile(mut self, profile: TraceProfile, min_duration_ms: i64) -> Self {
        self.requested_profile = profile;
        self.trace_min_duration_ms = min_duration_ms;
        self
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

    pub fn summary(&self) -> &Summary {
        &self.summary
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

    /// Mark an unfinished invocation as cut short by conduit shutdown.
    /// Call before [`Self::finish`].
    pub fn shutdown(&mut self) {
        if let Some(mapper) = &mut self.mapper {
            mapper.on_shutdown();
        }
    }
}

impl EventRouter {
    fn apply_started_event(&mut self, args: StartedEventArgs) {
        if let Some(mapper) = &mut self.mapper {
            mapper.reset();
        }
        self.trace_policy = TracePolicy::default();
        self.summary = Summary {
            invocation_id: Some(args.uuid.clone()),
            command: Some(args.command.clone()),
            ..Summary::default()
        };

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
    }

    fn apply_options_parsed_event(&mut self, args: OptionsParsedArgs) {
        let all_options: Vec<&str> = args
            .cmd_line
            .iter()
            .chain(&args.explicit_cmd_line)
            .map(String::as_str)
            .collect();
        let has_all_actions = has_publish_all_actions_flag(&all_options);
        let policy = TracePolicy::resolve(
            self.requested_profile,
            self.trace_min_duration_ms,
            has_all_actions,
        );
        self.trace_policy = policy;
        if let Some(mapper) = &mut self.mapper {
            mapper.set_trace_policy(policy);
        }

        if self.exec_log {
            if let Some(path) = extract_exec_log(&all_options, self.mapper.as_mut()) {
                self.summary.exec_log = Some(path.clone());
                if let Some(mapper) = &mut self.mapper {
                    mapper.on_exec_log_detected(path);
                }
            }
        }

        if let Some(mapper) = &mut self.mapper {
            mapper.on_options_parsed(&args.startup_options, &args.explicit_cmd_line);
            if let Some(tool_tag) = args.tool_tag.filter(|s| !s.is_empty()) {
                mapper.on_tool_tag(&tool_tag);
            }
        }
    }

    fn apply_action_completed_event(&mut self, args: ActionCompletedArgs) {
        let duration_ms = match (args.start_time_nanos, args.end_time_nanos) {
            (Some(start), Some(end)) if end >= start => Some((end - start) / 1_000_000),
            _ => None,
        };
        let should_process = self
            .trace_policy
            .should_emit_action(args.success, args.cached, duration_ms);
        if !should_process {
            if let Some(mapper) = &mut self.mapper {
                mapper.note_suppressed_action();
            }
            return;
        }

        self.summary.actions += 1;
        if !args.success {
            self.summary.failed_actions += 1;
        }

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
                cached: args.cached,
                hostname: None,
                cached_remotely: None,
                runner: args.runner.as_deref(),
            });
        }
    }

    /// Route a decoded BEP `BuildEvent`. `event_time_nanos` is the BES event
    /// time (gRPC); `None` on file replay.
    pub fn route_build_event(
        &mut self,
        event: &BuildEvent,
        event_time_nanos: Option<i64>,
    ) -> Result<(), RouterError> {
        let id = match event.id.as_ref().and_then(|i| i.id.as_ref()) {
            Some(id) => id,
            None => return Err(RouterError::MissingEventId),
        };
        if let Some(t) = payload_time_nanos(event) {
            self.bep_clock_nanos = match (&event.payload, self.bep_clock_nanos) {
                (Some(BepPayload::Started(_)), _) | (_, None) => Some(t),
                (_, Some(c)) => Some(c.max(t)),
            };
        }
        let event_time_nanos = event_time_nanos.or(self.bep_clock_nanos);

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
                    if !u.args.is_empty() {
                        if let Some(mapper) = &mut self.mapper {
                            mapper.on_command_line(&u.args);
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
                        .map(|ts| ts.seconds * 1_000_000_000 + i64::from(ts.nanos));
                    self.summary.exit_code = exit_code;
                    if let Some(mapper) = &mut self.mapper {
                        mapper.on_build_finished(exit_code, finish_time_nanos, exit_code_name);
                    }
                }
                Ok(())
            }
            BepId::BuildMetrics(_) => {
                if let Some(BepPayload::BuildMetrics(m)) = &event.payload {
                    if let Some(mapper) = &mut self.mapper {
                        mapper.on_build_metrics(m);
                    }
                }
                Ok(())
            }
            BepId::Pattern(p) => {
                if let Some(BepPayload::Expanded(_)) = &event.payload {
                    if let Some(mapper) = &mut self.mapper {
                        mapper.on_pattern(&p.pattern);
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
                    self.summary.targets += 1;
                    self.summary.failed_targets += 1;
                    if let Some(mapper) = &mut self.mapper {
                        mapper.on_target_skipped(
                            &label,
                            Some(a.reason().as_str_name()),
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
                    self.summary.targets += 1;
                    if !c.success {
                        self.summary.failed_targets += 1;
                    }
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
                    let (cached, runner) = spawn_strategy(
                        act.strategy_details
                            .iter()
                            .map(|a| (a.type_url.as_str(), &a.value[..])),
                    );
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
                        // Internal actions (Symlink, ...) carry no times.
                        end_time_nanos: end_nanos.or(event_time_nanos),
                        cached,
                        runner,
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
                    if let Some(mapper) = &mut self.mapper {
                        mapper.on_build_metadata(bm);
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
                    if let Some(mapper) = &mut self.mapper {
                        mapper.on_target_skipped(
                            &label,
                            Some(a.reason().as_str_name()),
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
                    if let Some(mapper) = &mut self.mapper {
                        mapper.on_target_skipped(
                            &label,
                            Some(a.reason().as_str_name()),
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
                        status = test_status_name(pr.status),
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
                            status: Some(test_status_name(pr.status)),
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
                            Some(test_status_name(ps.overall_status)),
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
                            Some(test_status_name(ps.overall_test_status)),
                        );
                    }
                }
                Ok(())
            }
            BepId::BuildToolLogs(_) => {
                if let Some(BepPayload::BuildToolLogs(btl)) = &event.payload {
                    if let Some(mapper) = &mut self.mapper {
                        mapper.on_build_tool_logs(btl);
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
}

impl Default for EventRouter {
    fn default() -> Self {
        Self::new()
    }
}

/// Action-level cache hit and runner(s) from `ActionExecuted.strategy_details`,
/// where Bazel 10+ packs one `SpawnExec` per spawn with only `runner` and
/// `cache_hit` set. `(None, None)` for older Bazel.
fn spawn_strategy<'a>(
    details: impl Iterator<Item = (&'a str, &'a [u8])>,
) -> (Option<bool>, Option<String>) {
    use prost::Message;
    use spawn_proto::tools::protos::SpawnExec;
    let spawns: Vec<SpawnExec> = details
        .filter(|(type_url, _)| type_url.ends_with("/tools.protos.SpawnExec"))
        .filter_map(|(_, value)| SpawnExec::decode(value).ok())
        .collect();
    if spawns.is_empty() {
        return (None, None);
    }
    let mut runners: Vec<&str> = spawns
        .iter()
        .map(|s| s.runner.as_str())
        .filter(|r| !r.is_empty())
        .collect();
    runners.sort_unstable();
    runners.dedup();
    let cached = spawns.iter().all(|s| s.cache_hit);
    (Some(cached), (!runners.is_empty()).then(|| runners.join(",")))
}

/// Prost's enum getters map values newer than the vendored proto to the
/// default (`NO_STATUS`); report those as `UNKNOWN` instead.
fn test_status_name(raw: i32) -> &'static str {
    crate::build_event_stream::TestStatus::try_from(raw).map_or("UNKNOWN", |s| s.as_str_name())
}

fn payload_time_nanos(event: &BuildEvent) -> Option<i64> {
    let ts = match event.payload.as_ref()? {
        BepPayload::Started(s) => s.start_time.as_ref(),
        BepPayload::Action(a) => a.end_time.as_ref(),
        BepPayload::Finished(f) => f.finish_time.as_ref(),
        _ => None,
    }?;
    Some(ts.seconds * 1_000_000_000 + i64::from(ts.nanos))
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

#[cfg(test)]
mod tests {
    use super::{spawn_strategy, test_status_name};
    use prost::Message;
    use spawn_proto::tools::protos::SpawnExec;

    const SPAWN_URL: &str = "type.googleapis.com/tools.protos.SpawnExec";

    fn spawn(runner: &str, cache_hit: bool) -> Vec<u8> {
        SpawnExec {
            runner: runner.into(),
            cache_hit,
            ..Default::default()
        }
        .encode_to_vec()
    }

    #[test]
    fn spawn_strategy_absent_before_bazel_10() {
        assert_eq!(spawn_strategy(std::iter::empty()), (None, None));
    }

    #[test]
    fn spawn_strategy_all_hits_is_cached() {
        let (a, b) = (spawn("remote cache hit", true), spawn("disk cache hit", true));
        let details = [(SPAWN_URL, &a[..]), (SPAWN_URL, &b[..])];
        assert_eq!(
            spawn_strategy(details.into_iter()),
            (Some(true), Some("disk cache hit,remote cache hit".into()))
        );
    }

    #[test]
    fn spawn_strategy_any_miss_is_not_cached() {
        let (a, b) = (spawn("remote cache hit", true), spawn("linux-sandbox", false));
        let other = b"ignored".to_vec();
        let details = [
            (SPAWN_URL, &a[..]),
            (SPAWN_URL, &b[..]),
            ("type.googleapis.com/other.Message", &other[..]),
        ];
        assert_eq!(
            spawn_strategy(details.into_iter()),
            (Some(false), Some("linux-sandbox,remote cache hit".into()))
        );
    }

    #[test]
    fn lightweight_keeps_fast_action_only_on_cache_miss() {
        use crate::build_event_stream::build_event::Payload;
        use crate::build_event_stream::build_event_id::{ActionCompletedId, Id};
        use crate::build_event_stream::{ActionExecuted, BuildEvent, BuildEventId};
        use crate::state::TraceProfile;

        let action = |cache_hit: bool| {
            let mut act = ActionExecuted {
                success: true,
                ..Default::default()
            };
            act.strategy_details.push(Default::default());
            let any = act.strategy_details.last_mut().unwrap();
            any.type_url = SPAWN_URL.into();
            any.value = spawn("remote", cache_hit).into();
            BuildEvent {
                id: Some(BuildEventId {
                    id: Some(Id::ActionCompleted(ActionCompletedId::default())),
                }),
                payload: Some(Payload::Action(act)),
                ..Default::default()
            }
        };
        let mut router = super::EventRouter::new().with_trace_profile(TraceProfile::Lightweight, 500);
        let options = BuildEvent {
            id: Some(BuildEventId {
                id: Some(Id::OptionsParsed(Default::default())),
            }),
            payload: Some(Payload::OptionsParsed(Default::default())),
            ..Default::default()
        };
        router.route_build_event(&options, None).unwrap();
        router.route_build_event(&action(true), None).unwrap();
        assert_eq!(router.summary().actions, 0);
        router.route_build_event(&action(false), None).unwrap();
        assert_eq!(router.summary().actions, 1);
    }

    #[test]
    fn test_status_names() {
        assert_eq!(test_status_name(1), "PASSED");
        assert_eq!(test_status_name(0), "NO_STATUS");
        assert_eq!(test_status_name(999), "UNKNOWN");
    }
}
