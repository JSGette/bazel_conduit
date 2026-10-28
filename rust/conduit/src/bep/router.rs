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

use crate::otel::span::{Logger, Tracer};
use crate::build_event_stream::build_event::Payload as BepPayload;
use crate::build_event_stream::build_event_id::Id as BepId;
use crate::build_event_stream::BuildEvent;
use crate::otel::{ActionCompletedEvent, OtelMapper, Redactor, TestResultEvent};
use crate::state::{
    TracePolicy, TraceProfile, DEFAULT_MAX_ACTION_SPANS, DEFAULT_TRACE_MIN_DURATION_MS,
};
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
    host: Option<String>,
    user: Option<String>,
}

struct OptionsParsedArgs {
    cmd_line: Vec<String>,
    explicit_cmd_line: Vec<String>,
    tool_tag: Option<String>,
}

/// Per-invocation counts, logged when an invocation finalizes.
#[derive(Debug, Default, Clone)]
pub struct Summary {
    pub invocation_id: Option<String>,
    pub command: Option<String>,
    pub exit_code: Option<i32>,
    pub targets: usize,
    pub failed_targets: usize,
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
            trace_policy: TracePolicy::new(
                TraceProfile::default(),
                DEFAULT_TRACE_MIN_DURATION_MS,
                DEFAULT_MAX_ACTION_SPANS,
            ),
            mapper: None,
            redactor: Redactor::default_enabled(),
            exec_log_max_message_bytes: crate::exec_log::DEFAULT_EXECLOG_MAX_MESSAGE_BYTES,
            exec_log_max_decompressed_bytes:
                crate::exec_log::DEFAULT_EXECLOG_MAX_DECOMPRESSED_BYTES,
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

    pub fn with_trace_policy(mut self, policy: TracePolicy) -> Self {
        self.trace_policy = policy;
        self
    }

    /// Attach OTel export. Per-invocation attributes go on the root span,
    /// not on the resource, which is shared by every invocation.
    pub fn with_export(
        mut self,
        tracer: Tracer,
        logger: Logger,
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
                args.host.as_deref(),
                args.user.as_deref(),
            );
            mapper.set_trace_policy(self.trace_policy);
        }
    }

    fn apply_options_parsed_event(&mut self, args: OptionsParsedArgs) {
        let all_options: Vec<&str> = args
            .cmd_line
            .iter()
            .chain(&args.explicit_cmd_line)
            .map(String::as_str)
            .collect();
        if self.exec_log {
            if let Some(path) = extract_exec_log(&all_options, self.mapper.as_mut()) {
                self.summary.exec_log = Some(path.clone());
                if let Some(mapper) = &mut self.mapper {
                    mapper.on_exec_log_detected(path);
                }
            }
        }

        if let Some(mapper) = &mut self.mapper {
            mapper.on_options_parsed(&args.explicit_cmd_line);
            if let Some(tool_tag) = args.tool_tag.filter(|s| !s.is_empty()) {
                mapper.on_tool_tag(&tool_tag);
            }
        }
    }

    fn apply_action_completed_event(&mut self, ev: ActionCompletedEvent) {
        self.summary.actions += 1;
        if !ev.success {
            self.summary.failed_actions += 1;
        }
        if let Some(mapper) = &mut self.mapper {
            mapper.on_action_completed(ev);
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
                        host: (!s.host.is_empty()).then_some(s.host.clone()),
                        user: (!s.user.is_empty()).then_some(s.user.clone()),
                    });
                }
                Ok(())
            }
            BepId::OptionsParsed(_) => {
                if let Some(BepPayload::OptionsParsed(o)) = &event.payload {
                    self.apply_options_parsed_event(OptionsParsedArgs {
                        cmd_line: o.cmd_line.clone(),
                        explicit_cmd_line: o.explicit_cmd_line.clone(),
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
                    if let Some(mapper) = &mut self.mapper {
                        mapper.on_configuration(&c.id, &cfg.mnemonic);
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
                    let tags = c.tag.clone();
                    self.summary.targets += 1;
                    if !c.success {
                        self.summary.failed_targets += 1;
                    }
                    if let Some(mapper) = &mut self.mapper {
                        mapper.on_target_completed(
                            &label,
                            c.success,
                            &tags,
                            event_time_nanos,
                        );
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
                    self.apply_action_completed_event(ActionCompletedEvent {
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
            BepId::Fetch(_) => {
                if let Some(BepPayload::Fetch(p)) = &event.payload {
                    if let Some(mapper) = &mut self.mapper {
                        mapper.on_fetch(p.success);
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
            // TargetSummary duplicates target success; NamedSet output files
            // and BuildToolLogs are not exported; the unstructured command
            // line duplicates OptionsParsed's explicit one.
            BepId::TargetSummary(_)
            | BepId::NamedSet(_)
            | BepId::BuildToolLogs(_)
            | BepId::UnstructuredCommandLine(_)
            | BepId::StructuredCommandLine(_)
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
    fn cli_policy_budgets_successful_actions() {
        use crate::build_event_stream::build_event::Payload;
        use crate::build_event_stream::build_event_id::{ActionCompletedId, Id};
        use crate::build_event_stream::{ActionExecuted, BuildEvent, BuildEventId, BuildStarted};
        use crate::otel::span::{Sink, Tracer};
        use crate::otel::OtelMapper;
        use crate::state::{TracePolicy, TraceProfile};

        let ts = |seconds: i64| spawn_proto::timestamp_proto::google::protobuf::Timestamp {
            seconds,
            nanos: 0,
        };
        let action = |label: &str, success: bool, secs: i64| BuildEvent {
            id: Some(BuildEventId {
                id: Some(Id::ActionCompleted(ActionCompletedId {
                    label: label.into(),
                    ..Default::default()
                })),
            }),
            payload: Some(Payload::Action(ActionExecuted {
                success,
                r#type: "Genrule".into(),
                start_time: Some(ts(100)),
                end_time: Some(ts(100 + secs)),
                ..Default::default()
            })),
            ..Default::default()
        };
        let started = BuildEvent {
            id: Some(BuildEventId {
                id: Some(Id::Started(Default::default())),
            }),
            payload: Some(Payload::Started(BuildStarted {
                uuid: "u".into(),
                command: "build".into(),
                start_time: Some(ts(100)),
                ..Default::default()
            })),
            ..Default::default()
        };

        let (sink, spans) = Sink::memory();
        let mut router = super::EventRouter::new()
            .with_mapper(OtelMapper::new(Tracer::new(sink), None))
            .with_trace_policy(TracePolicy::new(TraceProfile::Lightweight, 500, 1));
        router.route_build_event(&started, None).unwrap();
        for ev in [
            action("//:fast", true, 1),
            action("//:slow", true, 5),
            action("//:broken", false, 1),
        ] {
            router.route_build_event(&ev, None).unwrap();
        }
        router.finish();

        assert_eq!(router.summary().actions, 3);
        let mut names: Vec<String> = spans
            .snapshot()
            .into_iter()
            .map(|s| s.name.to_string())
            .filter(|n| n.starts_with("action "))
            .collect();
        names.sort();
        assert_eq!(names, ["action Genrule //:broken", "action Genrule //:slow"]);
    }

    #[test]
    fn test_status_names() {
        assert_eq!(test_status_name(1), "PASSED");
        assert_eq!(test_status_name(0), "NO_STATUS");
        assert_eq!(test_status_name(999), "UNKNOWN");
    }
}
