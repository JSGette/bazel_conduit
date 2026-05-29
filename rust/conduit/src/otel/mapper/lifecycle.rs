use std::collections::HashMap;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use opentelemetry::trace::{SpanKind, Status, TraceContextExt, Tracer};
use opentelemetry::KeyValue;
use tracing::{debug, info, warn};

use super::{trace_context, ExecLogState, OtelMapper, TestResultEvent};
use super::super::attributes::*;

impl OtelMapper {
    /// BuildStarted -> create root span `bazel.invocation`.
    /// Prefers event_time_nanos (BES event time) when available so the trace reflects when the
    /// build was observed; otherwise uses start_time_nanos from the payload.
    pub fn on_build_started(
        &mut self,
        uuid: &str,
        command: &str,
        start_time_nanos: Option<i64>,
        event_time_nanos: Option<i64>,
    ) {
        self.cached_command = Some(command.to_string());

        let trace_id = trace_context::uuid_to_trace_id(uuid);
        let parent_cx = trace_context::make_root_context(trace_id);

        let span_name = format!("bazel {command}");
        let mut builder = self
            .tracer
            .span_builder(span_name)
            .with_kind(SpanKind::Internal)
            .with_attributes(vec![
                KeyValue::new(BAZEL_INVOCATION_ID, uuid.to_string()),
                KeyValue::new(BAZEL_COMMAND, command.to_string()),
            ]);

        // Sanity-check: if the BEP start time is more than 5 minutes
        // from wall clock, it's probably stale (long-lived Bazel daemon).
        // Fall back to wall clock to avoid multi-day trace durations.
        let now_nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as i64;
        let five_min_nanos: i64 = 5 * 60 * 1_000_000_000;

        let effective_start = event_time_nanos.or(start_time_nanos);
        let sanitized_start = match effective_start {
            Some(nanos) if (now_nanos - nanos).abs() > five_min_nanos => {
                warn!(
                    bep_start_nanos = nanos,
                    wall_clock_nanos = now_nanos,
                    drift_seconds = (now_nanos - nanos) / 1_000_000_000,
                    "BEP start time deviates from wall clock by >5 min; using wall clock"
                );
                Some(now_nanos)
            }
            other => other,
        };
        if let Some(nanos) = sanitized_start {
            builder = builder.with_start_time(super::nanos_to_system_time(nanos));
            self.root_span_start_nanos = Some(nanos);
        }
        let span = self.tracer.build_with_context(builder, &parent_cx);
        let cx = opentelemetry::Context::new().with_span(span);

        info!(trace_id = %trace_id, "Created root span for invocation {uuid}");
        self.root_context = Some(cx);

        // Replay any fetch events that arrived before the root span.
        if !self.pending_fetches.is_empty() {
            let buffered: Vec<_> = std::mem::take(&mut self.pending_fetches);
            info!("Replaying {} buffered fetch events", buffered.len());
            for (url, success, arrival, downloader) in buffered {
                self.emit_fetch_span(&url, success, arrival, downloader.as_deref());
            }
        }

        // Replay test results that arrived before the root span (e.g. BES stream order).
        if !self.pending_test_results.is_empty() {
            let buffered: Vec<_> = std::mem::take(&mut self.pending_test_results);
            info!("Replaying {} buffered test result events", buffered.len());
            for b in &buffered {
                self.on_test_result(&TestResultEvent {
                    label: &b.label,
                    status: b.status.as_deref(),
                    attempt: b.attempt,
                    run: b.run,
                    shard: b.shard,
                    cached: b.cached,
                    strategy: b.strategy.as_deref(),
                    start_time_nanos: b.start_time_nanos,
                    duration_nanos: b.duration_nanos,
                    hostname: b.hostname.as_deref(),
                    cached_remotely: b.cached_remotely,
                });
            }
        }
    }

    /// Extended BuildStarted fields -> set attributes on root span.
    pub fn on_build_started_extended(
        &mut self,
        workspace_dir: Option<&str>,
        working_dir: Option<&str>,
        build_tool_version: Option<&str>,
        server_pid: Option<i64>,
        host: Option<&str>,
        user: Option<&str>,
    ) {
        if self.root_context.is_none() {
            return;
        }
        if let Some(v) = workspace_dir {
            self.set_root_attr(KeyValue::new(BAZEL_WORKSPACE_DIR, v.to_string()));
            if !v.is_empty() {
                let path = PathBuf::from(v);
                if let Some(name) = super::detect_workspace_name(&path) {
                    self.set_root_attr(KeyValue::new(VCS_REPOSITORY_NAME, name));
                }
                self.workspace_directory = Some(path);
            }
        }
        // Retry a previously skipped relative exec-log path once workspace is known.
        if let Some(path) = self.pending_exec_log_path.take() {
            self.on_exec_log_detected(path);
        }
        if let Some(v) = working_dir {
            self.set_root_attr(KeyValue::new(BAZEL_WORKING_DIR, v.to_string()));
        }
        if let Some(v) = build_tool_version {
            self.set_root_attr(KeyValue::new(BAZEL_BUILD_TOOL_VERSION, v.to_string()));
        }
        if let Some(v) = server_pid {
            self.set_root_attr(KeyValue::new(BAZEL_SERVER_PID, v));
        }
        if let Some(v) = host {
            self.set_root_attr(KeyValue::new(BAZEL_WORKSPACE_HOST, v.to_string()));
        }
        if let Some(v) = user {
            self.set_root_attr(KeyValue::new(BAZEL_WORKSPACE_USER, v.to_string()));
        }
    }

    /// OptionsParsed -> add attributes to root span.
    pub fn on_options_parsed(
        &mut self,
        startup_options: &[String],
        explicit_cmd_line: &[String],
    ) {
        if !startup_options.is_empty() {
            let scrubbed = self.redactor.scrub_args(startup_options);
            self.set_root_attr(KeyValue::new(BAZEL_STARTUP_OPTIONS, scrubbed.join(" ")));
        }
        if !explicit_cmd_line.is_empty() {
            let scrubbed = self.redactor.scrub_args(explicit_cmd_line);
            self.set_root_attr(KeyValue::new(BAZEL_EXPLICIT_CMD_LINE, scrubbed.join(" ")));
        }
    }

    /// OptionsParsed -> extract tool_tag if present.
    pub fn on_tool_tag(&mut self, tool_tag: &str) {
        if !tool_tag.is_empty() {
            self.set_root_attr(KeyValue::new(BAZEL_TOOL_TAG, tool_tag.to_string()));
        }
    }

    /// ActionMode -> record the detected processing mode on the root span.
    pub fn on_action_mode(&mut self, mode: crate::state::ActionProcessingMode) {
        self.set_root_attr(KeyValue::new(BAZEL_ACTION_MODE, mode.to_string()));
    }

    /// Compact-exec-log path detected (typically from BEP `OptionsParsed`).
    /// Spawns the tail-follow worker; arriving [`SpawnExec`] entries are
    /// drained at every BEP-event boundary via [`Self::pump_compact_spawns`].
    ///
    /// Bazel resolves `--execution_log_compact_file=<rel>` against the client
    /// cwd (i.e. the workspace), not against conduit's cwd. We replicate that
    /// here so the tailer opens the same file Bazel writes.
    pub fn on_exec_log_detected(&mut self, path: PathBuf) {
        if self.compact_streaming_active {
            warn!("on_exec_log_detected called twice; ignoring second");
            return;
        }
        let resolved = if path.is_absolute() {
            path
        } else {
            match self.workspace_directory.as_ref() {
                Some(ws) => ws.join(&path),
                None => {
                    self.pending_exec_log_path = Some(path.clone());
                    warn!(
                        path = %path.display(),
                        "Compact exec log path is relative but workspace_directory is not set yet; skipping tailer"
                    );
                    self.add_root_event(
                        "bazel.exec_log.tailer_skipped",
                        vec![
                            KeyValue::new("path", path.display().to_string()),
                            KeyValue::new("reason", "workspace_directory_not_set"),
                        ],
                    );
                    return;
                }
            }
        };
        self.start_compact_tailer(resolved);
    }

    fn start_compact_tailer(&mut self, path: PathBuf) {
        let handle = crate::exec_log::tailer::spawn(
            path.clone(),
            self.exec_log_max_message_bytes,
            self.exec_log_max_decompressed_bytes,
        );
        info!(path = %path.display(), "Started compact exec log tailer");
        self.add_root_event(
            "bazel.exec_log.tailer_started",
            vec![KeyValue::new("path", path.display().to_string())],
        );
        self.exec_log_state = Some(ExecLogState { path, handle });
        self.compact_streaming_active = true;
    }

    /// `--execution_log_binary_file` / `--execution_log_json_file` observed
    /// in BEP `OptionsParsed`. Neither format is supported by conduit's
    /// live streaming pipeline (no dedup table, no zstd framing); record a
    /// root-span event so users can spot the misconfiguration in their
    /// trace UI without grepping the conduit log.
    pub fn on_unsupported_exec_log_flag(&mut self, flags: &str) {
        self.add_root_event(
            "bazel.exec_log.unsupported_format",
            vec![
                KeyValue::new("flags", flags.to_string()),
                KeyValue::new("supported", "--execution_log_compact_file".to_string()),
            ],
        );
    }

    /// WorkspaceStatus -> add workspace attributes to root span.
    /// BUILD_USER and BUILD_HOST are mapped to bazel.user/bazel.host directly
    /// (same semantic as the BuildStarted fields) to avoid duplication.
    ///
    /// `STABLE_*` keys produced by `--workspace_status_command` routinely
    /// carry CI-injected secrets (`STABLE_CI_JOB_TOKEN`, `STABLE_GIT_TOKEN`,
    /// etc.), so each value is run through the redactor's name-based
    /// scrubber before it lands on the root span.
    pub fn on_workspace_status(&mut self, items: &HashMap<String, String>) {
        for (key, value) in items {
            let attr_key = match key.as_str() {
                "BUILD_USER" => BAZEL_WORKSPACE_USER.to_string(),
                "BUILD_HOST" => BAZEL_WORKSPACE_HOST.to_string(),
                _ => format!("bazel.workspace.{}", key.to_lowercase()),
            };
            let scrubbed = self.redactor.scrub_value_by_name(key, value).into_owned();
            self.set_root_attr(KeyValue::new(attr_key, scrubbed));
        }
    }

    /// UnstructuredCommandLine -> add full command line as attribute.
    pub fn on_command_line(&mut self, args: &[String]) {
        if !args.is_empty() {
            let scrubbed = self.redactor.scrub_args(args);
            self.set_root_attr(KeyValue::new(BAZEL_COMMAND_LINE, scrubbed.join(" ")));
        }
    }

    /// Pattern expanded -> add build patterns as attribute and enrich root span name.
    pub fn on_pattern(&mut self, patterns: &[String]) {
        if !patterns.is_empty() {
            self.cached_patterns = patterns.to_vec();
            self.set_root_attr(KeyValue::new(BAZEL_PATTERNS, patterns.join(", ")));
            if let (Some(cmd), Some(cx)) = (&self.cached_command, &self.root_context) {
                cx.span().update_name(format!("bazel {} {}", cmd, patterns.join(" ")));
            }
        }
    }

    /// PatternSkipped -> add skipped patterns as span event on root.
    pub fn on_pattern_skipped(&mut self, patterns: &[String]) {
        if !patterns.is_empty() {
            self.add_root_event(
                "pattern_skipped",
                vec![KeyValue::new(BAZEL_PATTERNS, patterns.join(", "))],
            );
        }
    }

    /// UnconfiguredLabel / ConfiguredLabel with Aborted payload ->
    /// record as a span under the "skipped" parent (root-cause failure).
    pub fn on_aborted_label(
        &mut self,
        label: &str,
        reason: Option<&str>,
        description: Option<&str>,
        event_time_nanos: Option<i64>,
    ) {
        Self::ensure_group_span(
            &mut self.skipped_context,
            &self.root_context,
            &self.tracer,
            "skipped targets",
        );

        let Some(parent) = &self.skipped_context else {
            return;
        };

        let short = shorten_label(label);
        let mut attrs = vec![
            KeyValue::new(BAZEL_TARGET_LABEL, label.to_string()),
            KeyValue::new(BAZEL_TARGET_LABEL_SHORT, short.to_string()),
            KeyValue::new(BAZEL_TARGET_SUCCESS, false),
        ];
        if let Some(r) = reason {
            attrs.push(KeyValue::new(BAZEL_TARGET_ABORT_REASON, r.to_string()));
        }
        if let Some(d) = description {
            attrs.push(KeyValue::new(BAZEL_TARGET_ABORT_DESCRIPTION, d.to_string()));
        }

        self.build_and_end_child_span(
            parent,
            format!("target {short}"),
            SpanKind::Internal,
            attrs,
            Status::Error {
                description: std::borrow::Cow::Owned(description.unwrap_or("aborted").to_string()),
            },
            None,
            event_time_nanos,
        );
        debug!("Created aborted-label span for {label}");
    }

    /// Configuration -> span event on root span. Also caches mnemonic for action enrichment.
    pub fn on_configuration(
        &mut self,
        config_id: &str,
        mnemonic: Option<&str>,
        platform: Option<&str>,
    ) {
        if let Some(m) = mnemonic {
            self.configurations
                .insert(config_id.to_string(), m.to_string());
        }
        let mut attrs = vec![KeyValue::new(BAZEL_CONFIG_ID, config_id.to_string())];
        if let Some(m) = mnemonic {
            attrs.push(KeyValue::new(BAZEL_CONFIG_MNEMONIC, m.to_string()));
        }
        if let Some(p) = platform {
            attrs.push(KeyValue::new(BAZEL_CONFIG_PLATFORM, p.to_string()));
        }
        self.add_root_event("configuration", attrs);
        debug!("Added configuration event: {config_id}");
    }

    /// Configuration with cpu/is_tool -> add to root span event.
    pub fn on_configuration_extended(
        &mut self,
        config_id: &str,
        cpu: Option<&str>,
        is_tool: Option<bool>,
    ) {
        let mut attrs = vec![KeyValue::new(BAZEL_CONFIG_ID, config_id.to_string())];
        if let Some(c) = cpu {
            attrs.push(KeyValue::new(BAZEL_CONFIG_CPU, c.to_string()));
        }
        if let Some(t) = is_tool {
            attrs.push(KeyValue::new(BAZEL_CONFIG_IS_TOOL, t));
        }
        self.add_root_event("configuration_detail", attrs);
    }
}
