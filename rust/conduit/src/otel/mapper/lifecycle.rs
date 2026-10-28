// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

use std::collections::HashMap;
use std::path::PathBuf;
use tracing::{debug, info, warn};

use super::super::attributes::*;
use super::{ExecLogState, OtelMapper};
use crate::otel::span::{self, Context, KeyValue, SpanContext, SpanId, SpanKind, Status};

impl OtelMapper {
    /// BuildStarted -> create root span `bazel.invocation`, starting at the
    /// command start time (falls back to the BES event time).
    pub fn on_build_started(
        &mut self,
        uuid: &str,
        command: &str,
        start_time_nanos: Option<i64>,
        event_time_nanos: Option<i64>,
    ) {
        self.cached_command = Some(command.to_string());
        self.build_start_nanos = start_time_nanos.or(event_time_nanos);

        let trace_id = span::uuid_to_trace_id(uuid);
        let parent_cx =
            Context::new().with_remote_span_context(SpanContext::new(trace_id, SpanId::INVALID));

        let span_name = format!("bazel {command}");
        let mut builder = self
            .tracer
            .span_builder(span_name)
            .with_kind(SpanKind::Internal)
            .with_attributes(vec![
                KeyValue::new(BAZEL_INVOCATION_ID, uuid.to_string()),
                KeyValue::new(BAZEL_COMMAND, command.to_string()),
            ]);

        if let Some(nanos) = start_time_nanos.or(event_time_nanos) {
            builder = builder.with_start_time(super::nanos_to_system_time(nanos));
            self.root_span_start_nanos = Some(nanos);
        }
        let span = self.tracer.build_with_context(builder, &parent_cx);
        let cx = Context::new().with_span(span);

        info!(trace_id = %trace_id, "Created root span for invocation {uuid}");
        self.root_context = Some(cx);
    }

    /// Extended BuildStarted fields -> set attributes on root span.
    pub fn on_build_started_extended(
        &mut self,
        workspace_dir: Option<&str>,
        working_dir: Option<&str>,
        build_tool_version: Option<&str>,
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
        if let Some(v) = working_dir {
            self.set_root_attr(KeyValue::new(BAZEL_WORKING_DIR, v.to_string()));
        }
        if let Some(v) = build_tool_version {
            self.set_root_attr(KeyValue::new(BAZEL_BUILD_TOOL_VERSION, v.to_string()));
        }
        if let Some(v) = host {
            self.set_root_attr(KeyValue::new(BAZEL_WORKSPACE_HOST, v.to_string()));
        }
        if let Some(v) = user {
            self.set_root_attr(KeyValue::new(BAZEL_WORKSPACE_USER, v.to_string()));
        }
    }

    /// OptionsParsed -> the explicit command line on the root span.
    pub fn on_options_parsed(&mut self, explicit_cmd_line: &[String]) {
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
                    warn!(
                        path = %path.display(),
                        "Compact exec log path is relative but workspace_directory is unknown; skipping tailer"
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
            self.build_start_nanos.map(super::nanos_to_system_time),
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
            self.root_span_start_nanos,
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

    /// Configuration -> cache its mnemonic (e.g. `k8-fastbuild`) for actions.
    pub fn on_configuration(&mut self, config_id: &str, mnemonic: &str) {
        if !mnemonic.is_empty() {
            self.configurations
                .insert(config_id.to_string(), mnemonic.to_string());
        }
    }
}
