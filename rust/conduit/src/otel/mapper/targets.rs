use std::borrow::Cow;
use std::collections::HashMap;
use std::time::SystemTime;

use opentelemetry::trace::{SpanKind, Status, TraceContextExt, Tracer};
use opentelemetry::KeyValue;
use tracing::{debug, warn};

use super::super::attributes::*;
use super::{bytestream_uri_to_display, ConfiguredTarget, NamedSetEntry, OtelMapper};

impl OtelMapper {
    /// TargetConfigured -> store metadata for deferred span creation.
    ///
    /// The actual span is created later in [`on_target_completed`] (or
    /// [`on_target_skipped`]) so we can choose the correct parent.
    /// If an action arrives before the target completes, the span is
    /// created lazily under root via [`target_parent_context`].
    pub fn on_target_configured(
        &mut self,
        label: &str,
        kind: Option<&str>,
        tags: &[String],
        event_time_nanos: Option<i64>,
        test_size: Option<&str>,
    ) {
        if self.root_context.is_none() {
            warn!("TargetConfigured before BuildStarted for {label}");
            return;
        }
        self.closed_targets.remove(label);

        self.configured_targets.insert(
            label.to_string(),
            ConfiguredTarget {
                kind: kind.map(String::from),
                tags: tags.to_vec(),
                event_time_nanos,
                test_size: test_size.filter(|s| !s.is_empty()).map(String::from),
            },
        );

        debug!("Stored configured target metadata for {label}");
    }

    /// Create a target span under a given parent context.
    /// When `start_nanos_override` is set (e.g. earliest action start), use it
    /// instead of configured event time so target duration reflects action timing.
    fn create_target_span(
        &self,
        label: &str,
        configured: &ConfiguredTarget,
        parent: &opentelemetry::Context,
        start_nanos_override: Option<i64>,
    ) -> opentelemetry::Context {
        let short = shorten_label(label);
        let mut attrs = vec![
            KeyValue::new(BAZEL_TARGET_LABEL, label.to_string()),
            KeyValue::new(BAZEL_TARGET_LABEL_SHORT, short.to_string()),
        ];
        if let Some(k) = &configured.kind {
            attrs.push(KeyValue::new(BAZEL_TARGET_KIND, k.clone()));
        }
        if !configured.tags.is_empty() {
            attrs.push(KeyValue::new(
                BAZEL_TARGET_TAGS,
                configured.tags.join(", "),
            ));
        }
        if let Some(ts) = &configured.test_size {
            attrs.push(KeyValue::new(BAZEL_TARGET_TEST_SIZE, ts.clone()));
        }

        let raw_start = start_nanos_override.or(configured.event_time_nanos);
        let (start_nanos, _) = self.clamp_to_invocation(raw_start, None);
        let mut builder = self
            .tracer
            .span_builder(format!("target {short}"))
            .with_kind(SpanKind::Internal)
            .with_attributes(attrs);

        if let Some(nanos) = start_nanos {
            builder = builder.with_start_time(super::nanos_to_system_time(nanos));
        }

        let span = self.tracer.build_with_context(builder, parent);
        opentelemetry::Context::new().with_span(span)
    }

    /// Resolve the parent context to use for action/test child spans.
    ///
    /// Lifecycle targets (`TargetConfigured` seen and not yet terminal) are
    /// kept in `target_contexts`. Synthetic targets are also persisted so
    /// action/test children share a stable target parent in exporters.
    pub(super) fn target_parent_context(
        &mut self,
        label: &str,
        start_nanos: Option<i64>,
    ) -> Option<opentelemetry::Context> {
        if let Some(existing) = self.target_contexts.get(label) {
            return Some(existing.clone());
        }
        let parent = self.choose_parent_for_label(label)?;
        if self.closed_targets.contains(label) {
            let cx = self.create_synthetic_target_span(label, &parent, start_nanos);
            self.synthetic_target_labels.insert(label.to_string());
            self.target_contexts.insert(label.to_string(), cx.clone());
            return Some(cx);
        }
        if let Some(configured) = self.configured_targets.get(label) {
            let cx = self.create_target_span(label, configured, &parent, start_nanos);
            debug!("Lazily created target span for {label} (action needed parent)");
            self.target_contexts.insert(label.to_string(), cx.clone());
            return Some(cx);
        }
        let cx = self.create_synthetic_target_span(label, &parent, start_nanos);
        self.synthetic_target_labels.insert(label.to_string());
        self.target_contexts.insert(label.to_string(), cx.clone());
        Some(cx)
    }

    /// Returns true if a label looks like an external dependency.
    fn is_external_label(label: &str) -> bool {
        label.starts_with("@@") || label.contains("+_repo_rules+")
    }

    /// Choose the correct parent for a target: root for local targets,
    /// `external deps` group span for external dependencies.
    fn choose_parent_for_label(&mut self, label: &str) -> Option<opentelemetry::Context> {
        if Self::is_external_label(label) {
            self.ensure_external_deps_span();
            self.external_deps_context.clone()
        } else {
            self.root_context.clone()
        }
    }

    /// Lazily create the `external deps` parent span.
    fn ensure_external_deps_span(&mut self) {
        Self::ensure_group_span(
            &mut self.external_deps_context,
            &self.root_context,
            &self.tracer,
            "external deps",
        );
    }

    /// Create a target span with only the label (no kind/tags). Used when we see
    /// an action with a label but no prior TargetConfigured event.
    fn create_synthetic_target_span(
        &self,
        label: &str,
        parent: &opentelemetry::Context,
        start_nanos: Option<i64>,
    ) -> opentelemetry::Context {
        let short = shorten_label(label);
        let attrs = vec![
            KeyValue::new(BAZEL_TARGET_LABEL, label.to_string()),
            KeyValue::new(BAZEL_TARGET_LABEL_SHORT, short.to_string()),
            KeyValue::new(BAZEL_TARGET_SYNTHETIC, true),
        ];
        let mut builder = self
            .tracer
            .span_builder(format!("target {short}"))
            .with_kind(SpanKind::Internal)
            .with_attributes(attrs);
        let (clamped_start, _) = self.clamp_to_invocation(start_nanos, None);
        if let Some(nanos) = clamped_start {
            builder = builder.with_start_time(super::nanos_to_system_time(nanos));
        }
        let span = self.tracer.build_with_context(builder, parent);
        opentelemetry::Context::new().with_span(span)
    }

    /// TargetCompleted -> create (or reuse) the target span, set attributes,
    /// resolve output file sets from whatever's currently in the
    /// `NamedSet` cache (`resolve_files` skips missing IDs), and end the
    /// span synchronously.
    pub fn on_target_completed(
        &mut self,
        label: &str,
        success: bool,
        file_set_ids: &[String],
        tags: &[String],
        event_time_nanos: Option<i64>,
    ) {
        // Drain any spawns that flushed between the last ActionExecuted and
        // this TargetCompleted; they need to land on the action span before
        // `end_open_actions_for_target` seals it.
        self.pump_compact_spawns();

        // Clamp end-of-target to the invocation window. Cached targets re-emit
        // their original action timestamps, so without this a span ending at
        // event_time will run from the cached `start` (weeks ago) to today.
        let (_, effective_end) = self.clamp_to_invocation(None, event_time_nanos);
        // Consume configured metadata up front regardless of whether the span
        // was already created lazily from an action/test event.
        let configured = self.configured_targets.remove(label);

        // Get or create the span under the appropriate parent.
        let cx = if let Some(cx) = self.target_contexts.remove(label) {
            cx
        } else if let Some(configured) = configured {
            let parent = self.choose_parent_for_label(label);
            if let Some(parent) = parent {
                let cx = self.create_target_span(label, &configured, &parent, None);
                debug!("Created target span for {label} at completion time");
                cx
            } else {
                warn!("TargetCompleted before BuildStarted for {label}");
                return;
            }
        } else {
            warn!("TargetCompleted for unknown target {label}");
            return;
        };

        cx.span()
            .set_attribute(KeyValue::new(BAZEL_TARGET_SUCCESS, success));

        let is_cached = !self
            .target_has_non_cached_action
            .get(label)
            .copied()
            .unwrap_or(false);
        cx.span()
            .set_attribute(KeyValue::new(BAZEL_TARGET_CACHED, is_cached));

        if !tags.is_empty() {
            cx.span()
                .set_attribute(KeyValue::new(BAZEL_TARGET_TAGS, tags.join(", ")));
        }

        let status = if success {
            Status::Ok
        } else {
            Status::Error {
                description: Cow::Borrowed("target failed"),
            }
        };
        cx.span().set_status(status);

        // End any action spans we held open for this target. Deterministic
        // boundary - Bazel guarantees no further ActionExecuted events for
        // this target. Anything still open without a matching SpawnExec at
        // this point picks up `bazel.action.spawn.missing=true`.
        self.end_open_actions_for_target(label, effective_end);

        Self::set_output_attributes_and_end_target_span(
            &self.named_set_cache,
            &cx,
            file_set_ids,
            effective_end,
        );
        self.synthetic_target_labels.remove(label);
        self.closed_targets.insert(label.to_string());
        debug!("Ended target span for {label} (success={success})");
    }

    /// TargetCompleted with `aborted` payload -> create a brief span under
    /// the "skipped" parent and end it immediately.
    pub fn on_target_skipped(
        &mut self,
        label: &str,
        reason: Option<&str>,
        description: Option<&str>,
        event_time_nanos: Option<i64>,
    ) {
        // Consume the configured metadata (if any).
        let configured = self.configured_targets.remove(label);
        // End any open span from target_contexts to avoid leaks.
        if let Some(existing) = self.target_contexts.remove(label) {
            existing.span().set_status(Status::Unset);
            existing.span().end();
            debug!("Ended leaked target span for {label} on skip");
        }

        Self::ensure_group_span(
            &mut self.skipped_context,
            &self.root_context,
            &self.tracer,
            "skipped targets",
        );

        let parent = match &self.skipped_context {
            Some(cx) => cx,
            None => return,
        };

        let short = shorten_label(label);
        let mut attrs = vec![
            KeyValue::new(BAZEL_TARGET_LABEL, label.to_string()),
            KeyValue::new(BAZEL_TARGET_LABEL_SHORT, short.to_string()),
            KeyValue::new(BAZEL_TARGET_SUCCESS, false),
        ];
        if let Some(configured) = &configured {
            if let Some(k) = &configured.kind {
                attrs.push(KeyValue::new(BAZEL_TARGET_KIND, k.clone()));
            }
        }
        if let Some(r) = reason {
            attrs.push(KeyValue::new(BAZEL_TARGET_ABORT_REASON, r.to_string()));
        }
        if let Some(d) = description {
            attrs.push(KeyValue::new(BAZEL_TARGET_ABORT_DESCRIPTION, d.to_string()));
        }

        let (_, clamped_event) = self.clamp_to_invocation(None, event_time_nanos);
        let instant = clamped_event
            .map(super::nanos_to_system_time)
            .unwrap_or_else(SystemTime::now);

        let builder = self
            .tracer
            .span_builder(format!("target {short}"))
            .with_kind(SpanKind::Internal)
            .with_attributes(attrs)
            .with_start_time(instant);

        let span = self.tracer.build_with_context(builder, parent);
        let cx = opentelemetry::Context::new().with_span(span);

        cx.span().set_status(Status::Unset);
        cx.span().end_with_timestamp(instant);
        self.synthetic_target_labels.remove(label);
        self.closed_targets.insert(label.to_string());

        debug!("Created and ended skipped target span for {label}");
    }

    /// Set output file attributes on the target span and end it.
    fn set_output_attributes_and_end_target_span(
        cache: &HashMap<String, NamedSetEntry>,
        cx: &opentelemetry::Context,
        file_set_ids: &[String],
        event_time_nanos: Option<i64>,
    ) {
        let all_files = Self::resolve_files(cache, file_set_ids);

        if !all_files.is_empty() {
            let display_files: Vec<String> = all_files
                .iter()
                .map(|s| bytestream_uri_to_display(s).into_owned())
                .collect();
            let file_list = if display_files.len() <= 20 {
                display_files.join(", ")
            } else {
                format!(
                    "{} ... and {} more",
                    display_files[..20].join(", "),
                    display_files.len() - 20
                )
            };
            cx.span()
                .set_attribute(KeyValue::new(BAZEL_NAMED_SET_FILE_COUNT, all_files.len() as i64));
            cx.span()
                .set_attribute(KeyValue::new(BAZEL_TARGET_OUTPUT_FILES, file_list));
        }

        if let Some(nanos) = event_time_nanos {
            cx.span()
                .end_with_timestamp(super::nanos_to_system_time(nanos));
        } else {
            cx.span().end();
        }
    }

    /// Recursively collect all files reachable from the given NamedSet IDs.
    /// IDs missing from the cache are silently skipped.
    fn resolve_files(cache: &HashMap<String, NamedSetEntry>, set_ids: &[String]) -> Vec<String> {
        let mut files = Vec::new();
        let mut stack: Vec<&str> = set_ids.iter().map(String::as_str).collect();
        let mut visited = std::collections::HashSet::new();
        while let Some(id) = stack.pop() {
            if !visited.insert(id) {
                continue;
            }
            if let Some(entry) = cache.get(id) {
                files.extend(entry.files.iter().cloned());
                for child in &entry.child_set_ids {
                    stack.push(child.as_str());
                }
            }
        }
        files
    }

    /// Cache a NamedSet so its files can be resolved when targets complete.
    ///
    /// A NamedSet can contain direct `files` AND references to other
    /// NamedSets (`child_set_ids`); both are stored so [`resolve_files`]
    /// can collect the transitive closure.
    pub fn on_named_set(&mut self, set_id: &str, files: Vec<String>, child_set_ids: &[String]) {
        self.named_set_cache.insert(
            set_id.to_string(),
            NamedSetEntry {
                files,
                child_set_ids: child_set_ids.to_vec(),
            },
        );
    }
}
