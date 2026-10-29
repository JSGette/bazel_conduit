// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

use crate::otel::span::{Context, KeyValue, SpanKind, Status};
use std::borrow::Cow;
use std::time::SystemTime;

use tracing::{debug, warn};

use super::super::attributes::*;
use super::{ConfiguredTarget, DeferredTarget, OtelMapper};

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
        &mut self,
        label: &str,
        configured: &ConfiguredTarget,
        parent: &Context,
        start_nanos_override: Option<i64>,
    ) -> Context {
        let short = shorten_label(label);
        let mut attrs = vec![KeyValue::new(BAZEL_TARGET_LABEL, label.to_string())];
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
        // Always anchor at a real timestamp (fall back to invocation start) so
        // untimed child actions can be kept inside the target window instead of
        // racing a wall-clock creation time.
        let start_nanos = self
            .clamp_to_invocation(raw_start, None)
            .0
            .or(self.root_span_start_nanos);
        self.record_target_start(label, start_nanos);
        let mut builder = self
            .tracer
            .span_builder(format!("target {short}"))
            .with_kind(SpanKind::Internal)
            .with_attributes(attrs);

        if let Some(nanos) = start_nanos {
            builder = builder.with_start_time(super::nanos_to_system_time(nanos));
        }

        let span = self.tracer.build_with_context(builder, parent);
        Context::new().with_span(span)
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
    ) -> Option<Context> {
        if let Some(existing) = self.target_contexts.get(label) {
            return Some(existing.clone());
        }
        let parent = self.root_context.clone()?;
        if self.closed_targets.contains(label) {
            let cx = self.create_synthetic_target_span(label, &parent, start_nanos);
            self.synthetic_target_labels.insert(label.to_string());
            self.target_contexts.insert(label.to_string(), cx.clone());
            return Some(cx);
        }
        if let Some(configured) = self.configured_targets.get(label).cloned() {
            let cx = self.create_target_span(label, &configured, &parent, start_nanos);
            debug!("Lazily created target span for {label} (action needed parent)");
            self.target_contexts.insert(label.to_string(), cx.clone());
            return Some(cx);
        }
        let cx = self.create_synthetic_target_span(label, &parent, start_nanos);
        self.synthetic_target_labels.insert(label.to_string());
        self.target_contexts.insert(label.to_string(), cx.clone());
        Some(cx)
    }

    pub(super) fn record_target_child_end(&mut self, label: &str, end_nanos: Option<i64>) {
        // Clamp to the invocation window: cache hits can carry spawn/action
        // ends beyond it, which would push the synthetic target span past root
        // and its own children.
        let (_, end_nanos) = self.clamp_to_invocation(None, end_nanos);
        let Some(end_nanos) = end_nanos else {
            return;
        };
        self.target_child_end_nanos
            .entry(label.to_string())
            .and_modify(|current| *current = (*current).max(end_nanos))
            .or_insert(end_nanos);
    }

    /// Create a target span with only the label (no kind/tags). Used when we see
    /// an action with a label but no prior TargetConfigured event.
    fn create_synthetic_target_span(
        &mut self,
        label: &str,
        parent: &Context,
        start_nanos: Option<i64>,
    ) -> Context {
        let short = shorten_label(label);
        let attrs = vec![
            KeyValue::new(BAZEL_TARGET_LABEL, label.to_string()),
            KeyValue::new(BAZEL_TARGET_SYNTHETIC, true),
        ];
        let mut builder = self
            .tracer
            .span_builder(format!("target {short}"))
            .with_kind(SpanKind::Internal)
            .with_attributes(attrs);
        let clamped_start = self
            .clamp_to_invocation(start_nanos, None)
            .0
            .or(self.root_span_start_nanos);
        self.record_target_start(label, clamped_start);
        if let Some(nanos) = clamped_start {
            builder = builder.with_start_time(super::nanos_to_system_time(nanos));
        }
        let span = self.tracer.build_with_context(builder, parent);
        Context::new().with_span(span)
    }

    /// TargetCompleted. Targets complete at [`OtelMapper::finish`]: tests
    /// (and their summary) arrive after `TargetCompleted`, and the lightweight
    /// action budget is only settled there.
    pub fn on_target_completed(
        &mut self,
        label: &str,
        success: bool,
        tags: &[String],
        event_time_nanos: Option<i64>,
    ) {
        self.deferred_targets
            .entry(label.to_string())
            .and_modify(|t| {
                t.success &= success;
                t.event_time_nanos = t.event_time_nanos.max(event_time_nanos);
            })
            .or_insert_with(|| DeferredTarget {
                label: label.to_string(),
                success,
                tags: tags.to_vec(),
                event_time_nanos,
            });
    }

    pub(super) fn complete_target(
        &mut self,
        label: &str,
        success: bool,
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
        // Completion is deferred to finish, so tests and actions reported after
        // TargetCompleted can end later than it.
        let effective_end = match (effective_end, self.target_child_end_nanos.get(label).copied()) {
            (Some(end), Some(child_end)) => Some(end.max(child_end)),
            (end, child_end) => end.or(child_end),
        };
        let (_, effective_end) = self.clamp_to_invocation(None, effective_end);
        // Consume configured metadata up front regardless of whether the span
        // was already created lazily from an action/test event.
        let configured = self.configured_targets.remove(label);

        // Lightweight profile: a successful target whose span was never opened
        // by a retained child (failed/slow/miss action or a test) carries no
        // diagnostic signal, so drop it. `target_contexts` only holds a target
        // once a retained child lazily created it, so its presence is our
        // "has an interesting child" test. Failed targets always survive.
        if success
            && !self.trace_policy.is_full()
            && !self.target_contexts.contains_key(label)
        {
            self.synthetic_target_labels.remove(label);
            self.target_child_end_nanos.remove(label);
            self.closed_targets.insert(label.to_string());
            self.suppressed_targets = self.suppressed_targets.saturating_add(1);
            debug!("Suppressed empty successful target span for {label} (lightweight)");
            return;
        }

        // Get or create the span under the appropriate parent.
        let cx = if let Some(cx) = self.target_contexts.remove(label) {
            cx
        } else if let Some(configured) = configured {
            let parent = self.root_context.clone();
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
        // this target.
        self.end_open_actions_for_target(label, effective_end);

        match effective_end {
            Some(nanos) => cx.span().end_with_timestamp(super::nanos_to_system_time(nanos)),
            None => cx.span().end(),
        }
        self.synthetic_target_labels.remove(label);
        self.target_child_end_nanos.remove(label);
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
            self.root_span_start_nanos,
        );

        let parent = match &self.skipped_context {
            Some(cx) => cx,
            None => return,
        };

        let short = shorten_label(label);
        let mut attrs = vec![
            KeyValue::new(BAZEL_TARGET_LABEL, label.to_string()),
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
        let cx = Context::new().with_span(span);

        cx.span().set_status(Status::Unset);
        cx.span().end_with_timestamp(instant);
        self.synthetic_target_labels.remove(label);
        self.target_child_end_nanos.remove(label);
        self.closed_targets.insert(label.to_string());

        debug!("Created and ended skipped target span for {label}");
    }
}
