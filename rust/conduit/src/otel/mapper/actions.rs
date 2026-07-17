use opentelemetry::trace::{Span, SpanKind};
use opentelemetry::KeyValue;
use spawn_proto::tools::protos::SpawnExec;
use tracing::{debug, warn};

use super::super::attributes::*;
use super::{action_status, build_action_attrs, build_action_span_name};
use super::{clamp_time_range, normalize_label, ActionCompletedEvent, ActionSpanInfo, ActionSpanKey, OpenAction, OtelMapper};

impl OtelMapper {
    /// ActionCompleted -> create `action {mnemonic} {label}` span. With the
    /// compact exec log enabled the span is kept open so arriving spawns can
    /// backfill attrs onto it; without it the span ends here as today.
    ///
    /// In **lightweight** mode only failed actions reach this method.
    /// In **full** mode every action (success or failure) is mapped.
    ///
    /// When `start_time_nanos` / `end_time_nanos` are available (from the
    /// `ActionExecuted.start_time` / `end_time` proto fields) they are used
    /// for accurate span timing.
    pub fn on_action_completed(&mut self, ev: &ActionCompletedEvent<'_>) {
        // Drain whatever the tailer has produced so far. Cheap; runs the
        // backfill against actions that already landed in the cache.
        self.pump_compact_spawns();

        let parent = match ev.label {
            Some(label) => self
                .target_parent_context(label, ev.start_time_nanos)
                .or_else(|| self.root_context.clone()),
            None => self.root_context.clone(),
        };
        let Some(parent) = parent else {
            warn!("ActionCompleted with no parent context");
            return;
        };

        // Track action for cached-target detection.
        if let Some(l) = ev.label {
            *self.target_action_counts.entry(l.to_string()).or_insert(0) += 1;
            if ev.cached == Some(false) {
                self.target_has_non_cached_action.insert(l.to_string(), true);
            }
        }

        let span_name = build_action_span_name(ev);
        let attrs = build_action_attrs(ev, &self.configurations, &self.redactor);
        let status = action_status(ev.success);

        // Clamp action timing to root span bounds so cached-action timestamps
        // (which reflect original remote execution time) don't escape the build.
        let (clamped_start, clamped_end) = clamp_time_range(
            ev.start_time_nanos,
            ev.end_time_nanos,
            self.root_span_start_nanos,
            self.root_span_end_from_wall_nanos.or(self.finish_time_nanos),
        );

        let key = ActionSpanKey::new(ev.label, ev.mnemonic, ev.primary_output);
        let target_label = ev.label.map(str::to_string).unwrap_or_default();
        let defer_end = self.compact_streaming_active;

        let mut span =
            self.build_child_span(&parent, span_name, SpanKind::Internal, attrs, clamped_start);
        span.set_status(status);
        let span_context = span.span_context().clone();

        let info = ActionSpanInfo {
            span_context,
            start_nanos: clamped_start,
            end_nanos: clamped_end,
        };

        let open = if defer_end {
            OpenAction {
                info,
                span: Some(span),
                target_label,
                spawn_count: 0,
            }
        } else {
            if let Some(nanos) = clamped_end {
                span.end_with_timestamp(super::nanos_to_system_time(nanos));
            } else {
                span.end();
            }
            OpenAction {
                info,
                span: None,
                target_label,
                spawn_count: 0,
            }
        };
        self.action_span_cache.insert(key.clone(), open);

        // Re-check pending spawns for any spawn that the just-inserted action
        // is the parent of. Order: a spawn that arrived first via the tailer,
        // then its ActionExecuted via BEP - flush the buffered spawn now so
        // it backfills attrs on the freshly-built span.
        if defer_end {
            self.flush_pending_spawns_for_action(&key);
        }

        debug!(
            success = ev.success,
            label = ?ev.label,
            defer_end,
            "Recorded action span",
        );
    }

    /// Drain everything the compact-log tailer has produced through the mpsc.
    /// Each [`SpawnExec`] either finds its parent action in
    /// [`Self::action_span_cache`] (backfill + emit child span) or lands in
    /// [`Self::pending_spawns`] to be retried by
    /// [`Self::flush_pending_spawns_for_action`] when the parent action
    /// arrives, or finally synthesised at [`Self::finish`].
    ///
    /// Non-blocking: called from BEP event handlers and the BES routing
    /// worker's periodic pump. The tailer's mpsc buffer absorbs bursts.
    pub(crate) fn pump_compact_spawns(&mut self) {
        let mut drained = Vec::new();
        if let Some(state) = self.exec_log_state.as_mut() {
            while let Ok(spawn) = state.handle.rx.try_recv() {
                drained.push(spawn);
            }
        }
        for spawn in drained {
            self.on_compact_spawn(spawn);
        }
    }

    /// One [`SpawnExec`] from the tailer: match it against an open action,
    /// emit the child spawn span, and (on the action's first spawn only)
    /// backfill curated attrs onto the parent. Unmatched spawns are queued by
    /// action key for targeted flush once their ActionExecuted arrives.
    pub(crate) fn on_compact_spawn(&mut self, spawn: SpawnExec) {
        self.compact_spawns_received = self.compact_spawns_received.saturating_add(1);
        // Exact-match path. Fuzzy matching (output-dir / suffix) is
        // deliberately not run live - multi-spawn actions confuse fuzzy
        // matching, and we'd rather emit a child span under a synthesised
        // parent at finish() than misattribute attrs onto a sibling action.
        let key = crate::exec_log::spawn_exec_candidate_keys(&spawn)
            .into_iter()
            .find(|k| self.action_span_cache.contains_key(k));
        let Some(key) = key else {
            self.push_pending_spawn(spawn);
            return;
        };
        self.apply_spawn_to_action(&key, spawn);
    }

    /// Apply a single spawn to a known-present action cache entry: bump the
    /// counter, backfill attrs on the first one, emit the child span.
    fn apply_spawn_to_action(&mut self, key: &ActionSpanKey, spawn: SpawnExec) {
        let (parent_info, is_first) = {
            let action = self
                .action_span_cache
                .get_mut(key)
                .expect("apply_spawn_to_action called with key not in cache");
            let is_first = action.spawn_count == 0;
            if is_first {
                if let Some(span) = action.span.as_mut() {
                    crate::exec_log::apply_spawn_attrs_to_action(span, &spawn);
                }
            }
            action.spawn_count = action.spawn_count.saturating_add(1);
            (action.info.clone(), is_first)
        };
        debug!(
            label = %key.target_label,
            mnemonic = %key.mnemonic,
            first = is_first,
            "Applied compact spawn to action span",
        );
        crate::exec_log::emit_spawn_span(&self.tracer, &parent_info, &spawn, &self.redactor);
    }

    fn push_pending_spawn(&mut self, spawn: SpawnExec) {
        let key = crate::exec_log::spawn_exec_candidate_keys(&spawn)
            .into_iter()
            .next()
            .unwrap_or_else(|| {
                ActionSpanKey::new(
                    Some(spawn.target_label.as_str()),
                    Some(spawn.mnemonic.as_str()),
                    Some(""),
                )
            });
        self.pending_spawns.entry(key).or_default().push(spawn);
    }

    /// Move buffered spawns for `key` from [`Self::pending_spawns`] into the
    /// action cache once ActionExecuted has arrived.
    fn flush_pending_spawns_for_action(&mut self, key: &ActionSpanKey) {
        let Some(matched) = self.pending_spawns.remove(key) else {
            return;
        };
        for spawn in matched {
            self.apply_spawn_to_action(key, spawn);
        }
    }

    /// Record an end timestamp hint for every still-open action span belonging
    /// to `target_label`. The action spans stay open until `finish()` so late
    /// tailer arrivals can still backfill attrs before finalisation.
    ///
    /// `fallback_end_nanos` is used when the action's own clamped end is
    /// `None` (an action completed without timing data) - typically the
    /// target's effective end time.
    pub(super) fn end_open_actions_for_target(
        &mut self,
        target_label: &str,
        fallback_end_nanos: Option<i64>,
    ) {
        let normalized = normalize_label(target_label);
        for action in self.action_span_cache.values_mut() {
            if action.span.is_some()
                && normalize_label(&action.target_label) == normalized
                && action.info.end_nanos.is_none()
            {
                action.info.end_nanos = fallback_end_nanos;
            }
        }
    }

    fn finalize_action_summary_attrs(span: &mut opentelemetry_sdk::trace::Span, spawn_count: u32) {
        span.set_attribute(KeyValue::new(BAZEL_ACTION_SPAWN_COUNT, i64::from(spawn_count)));
        if spawn_count == 0 {
            span.set_attribute(KeyValue::new(BAZEL_ACTION_SPAWN_MISSING, true));
        }
    }

    /// Finalize and end the in-place action span for `key`. No-op if the span
    /// has already been ended (e.g. compact log disabled).
    pub(super) fn finalize_and_end_action_entry(
        &mut self,
        key: &ActionSpanKey,
        fallback_end_nanos: Option<i64>,
    ) {
        let Some(action) = self.action_span_cache.get_mut(key) else {
            return;
        };
        let Some(mut span) = action.span.take() else {
            return;
        };
        Self::finalize_action_summary_attrs(&mut span, action.spawn_count);
        let end_nanos = action.info.end_nanos.or(fallback_end_nanos);
        if let Some(nanos) = end_nanos {
            span.end_with_timestamp(super::nanos_to_system_time(nanos));
        } else {
            span.end();
        }
    }
}
