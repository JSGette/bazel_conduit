// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

use crate::otel::span::{KeyValue, Span, SpanKind};
use spawn_proto::tools::protos::SpawnExec;
use tracing::{debug, warn};

use super::super::attributes::*;
use super::{action_status, build_action_attrs, build_action_span_name};
use super::{
    clamp_time_range, ActionCompletedEvent, ActionSpanInfo, ActionSpanKey, BudgetedAction,
    OpenAction, OtelMapper,
};
use std::cmp::Reverse;

impl OtelMapper {
    /// ActionCompleted. Failed actions (and every action under the full
    /// profile) become spans now; successful ones under lightweight compete
    /// for the span budget and the slowest are emitted at [`Self::finish`].
    pub fn on_action_completed(&mut self, ev: ActionCompletedEvent) {
        if let Some(l) = &ev.label {
            if ev.cached == Some(false) {
                self.target_has_non_cached_action.insert(l.clone(), true);
            }
        }
        if !ev.success || self.trace_policy.is_full() {
            self.emit_action(&ev);
            return;
        }
        let duration_nanos = match (ev.start_time_nanos, ev.end_time_nanos) {
            (Some(start), Some(end)) => (end - start).max(0),
            _ => 0,
        };
        self.action_seq += 1;
        self.action_budget.push(Reverse(BudgetedAction {
            duration_nanos,
            seq: self.action_seq,
            event: ev,
        }));
        if self.action_budget.len() > self.trace_policy.max_action_spans() {
            if let Some(Reverse(dropped)) = self.action_budget.pop() {
                self.suppressed_actions = self.suppressed_actions.saturating_add(1);
                if self.compact_streaming_active {
                    self.budget_dropped.insert(dropped.event.key());
                }
            }
        }
    }

    /// Emit the actions that survived the budget, earliest first so each
    /// target span opens at its first kept action.
    pub(super) fn emit_budgeted_actions(&mut self) -> usize {
        let mut kept: Vec<ActionCompletedEvent> = std::mem::take(&mut self.action_budget)
            .into_iter()
            .map(|Reverse(b)| b.event)
            .collect();
        kept.sort_by_key(|ev| ev.start_time_nanos.or(ev.end_time_nanos));
        for ev in &kept {
            self.emit_action(ev);
        }
        kept.len()
    }

    /// Create the `action {mnemonic} {label}` span. With the compact exec log
    /// enabled the span is kept open so arriving spawns can backfill attrs
    /// onto it; without it the span ends here.
    fn emit_action(&mut self, ev: &ActionCompletedEvent) {
        // Drain whatever the tailer has produced so far. Cheap; runs the
        // backfill against actions that already landed in the cache.
        self.pump_compact_spawns();
        let label = ev.label.as_deref();

        // Clamp action timing before creating a synthetic target so its
        // lifecycle can use the same child bounds.
        let (clamped_start, clamped_end) = clamp_time_range(
            ev.start_time_nanos,
            ev.end_time_nanos,
            self.root_span_start_nanos,
            self.root_span_end_from_wall_nanos.or(self.finish_time_nanos),
        );
        let parent = match label {
            Some(label) => self
                .target_parent_context(label, clamped_start)
                .or_else(|| self.root_context.clone()),
            None => self.root_context.clone(),
        };
        let Some(parent) = parent else {
            warn!("ActionCompleted with no parent context");
            return;
        };

        // Anchor the span start. Some internal actions (RunfilesTree, symlink
        // manifests) carry no start_time; fall back to their end, then the
        // parent target's start, rather than a wall-clock creation time. Also
        // clamp up to the parent target's start: a cache-hit target span may be
        // created at TargetComplete (later) than an action that really began
        // near the build start, which would push the child before its parent.
        let target_start = label.and_then(|l| self.target_start_for(l));
        let clamped_start = match (clamped_start.or(clamped_end), target_start) {
            (Some(start), Some(parent_start)) => Some(start.max(parent_start)),
            (start, parent_start) => start.or(parent_start),
        };

        if let Some(label) = label {
            self.record_target_child_end(label, clamped_end);
        }

        let span_name = build_action_span_name(ev);
        let attrs = build_action_attrs(ev, &self.configurations, &self.redactor);
        let status = action_status(ev.success);

        let key = ev.key();
        let target_label = label.unwrap_or_default().to_string();
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
            // Pin untimed ends so the deferred target can still cover them.
            let end = clamped_end.or_else(super::utils::wall_clock_now_nanos);
            if let Some(label) = label {
                self.record_target_child_end(label, end);
            }
            if let Some(nanos) = end {
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
            label = ?label,
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
        // The first spawn always backfills curated attrs onto the (retained)
        // parent action above; the child spawn span itself is subject to the
        // trace policy so lightweight traces keep only interesting spawns.
        let (exit_code, cache_hit, cacheable, duration_ms) =
            crate::exec_log::spawn_trace_signal(&spawn);
        if self
            .trace_policy
            .should_emit_spawn(exit_code, cache_hit, cacheable, duration_ms)
        {
            crate::exec_log::emit_spawn_span(&self.tracer, &parent_info, &spawn, &self.redactor);
        } else {
            self.suppressed_spawns = self.suppressed_spawns.saturating_add(1);
        }
    }

    fn push_pending_spawn(&mut self, spawn: SpawnExec) {
        let mut keys = crate::exec_log::spawn_exec_candidate_keys(&spawn).into_iter();
        let key = keys.next().expect("candidate keys are never empty");
        for alias in keys {
            self.pending_spawn_aliases.insert(alias, key.clone());
        }
        self.pending_spawns.entry(key).or_default().push(spawn);
    }

    /// Move buffered spawns for `key` from [`Self::pending_spawns`] into the
    /// action cache once ActionExecuted has arrived.
    fn flush_pending_spawns_for_action(&mut self, key: &ActionSpanKey) {
        let matched = match self.pending_spawns.remove(key) {
            Some(matched) => matched,
            None => match self.pending_spawn_aliases.remove(key) {
                Some(filed) => self.pending_spawns.remove(&filed).unwrap_or_default(),
                None => return,
            },
        };
        for spawn in matched {
            self.apply_spawn_to_action(key, spawn);
        }
    }

    /// Bound every still-open action span belonging to `target_label` by the
    /// target's end time. The action spans stay open until `finish()` so late
    /// tailer arrivals can still backfill attrs before finalisation.
    ///
    /// `target_end_nanos` is the parent target's clamped completion time. An
    /// action must not outlive its target: Bazel schedules some cheap actions
    /// (runfiles/symlink/module-map) lazily and reports an `end_time` *after*
    /// the target's `TargetComplete`, which would push the child span past its
    /// parent and break Datadog's containment requirement. We cap the action
    /// end to the target end (and fill it in when the action carried no timing
    /// of its own).
    pub(super) fn end_open_actions_for_target(
        &mut self,
        target_label: &str,
        target_end_nanos: Option<i64>,
    ) {
        for action in self.action_span_cache.values_mut() {
            if action.span.is_some() && action.target_label == target_label {
                action.info.end_nanos = match (action.info.end_nanos, target_end_nanos) {
                    (Some(end), Some(bound)) => Some(end.min(bound)),
                    (None, bound) => bound,
                    (end, None) => end,
                };
            }
        }
    }

    fn finalize_action_summary_attrs(span: &mut Span, spawn_count: u32) {
        span.set_attribute(KeyValue::new(BAZEL_ACTION_SPAWN_COUNT, i64::from(spawn_count)));
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
        // `fallback_end_nanos` is the parent target's end: cap to it so a span
        // whose own end overshoots (or is missing) still stays inside its
        // parent.
        let end_nanos = match (action.info.end_nanos, fallback_end_nanos) {
            (Some(end), Some(bound)) => Some(end.min(bound)),
            (Some(end), None) => Some(end),
            (None, bound) => bound,
        };
        // Never let the cap invert the span (end < start).
        let end_nanos = match (end_nanos, action.info.start_nanos) {
            (Some(end), Some(start)) if end < start => Some(start),
            (end, _) => end,
        };
        if let Some(nanos) = end_nanos {
            span.end_with_timestamp(super::nanos_to_system_time(nanos));
        } else {
            span.end();
        }
    }
}
