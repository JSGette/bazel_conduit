// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

use crate::otel::span::{KeyValue, SpanKind, Status};
use super::super::attributes::*;
use super::{nanos_to_system_time, shorten_label, spawn_time_range, ActionSpanInfo, ActionSpanKey, OtelMapper};
use spawn_proto::tools::protos::SpawnExec;
use std::time::Duration;
use tracing::{debug, info, warn};

/// `None` when no tailer ran, otherwise its outcome (`Err` carries the
/// worker error).
pub(super) fn drain_and_stop_tailer(mapper: &mut OtelMapper) -> Option<Result<(), String>> {
    let Some(mut state) = mapper.exec_log_state.take() else {
        return None;
    };
    state.handle.shutdown();
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    let mut drained = Vec::new();
    loop {
        let mut got_any = false;
        while let Ok(spawn) = state.handle.rx.try_recv() {
            drained.push(spawn);
            got_any = true;
        }
        if std::time::Instant::now() >= deadline {
            break;
        }
        if state.handle.is_finished() && !got_any {
            break;
        }
        std::thread::sleep(Duration::from_millis(20));
    }

    let worker_exit_deadline = std::time::Instant::now() + Duration::from_millis(300);
    while !state.handle.is_finished() && std::time::Instant::now() < worker_exit_deadline {
        std::thread::sleep(Duration::from_millis(20));
    }
    while let Ok(spawn) = state.handle.rx.try_recv() {
        drained.push(spawn);
    }
    debug!(
        count = drained.len(),
        "Drained compact exec log tailer at finish()",
    );
    for spawn in drained {
        mapper.on_compact_spawn(spawn);
    }

    let spawns_received = mapper.compact_spawns_received;
    match state.handle.take_error() {
        Some(error) => {
            warn!(spawns_received, error, "Compact exec log tailer failed");
            Some(Err(error))
        }
        None => {
            info!(spawns_received, "Compact exec log tailer finished");
            Some(Ok(()))
        }
    }
}

/// `budget` is how many successful actions may still get a span under the
/// lightweight profile; ignored under full.
pub(super) fn synthesise_orphan_actions(mapper: &mut OtelMapper, budget: usize) {
    if mapper.pending_spawns.is_empty() {
        return;
    }
    let mut groups: Vec<(ActionSpanKey, Vec<SpawnExec>)> = Vec::new();
    for (key, spawns) in std::mem::take(&mut mapper.pending_spawns) {
        let dropped_by_budget = !mapper.budget_dropped.is_empty()
            && spawns.iter().any(|s| {
                crate::exec_log::spawn_exec_candidate_keys(s)
                    .iter()
                    .any(|k| mapper.budget_dropped.contains(k))
            });
        let spawns = if dropped_by_budget {
            mapper.suppressed_spawns = mapper.suppressed_spawns.saturating_add(spawns.len() as u64);
            Vec::new()
        } else {
            retained_spawns(mapper, spawns)
        };
        if !spawns.is_empty() {
            groups.push((key, spawns));
        }
    }
    if !mapper.trace_policy.is_full() {
        // Groups with a failed attempt bypass the budget, like failed actions;
        // the rest compete by duration (ties broken by key for stable output).
        let (mut kept, mut successes): (Vec<_>, Vec<_>) = groups
            .into_iter()
            .partition(|(_, spawns)| spawns.iter().any(|s| s.exit_code != 0));
        successes.sort_by(|(ka, a), (kb, b)| {
            group_duration(b).cmp(&group_duration(a)).then_with(|| {
                (&ka.target_label, &ka.mnemonic, &ka.primary_output)
                    .cmp(&(&kb.target_label, &kb.mnemonic, &kb.primary_output))
            })
        });
        for (_, spawns) in successes.drain(budget.min(successes.len())..) {
            mapper.suppressed_actions = mapper.suppressed_actions.saturating_add(1);
            mapper.suppressed_spawns = mapper.suppressed_spawns.saturating_add(spawns.len() as u64);
        }
        kept.extend(successes);
        groups = kept;
    }
    let group_count = groups.len();
    for (key, spawns) in groups {
        synthesise_one_orphan_action(mapper, key, spawns);
    }
    debug!(
        groups = group_count,
        "Synthesised orphan action spans from unmatched compact-log spawns",
    );
}

/// Drop spawns the trace policy filters out, counting them for the root span.
/// Without this the compact log would resurrect a synthetic action + spawn
/// tree for every successful cache hit whose ActionExecuted never reached BEP.
fn retained_spawns(mapper: &mut OtelMapper, spawns: Vec<SpawnExec>) -> Vec<SpawnExec> {
    let total = spawns.len();
    let spawns: Vec<SpawnExec> = spawns
        .into_iter()
        .filter(|s| {
            let (exit_code, cache_hit, cacheable, duration_ms) =
                crate::exec_log::spawn_trace_signal(s);
            mapper
                .trace_policy
                .should_emit_spawn(exit_code, cache_hit, cacheable, duration_ms)
        })
        .collect();
    let dropped = total - spawns.len();
    mapper.suppressed_spawns = mapper.suppressed_spawns.saturating_add(dropped as u64);
    spawns
}

fn spawns_time_range(spawns: &[SpawnExec]) -> (Option<i64>, Option<i64>) {
    spawns
        .iter()
        .filter_map(spawn_time_range)
        .fold((None, None), |(s, e), (ns, ne)| {
            (
                Some(s.map_or(ns, |v: i64| v.min(ns))),
                Some(e.map_or(ne, |v: i64| v.max(ne))),
            )
        })
}

fn group_duration(spawns: &[SpawnExec]) -> i64 {
    match spawns_time_range(spawns) {
        (Some(start), Some(end)) => (end - start).max(0),
        _ => 0,
    }
}

pub(super) fn finalize_and_end_remaining_open_actions(mapper: &mut OtelMapper) {
    let root_end = mapper.root_span_end_from_wall_nanos.or(mapper.finish_time_nanos);
    let keys: Vec<ActionSpanKey> = mapper
        .action_span_cache
        .iter()
        .filter(|(_, action)| action.span.is_some())
        .map(|(k, _)| k.clone())
        .collect();
    for key in keys {
        // Bound each still-open action by its parent target's end (an untimed
        // internal action must not fall back to the whole-build end and spill
        // past the target it hangs under). Falls back to the root end when the
        // target end is unknown.
        let bound = match (mapper.target_end_for(&key.target_label), root_end) {
            (Some(target_end), Some(re)) => Some(target_end.min(re)),
            (Some(target_end), None) => Some(target_end),
            (None, re) => re,
        };
        mapper.finalize_and_end_action_entry(&key, bound);
    }
}

fn synthesise_one_orphan_action(mapper: &mut OtelMapper, key: ActionSpanKey, spawns: Vec<SpawnExec>) {
    let (raw_start, raw_end) = spawns_time_range(&spawns);
    let (clamped_start, clamped_end) = mapper.clamp_to_invocation(raw_start, raw_end);

    let parent_label = spawns
        .first()
        .map(|spawn| spawn.target_label.clone())
        .unwrap_or_else(|| key.target_label.clone());
    let parent = if parent_label.is_empty() {
        mapper.root_context.clone()
    } else {
        mapper
            .target_parent_context(&parent_label, clamped_start)
            .or_else(|| mapper.root_context.clone())
    };
    let Some(parent) = parent else {
        warn!(
            label = %key.target_label,
            mnemonic = %key.mnemonic,
            "Cannot synthesise orphan action: no parent context (root span missing?)",
        );
        return;
    };
    // Keep the synth action inside its parent target: the target span may have
    // been created earlier (from another spawn) with a later start than this
    // orphan's spawn timestamps, which would push the child before its parent.
    let clamped_start = match (clamped_start, mapper.target_start_for(&parent_label)) {
        (Some(start), Some(target_start)) => Some(start.max(target_start)),
        (None, target_start) => target_start,
        (start, None) => start,
    };
    let clamped_end = match (clamped_start, clamped_end) {
        (Some(start), Some(end)) => Some(end.max(start)),
        (_, end) => end,
    };
    mapper.record_target_child_end(&parent_label, clamped_end);

    let span_name = if key.mnemonic.is_empty() {
        format!("action {} (synth)", shorten_label(&key.target_label))
    } else {
        format!(
            "action {} {} (synth)",
            key.mnemonic,
            shorten_label(&key.target_label)
        )
    };

    let success = spawns.iter().all(|s| s.exit_code == 0);
    let mut attrs = vec![
        KeyValue::new(BAZEL_ACTION_SUCCESS, success),
        KeyValue::new(BAZEL_TARGET_SYNTHETIC, true),
    ];
    if !key.mnemonic.is_empty() {
        attrs.push(KeyValue::new(BAZEL_ACTION_MNEMONIC, key.mnemonic.clone()));
    }
    if !key.target_label.is_empty() {
        attrs.push(KeyValue::new(BAZEL_TARGET_LABEL, key.target_label.clone()));
    }
    if !key.primary_output.is_empty() {
        attrs.push(KeyValue::new(
            BAZEL_ACTION_PRIMARY_OUTPUT,
            key.primary_output.clone(),
        ));
    }

    let mut span = mapper.build_child_span(&parent, span_name, SpanKind::Internal, attrs, clamped_start);
    let span_context = span.span_context().clone();
    if let Some(first) = spawns.first() {
        crate::exec_log::apply_spawn_attrs_to_action(&mut span, first);
    }
    span.set_attribute(KeyValue::new(
        BAZEL_ACTION_SPAWN_COUNT,
        i64::try_from(spawns.len()).unwrap_or(i64::MAX),
    ));

    let info = ActionSpanInfo {
        span_context,
        start_nanos: clamped_start,
        end_nanos: clamped_end,
    };
    for spawn in &spawns {
        crate::exec_log::emit_spawn_span(&mapper.tracer, &info, spawn, &mapper.redactor);
    }
    if !success {
        span.set_status(Status::Error { description: "a spawn failed".into() });
    }

    if let Some(nanos) = clamped_end {
        span.end_with_timestamp(nanos_to_system_time(nanos));
    } else {
        span.end();
    }
}

