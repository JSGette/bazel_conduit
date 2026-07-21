// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

use super::super::attributes::*;
use super::{nanos_to_system_time, shorten_label, spawn_time_range, ActionSpanInfo, ActionSpanKey, OtelMapper};
use opentelemetry::trace::{Span, SpanKind};
use opentelemetry::KeyValue;
use spawn_proto::tools::protos::SpawnExec;
use std::collections::HashMap;
use std::time::Duration;
use tracing::{debug, warn};

pub(super) fn drain_and_stop_tailer(mapper: &mut OtelMapper) {
    let Some(mut state) = mapper.exec_log_state.take() else {
        return;
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

    let worker_error = state.handle.take_error();
    let mut attrs = vec![KeyValue::new(
        "spawns_received",
        mapper.compact_spawns_received as i64,
    )];
    if let Some(err) = worker_error {
        attrs.push(KeyValue::new("status", "failed"));
        attrs.push(KeyValue::new("error", err));
    } else {
        attrs.push(KeyValue::new("status", "ok"));
    }
    mapper.add_root_event("bazel.exec_log.tailer_finished", attrs);
}

pub(super) fn synthesise_orphan_actions(mapper: &mut OtelMapper) {
    if mapper.pending_spawns.is_empty() {
        return;
    }
    let mut groups: HashMap<ActionSpanKey, Vec<SpawnExec>> = HashMap::new();
    for (key, spawns) in mapper.pending_spawns.drain() {
        groups.entry(key).or_default().extend(spawns);
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

pub(super) fn finalize_and_end_remaining_open_actions(mapper: &mut OtelMapper) {
    let fallback = mapper.root_span_end_from_wall_nanos.or(mapper.finish_time_nanos);
    let keys: Vec<ActionSpanKey> = mapper
        .action_span_cache
        .iter()
        .filter(|(_, action)| action.span.is_some())
        .map(|(k, _)| k.clone())
        .collect();
    for key in keys {
        mapper.finalize_and_end_action_entry(&key, fallback);
    }
}

fn synthesise_one_orphan_action(mapper: &mut OtelMapper, key: ActionSpanKey, spawns: Vec<SpawnExec>) {
    let (raw_start, raw_end) = spawns
        .iter()
        .filter_map(spawn_time_range)
        .fold((None::<i64>, None::<i64>), |(s, e), (ns, ne)| {
            (
                Some(s.map_or(ns, |v| v.min(ns))),
                Some(e.map_or(ne, |v| v.max(ne))),
            )
        });
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
    mapper.record_synthetic_target_end(&parent_label, clamped_end);

    let span_name = if key.mnemonic.is_empty() {
        format!("action {} (synth)", shorten_label(&key.target_label))
    } else {
        format!(
            "action {} {} (synth)",
            key.mnemonic,
            shorten_label(&key.target_label)
        )
    };

    let mut attrs = vec![
        KeyValue::new(BAZEL_ACTION_SUCCESS, true),
        KeyValue::new(BAZEL_TARGET_SYNTHETIC, true),
    ];
    if !key.mnemonic.is_empty() {
        attrs.push(KeyValue::new(BAZEL_ACTION_MNEMONIC, key.mnemonic.clone()));
    }
    if !key.target_label.is_empty() {
        attrs.push(KeyValue::new(BAZEL_ACTION_LABEL, key.target_label.clone()));
        attrs.push(KeyValue::new(
            BAZEL_ACTION_LABEL_SHORT,
            shorten_label(&key.target_label).to_string(),
        ));
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

    if let Some(nanos) = clamped_end {
        span.end_with_timestamp(nanos_to_system_time(nanos));
    } else {
        span.end();
    }
}

