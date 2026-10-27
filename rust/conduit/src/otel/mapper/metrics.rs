// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

use super::super::attributes::*;
use super::OtelMapper;
use crate::build_event_stream::BuildMetrics;
use opentelemetry::KeyValue;

pub(super) fn apply_build_metrics(mapper: &mut OtelMapper, m: &BuildMetrics) {
    if mapper.root_context.is_none() {
        return;
    }
    let mut attrs: Vec<KeyValue> = Vec::new();

    if let Some(s) = &m.action_summary {
        attrs.push(KeyValue::new(BAZEL_METRICS_ACTIONS_CREATED, s.actions_created));
        attrs.push(KeyValue::new(BAZEL_METRICS_ACTIONS_EXECUTED, s.actions_executed));
        for ad in &s.action_data {
            attrs.push(KeyValue::new(
                format!("bazel.metrics.actions.{}", ad.mnemonic),
                ad.actions_executed,
            ));
        }
        if let Some(acs) = &s.action_cache_statistics {
            attrs.push(KeyValue::new(BAZEL_METRICS_CACHE_HITS, i64::from(acs.hits)));
            attrs.push(KeyValue::new(BAZEL_METRICS_CACHE_MISSES, i64::from(acs.misses)));
        }
        for r in &s.runner_count {
            attrs.push(KeyValue::new(
                format!("bazel.metrics.runner.{}", r.name),
                i64::from(r.count),
            ));
        }
    }

    if let Some(t) = &m.timing_metrics {
        attrs.extend([
            KeyValue::new(BAZEL_METRICS_WALL_TIME_MS, t.wall_time_in_ms),
            KeyValue::new(BAZEL_METRICS_CPU_TIME_MS, t.cpu_time_in_ms),
            KeyValue::new(BAZEL_METRICS_ANALYSIS_PHASE_MS, t.analysis_phase_time_in_ms),
            KeyValue::new(BAZEL_METRICS_EXECUTION_PHASE_MS, t.execution_phase_time_in_ms),
            KeyValue::new(
                BAZEL_METRICS_ACTIONS_EXECUTION_START_MS,
                t.actions_execution_start_in_ms,
            ),
        ]);
        if let Some(d) = &t.critical_path_time {
            attrs.push(KeyValue::new(
                BAZEL_METRICS_CRITICAL_PATH_MS,
                d.seconds * 1000 + i64::from(d.nanos) / 1_000_000,
            ));
        }
        if let Some(start) = mapper.root_span_start_nanos {
            // Wall time can stop short of the last action/target end, and
            // BuildFinished can precede it; the root must contain both.
            let wall_end = start + t.wall_time_in_ms.saturating_mul(1_000_000);
            mapper.root_span_end_from_wall_nanos =
                Some(mapper.finish_time_nanos.map_or(wall_end, |f| f.max(wall_end)));
        }
    }

    if let Some(mm) = &m.memory_metrics {
        attrs.push(KeyValue::new(BAZEL_METRICS_HEAP_POST_BUILD, mm.used_heap_size_post_build));
        attrs.push(KeyValue::new(BAZEL_METRICS_PEAK_HEAP_POST_GC, mm.peak_post_gc_heap_size));
    }
    if let Some(tm) = &m.target_metrics {
        attrs.push(KeyValue::new(BAZEL_METRICS_TARGETS_CONFIGURED, tm.targets_configured));
    }
    if let Some(pm) = &m.package_metrics {
        attrs.push(KeyValue::new(BAZEL_METRICS_PACKAGES_LOADED, pm.packages_loaded));
    }

    if let Some(am) = &m.artifact_metrics {
        for (metric, key) in [
            (&am.source_artifacts_read, BAZEL_METRICS_SOURCE_ARTIFACTS_COUNT),
            (&am.output_artifacts_seen, BAZEL_METRICS_OUTPUT_ARTIFACTS_COUNT),
            (
                &am.output_artifacts_from_action_cache,
                BAZEL_METRICS_ACTION_CACHE_ARTIFACTS_COUNT,
            ),
            (&am.top_level_artifacts, BAZEL_METRICS_TOP_LEVEL_ARTIFACTS_COUNT),
        ] {
            if let Some(f) = metric {
                attrs.push(KeyValue::new(key, i64::from(f.count)));
            }
        }
    }

    if let Some(sns) = m
        .network_metrics
        .as_ref()
        .and_then(|nm| nm.system_network_stats.as_ref())
    {
        attrs.push(KeyValue::new(BAZEL_METRICS_BYTES_SENT, sns.bytes_sent as i64));
        attrs.push(KeyValue::new(BAZEL_METRICS_BYTES_RECV, sns.bytes_recv as i64));
    }

    if let Some(cm) = &m.cumulative_metrics {
        attrs.push(KeyValue::new(
            BAZEL_METRICS_CUMULATIVE_NUM_ANALYSES,
            i64::from(cm.num_analyses),
        ));
        attrs.push(KeyValue::new(BAZEL_METRICS_CUMULATIVE_NUM_BUILDS, i64::from(cm.num_builds)));
    }

    for kv in attrs {
        mapper.set_root_attr(kv);
    }
}
