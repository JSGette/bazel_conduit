use super::super::attributes::*;
use super::OtelMapper;
use opentelemetry::KeyValue;

pub(super) fn apply_build_metrics(mapper: &mut OtelMapper, metrics: &serde_json::Value) {
    if mapper.root_context.is_none() {
        return;
    }
    let Some(obj) = metrics.as_object() else {
        return;
    };

    let summary = obj.get("actionSummary").or(Some(metrics));
    apply_action_summary_attrs(mapper, summary);
    apply_timing_metrics_attrs(mapper, obj);
    apply_memory_metrics_attrs(mapper, obj);
    apply_target_package_metrics_attrs(mapper, obj);
    apply_artifact_metrics_attrs(mapper, obj);
    apply_network_metrics_attrs(mapper, obj);
    apply_runner_count_attrs(mapper, summary);
    apply_cumulative_metrics_attrs(mapper, obj);
}

fn apply_action_summary_attrs(mapper: &OtelMapper, summary: Option<&serde_json::Value>) {
    let Some(s) = summary.and_then(|v| v.as_object()) else {
        return;
    };
    if let Some(v) = s.get("actionsCreated").and_then(|v| v.as_i64()) {
        mapper.set_root_attr(KeyValue::new(BAZEL_METRICS_ACTIONS_CREATED, v));
    }
    if let Some(v) = s.get("actionsExecuted").and_then(|v| v.as_i64()) {
        mapper.set_root_attr(KeyValue::new(BAZEL_METRICS_ACTIONS_EXECUTED, v));
    }
    if let Some(action_data) = s.get("actionData").and_then(|v| v.as_array()) {
        for entry in action_data {
            let mnemonic = entry
                .get("mnemonic")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown");
            let count = entry
                .get("actionsExecuted")
                .and_then(|v| v.as_i64())
                .unwrap_or(0);
            mapper.set_root_attr(KeyValue::new(format!("bazel.metrics.actions.{mnemonic}"), count));
        }
    }
    if let Some(acs) = s.get("actionCacheStatistics").and_then(|v| v.as_object()) {
        if let Some(v) = acs.get("hits").and_then(|v| v.as_i64()) {
            mapper.set_root_attr(KeyValue::new(BAZEL_METRICS_CACHE_HITS, v));
        }
        if let Some(v) = acs.get("misses").and_then(|v| v.as_i64()) {
            mapper.set_root_attr(KeyValue::new(BAZEL_METRICS_CACHE_MISSES, v));
        }
    }
}

fn apply_timing_metrics_attrs(
    mapper: &mut OtelMapper,
    obj: &serde_json::Map<String, serde_json::Value>,
) {
    let Some(t) = obj.get("timingMetrics").and_then(|v| v.as_object()) else {
        return;
    };
    if let Some(v) = t.get("wallTimeInMs").and_then(|v| v.as_i64()) {
        mapper.set_root_attr(KeyValue::new(BAZEL_METRICS_WALL_TIME_MS, v));
        if let Some(start) = mapper.root_span_start_nanos {
            mapper.root_span_end_from_wall_nanos = Some(start + v.saturating_mul(1_000_000));
        }
    }
    let timing_attrs: &[(&str, &str)] = &[
        ("cpuTimeInMs", BAZEL_METRICS_CPU_TIME_MS),
        ("analysisPhaseTimeInMs", BAZEL_METRICS_ANALYSIS_PHASE_MS),
        ("executionPhaseTimeInMs", BAZEL_METRICS_EXECUTION_PHASE_MS),
        ("criticalPathMs", BAZEL_METRICS_CRITICAL_PATH_MS),
        ("actionsExecutionStartInMs", BAZEL_METRICS_ACTIONS_EXECUTION_START_MS),
    ];
    for &(json_key, attr_key) in timing_attrs {
        if let Some(v) = t.get(json_key).and_then(|v| v.as_i64()) {
            mapper.set_root_attr(KeyValue::new(attr_key, v));
        }
    }
}

fn apply_memory_metrics_attrs(mapper: &OtelMapper, obj: &serde_json::Map<String, serde_json::Value>) {
    let Some(m) = obj.get("memoryMetrics").and_then(|v| v.as_object()) else {
        return;
    };
    if let Some(v) = m.get("usedHeapSizePostBuild").and_then(|v| v.as_i64()) {
        mapper.set_root_attr(KeyValue::new(BAZEL_METRICS_HEAP_POST_BUILD, v));
    }
    if let Some(v) = m.get("peakPostGcHeapSize").and_then(|v| v.as_i64()) {
        mapper.set_root_attr(KeyValue::new(BAZEL_METRICS_PEAK_HEAP_POST_GC, v));
    }
}

fn apply_target_package_metrics_attrs(
    mapper: &OtelMapper,
    obj: &serde_json::Map<String, serde_json::Value>,
) {
    if let Some(v) = obj
        .get("targetMetrics")
        .and_then(|v| v.get("targetsConfigured"))
        .and_then(|v| v.as_i64())
    {
        mapper.set_root_attr(KeyValue::new(BAZEL_METRICS_TARGETS_CONFIGURED, v));
    }
    if let Some(v) = obj
        .get("packageMetrics")
        .and_then(|v| v.get("packagesLoaded"))
        .and_then(|v| v.as_i64())
    {
        mapper.set_root_attr(KeyValue::new(BAZEL_METRICS_PACKAGES_LOADED, v));
    }
}

fn apply_artifact_metrics_attrs(mapper: &OtelMapper, obj: &serde_json::Map<String, serde_json::Value>) {
    let Some(am) = obj.get("artifactMetrics").and_then(|v| v.as_object()) else {
        return;
    };
    let artifact_attrs: &[(&str, &str)] = &[
        ("sourceArtifactsRead", BAZEL_METRICS_SOURCE_ARTIFACTS_COUNT),
        ("outputArtifactsSeen", BAZEL_METRICS_OUTPUT_ARTIFACTS_COUNT),
        (
            "outputArtifactsFromActionCache",
            BAZEL_METRICS_ACTION_CACHE_ARTIFACTS_COUNT,
        ),
        ("topLevelArtifacts", BAZEL_METRICS_TOP_LEVEL_ARTIFACTS_COUNT),
    ];
    for &(json_key, attr_key) in artifact_attrs {
        if let Some(v) = am
            .get(json_key)
            .and_then(|v| v.get("count"))
            .and_then(|v| v.as_i64())
        {
            mapper.set_root_attr(KeyValue::new(attr_key, v));
        }
    }
}

fn apply_network_metrics_attrs(mapper: &OtelMapper, obj: &serde_json::Map<String, serde_json::Value>) {
    let Some(nm) = obj.get("networkMetrics").and_then(|v| v.as_object()) else {
        return;
    };
    if let Some(v) = nm.get("bytesSent").and_then(|v| v.as_u64()) {
        mapper.set_root_attr(KeyValue::new(BAZEL_METRICS_BYTES_SENT, v as i64));
    }
    if let Some(v) = nm.get("bytesRecv").and_then(|v| v.as_u64()) {
        mapper.set_root_attr(KeyValue::new(BAZEL_METRICS_BYTES_RECV, v as i64));
    }
}

fn apply_runner_count_attrs(mapper: &OtelMapper, summary: Option<&serde_json::Value>) {
    let Some(runners) = summary
        .and_then(|s| s.as_object())
        .and_then(|s| s.get("runnerCount"))
        .and_then(|v| v.as_array())
    else {
        return;
    };
    for r in runners {
        let name = r.get("name").and_then(|v| v.as_str()).unwrap_or("unknown");
        let count = r.get("count").and_then(|v| v.as_i64()).unwrap_or(0);
        mapper.set_root_attr(KeyValue::new(format!("bazel.metrics.runner.{name}"), count));
    }
}

fn apply_cumulative_metrics_attrs(mapper: &OtelMapper, obj: &serde_json::Map<String, serde_json::Value>) {
    let Some(cm) = obj.get("cumulativeMetrics").and_then(|v| v.as_object()) else {
        return;
    };
    if let Some(v) = cm.get("numAnalyses").and_then(|v| v.as_i64()) {
        mapper.set_root_attr(KeyValue::new(BAZEL_METRICS_CUMULATIVE_NUM_ANALYSES, v));
    }
    if let Some(v) = cm.get("numBuilds").and_then(|v| v.as_i64()) {
        mapper.set_root_attr(KeyValue::new(BAZEL_METRICS_CUMULATIVE_NUM_BUILDS, v));
    }
}

