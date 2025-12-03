package bes

import (
	"fmt"
	"strings"

	build_event_stream "github.com/JSGette/bazel_conduit/proto/build_event_stream"
	"go.opentelemetry.io/otel/attribute"
)

// TargetStatus represents the completion status of a target
type TargetStatus int

const (
	TargetStatusSuccess TargetStatus = iota
	TargetStatusFailed
	TargetStatusAborted
)

// ActionStatus represents the completion status of an action
type ActionStatus int

const (
	ActionStatusSuccess ActionStatus = iota
	ActionStatusFailed
)

// ExtractStartedAttributes extracts attributes from a BuildStarted event.
func ExtractStartedAttributes(started *build_event_stream.BuildStarted) []attribute.KeyValue {
	attrs := []attribute.KeyValue{
		attribute.String("build.invocation_id", started.GetUuid()),
		attribute.String("build.tool_version", started.GetBuildToolVersion()),
		attribute.String("build.command", started.GetCommand()),
		attribute.String("build.working_directory", started.GetWorkingDirectory()),
		attribute.String("build.workspace_directory", started.GetWorkspaceDirectory()),
		attribute.Int64("build.server_pid", started.GetServerPid()),
		attribute.String("build.options_description", started.GetOptionsDescription()),
	}

	if started.GetStartTime() != nil {
		attrs = append(attrs, attribute.String("build.start_time",
			started.GetStartTime().AsTime().Format("2006-01-02T15:04:05.000Z")))
	}

	return attrs
}

// ExtractConfigurationAttributes extracts attributes from a Configuration event.
func ExtractConfigurationAttributes(config *build_event_stream.Configuration) []attribute.KeyValue {
	attrs := []attribute.KeyValue{
		attribute.String("build.platform_name", config.GetPlatformName()),
		attribute.String("build.cpu", config.GetCpu()),
		attribute.String("build.mnemonic", config.GetMnemonic()),
	}

	// Convert make variables to a string representation
	if len(config.GetMakeVariable()) > 0 {
		var parts []string
		for k, v := range config.GetMakeVariable() {
			parts = append(parts, fmt.Sprintf("%s=%s", k, v))
		}
		attrs = append(attrs, attribute.String("build.make_variables", strings.Join(parts, ",")))
	}

	return attrs
}

// ExtractWorkspaceStatusAttributes extracts attributes from a WorkspaceStatus event.
func ExtractWorkspaceStatusAttributes(status *build_event_stream.WorkspaceStatus) []attribute.KeyValue {
	var attrs []attribute.KeyValue

	for _, item := range status.GetItem() {
		switch item.GetKey() {
		case "BUILD_USER":
			attrs = append(attrs, attribute.String("build.user", item.GetValue()))
		case "BUILD_HOST":
			attrs = append(attrs, attribute.String("build.host", item.GetValue()))
		case "BUILD_TIMESTAMP":
			attrs = append(attrs, attribute.String("build.timestamp", item.GetValue()))
		case "FORMATTED_DATE":
			attrs = append(attrs, attribute.String("build.formatted_date", item.GetValue()))
		}
	}

	return attrs
}

// ExtractTargetConfiguredAttributes extracts attributes from a TargetConfigured event.
func ExtractTargetConfiguredAttributes(configured *build_event_stream.TargetConfigured, label string, configID string) []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.String("target.label", label),
		attribute.String("target.kind", configured.GetTargetKind()),
		attribute.String("target.configuration_id", configID),
	}
}

// ExtractTargetCompletedAttributes extracts attributes from a TargetComplete event.
// Returns attributes and the target status.
func ExtractTargetCompletedAttributes(completed *build_event_stream.TargetComplete, outputFiles []string) ([]attribute.KeyValue, TargetStatus) {
	attrs := []attribute.KeyValue{
		attribute.Bool("target.success", completed.GetSuccess()),
		attribute.Int64("target.output_group_count", int64(len(completed.GetOutputGroup()))),
	}

	// Add output files if provided
	if len(outputFiles) > 0 {
		attrs = append(attrs,
			attribute.Int64("target.output_file_count", int64(len(outputFiles))),
			attribute.StringSlice("target.output_files", outputFiles),
		)
	}

	// Determine status
	status := TargetStatusSuccess
	if !completed.GetSuccess() {
		status = TargetStatusFailed
	}

	return attrs, status
}

// ExtractBuildFinishedAttributes extracts attributes from a BuildFinished event.
func ExtractBuildFinishedAttributes(finished *build_event_stream.BuildFinished) []attribute.KeyValue {
	attrs := []attribute.KeyValue{
		attribute.Bool("build.overall_success", finished.GetOverallSuccess()),
	}

	if finished.GetExitCode() != nil {
		attrs = append(attrs,
			attribute.String("build.exit_code", finished.GetExitCode().GetName()),
			attribute.Int64("build.exit_code_value", int64(finished.GetExitCode().GetCode())),
		)
	}

	if finished.GetFinishTime() != nil {
		attrs = append(attrs, attribute.String("build.finish_time",
			finished.GetFinishTime().AsTime().Format("2006-01-02T15:04:05.000Z")))
	}

	return attrs
}

// ExtractBuildMetricsAttributes extracts comprehensive attributes from a BuildMetrics event.
func ExtractBuildMetricsAttributes(metrics *build_event_stream.BuildMetrics) []attribute.KeyValue {
	var attrs []attribute.KeyValue

	// Action Summary
	if as := metrics.GetActionSummary(); as != nil {
		attrs = append(attrs,
			attribute.Int64("build.action_summary.actions_created", as.GetActionsCreated()),
			attribute.Int64("build.action_summary.actions_executed", as.GetActionsExecuted()),
		)

		// Per-mnemonic action data
		for _, ad := range as.GetActionData() {
			mnemonic := ad.GetMnemonic()
			attrs = append(attrs,
				attribute.Int64(fmt.Sprintf("build.actions.%s.executed", mnemonic), ad.GetActionsExecuted()),
				attribute.Int64(fmt.Sprintf("build.actions.%s.created", mnemonic), ad.GetActionsCreated()),
			)
			if ad.GetFirstStartedMs() > 0 {
				attrs = append(attrs,
					attribute.Int64(fmt.Sprintf("build.actions.%s.first_started_ms", mnemonic), ad.GetFirstStartedMs()))
			}
			if ad.GetLastEndedMs() > 0 {
				attrs = append(attrs,
					attribute.Int64(fmt.Sprintf("build.actions.%s.last_ended_ms", mnemonic), ad.GetLastEndedMs()))
			}
		}

		// Runner counts
		for _, rc := range as.GetRunnerCount() {
			attrs = append(attrs,
				attribute.Int64(fmt.Sprintf("build.runners.%s.count", rc.GetName()), int64(rc.GetCount())),
			)
		}
	}

	// Timing Metrics
	if tm := metrics.GetTimingMetrics(); tm != nil {
		attrs = append(attrs,
			attribute.Int64("build.timing.cpu_time_ms", tm.GetCpuTimeInMs()),
			attribute.Int64("build.timing.wall_time_ms", tm.GetWallTimeInMs()),
			attribute.Int64("build.timing.analysis_phase_time_ms", tm.GetAnalysisPhaseTimeInMs()),
			attribute.Int64("build.timing.execution_phase_time_ms", tm.GetExecutionPhaseTimeInMs()),
			attribute.Int64("build.timing.actions_execution_start_ms", tm.GetActionsExecutionStartInMs()),
		)
	}

	// Target Metrics
	if tm := metrics.GetTargetMetrics(); tm != nil {
		attrs = append(attrs,
			attribute.Int64("build.targets_configured", tm.GetTargetsConfigured()),
			attribute.Int64("build.targets_configured_not_including_aspects", tm.GetTargetsConfiguredNotIncludingAspects()),
		)
	}

	// Package Metrics
	if pm := metrics.GetPackageMetrics(); pm != nil {
		attrs = append(attrs,
			attribute.Int64("build.packages_loaded", pm.GetPackagesLoaded()),
		)
	}

	// Cumulative Metrics
	if cm := metrics.GetCumulativeMetrics(); cm != nil {
		attrs = append(attrs,
			attribute.Int64("build.cumulative.num_analyses", int64(cm.GetNumAnalyses())),
			attribute.Int64("build.cumulative.num_builds", int64(cm.GetNumBuilds())),
		)
	}

	// Artifact Metrics
	if am := metrics.GetArtifactMetrics(); am != nil {
		if src := am.GetSourceArtifactsRead(); src != nil {
			attrs = append(attrs,
				attribute.Int64("build.artifacts.source_read.size_in_bytes", src.GetSizeInBytes()),
				attribute.Int64("build.artifacts.source_read.count", int64(src.GetCount())),
			)
		}
		if out := am.GetOutputArtifactsSeen(); out != nil {
			attrs = append(attrs,
				attribute.Int64("build.artifacts.output_seen.size_in_bytes", out.GetSizeInBytes()),
				attribute.Int64("build.artifacts.output_seen.count", int64(out.GetCount())),
			)
		}
		if cache := am.GetOutputArtifactsFromActionCache(); cache != nil {
			attrs = append(attrs,
				attribute.Int64("build.artifacts.output_from_cache.size_in_bytes", cache.GetSizeInBytes()),
				attribute.Int64("build.artifacts.output_from_cache.count", int64(cache.GetCount())),
			)
		}
		if top := am.GetTopLevelArtifacts(); top != nil {
			attrs = append(attrs,
				attribute.Int64("build.artifacts.top_level.size_in_bytes", top.GetSizeInBytes()),
				attribute.Int64("build.artifacts.top_level.count", int64(top.GetCount())),
			)
		}
	}

	// Memory Metrics
	if mm := metrics.GetMemoryMetrics(); mm != nil {
		attrs = append(attrs,
			attribute.Int64("build.memory.used_heap_size_post_build", mm.GetUsedHeapSizePostBuild()),
			attribute.Int64("build.memory.peak_post_gc_heap_size", mm.GetPeakPostGcHeapSize()),
		)
		for _, gm := range mm.GetGarbageMetrics() {
			attrs = append(attrs,
				attribute.Int64(fmt.Sprintf("build.memory.garbage.%s.collected", gm.GetType()), gm.GetGarbageCollected()),
			)
		}
	}

	// Build Graph Metrics
	if bg := metrics.GetBuildGraphMetrics(); bg != nil {
		attrs = append(attrs,
			attribute.Int64("build.graph.action_lookup_value_count", int64(bg.GetActionLookupValueCount())),
			attribute.Int64("build.graph.action_count", int64(bg.GetActionCount())),
			attribute.Int64("build.graph.input_file_configured_target_count", int64(bg.GetInputFileConfiguredTargetCount())),
			attribute.Int64("build.graph.output_file_configured_target_count", int64(bg.GetOutputFileConfiguredTargetCount())),
			attribute.Int64("build.graph.other_configured_target_count", int64(bg.GetOtherConfiguredTargetCount())),
			attribute.Int64("build.graph.output_artifact_count", int64(bg.GetOutputArtifactCount())),
			attribute.Int64("build.graph.post_invocation_skyframe_node_count", int64(bg.GetPostInvocationSkyframeNodeCount())),
		)
	}

	return attrs
}

// ExtractActionExecutedAttributes extracts attributes from an ActionExecuted event.
func ExtractActionExecutedAttributes(action *build_event_stream.ActionExecuted) ([]attribute.KeyValue, ActionStatus) {
	attrs := []attribute.KeyValue{
		attribute.String("action.type", action.GetType()),
		attribute.Bool("action.success", action.GetSuccess()),
		attribute.Int64("action.exit_code", int64(action.GetExitCode())),
	}

	if action.GetPrimaryOutput() != nil {
		attrs = append(attrs, attribute.String("action.primary_output", action.GetPrimaryOutput().GetName()))
	}

	status := ActionStatusSuccess
	if !action.GetSuccess() {
		status = ActionStatusFailed
	}

	return attrs, status
}

// ExtractNamedSetFiles extracts FileInfo from a NamedSetOfFiles event.
func ExtractNamedSetFiles(namedSet *build_event_stream.NamedSetOfFiles) []FileInfo {
	var files []FileInfo

	for _, f := range namedSet.GetFiles() {
		pathPrefix := ""
		if len(f.GetPathPrefix()) > 0 {
			pathPrefix = strings.Join(f.GetPathPrefix(), "/")
		}

		fileInfo := FileInfo{
			Name:       f.GetName(),
			Digest:     f.GetDigest(),
			Length:     f.GetLength(),
			PathPrefix: pathPrefix,
		}

		// Extract URI if available (can be bytestream or file)
		if f.GetUri() != "" {
			fileInfo.URI = f.GetUri()
		}

		files = append(files, fileInfo)
	}

	return files
}

// ExtractPatterns extracts patterns from a pattern event.
func ExtractPatterns(patterns []string) []string {
	return patterns
}

// ExtractWorkspaceInfoAttributes extracts attributes from WorkspaceInfo.
func ExtractWorkspaceInfoAttributes(workspace *build_event_stream.WorkspaceConfig) []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.String("build.local_exec_root", workspace.GetLocalExecRoot()),
	}
}

