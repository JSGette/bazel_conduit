package bes

import (
	"testing"

	build_event_stream "github.com/JSGette/bazel_conduit/proto/build_event_stream"
	"go.opentelemetry.io/otel/attribute"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Helper to find an attribute by key
func findAttr(attrs []attribute.KeyValue, key string) (attribute.KeyValue, bool) {
	for _, attr := range attrs {
		if string(attr.Key) == key {
			return attr, true
		}
	}
	return attribute.KeyValue{}, false
}

// TestExtractStartedAttributes tests extracting attributes from started event
func TestExtractStartedAttributes(t *testing.T) {
	started := &build_event_stream.BuildStarted{
		Uuid:                "ada9c62f-776d-4388-b8bb-e56064ba5727",
		BuildToolVersion:    "8.4.2",
		Command:             "build",
		WorkingDirectory:    "/Users/joseph/REPOS/datadog-agent",
		WorkspaceDirectory:  "/Users/joseph/REPOS/datadog-agent",
		ServerPid:           69875,
		OptionsDescription:  "--enable_platform_specific_config",
		StartTime:           timestamppb.Now(),
	}

	attrs := ExtractStartedAttributes(started)

	// Verify key attributes
	if attr, ok := findAttr(attrs, "build.invocation_id"); !ok || attr.Value.AsString() != "ada9c62f-776d-4388-b8bb-e56064ba5727" {
		t.Error("build.invocation_id mismatch")
	}
	if attr, ok := findAttr(attrs, "build.tool_version"); !ok || attr.Value.AsString() != "8.4.2" {
		t.Error("build.tool_version mismatch")
	}
	if attr, ok := findAttr(attrs, "build.command"); !ok || attr.Value.AsString() != "build" {
		t.Error("build.command mismatch")
	}
	if _, ok := findAttr(attrs, "build.working_directory"); !ok {
		t.Error("build.working_directory missing")
	}
}

// TestExtractConfigurationAttributes tests extracting from configuration event
func TestExtractConfigurationAttributes(t *testing.T) {
	config := &build_event_stream.Configuration{
		Mnemonic:     "darwin_arm64-fastbuild",
		PlatformName: "darwin_arm64",
		Cpu:          "darwin_arm64",
		MakeVariable: map[string]string{
			"TARGET_CPU":       "darwin_arm64",
			"COMPILATION_MODE": "fastbuild",
		},
	}

	attrs := ExtractConfigurationAttributes(config)

	if attr, ok := findAttr(attrs, "build.platform_name"); !ok || attr.Value.AsString() != "darwin_arm64" {
		t.Error("build.platform_name mismatch")
	}
	if attr, ok := findAttr(attrs, "build.cpu"); !ok || attr.Value.AsString() != "darwin_arm64" {
		t.Error("build.cpu mismatch")
	}
	if attr, ok := findAttr(attrs, "build.mnemonic"); !ok || attr.Value.AsString() != "darwin_arm64-fastbuild" {
		t.Error("build.mnemonic mismatch")
	}
}

// TestExtractWorkspaceStatusAttributes tests extracting from workspaceStatus event
func TestExtractWorkspaceStatusAttributes(t *testing.T) {
	status := &build_event_stream.WorkspaceStatus{
		Item: []*build_event_stream.WorkspaceStatus_Item{
			{Key: "BUILD_USER", Value: "joseph.gette"},
			{Key: "BUILD_HOST", Value: "COMP-MGK60Y6X20"},
			{Key: "BUILD_TIMESTAMP", Value: "1764755031"},
		},
	}

	attrs := ExtractWorkspaceStatusAttributes(status)

	if attr, ok := findAttr(attrs, "build.user"); !ok || attr.Value.AsString() != "joseph.gette" {
		t.Error("build.user mismatch")
	}
	if attr, ok := findAttr(attrs, "build.host"); !ok || attr.Value.AsString() != "COMP-MGK60Y6X20" {
		t.Error("build.host mismatch")
	}
}

// TestExtractTargetConfiguredAttributes tests extracting from targetConfigured event
func TestExtractTargetConfiguredAttributes(t *testing.T) {
	configured := &build_event_stream.TargetConfigured{
		TargetKind: "go_library rule",
	}
	label := "//deps/foo:bar"
	configID := "096a4b319133cdcd98a221c3f6665f696be55030d03063bfae0cdd290e1a4323"

	attrs := ExtractTargetConfiguredAttributes(configured, label, configID)

	if attr, ok := findAttr(attrs, "target.label"); !ok || attr.Value.AsString() != "//deps/foo:bar" {
		t.Error("target.label mismatch")
	}
	if attr, ok := findAttr(attrs, "target.kind"); !ok || attr.Value.AsString() != "go_library rule" {
		t.Error("target.kind mismatch")
	}
	if attr, ok := findAttr(attrs, "target.configuration_id"); !ok || attr.Value.AsString() != configID {
		t.Error("target.configuration_id mismatch")
	}
}

// TestExtractTargetCompletedAttributesSuccess tests extracting from successful targetCompleted
func TestExtractTargetCompletedAttributesSuccess(t *testing.T) {
	completed := &build_event_stream.TargetComplete{
		Success: true,
		OutputGroup: []*build_event_stream.OutputGroup{
			{Name: "default", FileSets: []*build_event_stream.BuildEventId_NamedSetOfFilesId{{Id: "0"}}},
		},
	}

	attrs, status := ExtractTargetCompletedAttributes(completed, nil)

	if attr, ok := findAttr(attrs, "target.success"); !ok || attr.Value.AsBool() != true {
		t.Error("target.success mismatch")
	}
	if attr, ok := findAttr(attrs, "target.output_group_count"); !ok || attr.Value.AsInt64() != 1 {
		t.Error("target.output_group_count mismatch")
	}
	if status != TargetStatusSuccess {
		t.Errorf("expected TargetStatusSuccess, got %v", status)
	}
}

// TestExtractTargetCompletedAttributesFailed tests extracting from failed targetCompleted
func TestExtractTargetCompletedAttributesFailed(t *testing.T) {
	completed := &build_event_stream.TargetComplete{
		Success: false,
	}

	attrs, status := ExtractTargetCompletedAttributes(completed, nil)

	if attr, ok := findAttr(attrs, "target.success"); !ok || attr.Value.AsBool() != false {
		t.Error("target.success should be false")
	}
	if status != TargetStatusFailed {
		t.Errorf("expected TargetStatusFailed, got %v", status)
	}
	_ = attrs // suppress unused warning
}

// TestExtractTargetCompletedWithOutputFiles tests including output files
func TestExtractTargetCompletedWithOutputFiles(t *testing.T) {
	completed := &build_event_stream.TargetComplete{
		Success: true,
	}

	outputFiles := []string{"file1.go", "file2.go", "file3.go"}
	attrs, _ := ExtractTargetCompletedAttributes(completed, outputFiles)

	if attr, ok := findAttr(attrs, "target.output_file_count"); !ok || attr.Value.AsInt64() != 3 {
		t.Error("target.output_file_count mismatch")
	}
	if attr, ok := findAttr(attrs, "target.output_files"); !ok {
		t.Error("target.output_files missing")
	} else {
		slice := attr.Value.AsStringSlice()
		if len(slice) != 3 {
			t.Errorf("expected 3 output files, got %d", len(slice))
		}
	}
}

// TestExtractBuildFinishedAttributes tests extracting from buildFinished event
func TestExtractBuildFinishedAttributes(t *testing.T) {
	finished := &build_event_stream.BuildFinished{
		OverallSuccess: true,
		ExitCode: &build_event_stream.BuildFinished_ExitCode{
			Name: "SUCCESS",
			Code: 0,
		},
		FinishTime: timestamppb.Now(),
	}

	attrs := ExtractBuildFinishedAttributes(finished)

	if attr, ok := findAttr(attrs, "build.overall_success"); !ok || attr.Value.AsBool() != true {
		t.Error("build.overall_success mismatch")
	}
	if attr, ok := findAttr(attrs, "build.exit_code"); !ok || attr.Value.AsString() != "SUCCESS" {
		t.Error("build.exit_code mismatch")
	}
	if _, ok := findAttr(attrs, "build.finish_time"); !ok {
		t.Error("build.finish_time missing")
	}
}

// TestExtractBuildMetricsActionSummary tests extracting actionSummary from buildMetrics
func TestExtractBuildMetricsActionSummary(t *testing.T) {
	metrics := &build_event_stream.BuildMetrics{
		ActionSummary: &build_event_stream.BuildMetrics_ActionSummary{
			ActionsCreated:  100,
			ActionsExecuted: 50,
			ActionData: []*build_event_stream.BuildMetrics_ActionSummary_ActionData{
				{
					Mnemonic:        "CppCompile",
					ActionsExecuted: 30,
					ActionsCreated:  40,
				},
				{
					Mnemonic:        "GoCompile",
					ActionsExecuted: 20,
					ActionsCreated:  25,
				},
			},
		},
	}

	attrs := ExtractBuildMetricsAttributes(metrics)

	if attr, ok := findAttr(attrs, "build.action_summary.actions_created"); !ok || attr.Value.AsInt64() != 100 {
		t.Error("build.action_summary.actions_created mismatch")
	}
	if attr, ok := findAttr(attrs, "build.action_summary.actions_executed"); !ok || attr.Value.AsInt64() != 50 {
		t.Error("build.action_summary.actions_executed mismatch")
	}
	if attr, ok := findAttr(attrs, "build.actions.CppCompile.executed"); !ok || attr.Value.AsInt64() != 30 {
		t.Error("build.actions.CppCompile.executed mismatch")
	}
	if attr, ok := findAttr(attrs, "build.actions.GoCompile.created"); !ok || attr.Value.AsInt64() != 25 {
		t.Error("build.actions.GoCompile.created mismatch")
	}
}

// TestExtractBuildMetricsTimingMetrics tests extracting timing metrics
func TestExtractBuildMetricsTimingMetrics(t *testing.T) {
	metrics := &build_event_stream.BuildMetrics{
		TimingMetrics: &build_event_stream.BuildMetrics_TimingMetrics{
			CpuTimeInMs:                3464,
			WallTimeInMs:               725,
			AnalysisPhaseTimeInMs:      306,
			ExecutionPhaseTimeInMs:     311,
			ActionsExecutionStartInMs:  412,
		},
	}

	attrs := ExtractBuildMetricsAttributes(metrics)

	if attr, ok := findAttr(attrs, "build.timing.cpu_time_ms"); !ok || attr.Value.AsInt64() != 3464 {
		t.Error("build.timing.cpu_time_ms mismatch")
	}
	if attr, ok := findAttr(attrs, "build.timing.wall_time_ms"); !ok || attr.Value.AsInt64() != 725 {
		t.Error("build.timing.wall_time_ms mismatch")
	}
	if attr, ok := findAttr(attrs, "build.timing.analysis_phase_time_ms"); !ok || attr.Value.AsInt64() != 306 {
		t.Error("build.timing.analysis_phase_time_ms mismatch")
	}
}

// TestExtractBuildMetricsTargetMetrics tests extracting target metrics
func TestExtractBuildMetricsTargetMetrics(t *testing.T) {
	metrics := &build_event_stream.BuildMetrics{
		TargetMetrics: &build_event_stream.BuildMetrics_TargetMetrics{
			TargetsConfigured:                      321,
			TargetsConfiguredNotIncludingAspects:   321,
		},
	}

	attrs := ExtractBuildMetricsAttributes(metrics)

	if attr, ok := findAttr(attrs, "build.targets_configured"); !ok || attr.Value.AsInt64() != 321 {
		t.Error("build.targets_configured mismatch")
	}
}

// TestExtractBuildMetricsPackageMetrics tests extracting package metrics
func TestExtractBuildMetricsPackageMetrics(t *testing.T) {
	metrics := &build_event_stream.BuildMetrics{
		PackageMetrics: &build_event_stream.BuildMetrics_PackageMetrics{
			PackagesLoaded: 32,
		},
	}

	attrs := ExtractBuildMetricsAttributes(metrics)

	if attr, ok := findAttr(attrs, "build.packages_loaded"); !ok || attr.Value.AsInt64() != 32 {
		t.Error("build.packages_loaded mismatch")
	}
}

// TestExtractBuildMetricsCumulativeMetrics tests extracting cumulative metrics
func TestExtractBuildMetricsCumulativeMetrics(t *testing.T) {
	metrics := &build_event_stream.BuildMetrics{
		CumulativeMetrics: &build_event_stream.BuildMetrics_CumulativeMetrics{
			NumAnalyses: 2,
			NumBuilds:   2,
		},
	}

	attrs := ExtractBuildMetricsAttributes(metrics)

	if attr, ok := findAttr(attrs, "build.cumulative.num_analyses"); !ok || attr.Value.AsInt64() != 2 {
		t.Error("build.cumulative.num_analyses mismatch")
	}
	if attr, ok := findAttr(attrs, "build.cumulative.num_builds"); !ok || attr.Value.AsInt64() != 2 {
		t.Error("build.cumulative.num_builds mismatch")
	}
}

// TestExtractBuildMetricsArtifactMetrics tests extracting artifact metrics
func TestExtractBuildMetricsArtifactMetrics(t *testing.T) {
	metrics := &build_event_stream.BuildMetrics{
		ArtifactMetrics: &build_event_stream.BuildMetrics_ArtifactMetrics{
			OutputArtifactsSeen: &build_event_stream.BuildMetrics_ArtifactMetrics_FilesMetric{
				SizeInBytes: 1490012,
				Count:       18,
			},
			OutputArtifactsFromActionCache: &build_event_stream.BuildMetrics_ArtifactMetrics_FilesMetric{
				SizeInBytes: 1489875,
				Count:       16,
			},
		},
	}

	attrs := ExtractBuildMetricsAttributes(metrics)

	if attr, ok := findAttr(attrs, "build.artifacts.output_seen.size_in_bytes"); !ok || attr.Value.AsInt64() != 1490012 {
		t.Error("build.artifacts.output_seen.size_in_bytes mismatch")
	}
	if attr, ok := findAttr(attrs, "build.artifacts.output_seen.count"); !ok || attr.Value.AsInt64() != 18 {
		t.Error("build.artifacts.output_seen.count mismatch")
	}
	if attr, ok := findAttr(attrs, "build.artifacts.output_from_cache.count"); !ok || attr.Value.AsInt64() != 16 {
		t.Error("build.artifacts.output_from_cache.count mismatch")
	}
}

// TestExtractBuildMetricsMemoryMetrics tests extracting memory metrics
func TestExtractBuildMetricsMemoryMetrics(t *testing.T) {
	metrics := &build_event_stream.BuildMetrics{
		MemoryMetrics: &build_event_stream.BuildMetrics_MemoryMetrics{
			UsedHeapSizePostBuild:    1073741824,
			PeakPostGcHeapSize:       536870912,
			GarbageMetrics: []*build_event_stream.BuildMetrics_MemoryMetrics_GarbageMetrics{
				{Type: "G1 Eden Space", GarbageCollected: 167772160},
			},
		},
	}

	attrs := ExtractBuildMetricsAttributes(metrics)

	if attr, ok := findAttr(attrs, "build.memory.used_heap_size_post_build"); !ok || attr.Value.AsInt64() != 1073741824 {
		t.Error("build.memory.used_heap_size_post_build mismatch")
	}
	if attr, ok := findAttr(attrs, "build.memory.garbage.G1 Eden Space.collected"); !ok || attr.Value.AsInt64() != 167772160 {
		t.Error("garbage metrics mismatch")
	}
}

// TestExtractBuildMetricsBuildGraphMetrics tests extracting build graph metrics
func TestExtractBuildMetricsBuildGraphMetrics(t *testing.T) {
	metrics := &build_event_stream.BuildMetrics{
		BuildGraphMetrics: &build_event_stream.BuildMetrics_BuildGraphMetrics{
			ActionLookupValueCount:                   4252,
			ActionCount:                              1503,
			InputFileConfiguredTargetCount:           3998,
			OutputFileConfiguredTargetCount:          12,
			OutputArtifactCount:                      2688,
			PostInvocationSkyframeNodeCount:          44376,
		},
	}

	attrs := ExtractBuildMetricsAttributes(metrics)

	if attr, ok := findAttr(attrs, "build.graph.action_lookup_value_count"); !ok || attr.Value.AsInt64() != 4252 {
		t.Error("build.graph.action_lookup_value_count mismatch")
	}
	if attr, ok := findAttr(attrs, "build.graph.action_count"); !ok || attr.Value.AsInt64() != 1503 {
		t.Error("build.graph.action_count mismatch")
	}
	if attr, ok := findAttr(attrs, "build.graph.output_artifact_count"); !ok || attr.Value.AsInt64() != 2688 {
		t.Error("build.graph.output_artifact_count mismatch")
	}
}

// TestExtractActionExecutedAttributes tests extracting from actionExecuted event
func TestExtractActionExecutedAttributes(t *testing.T) {
	action := &build_event_stream.ActionExecuted{
		Type:     "CppCompile",
		Success:  true,
		ExitCode: 0,
		Stdout:   &build_event_stream.File{Name: "stdout"},
		Stderr:   &build_event_stream.File{Name: "stderr"},
	}

	attrs, status := ExtractActionExecutedAttributes(action)

	if attr, ok := findAttr(attrs, "action.type"); !ok || attr.Value.AsString() != "CppCompile" {
		t.Error("action.type mismatch")
	}
	if attr, ok := findAttr(attrs, "action.success"); !ok || attr.Value.AsBool() != true {
		t.Error("action.success mismatch")
	}
	if attr, ok := findAttr(attrs, "action.exit_code"); !ok || attr.Value.AsInt64() != 0 {
		t.Error("action.exit_code mismatch")
	}
	if status != ActionStatusSuccess {
		t.Errorf("expected ActionStatusSuccess, got %v", status)
	}
}

// TestExtractActionExecutedAttributesFailed tests extracting from failed action
func TestExtractActionExecutedAttributesFailed(t *testing.T) {
	action := &build_event_stream.ActionExecuted{
		Type:     "CppCompile",
		Success:  false,
		ExitCode: 1,
	}

	attrs, status := ExtractActionExecutedAttributes(action)

	if attr, ok := findAttr(attrs, "action.success"); !ok || attr.Value.AsBool() != false {
		t.Error("action.success should be false")
	}
	if status != ActionStatusFailed {
		t.Errorf("expected ActionStatusFailed, got %v", status)
	}
	_ = attrs
}

// TestExtractNamedSetFiles tests extracting files from namedSetOfFiles
func TestExtractNamedSetFiles(t *testing.T) {
	namedSet := &build_event_stream.NamedSetOfFiles{
		Files: []*build_event_stream.File{
			{
				Name:       "deps/compile_policy/LICENSE",
				Digest:     "7dc0b59325e0a735cbdd089dc802c7e68e3796b36468c72e4b640ac0c0f0557c",
				Length:     11342,
				PathPrefix: []string{"bazel-out", "darwin_arm64-fastbuild", "bin"},
			},
			{
				Name:   "deps/gstatus/LICENSE",
				Digest: "589ed823e9a84c56feb95ac58e7cf384626b9cbf4fda2a907bc36e103de1bad2",
				Length: 35141,
			},
		},
	}

	files := ExtractNamedSetFiles(namedSet)

	if len(files) != 2 {
		t.Errorf("expected 2 files, got %d", len(files))
	}
	if files[0].Name != "deps/compile_policy/LICENSE" {
		t.Error("first file name mismatch")
	}
	if files[0].PathPrefix != "bazel-out/darwin_arm64-fastbuild/bin" {
		t.Errorf("first file path prefix mismatch: got %s", files[0].PathPrefix)
	}
	if files[1].PathPrefix != "" {
		t.Error("second file should have no path prefix")
	}
}

// TestExtractPatternFromEvent tests extracting patterns from pattern event
func TestExtractPatternFromEvent(t *testing.T) {
	patterns := []string{"//deps/...", "//pkg/..."}
	result := ExtractPatterns(patterns)

	if len(result) != 2 {
		t.Errorf("expected 2 patterns, got %d", len(result))
	}
	if result[0] != "//deps/..." || result[1] != "//pkg/..." {
		t.Error("pattern mismatch")
	}
}

