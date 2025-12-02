package bes

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log/slog"
	"sync"
	"time"

	build_event_stream "github.com/JSGette/bazel_conduit/proto/build_event_stream"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.28.0"
	"go.opentelemetry.io/otel/trace"
	build "google.golang.org/genproto/googleapis/devtools/build/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// BuildEventParser manages parsing BEP events and creating OpenTelemetry spans
type BuildEventParser struct {
	logger *slog.Logger
	tracer trace.Tracer

	// Track active builds (invocationID -> BuildContext)
	activeBuilds sync.Map

	mu sync.RWMutex
}

// BuildContext tracks span state for a single build invocation
type BuildContext struct {
	buildID      string
	invocationID string
	traceID      trace.TraceID
	rootCtx      context.Context
	rootSpan     trace.Span // Root span for the entire invocation

	// Map event IDs to their contexts (for parenting)
	eventContexts map[string]context.Context // eventID -> context containing span
	eventSpans    map[string]trace.Span      // eventID -> actual span

	// Track parent relationships from children announcements
	childToParent map[string]string // childEventID -> parentEventID

	// Timing and lifecycle
	startTime       time.Time
	lastActivity    time.Time
	finished        bool
	rootSpanStarted bool // Track if root span has been created

	mu sync.RWMutex
}

func newTracerProvider(exp sdktrace.SpanExporter) *sdktrace.TracerProvider {
	// Create resource with service name (schemaless to avoid conflicts)
	r, err := resource.New(
		context.Background(),
		resource.WithAttributes(
			semconv.ServiceName("bazel-conduit"),
		),
	)

	if err != nil {
		panic(err)
	}

	return sdktrace.NewTracerProvider(
		//	sdktrace.WithBatcher(exp), TODO: Needed for Exporter. Implement this for Datadog.
		sdktrace.WithResource(r),
	)
}

func NewBuildEventParser(logger *slog.Logger) *BuildEventParser {
	if logger == nil {
		logger = slog.Default()
	}

	// TODO: Implement Exporter to pass instead of nil.
	tp := newTracerProvider(nil)
	otel.SetTracerProvider(tp)

	tracer := tp.Tracer("github.com/JSGette/bazel_conduit/internal/bes")

	return &BuildEventParser{
		logger: logger,
		tracer: tracer,
	}
}

// ParseBuildEventStreamBuildEvent processes a BEP event and creates nested OTel spans
func (p *BuildEventParser) ParseBuildEventStreamBuildEvent(
	ctx context.Context,
	buildID string,
	invocationID string,
	event *build.BuildEvent,
) error {
	// 1. Handle ComponentStreamFinished (signals end of stream)
	if csf := event.GetComponentStreamFinished(); csf != nil {
		p.logger.Debug("Received ComponentStreamFinished",
			"build_id", buildID,
			"invocation_id", invocationID,
			"type", csf.Type.String())

		// End all open child spans (but not the root span - that's for lifecycle events)
		buildCtx := p.getBuildContext(invocationID)
		if buildCtx != nil {
			buildCtx.finishChildSpans()
		}
		return nil
	}

	// 2. Check if this is a BazelEvent (most common case)
	if event.GetBazelEvent() == nil {
		// Log other non-BazelEvent types for debugging
		p.logger.Debug("Received non-BazelEvent",
			"build_id", buildID,
			"invocation_id", invocationID,
			"event_type", fmt.Sprintf("%T", event.GetEvent()),
			"event", event.String())
		return nil
	}

	// 2. Unmarshal BazelEvent from Any
	bazelEvent := &build_event_stream.BuildEvent{}
	if err := event.GetBazelEvent().UnmarshalTo(bazelEvent); err != nil {
		p.logger.Error("Failed to unmarshal Bazel event", "error", err)
		return err
	}

	// 3. Extract event ID and type
	eventID := extractEventID(bazelEvent.Id)
	eventType := getEventType(bazelEvent)

	p.logger.Debug("Processing event",
		"event_id", eventID,
		"event_type", eventType,
		"build_id", buildID,
		"invocation_id", invocationID)

	// 4. Get or create build context
	buildCtx := p.getOrCreateBuildContext(ctx, buildID, invocationID)

	// 5. Find parent context
	parentCtx := buildCtx.findParentContext(eventID)

	// 6. Create span with parent
	spanCtx, span := p.tracer.Start(
		parentCtx,
		eventType, // Use event type as span name
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithTimestamp(getEventStartTime(bazelEvent)),
	)

	// 7. Add attributes
	p.addSpanAttributes(span, bazelEvent, buildID, invocationID, eventID)

	// 8. Store span context for children
	buildCtx.storeEventContext(eventID, spanCtx, span)

	// 9. Register announced children
	buildCtx.registerChildren(eventID, bazelEvent.Children)

	// 10. Handle span lifecycle
	if shouldEndSpanImmediately(bazelEvent) {
		span.End(trace.WithTimestamp(getEventEndTime(bazelEvent)))
	}
	// Otherwise, span stays open for children to nest under

	// 11. On BuildFinished, end child spans (root span ends via lifecycle event)
	if eventType == "BuildFinished" {
		buildCtx.finishChildSpans()
		// Note: Don't cleanup yet - wait for InvocationAttemptFinished lifecycle event
	}

	return nil
}

// getBuildContext gets existing build context (without creating)
func (p *BuildEventParser) getBuildContext(invocationID string) *BuildContext {
	if val, ok := p.activeBuilds.Load(invocationID); ok {
		return val.(*BuildContext)
	}
	return nil
}

// getOrCreateBuildContext gets existing or creates new build context
func (p *BuildEventParser) getOrCreateBuildContext(
	ctx context.Context,
	buildID string,
	invocationID string,
) *BuildContext {
	// Try to load existing
	if val, ok := p.activeBuilds.Load(invocationID); ok {
		return val.(*BuildContext)
	}

	// Create new - use invocationID as the trace ID
	traceID := generateTraceIDFromInvocationID(invocationID)
	rootCtx := trace.ContextWithSpanContext(ctx, trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: traceID,
	}))

	buildCtx := &BuildContext{
		buildID:         buildID,
		invocationID:    invocationID,
		traceID:         traceID,
		rootCtx:         rootCtx,
		eventContexts:   make(map[string]context.Context),
		eventSpans:      make(map[string]trace.Span),
		childToParent:   make(map[string]string),
		startTime:       time.Now(),
		lastActivity:    time.Now(),
		rootSpanStarted: false,
	}

	p.activeBuilds.Store(invocationID, buildCtx)

	p.logger.Info("Created build context",
		"build_id", buildID,
		"invocation_id", invocationID,
		"trace_id", traceID.String())

	return buildCtx
}

// StartInvocationSpan creates the root span for the invocation (from lifecycle event)
func (p *BuildEventParser) StartInvocationSpan(
	ctx context.Context,
	buildID string,
	invocationID string,
	eventType string,
	eventTime *timestamppb.Timestamp,
) error {
	buildCtx := p.getOrCreateBuildContext(ctx, buildID, invocationID)

	buildCtx.mu.Lock()
	defer buildCtx.mu.Unlock()

	// Don't create multiple root spans
	if buildCtx.rootSpanStarted {
		p.logger.Debug("Root span already started",
			"invocation_id", invocationID)
		return nil
	}

	// Use event time if available, otherwise current time
	startTime := time.Now()
	if eventTime != nil && eventTime.IsValid() {
		startTime = eventTime.AsTime()
	}

	// Create root span for the entire invocation
	spanCtx, span := p.tracer.Start(
		buildCtx.rootCtx,
		"Bazel Invocation",
		trace.WithSpanKind(trace.SpanKindServer),
		trace.WithTimestamp(startTime),
	)

	// Add invocation attributes
	span.SetAttributes(
		attribute.String("invocation.id", invocationID),
		attribute.String("invocation.build_id", buildID),
		attribute.String("invocation.event", eventType),
	)

	buildCtx.rootSpan = span
	buildCtx.rootCtx = spanCtx
	buildCtx.rootSpanStarted = true
	buildCtx.startTime = startTime

	p.logger.Info("Started root invocation span",
		"invocation_id", invocationID,
		"build_id", buildID,
		"trace_id", buildCtx.traceID.String(),
		"start_time", startTime)

	return nil
}

// EndInvocationSpan ends the root span for the invocation (from lifecycle event)
func (p *BuildEventParser) EndInvocationSpan(
	invocationID string,
	success bool,
	eventTime *timestamppb.Timestamp,
) error {
	buildCtx := p.getBuildContext(invocationID)
	if buildCtx == nil {
		p.logger.Warn("No build context found for invocation",
			"invocation_id", invocationID)
		return nil
	}

	buildCtx.mu.Lock()

	if !buildCtx.rootSpanStarted {
		buildCtx.mu.Unlock()
		p.logger.Debug("Root span not started",
			"invocation_id", invocationID)
		return nil
	}

	if buildCtx.rootSpan != nil && buildCtx.rootSpan.IsRecording() {
		// Set status based on success
		if success {
			buildCtx.rootSpan.SetStatus(codes.Ok, "Invocation completed successfully")
		} else {
			buildCtx.rootSpan.SetStatus(codes.Error, "Invocation failed")
		}

		// Use event time if available, otherwise current time
		endTime := time.Now()
		if eventTime != nil && eventTime.IsValid() {
			endTime = eventTime.AsTime()
		}

		buildCtx.rootSpan.End(trace.WithTimestamp(endTime))

		p.logger.Info("Ended root invocation span",
			"invocation_id", invocationID,
			"success", success,
			"end_time", endTime,
			"duration", endTime.Sub(buildCtx.startTime))
	}

	buildCtx.mu.Unlock()

	// Schedule cleanup after ending the root span
	p.scheduleCleanup(invocationID)

	return nil
}

// findParentContext finds the parent context for an event
func (bc *BuildContext) findParentContext(eventID string) context.Context {
	bc.mu.RLock()
	defer bc.mu.RUnlock()

	// Check if this event was announced as a child
	if parentID, ok := bc.childToParent[eventID]; ok {
		if parentCtx, ok := bc.eventContexts[parentID]; ok {
			return parentCtx
		}
	}

	// No parent found, use root context
	return bc.rootCtx
}

// storeEventContext stores the span context for this event
func (bc *BuildContext) storeEventContext(
	eventID string,
	ctx context.Context,
	span trace.Span,
) {
	bc.mu.Lock()
	defer bc.mu.Unlock()

	bc.eventContexts[eventID] = ctx
	bc.eventSpans[eventID] = span
	bc.lastActivity = time.Now()
}

// registerChildren registers child event IDs
func (bc *BuildContext) registerChildren(
	parentID string,
	children []*build_event_stream.BuildEventId,
) {
	bc.mu.Lock()
	defer bc.mu.Unlock()

	for _, childID := range children {
		childIDStr := extractEventID(childID)
		if childIDStr != "" {
			bc.childToParent[childIDStr] = parentID
		}
	}
}

// finishChildSpans ends all open child spans (but not the root span)
func (bc *BuildContext) finishChildSpans() {
	bc.mu.Lock()
	defer bc.mu.Unlock()

	// End all remaining open child spans
	for _, span := range bc.eventSpans {
		if span.IsRecording() {
			span.End()
		}
	}
}

// finishBuild marks the build as finished and ends all spans including root (idempotent)
func (bc *BuildContext) finishBuild() {
	bc.mu.Lock()
	defer bc.mu.Unlock()

	// Already finished, nothing to do
	if bc.finished {
		return
	}

	bc.finished = true

	// End all remaining open child spans
	for _, span := range bc.eventSpans {
		if span.IsRecording() {
			span.End()
		}
	}

	// End root span if it exists
	if bc.rootSpan != nil && bc.rootSpan.IsRecording() {
		bc.rootSpan.End()
	}
}

// scheduleCleanup schedules cleanup of build context
func (p *BuildEventParser) scheduleCleanup(invocationID string) {
	go func() {
		// Wait before cleanup to allow late events
		time.Sleep(5 * time.Minute)
		p.activeBuilds.Delete(invocationID)
		p.logger.Info("Cleaned up build context", "invocation_id", invocationID)
	}()
}

// extractEventID converts BuildEventId proto to a string key
func extractEventID(id *build_event_stream.BuildEventId) string {
	if id == nil {
		return ""
	}

	switch v := id.Id.(type) {
	case *build_event_stream.BuildEventId_Started:
		return "BuildStarted"
	case *build_event_stream.BuildEventId_BuildFinished:
		return "BuildFinished"
	case *build_event_stream.BuildEventId_Pattern:
		return fmt.Sprintf("pattern:%v", v.Pattern.Pattern)
	case *build_event_stream.BuildEventId_TargetConfigured:
		return fmt.Sprintf("target_configured:%s:%s",
			v.TargetConfigured.Label, v.TargetConfigured.Aspect)
	case *build_event_stream.BuildEventId_TargetCompleted:
		return fmt.Sprintf("target_completed:%s:%s",
			v.TargetCompleted.Label, v.TargetCompleted.Aspect)
	case *build_event_stream.BuildEventId_ActionCompleted:
		return fmt.Sprintf("action:%s:%s",
			v.ActionCompleted.Label, v.ActionCompleted.PrimaryOutput)
	case *build_event_stream.BuildEventId_TestResult:
		return fmt.Sprintf("test_result:%s:run%d:shard%d:attempt%d",
			v.TestResult.Label, v.TestResult.Run,
			v.TestResult.Shard, v.TestResult.Attempt)
	case *build_event_stream.BuildEventId_TestSummary:
		return fmt.Sprintf("test_summary:%s", v.TestSummary.Label)
	case *build_event_stream.BuildEventId_NamedSet:
		return fmt.Sprintf("named_set:%s", v.NamedSet.Id)
	case *build_event_stream.BuildEventId_Progress:
		return fmt.Sprintf("progress:%d", v.Progress.OpaqueCount)
	case *build_event_stream.BuildEventId_UnstructuredCommandLine:
		return "unstructured_command_line"
	case *build_event_stream.BuildEventId_StructuredCommandLine:
		return fmt.Sprintf("structured_command_line:%s",
			v.StructuredCommandLine.CommandLineLabel)
	case *build_event_stream.BuildEventId_OptionsParsed:
		return "options_parsed"
	case *build_event_stream.BuildEventId_WorkspaceStatus:
		return "workspace_status"
	case *build_event_stream.BuildEventId_Configuration:
		return fmt.Sprintf("configuration:%s", v.Configuration.Id)
	case *build_event_stream.BuildEventId_BuildMetrics:
		return "build_metrics"
	case *build_event_stream.BuildEventId_BuildToolLogs:
		return "build_tool_logs"
	case *build_event_stream.BuildEventId_Workspace:
		return "workspace_info"
	case *build_event_stream.BuildEventId_BuildMetadata:
		return "build_metadata"
	case *build_event_stream.BuildEventId_ConvenienceSymlinksIdentified:
		return "convenience_symlinks_identified"
	default:
		return fmt.Sprintf("unknown:%T", v)
	}
}

// getEventType extracts the event type name from payload (used as span name)
func getEventType(event *build_event_stream.BuildEvent) string {
	switch event.Payload.(type) {
	case *build_event_stream.BuildEvent_Started:
		return "BuildStarted"
	case *build_event_stream.BuildEvent_Finished:
		return "BuildFinished"
	case *build_event_stream.BuildEvent_Progress:
		return "Progress"
	case *build_event_stream.BuildEvent_Aborted:
		return "Aborted"
	case *build_event_stream.BuildEvent_UnstructuredCommandLine:
		return "UnstructuredCommandLine"
	case *build_event_stream.BuildEvent_StructuredCommandLine:
		return "StructuredCommandLine"
	case *build_event_stream.BuildEvent_OptionsParsed:
		return "OptionsParsed"
	case *build_event_stream.BuildEvent_WorkspaceStatus:
		return "WorkspaceStatus"
	case *build_event_stream.BuildEvent_Expanded:
		return "PatternExpanded"
	case *build_event_stream.BuildEvent_Configured:
		return "TargetConfigured"
	case *build_event_stream.BuildEvent_Completed:
		return "TargetComplete"
	case *build_event_stream.BuildEvent_Action:
		return "ActionExecuted"
	case *build_event_stream.BuildEvent_NamedSetOfFiles:
		return "NamedSetOfFiles"
	case *build_event_stream.BuildEvent_TestResult:
		return "TestResult"
	case *build_event_stream.BuildEvent_TestSummary:
		return "TestSummary"
	case *build_event_stream.BuildEvent_BuildMetrics:
		return "BuildMetrics"
	case *build_event_stream.BuildEvent_BuildToolLogs:
		return "BuildToolLogs"
	case *build_event_stream.BuildEvent_WorkspaceInfo:
		return "WorkspaceInfo"
	case *build_event_stream.BuildEvent_BuildMetadata:
		return "BuildMetadata"
	case *build_event_stream.BuildEvent_ConvenienceSymlinksIdentified:
		return "ConvenienceSymlinksIdentified"
	default:
		return "UnknownEvent"
	}
}

// addSpanAttributes adds attributes to the span based on event type
func (p *BuildEventParser) addSpanAttributes(
	span trace.Span,
	event *build_event_stream.BuildEvent,
	buildID string,
	invocationID string,
	eventID string,
) {
	// Common attributes
	span.SetAttributes(
		attribute.String("build.id", buildID),
		attribute.String("build.invocation_id", invocationID),
		attribute.String("build.event_id", eventID),
	)

	// Event-specific attributes
	switch payload := event.Payload.(type) {
	case *build_event_stream.BuildEvent_Started:
		span.SetAttributes(
			attribute.String("bazel.uuid", payload.Started.Uuid),
			attribute.String("bazel.command", payload.Started.Command),
			attribute.String("bazel.workspace_dir", payload.Started.WorkspaceDirectory),
			attribute.String("bazel.build_tool_version", payload.Started.BuildToolVersion),
		)

	case *build_event_stream.BuildEvent_Finished:
		span.SetAttributes(
			attribute.Bool("bazel.success", payload.Finished.OverallSuccess),
			attribute.Int("bazel.exit_code", int(payload.Finished.ExitCode.Code)),
		)
		if !payload.Finished.OverallSuccess {
			span.SetStatus(codes.Error, "Build failed")
		} else {
			span.SetStatus(codes.Ok, "Build succeeded")
		}

	case *build_event_stream.BuildEvent_Configured:
		if payload.Configured != nil {
			span.SetAttributes(
				attribute.String("bazel.target.kind", payload.Configured.TargetKind),
			)
		}

	case *build_event_stream.BuildEvent_Completed:
		span.SetAttributes(
			attribute.Bool("bazel.target.success", payload.Completed.Success),
		)
		if !payload.Completed.Success {
			span.SetStatus(codes.Error, "Target failed")
		} else {
			span.SetStatus(codes.Ok, "Target succeeded")
		}

	case *build_event_stream.BuildEvent_Action:
		span.SetAttributes(
			attribute.Bool("bazel.action.success", payload.Action.Success),
			attribute.String("bazel.action.type", payload.Action.Type),
		)
		if payload.Action.ExitCode != 0 {
			span.SetStatus(codes.Error, fmt.Sprintf("Action failed with exit code %d", payload.Action.ExitCode))
		}

	case *build_event_stream.BuildEvent_TestResult:
		span.SetAttributes(
			attribute.String("bazel.test.status", payload.TestResult.Status.String()),
		)
		if payload.TestResult.Status != build_event_stream.TestStatus_PASSED {
			span.SetStatus(codes.Error, fmt.Sprintf("Test %s", payload.TestResult.Status.String()))
		}

	case *build_event_stream.BuildEvent_TestSummary:
		span.SetAttributes(
			attribute.String("bazel.test.overall_status", payload.TestSummary.OverallStatus.String()),
			attribute.Int("bazel.test.total_runs", int(payload.TestSummary.TotalRunCount)),
		)
	}
}

// shouldEndSpanImmediately determines if a span should end immediately
func shouldEndSpanImmediately(event *build_event_stream.BuildEvent) bool {
	switch event.Payload.(type) {
	// These are point-in-time events - end immediately
	case *build_event_stream.BuildEvent_Progress,
		*build_event_stream.BuildEvent_UnstructuredCommandLine,
		*build_event_stream.BuildEvent_StructuredCommandLine,
		*build_event_stream.BuildEvent_OptionsParsed,
		*build_event_stream.BuildEvent_WorkspaceStatus,
		*build_event_stream.BuildEvent_BuildMetrics,
		*build_event_stream.BuildEvent_BuildToolLogs,
		*build_event_stream.BuildEvent_BuildMetadata,
		*build_event_stream.BuildEvent_WorkspaceInfo,
		*build_event_stream.BuildEvent_ConvenienceSymlinksIdentified:
		return true

	// These represent durations - keep open for children
	case *build_event_stream.BuildEvent_Started,
		*build_event_stream.BuildEvent_Configured,
		*build_event_stream.BuildEvent_Completed,
		*build_event_stream.BuildEvent_Action,
		*build_event_stream.BuildEvent_TestSummary,
		*build_event_stream.BuildEvent_Expanded:
		return false

	// BuildFinished and test results end immediately
	case *build_event_stream.BuildEvent_Finished,
		*build_event_stream.BuildEvent_TestResult:
		return true

	default:
		return true
	}
}

// getEventStartTime extracts start time from event
func getEventStartTime(event *build_event_stream.BuildEvent) time.Time {
	if started := event.GetStarted(); started != nil && started.StartTime != nil {
		return started.StartTime.AsTime()
	}
	return time.Now()
}

// getEventEndTime extracts end time from event
func getEventEndTime(event *build_event_stream.BuildEvent) time.Time {
	// For most events, end time = start time (point events)
	// Could be enhanced to track actual durations
	return getEventStartTime(event)
}

// generateTraceIDFromInvocationID creates a trace ID from invocation ID
func generateTraceIDFromInvocationID(invocationID string) trace.TraceID {
	hash := sha256.Sum256([]byte(invocationID))
	var traceID trace.TraceID
	copy(traceID[:], hash[:16]) // Use first 16 bytes (128 bits)

	// Log the mapping for debugging
	hexTraceID := hex.EncodeToString(traceID[:])
	slog.Debug("Generated trace ID",
		"invocation_id", invocationID,
		"trace_id", hexTraceID)

	return traceID
}
