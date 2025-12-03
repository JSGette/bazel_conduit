package bes

import (
	"context"
	"log/slog"
	"time"

	build_event_stream "github.com/JSGette/bazel_conduit/proto/build_event_stream"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	build "google.golang.org/genproto/googleapis/devtools/build/v1"
)

// BuildEventParser processes BEP events and converts them to OTEL traces.
// It maintains state across events to build complete traces and spans.
type BuildEventParser struct {
	logger        *slog.Logger
	exporter      TraceExporter
	traceCtx      *TraceContext
	spanRegistry  *SpanRegistry
	namedSetCache *NamedSetCache

	// Legacy fields for backward compatibility
	toJson bool
	toOTel bool

	// OTEL tracing infrastructure
	tracerProvider *sdktrace.TracerProvider
	tracer         trace.Tracer
	spanRecorder   *tracetest.SpanRecorder

	// Root span and context for the entire build trace
	rootSpan trace.Span
	ctx      context.Context

	// Flag to prevent double finalization
	traceFinalized bool
}

// NewBuildEventParserWithExporter creates a new BuildEventParser with a custom exporter.
func NewBuildEventParserWithExporter(logger *slog.Logger, exporter TraceExporter) *BuildEventParser {
	if logger == nil {
		logger = slog.Default()
	}

	// Create a span recorder for capturing spans
	spanRecorder := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(spanRecorder))

	return &BuildEventParser{
		logger:         logger,
		exporter:       exporter,
		spanRegistry:   NewSpanRegistry(),
		namedSetCache:  NewNamedSetCache(),
		tracerProvider: tp,
		tracer:         tp.Tracer("bazel-conduit"),
		spanRecorder:   spanRecorder,
	}
}

// NewBuildEventParser creates a new BuildEventParser (legacy interface).
// For OTEL export, use NewBuildEventParserWithExporter instead.
func NewBuildEventParser(logger *slog.Logger, toJson bool, toOTel bool) *BuildEventParser {
	if logger == nil {
		logger = slog.Default()
	}

	return &BuildEventParser{
		logger:        logger,
		toJson:        toJson,
		toOTel:        toOTel,
		spanRegistry:  NewSpanRegistry(),
		namedSetCache: NewNamedSetCache(),
	}
}

// ProcessEvent processes a single BEP event and updates internal state.
// This is the main entry point for the new OTEL-based processing.
func (p *BuildEventParser) ProcessEvent(ctx context.Context, event *build.BuildEvent) error {
	// Extract the Bazel event from the wrapper
	bazelEvent := &build_event_stream.BuildEvent{}
	if err := event.GetBazelEvent().UnmarshalTo(bazelEvent); err != nil {
		p.logger.Error("Failed to unmarshal Bazel event", "error", err)
		return err
	}

	// Extract event timestamp from the wrapper
	eventTime := time.Now()
	if event.GetEventTime() != nil {
		eventTime = event.GetEventTime().AsTime()
	}

	// Route to appropriate handler based on event type
	return p.handleEvent(ctx, eventTime, bazelEvent)
}

// handleEvent routes the event to the appropriate handler based on its ID type.
func (p *BuildEventParser) handleEvent(ctx context.Context, eventTime time.Time, event *build_event_stream.BuildEvent) error {
	switch id := event.GetId().GetId().(type) {
	case *build_event_stream.BuildEventId_Started:
		return p.handleStarted(ctx, eventTime, event)

	case *build_event_stream.BuildEventId_Pattern:
		return p.handlePattern(ctx, eventTime, id.Pattern, event)

	case *build_event_stream.BuildEventId_TargetConfigured:
		return p.handleTargetConfigured(ctx, eventTime, id.TargetConfigured, event)

	case *build_event_stream.BuildEventId_TargetCompleted:
		return p.handleTargetCompleted(ctx, eventTime, id.TargetCompleted, event)

	case *build_event_stream.BuildEventId_NamedSet:
		return p.handleNamedSet(ctx, eventTime, id.NamedSet, event)

	case *build_event_stream.BuildEventId_BuildFinished:
		return p.handleBuildFinished(ctx, eventTime, event)

	case *build_event_stream.BuildEventId_BuildMetrics:
		return p.handleBuildMetrics(ctx, eventTime, event)

	case *build_event_stream.BuildEventId_Configuration:
		return p.handleConfiguration(ctx, eventTime, event)

	case *build_event_stream.BuildEventId_WorkspaceStatus:
		return p.handleWorkspaceStatus(ctx, eventTime, event)

	case *build_event_stream.BuildEventId_OptionsParsed:
		return p.handleOptionsParsed(ctx, eventTime, event)

	default:
		// Ignore other event types (progress, etc.)
		return nil
	}
}

// handleStarted initializes the trace from a started event.
func (p *BuildEventParser) handleStarted(ctx context.Context, eventTime time.Time, event *build_event_stream.BuildEvent) error {
	started := event.GetStarted()
	if started == nil {
		return nil
	}

	// Guard against multiple Started events
	if p.traceCtx != nil || p.rootSpan != nil {
		p.logger.Warn("Received multiple BuildStarted events, ignoring subsequent ones")
		return nil
	}

	// Parse start time
	startTime := time.Now()
	if started.GetStartTime() != nil {
		startTime = started.GetStartTime().AsTime()
	}

	// Create trace context
	traceCtx, err := NewTraceContext(started.GetUuid(), startTime)
	if err != nil {
		p.logger.Error("Failed to create trace context", "error", err)
		return err
	}

	p.traceCtx = traceCtx

	// Add attributes from started event
	attrs := ExtractStartedAttributes(started)
	p.traceCtx.AddAttributes(attrs)

	// Create a root span with the correct trace ID
	// We need to create a SpanContext with our desired trace ID
	spanID := trace.SpanID{}
	// Generate a span ID for the root span (can be derived from trace ID or random)
	copy(spanID[:], traceCtx.TraceID[:8]) // Use first 8 bytes of trace ID

	spanContext := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    traceCtx.TraceID,
		SpanID:     spanID,
		TraceFlags: trace.FlagsSampled,
	})

	// Create a context with this span context
	ctxWithSpan := trace.ContextWithSpanContext(context.Background(), spanContext)

	// Start the root span using this context
	p.ctx, p.rootSpan = p.tracer.Start(ctxWithSpan, "Bazel Build",
		trace.WithTimestamp(startTime),
		trace.WithAttributes(attrs...),
	)

	p.logger.Info("Trace initialized",
		"trace_id", traceCtx.TraceID.String(),
		"span_id", spanID.String(),
		"invocation_id", started.GetUuid(),
	)

	return nil
}

// handlePattern sets the trace name from the pattern event.
func (p *BuildEventParser) handlePattern(ctx context.Context, eventTime time.Time, id *build_event_stream.BuildEventId_PatternExpandedId, event *build_event_stream.BuildEvent) error {
	if p.traceCtx == nil {
		return nil
	}

	patterns := id.GetPattern()
	p.traceCtx.SetTraceName(patterns)
	p.traceCtx.AddAttribute(attribute.String("build.pattern", p.traceCtx.TraceName))

	p.logger.Debug("Trace name set", "pattern", p.traceCtx.TraceName)
	return nil
}

// handleTargetConfigured creates a new span for a configured target.
func (p *BuildEventParser) handleTargetConfigured(ctx context.Context, eventTime time.Time, id *build_event_stream.BuildEventId_TargetConfiguredId, event *build_event_stream.BuildEvent) error {
	if p.traceCtx == nil || p.tracer == nil || p.ctx == nil {
		return nil
	}

	configured := event.GetConfigured()
	if configured == nil {
		return nil
	}

	label := id.GetLabel()

	// Create a new span for this target as a child of the root span
	// Use p.ctx which contains the root span context with the correct trace ID
	// Use the actual event timestamp for the span start time
	_, span := p.tracer.Start(p.ctx, label,
		trace.WithTimestamp(eventTime),
	)

	// Add attributes
	configID := ""
	if len(event.GetChildren()) > 0 {
		for _, child := range event.GetChildren() {
			if tc := child.GetTargetCompleted(); tc != nil {
				if tc.GetConfiguration() != nil {
					configID = tc.GetConfiguration().GetId()
				}
				break
			}
		}
	}

	attrs := ExtractTargetConfiguredAttributes(configured, label, configID)
	span.SetAttributes(attrs...)

	// Register the span with the event timestamp
	p.spanRegistry.AddTarget(label, span, eventTime)

	p.logger.Debug("Target span started",
		"label", label,
		"kind", configured.GetTargetKind(),
		"trace_id", p.traceCtx.TraceID.String(),
	)

	return nil
}

// handleTargetCompleted completes and exports the span for a completed target.
func (p *BuildEventParser) handleTargetCompleted(ctx context.Context, eventTime time.Time, id *build_event_stream.BuildEventId_TargetCompletedId, event *build_event_stream.BuildEvent) error {
	label := id.GetLabel()

	span, exists := p.spanRegistry.GetTarget(label)
	if !exists {
		p.logger.Warn("Received targetCompleted for unknown target", "label", label)
		return nil
	}

	completed := event.GetCompleted()
	if completed == nil {
		return nil
	}

	// Resolve output files from namedSets
	var outputFiles []string
	for _, og := range completed.GetOutputGroup() {
		for _, fs := range og.GetFileSets() {
			files, exists := p.namedSetCache.Get(fs.GetId())
			if exists {
				for _, f := range files {
					outputFiles = append(outputFiles, f.FullPath())
				}
			}
		}
	}

	// Extract attributes and status
	attrs, status := ExtractTargetCompletedAttributes(completed, outputFiles)
	span.SetAttributes(attrs...)

	// Set span status
	switch status {
	case TargetStatusSuccess:
		span.SetStatus(codes.Ok, "Target built successfully")
	case TargetStatusFailed:
		span.SetStatus(codes.Error, "Target build failed")
	case TargetStatusAborted:
		span.SetStatus(codes.Error, "Target aborted")
	}

	// End the span with the actual event timestamp
	span.End(trace.WithTimestamp(eventTime))

	// Export the span - get the most recently ended span
	if p.exporter != nil && p.spanRecorder != nil {
		endedSpans := p.spanRecorder.Ended()
		if len(endedSpans) > 0 {
			// The most recently ended span should be the last one
			lastSpan := endedSpans[len(endedSpans)-1]
			// Verify it's the span we just ended
			if lastSpan.Name() == label {
				if err := p.exporter.ExportSpan(ctx, lastSpan); err != nil {
					p.logger.Error("Failed to export span", "label", label, "error", err)
				}
			}
		}
	}

	// Remove from registry
	p.spanRegistry.RemoveTarget(label)

	p.logger.Debug("Target span completed",
		"label", label,
		"success", completed.GetSuccess(),
		"output_files", len(outputFiles),
	)

	return nil
}

// handleNamedSet caches file information from a namedSet event.
func (p *BuildEventParser) handleNamedSet(ctx context.Context, eventTime time.Time, id *build_event_stream.BuildEventId_NamedSetOfFilesId, event *build_event_stream.BuildEvent) error {
	namedSet := event.GetNamedSetOfFiles()
	if namedSet == nil {
		return nil
	}

	files := ExtractNamedSetFiles(namedSet)
	p.namedSetCache.Store(id.GetId(), files)

	p.logger.Debug("NamedSet cached", "id", id.GetId(), "files", len(files))
	return nil
}

// handleBuildFinished updates trace with build finish information.
func (p *BuildEventParser) handleBuildFinished(ctx context.Context, eventTime time.Time, event *build_event_stream.BuildEvent) error {
	if p.traceCtx == nil {
		return nil
	}

	finished := event.GetFinished()
	if finished == nil {
		return nil
	}

	// Set end time
	endTime := time.Now()
	if finished.GetFinishTime() != nil {
		endTime = finished.GetFinishTime().AsTime()
	}
	p.traceCtx.SetEndTime(endTime)

	// Add attributes
	attrs := ExtractBuildFinishedAttributes(finished)
	p.traceCtx.AddAttributes(attrs)

	p.logger.Info("Build finished",
		"success", finished.GetOverallSuccess(),
		"exit_code", finished.GetExitCode().GetName(),
	)

	return nil
}

// handleBuildMetrics finalizes the trace when lastMessage is received.
func (p *BuildEventParser) handleBuildMetrics(ctx context.Context, eventTime time.Time, event *build_event_stream.BuildEvent) error {
	if p.traceCtx == nil {
		return nil
	}

	metrics := event.GetBuildMetrics()
	if metrics != nil {
		attrs := ExtractBuildMetricsAttributes(metrics)
		p.traceCtx.AddAttributes(attrs)
	}

	// Check for lastMessage - indicates end of stream
	if event.GetLastMessage() {
		return p.finalizeTrace(ctx)
	}

	return nil
}

// handleConfiguration adds configuration attributes to the trace.
func (p *BuildEventParser) handleConfiguration(ctx context.Context, eventTime time.Time, event *build_event_stream.BuildEvent) error {
	if p.traceCtx == nil {
		return nil
	}

	config := event.GetConfiguration()
	if config == nil {
		return nil
	}

	attrs := ExtractConfigurationAttributes(config)
	p.traceCtx.AddAttributes(attrs)

	return nil
}

// handleWorkspaceStatus adds workspace status attributes to the trace.
func (p *BuildEventParser) handleWorkspaceStatus(ctx context.Context, eventTime time.Time, event *build_event_stream.BuildEvent) error {
	if p.traceCtx == nil {
		return nil
	}

	status := event.GetWorkspaceStatus()
	if status == nil {
		return nil
	}

	attrs := ExtractWorkspaceStatusAttributes(status)
	p.traceCtx.AddAttributes(attrs)

	return nil
}

// handleOptionsParsed adds options attributes to the trace.
func (p *BuildEventParser) handleOptionsParsed(ctx context.Context, eventTime time.Time, event *build_event_stream.BuildEvent) error {
	if p.traceCtx == nil {
		return nil
	}

	options := event.GetOptionsParsed()
	if options == nil {
		return nil
	}

	// Add cmdLine as attributes
	if len(options.GetCmdLine()) > 0 {
		p.traceCtx.AddAttribute(attribute.StringSlice("build.cmd_line", options.GetCmdLine()))
	}
	if len(options.GetExplicitCmdLine()) > 0 {
		p.traceCtx.AddAttribute(attribute.StringSlice("build.explicit_cmd_line", options.GetExplicitCmdLine()))
	}

	return nil
}

// finalizeTrace exports the trace metadata and handles any orphaned spans.
func (p *BuildEventParser) finalizeTrace(ctx context.Context) error {
	// Guard against double finalization
	if p.traceFinalized {
		p.logger.Debug("Trace already finalized, skipping")
		return nil
	}
	p.traceFinalized = true

	// Log any orphaned spans (configured but not completed)
	// These are typically platform-specific targets that were configured but skipped
	orphanedLabels := p.spanRegistry.GetAllLabels()
	if len(orphanedLabels) > 0 {
		p.logger.Info("Skipping orphaned spans (likely platform-specific targets)",
			"count", len(orphanedLabels),
			"labels", orphanedLabels,
		)

		// End orphaned spans without error status (they're just skipped)
		// Don't export them to keep the trace clean
		for _, label := range orphanedLabels {
			if span, exists := p.spanRegistry.GetTarget(label); exists {
				span.SetAttributes(attribute.Bool("target.skipped", true))
				span.End()
				// Don't export skipped spans
			}
		}
	}

	// End the root span
	if p.rootSpan != nil {
		// Add final attributes to the root span
		if p.traceCtx != nil {
			p.rootSpan.SetAttributes(p.traceCtx.Attributes...)
			if !p.traceCtx.EndTime.IsZero() {
				p.rootSpan.End(trace.WithTimestamp(p.traceCtx.EndTime))
			} else {
				p.rootSpan.End()
			}
		} else {
			p.rootSpan.End()
		}

		// Export the root span - get the most recently ended span
		if p.exporter != nil && p.spanRecorder != nil {
			spans := p.spanRecorder.Ended()
			if len(spans) > 0 {
				// The root span should be the last one we just ended
				lastSpan := spans[len(spans)-1]
				if lastSpan.Name() == "Bazel Build" {
					if err := p.exporter.ExportSpan(ctx, lastSpan); err != nil {
						p.logger.Error("Failed to export root span", "error", err)
					}
				}
			}
		}
	}

	// Export the trace metadata
	if p.exporter != nil && p.traceCtx != nil {
		meta := p.traceCtx.ToMetadata()
		if err := p.exporter.ExportTrace(ctx, meta); err != nil {
			p.logger.Error("Failed to export trace metadata", "error", err)
			return err
		}

		p.logger.Info("Trace exported",
			"trace_id", meta.TraceID,
			"trace_name", meta.TraceName,
			"duration", meta.EndTime.Sub(meta.StartTime),
		)
	}

	return nil
}

// ParseBuildEvent parses and processes a build event.
// If an exporter is configured, it will convert to OTEL and export.
func (p *BuildEventParser) ParseBuildEvent(event *build.BuildEvent) (*build_event_stream.BuildEvent, error) {
	bazelEvent := &build_event_stream.BuildEvent{}
	err := event.GetBazelEvent().UnmarshalTo(bazelEvent)
	if err != nil {
		p.logger.Error("Failed to unmarshal Bazel event",
			"BuildEventParserError", err,
		)
		return nil, err
	}

	// If we have an exporter, process the event for OTEL conversion
	if p.exporter != nil {
		ctx := context.Background()
		if err := p.ProcessEvent(ctx, event); err != nil {
			p.logger.Error("Failed to process event for OTEL", "error", err)
			// Don't fail - continue with returning the event
		}
	}

	if p.toJson {
		p.logger.Debug("JSON conversion requested but not yet implemented")
	}

	return bazelEvent, nil
}

// Close finalizes any pending traces and closes the exporter.
func (p *BuildEventParser) Close() error {
	p.logger.Info("Closing BuildEventParser")

	// Finalize any active trace
	if p.traceCtx != nil {
		ctx := context.Background()
		if err := p.finalizeTrace(ctx); err != nil {
			p.logger.Error("Failed to finalize trace", "error", err)
		}
	}

	// Shutdown tracer provider if present
	if p.tracerProvider != nil {
		ctx := context.Background()
		if err := p.tracerProvider.Shutdown(ctx); err != nil {
			p.logger.Error("Failed to shutdown tracer provider", "error", err)
		}
	}

	// Close exporter
	if p.exporter != nil {
		return p.exporter.Close()
	}

	return nil
}
