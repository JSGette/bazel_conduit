package bes

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// TraceExporter defines the interface for exporting OTEL traces and spans.
// Implementations can export to JSON files, OTEL collectors, Datadog, etc.
type TraceExporter interface {
	// ExportSpan exports a completed span. Called when a target or action span finishes.
	ExportSpan(ctx context.Context, span sdktrace.ReadOnlySpan) error

	// ExportTrace exports trace-level metadata. Called when the build finishes (lastMessage).
	ExportTrace(ctx context.Context, traceData TraceMetadata) error

	// Close flushes any buffered data and releases resources.
	Close() error
}

// TraceMetadata contains trace-level information extracted from BEP lifecycle events.
// This includes data from started, buildFinished, buildMetrics, and other lifecycle events.
type TraceMetadata struct {
	// TraceID is derived from the invocation UUID in the started event
	TraceID string

	// TraceName is the display name, extracted from the pattern event (e.g., "//deps/...")
	TraceName string

	// StartTime is when the build started (from started event)
	StartTime time.Time

	// EndTime is when the build finished (from buildFinished event)
	EndTime time.Time

	// Attributes contains all trace-level attributes extracted from lifecycle events
	Attributes []attribute.KeyValue
}

