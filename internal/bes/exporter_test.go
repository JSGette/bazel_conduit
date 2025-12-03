package bes

import (
	"context"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

// MockExporter implements TraceExporter for testing
type MockExporter struct {
	ExportedSpans  []sdktrace.ReadOnlySpan
	ExportedTraces []TraceMetadata
	Closed         bool
}

func NewMockExporter() *MockExporter {
	return &MockExporter{
		ExportedSpans:  make([]sdktrace.ReadOnlySpan, 0),
		ExportedTraces: make([]TraceMetadata, 0),
	}
}

func (m *MockExporter) ExportSpan(ctx context.Context, span sdktrace.ReadOnlySpan) error {
	m.ExportedSpans = append(m.ExportedSpans, span)
	return nil
}

func (m *MockExporter) ExportTrace(ctx context.Context, traceData TraceMetadata) error {
	m.ExportedTraces = append(m.ExportedTraces, traceData)
	return nil
}

func (m *MockExporter) Close() error {
	m.Closed = true
	return nil
}

// TestMockExporterImplementsInterface verifies MockExporter implements TraceExporter
func TestMockExporterImplementsInterface(t *testing.T) {
	var _ TraceExporter = (*MockExporter)(nil)
}

// TestTraceExporterExportSpan tests that a span can be exported
func TestTraceExporterExportSpan(t *testing.T) {
	exporter := NewMockExporter()

	// Create a test span using tracetest
	spanRecorder := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(spanRecorder))
	tracer := tp.Tracer("test")

	ctx := context.Background()
	_, span := tracer.Start(ctx, "test-span")
	span.SetAttributes(attribute.String("test.key", "test.value"))
	span.SetStatus(codes.Ok, "success")
	span.End()

	// Get recorded spans
	spans := spanRecorder.Ended()
	if len(spans) == 0 {
		t.Fatal("expected at least one span to be recorded")
	}

	// Export the span
	err := exporter.ExportSpan(ctx, spans[0])
	if err != nil {
		t.Fatalf("ExportSpan failed: %v", err)
	}

	if len(exporter.ExportedSpans) != 1 {
		t.Errorf("expected 1 exported span, got %d", len(exporter.ExportedSpans))
	}

	if exporter.ExportedSpans[0].Name() != "test-span" {
		t.Errorf("expected span name 'test-span', got '%s'", exporter.ExportedSpans[0].Name())
	}
}

// TestTraceExporterExportTrace tests that trace metadata can be exported
func TestTraceExporterExportTrace(t *testing.T) {
	exporter := NewMockExporter()

	traceData := TraceMetadata{
		TraceID:   "test-trace-id",
		TraceName: "//deps/...",
		StartTime: time.Now(),
		EndTime:   time.Now().Add(time.Second * 10),
		Attributes: []attribute.KeyValue{
			attribute.String("build.command", "build"),
			attribute.Bool("build.overall_success", true),
		},
	}

	ctx := context.Background()
	err := exporter.ExportTrace(ctx, traceData)
	if err != nil {
		t.Fatalf("ExportTrace failed: %v", err)
	}

	if len(exporter.ExportedTraces) != 1 {
		t.Errorf("expected 1 exported trace, got %d", len(exporter.ExportedTraces))
	}

	if exporter.ExportedTraces[0].TraceName != "//deps/..." {
		t.Errorf("expected trace name '//deps/...', got '%s'", exporter.ExportedTraces[0].TraceName)
	}
}

// TestTraceExporterClose tests that the exporter can be closed
func TestTraceExporterClose(t *testing.T) {
	exporter := NewMockExporter()

	if exporter.Closed {
		t.Error("exporter should not be closed initially")
	}

	err := exporter.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	if !exporter.Closed {
		t.Error("exporter should be closed after Close()")
	}
}

// TestTraceMetadataFields tests TraceMetadata struct fields
func TestTraceMetadataFields(t *testing.T) {
	now := time.Now()
	traceID := trace.TraceID{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10}

	meta := TraceMetadata{
		TraceID:   traceID.String(),
		TraceName: "//pkg/...",
		StartTime: now,
		EndTime:   now.Add(time.Minute),
		Attributes: []attribute.KeyValue{
			attribute.String("build.invocation_id", "abc-123"),
		},
	}

	if meta.TraceID != traceID.String() {
		t.Errorf("TraceID mismatch")
	}
	if meta.TraceName != "//pkg/..." {
		t.Errorf("TraceName mismatch")
	}
	if !meta.StartTime.Equal(now) {
		t.Errorf("StartTime mismatch")
	}
	if meta.EndTime.Sub(meta.StartTime) != time.Minute {
		t.Errorf("Duration should be 1 minute")
	}
	if len(meta.Attributes) != 1 {
		t.Errorf("expected 1 attribute, got %d", len(meta.Attributes))
	}
}

