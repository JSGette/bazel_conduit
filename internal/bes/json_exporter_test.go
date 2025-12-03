package bes

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

// TestJSONFileExporterImplementsInterface verifies JSONFileExporter implements TraceExporter
func TestJSONFileExporterImplementsInterface(t *testing.T) {
	var _ TraceExporter = (*JSONFileExporter)(nil)
}

// TestJSONFileExporterCreate tests creating a new JSONFileExporter
func TestJSONFileExporterCreate(t *testing.T) {
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "trace.json")

	exporter, err := NewJSONFileExporter(filePath)
	if err != nil {
		t.Fatalf("NewJSONFileExporter failed: %v", err)
	}
	defer exporter.Close()

	// Verify file was created
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		t.Error("expected file to be created")
	}
}

// TestJSONFileExporterCreateInvalidPath tests creating exporter with invalid path
func TestJSONFileExporterCreateInvalidPath(t *testing.T) {
	_, err := NewJSONFileExporter("/nonexistent/dir/trace.json")
	if err == nil {
		t.Error("expected error for invalid path")
	}
}

// TestJSONFileExporterExportSpan tests exporting a span in OTLP JSON format
func TestJSONFileExporterExportSpan(t *testing.T) {
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "trace.json")

	exporter, err := NewJSONFileExporter(filePath)
	if err != nil {
		t.Fatalf("NewJSONFileExporter failed: %v", err)
	}

	// Create a test span
	spanRecorder := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(spanRecorder))
	tracer := tp.Tracer("test")

	ctx := context.Background()
	_, span := tracer.Start(ctx, "//deps/foo:bar")
	span.SetAttributes(
		attribute.String("target.label", "//deps/foo:bar"),
		attribute.String("target.kind", "go_library"),
		attribute.Bool("target.success", true),
	)
	span.SetStatus(codes.Ok, "Target built successfully")
	span.End()

	spans := spanRecorder.Ended()
	if len(spans) == 0 {
		t.Fatal("expected at least one span")
	}

	// Export the span (collected, not written yet)
	err = exporter.ExportSpan(ctx, spans[0])
	if err != nil {
		t.Fatalf("ExportSpan failed: %v", err)
	}

	// Close to flush OTLP JSON
	err = exporter.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Read and verify the OTLP JSON file
	data, err := os.ReadFile(filePath)
	if err != nil {
		t.Fatalf("failed to read output file: %v", err)
	}

	// Parse OTLP JSON
	var otlpData otlpTrace
	err = json.Unmarshal(data, &otlpData)
	if err != nil {
		t.Fatalf("failed to parse OTLP JSON: %v\nContent: %s", err, string(data))
	}

	// Verify OTLP structure
	if len(otlpData.ResourceSpans) == 0 {
		t.Fatal("expected at least one resourceSpan")
	}

	rs := otlpData.ResourceSpans[0]
	if len(rs.ScopeSpans) == 0 {
		t.Fatal("expected at least one scopeSpan")
	}

	ss := rs.ScopeSpans[0]
	if len(ss.Spans) == 0 {
		t.Fatal("expected at least one span")
	}

	spanData := ss.Spans[0]

	// Verify span name
	if spanData.Name != "//deps/foo:bar" {
		t.Errorf("expected span name '//deps/foo:bar', got '%s'", spanData.Name)
	}

	// Verify attributes
	found := false
	for _, attr := range spanData.Attributes {
		if attr.Key == "target.label" && attr.Value.StringValue != nil && *attr.Value.StringValue == "//deps/foo:bar" {
			found = true
			break
		}
	}

	if !found {
		t.Error("expected target.label attribute with value '//deps/foo:bar'")
	}
}

// TestJSONFileExporterExportMultipleSpans tests exporting multiple spans
func TestJSONFileExporterExportMultipleSpans(t *testing.T) {
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "trace.json")

	exporter, err := NewJSONFileExporter(filePath)
	if err != nil {
		t.Fatalf("NewJSONFileExporter failed: %v", err)
	}

	spanRecorder := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(spanRecorder))
	tracer := tp.Tracer("test")

	ctx := context.Background()

	// Create multiple spans
	spanNames := []string{"//deps/a:lib", "//deps/b:lib", "//deps/c:lib"}
	for _, name := range spanNames {
		_, span := tracer.Start(ctx, name)
		span.SetAttributes(attribute.String("target.label", name))
		span.End()
	}

	spans := spanRecorder.Ended()
	for _, span := range spans {
		err = exporter.ExportSpan(ctx, span)
		if err != nil {
			t.Fatalf("ExportSpan failed: %v", err)
		}
	}

	err = exporter.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Read and verify
	data, err := os.ReadFile(filePath)
	if err != nil {
		t.Fatalf("failed to read file: %v", err)
	}

	// Parse OTLP JSON
	var otlpData otlpTrace
	err = json.Unmarshal(data, &otlpData)
	if err != nil {
		t.Fatalf("failed to parse OTLP JSON: %v", err)
	}

	// Verify we have 3 spans
	if len(otlpData.ResourceSpans) == 0 || len(otlpData.ResourceSpans[0].ScopeSpans) == 0 {
		t.Fatal("expected scopeSpans")
	}

	exportedSpans := otlpData.ResourceSpans[0].ScopeSpans[0].Spans
	if len(exportedSpans) != 3 {
		t.Errorf("expected 3 spans, got %d", len(exportedSpans))
	}

	// Verify span names
	foundNames := make(map[string]bool)
	for _, s := range exportedSpans {
		foundNames[s.Name] = true
	}

	for _, name := range spanNames {
		if !foundNames[name] {
			t.Errorf("expected span '%s' not found", name)
		}
	}
}

// TestJSONFileExporterExportTrace tests exporting trace metadata
func TestJSONFileExporterExportTrace(t *testing.T) {
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "trace.json")

	exporter, err := NewJSONFileExporter(filePath)
	if err != nil {
		t.Fatalf("NewJSONFileExporter failed: %v", err)
	}

	// Export trace metadata
	traceID := trace.TraceID([16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})
	traceMeta := TraceMetadata{
		TraceID:   traceID.String(),
		TraceName: "//deps/...",
		StartTime: time.Now().Add(-time.Second),
		EndTime:   time.Now(),
		Attributes: []attribute.KeyValue{
			attribute.String("build.invocation_id", "test-uuid"),
		},
	}

	ctx := context.Background()
	err = exporter.ExportTrace(ctx, traceMeta)
	if err != nil {
		t.Fatalf("ExportTrace failed: %v", err)
	}

	err = exporter.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Read and verify
	data, err := os.ReadFile(filePath)
	if err != nil {
		t.Fatalf("failed to read file: %v", err)
	}

	// Parse OTLP JSON
	var otlpData otlpTrace
	err = json.Unmarshal(data, &otlpData)
	if err != nil {
		t.Fatalf("failed to parse OTLP JSON: %v", err)
	}

	// Verify resource attributes contain trace name
	if len(otlpData.ResourceSpans) == 0 {
		t.Fatal("expected resourceSpans")
	}

	foundTraceName := false
	for _, attr := range otlpData.ResourceSpans[0].Resource.Attributes {
		if attr.Key == "trace.name" && attr.Value.StringValue != nil && *attr.Value.StringValue == "//deps/..." {
			foundTraceName = true
			break
		}
	}

	if !foundTraceName {
		t.Error("expected trace.name attribute with value '//deps/...'")
	}
}

// TestJSONFileExporterSpanAttributes tests various attribute types
func TestJSONFileExporterSpanAttributes(t *testing.T) {
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "trace.json")

	exporter, err := NewJSONFileExporter(filePath)
	if err != nil {
		t.Fatalf("NewJSONFileExporter failed: %v", err)
	}

	spanRecorder := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(spanRecorder))
	tracer := tp.Tracer("test")

	ctx := context.Background()
	_, span := tracer.Start(ctx, "test-span")
	span.SetAttributes(
		attribute.String("string_attr", "value"),
		attribute.Int64("int_attr", 42),
		attribute.Bool("bool_attr", true),
		attribute.Float64("float_attr", 3.14),
		attribute.StringSlice("string_slice", []string{"a", "b", "c"}),
	)
	span.End()

	spans := spanRecorder.Ended()
	err = exporter.ExportSpan(ctx, spans[0])
	if err != nil {
		t.Fatalf("ExportSpan failed: %v", err)
	}

	err = exporter.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Read and verify
	data, err := os.ReadFile(filePath)
	if err != nil {
		t.Fatalf("failed to read file: %v", err)
	}

	var otlpData otlpTrace
	err = json.Unmarshal(data, &otlpData)
	if err != nil {
		t.Fatalf("failed to parse OTLP JSON: %v", err)
	}

	// Verify all attribute types
	if len(otlpData.ResourceSpans) == 0 || len(otlpData.ResourceSpans[0].ScopeSpans) == 0 ||
		len(otlpData.ResourceSpans[0].ScopeSpans[0].Spans) == 0 {
		t.Fatal("expected spans in OTLP structure")
	}

	attrs := otlpData.ResourceSpans[0].ScopeSpans[0].Spans[0].Attributes

	// Check each attribute type
	attrMap := make(map[string]otlpValue)
	for _, attr := range attrs {
		attrMap[attr.Key] = attr.Value
	}

	// String attribute
	if val, ok := attrMap["string_attr"]; !ok || val.StringValue == nil || *val.StringValue != "value" {
		t.Error("string_attr not found or incorrect")
	}

	// Int attribute
	if val, ok := attrMap["int_attr"]; !ok || val.IntValue == nil || *val.IntValue != 42 {
		t.Error("int_attr not found or incorrect")
	}

	// Bool attribute
	if val, ok := attrMap["bool_attr"]; !ok || val.BoolValue == nil || *val.BoolValue != true {
		t.Error("bool_attr not found or incorrect")
	}

	// Float attribute
	if val, ok := attrMap["float_attr"]; !ok || val.DoubleValue == nil || *val.DoubleValue != 3.14 {
		t.Error("float_attr not found or incorrect")
	}

	// String slice attribute
	if val, ok := attrMap["string_slice"]; !ok || val.ArrayValue == nil || len(val.ArrayValue.Values) != 3 {
		t.Error("string_slice not found or incorrect")
	}
}
