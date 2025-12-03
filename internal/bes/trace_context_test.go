package bes

import (
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// TestNewTraceContext tests creating a new TraceContext with UUID and start time
func TestNewTraceContext(t *testing.T) {
	uuid := "ada9c62f-776d-4388-b8bb-e56064ba5727"
	startTime := time.Date(2025, 12, 3, 9, 43, 50, 811000000, time.UTC)

	tc, err := NewTraceContext(uuid, startTime)
	if err != nil {
		t.Fatalf("NewTraceContext failed: %v", err)
	}

	// Verify TraceID is derived from UUID
	if tc.TraceID.IsValid() == false {
		t.Error("TraceID should be valid")
	}

	// Verify start time
	if !tc.StartTime.Equal(startTime) {
		t.Errorf("StartTime mismatch: got %v, want %v", tc.StartTime, startTime)
	}
}

// TestNewTraceContextInvalidUUID tests creating TraceContext with invalid UUID
func TestNewTraceContextInvalidUUID(t *testing.T) {
	_, err := NewTraceContext("not-a-valid-uuid", time.Now())
	if err == nil {
		t.Error("expected error for invalid UUID")
	}
}

// TestTraceContextUUIDToTraceID tests UUID to TraceID conversion
func TestTraceContextUUIDToTraceID(t *testing.T) {
	// UUID: ada9c62f-776d-4388-b8bb-e56064ba5727
	// Should convert to TraceID by removing hyphens and parsing as hex
	uuid := "ada9c62f-776d-4388-b8bb-e56064ba5727"
	startTime := time.Now()

	tc, err := NewTraceContext(uuid, startTime)
	if err != nil {
		t.Fatalf("NewTraceContext failed: %v", err)
	}

	// The TraceID should be the UUID bytes without hyphens
	expectedHex := "ada9c62f776d4388b8bbe56064ba5727"
	if tc.TraceID.String() != expectedHex {
		t.Errorf("TraceID mismatch: got %s, want %s", tc.TraceID.String(), expectedHex)
	}
}

// TestTraceContextSetTraceName tests setting trace name from pattern
func TestTraceContextSetTraceName(t *testing.T) {
	tc, _ := NewTraceContext("ada9c62f-776d-4388-b8bb-e56064ba5727", time.Now())

	patterns := []string{"//deps/..."}
	tc.SetTraceName(patterns)

	if tc.TraceName != "//deps/..." {
		t.Errorf("TraceName mismatch: got %s, want //deps/...", tc.TraceName)
	}
}

// TestTraceContextSetTraceNameMultiple tests setting trace name from multiple patterns
func TestTraceContextSetTraceNameMultiple(t *testing.T) {
	tc, _ := NewTraceContext("ada9c62f-776d-4388-b8bb-e56064ba5727", time.Now())

	patterns := []string{"//deps/...", "//pkg/..."}
	tc.SetTraceName(patterns)

	if tc.TraceName != "//deps/...,//pkg/..." {
		t.Errorf("TraceName mismatch: got %s, want '//deps/...,//pkg/...'", tc.TraceName)
	}
}

// TestTraceContextSetTraceNameEmpty tests setting trace name with empty patterns
func TestTraceContextSetTraceNameEmpty(t *testing.T) {
	tc, _ := NewTraceContext("ada9c62f-776d-4388-b8bb-e56064ba5727", time.Now())

	tc.SetTraceName([]string{})

	if tc.TraceName != "" {
		t.Errorf("TraceName should be empty, got %s", tc.TraceName)
	}
}

// TestTraceContextAddAttribute tests adding attributes to trace
func TestTraceContextAddAttribute(t *testing.T) {
	tc, _ := NewTraceContext("ada9c62f-776d-4388-b8bb-e56064ba5727", time.Now())

	tc.AddAttribute(attribute.String("build.command", "build"))
	tc.AddAttribute(attribute.String("build.tool_version", "8.4.2"))
	tc.AddAttribute(attribute.Bool("build.overall_success", true))

	if len(tc.Attributes) != 3 {
		t.Errorf("expected 3 attributes, got %d", len(tc.Attributes))
	}

	// Verify specific attributes
	found := false
	for _, attr := range tc.Attributes {
		if string(attr.Key) == "build.command" && attr.Value.AsString() == "build" {
			found = true
			break
		}
	}
	if !found {
		t.Error("build.command attribute not found")
	}
}

// TestTraceContextAddAttributes tests adding multiple attributes at once
func TestTraceContextAddAttributes(t *testing.T) {
	tc, _ := NewTraceContext("ada9c62f-776d-4388-b8bb-e56064ba5727", time.Now())

	attrs := []attribute.KeyValue{
		attribute.String("build.command", "build"),
		attribute.String("build.tool_version", "8.4.2"),
		attribute.Int64("build.packages_loaded", 42),
	}
	tc.AddAttributes(attrs)

	if len(tc.Attributes) != 3 {
		t.Errorf("expected 3 attributes, got %d", len(tc.Attributes))
	}
}

// TestTraceContextSetEndTime tests setting the end time
func TestTraceContextSetEndTime(t *testing.T) {
	startTime := time.Date(2025, 12, 3, 9, 43, 50, 0, time.UTC)
	tc, _ := NewTraceContext("ada9c62f-776d-4388-b8bb-e56064ba5727", startTime)

	endTime := time.Date(2025, 12, 3, 9, 43, 51, 567000000, time.UTC)
	tc.SetEndTime(endTime)

	if !tc.EndTime.Equal(endTime) {
		t.Errorf("EndTime mismatch: got %v, want %v", tc.EndTime, endTime)
	}
}

// TestTraceContextDuration tests calculating trace duration
func TestTraceContextDuration(t *testing.T) {
	startTime := time.Date(2025, 12, 3, 9, 43, 50, 0, time.UTC)
	tc, _ := NewTraceContext("ada9c62f-776d-4388-b8bb-e56064ba5727", startTime)

	endTime := startTime.Add(10 * time.Second)
	tc.SetEndTime(endTime)

	duration := tc.Duration()
	if duration != 10*time.Second {
		t.Errorf("Duration mismatch: got %v, want 10s", duration)
	}
}

// TestTraceContextToMetadata tests converting TraceContext to TraceMetadata
func TestTraceContextToMetadata(t *testing.T) {
	startTime := time.Date(2025, 12, 3, 9, 43, 50, 0, time.UTC)
	tc, _ := NewTraceContext("ada9c62f-776d-4388-b8bb-e56064ba5727", startTime)

	tc.SetTraceName([]string{"//deps/..."})
	tc.SetEndTime(startTime.Add(10 * time.Second))
	tc.AddAttribute(attribute.String("build.command", "build"))

	meta := tc.ToMetadata()

	if meta.TraceID != tc.TraceID.String() {
		t.Errorf("TraceID mismatch")
	}
	if meta.TraceName != "//deps/..." {
		t.Errorf("TraceName mismatch")
	}
	if !meta.StartTime.Equal(startTime) {
		t.Errorf("StartTime mismatch")
	}
	if len(meta.Attributes) != 1 {
		t.Errorf("Attributes count mismatch")
	}
}

// TestTraceContextGetTraceID tests getting the trace ID
func TestTraceContextGetTraceID(t *testing.T) {
	tc, _ := NewTraceContext("ada9c62f-776d-4388-b8bb-e56064ba5727", time.Now())

	traceID := tc.GetTraceID()

	if traceID == (trace.TraceID{}) {
		t.Error("TraceID should not be zero")
	}
	if !traceID.IsValid() {
		t.Error("TraceID should be valid")
	}
}

