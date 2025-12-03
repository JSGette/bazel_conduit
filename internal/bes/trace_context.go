package bes

import (
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// TraceContext manages the OTEL trace lifecycle for a Bazel build.
// It accumulates trace-level attributes from BEP lifecycle events.
type TraceContext struct {
	// TraceID is derived from the invocation UUID in the started event
	TraceID trace.TraceID

	// TraceName is the display name, extracted from the pattern event
	TraceName string

	// StartTime is when the build started (from started event)
	StartTime time.Time

	// EndTime is when the build finished (from buildFinished event)
	EndTime time.Time

	// Attributes contains all trace-level attributes from lifecycle events
	Attributes []attribute.KeyValue
}

// NewTraceContext creates a new TraceContext from an invocation UUID and start time.
// The UUID is converted to an OTEL TraceID.
func NewTraceContext(uuid string, startTime time.Time) (*TraceContext, error) {
	traceID, err := uuidToTraceID(uuid)
	if err != nil {
		return nil, fmt.Errorf("invalid UUID: %w", err)
	}

	return &TraceContext{
		TraceID:    traceID,
		StartTime:  startTime,
		Attributes: make([]attribute.KeyValue, 0),
	}, nil
}

// uuidToTraceID converts a UUID string to an OTEL TraceID.
// UUID format: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx (36 chars)
// TraceID format: 16 bytes (32 hex chars)
func uuidToTraceID(uuid string) (trace.TraceID, error) {
	// Remove hyphens from UUID
	hexStr := strings.ReplaceAll(uuid, "-", "")

	if len(hexStr) != 32 {
		return trace.TraceID{}, fmt.Errorf("invalid UUID length: expected 32 hex chars, got %d", len(hexStr))
	}

	// Decode hex string to bytes
	bytes, err := hex.DecodeString(hexStr)
	if err != nil {
		return trace.TraceID{}, fmt.Errorf("invalid hex in UUID: %w", err)
	}

	var traceID trace.TraceID
	copy(traceID[:], bytes)

	return traceID, nil
}

// SetTraceName sets the trace display name from pattern(s).
// Multiple patterns are joined with commas.
func (tc *TraceContext) SetTraceName(patterns []string) {
	tc.TraceName = strings.Join(patterns, ",")
}

// AddAttribute adds a single attribute to the trace.
func (tc *TraceContext) AddAttribute(attr attribute.KeyValue) {
	tc.Attributes = append(tc.Attributes, attr)
}

// AddAttributes adds multiple attributes to the trace.
func (tc *TraceContext) AddAttributes(attrs []attribute.KeyValue) {
	tc.Attributes = append(tc.Attributes, attrs...)
}

// SetEndTime sets the trace end time.
func (tc *TraceContext) SetEndTime(endTime time.Time) {
	tc.EndTime = endTime
}

// Duration returns the trace duration (EndTime - StartTime).
func (tc *TraceContext) Duration() time.Duration {
	return tc.EndTime.Sub(tc.StartTime)
}

// GetTraceID returns the trace ID.
func (tc *TraceContext) GetTraceID() trace.TraceID {
	return tc.TraceID
}

// ToMetadata converts the TraceContext to a TraceMetadata for export.
func (tc *TraceContext) ToMetadata() TraceMetadata {
	return TraceMetadata{
		TraceID:    tc.TraceID.String(),
		TraceName:  tc.TraceName,
		StartTime:  tc.StartTime,
		EndTime:    tc.EndTime,
		Attributes: tc.Attributes,
	}
}

