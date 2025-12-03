package bes

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// JSONFileExporter exports OTEL spans and traces to a JSON file in OTLP format.
// Spans are collected in memory and exported as a complete OTLP JSON structure on Close.
type JSONFileExporter struct {
	file   *os.File
	writer *bufio.Writer
	mu     sync.Mutex
	closed bool

	// Batch collection
	spans     []sdktrace.ReadOnlySpan
	traceMeta *TraceMetadata
}

// NewJSONFileExporter creates a new JSONFileExporter that writes to the specified file path.
func NewJSONFileExporter(filePath string) (*JSONFileExporter, error) {
	file, err := os.Create(filePath)
	if err != nil {
		return nil, err
	}

	return &JSONFileExporter{
		file:   file,
		writer: bufio.NewWriter(file),
		spans:  make([]sdktrace.ReadOnlySpan, 0),
	}, nil
}

// OTLP JSON structures (simplified for file export)
type otlpTrace struct {
	ResourceSpans []otlpResourceSpan `json:"resourceSpans"`
}

type otlpResourceSpan struct {
	Resource   otlpResource    `json:"resource"`
	ScopeSpans []otlpScopeSpan `json:"scopeSpans"`
}

type otlpResource struct {
	Attributes []otlpAttribute `json:"attributes"`
}

type otlpScopeSpan struct {
	Scope otlpScope  `json:"scope"`
	Spans []otlpSpan `json:"spans"`
}

type otlpScope struct {
	Name    string `json:"name"`
	Version string `json:"version,omitempty"`
}

type otlpSpan struct {
	TraceID           string          `json:"traceId"`
	SpanID            string          `json:"spanId"`
	ParentSpanID      string          `json:"parentSpanId,omitempty"`
	Name              string          `json:"name"`
	Kind              int             `json:"kind"`
	StartTimeUnixNano string          `json:"startTimeUnixNano"`
	EndTimeUnixNano   string          `json:"endTimeUnixNano"`
	Attributes        []otlpAttribute `json:"attributes,omitempty"`
	Status            otlpStatus      `json:"status"`
}

type otlpAttribute struct {
	Key   string    `json:"key"`
	Value otlpValue `json:"value"`
}

type otlpValue struct {
	StringValue *string         `json:"stringValue,omitempty"`
	IntValue    *int64          `json:"intValue,omitempty"`
	DoubleValue *float64        `json:"doubleValue,omitempty"`
	BoolValue   *bool           `json:"boolValue,omitempty"`
	ArrayValue  *otlpArrayValue `json:"arrayValue,omitempty"`
}

type otlpArrayValue struct {
	Values []otlpValue `json:"values"`
}

type otlpStatus struct {
	Code    int    `json:"code"`
	Message string `json:"message,omitempty"`
}

// ExportSpan collects a span for later batch export.
func (e *JSONFileExporter) ExportSpan(ctx context.Context, span sdktrace.ReadOnlySpan) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.closed {
		return nil
	}

	// Collect the span for batch export
	e.spans = append(e.spans, span)
	return nil
}

// ExportTrace stores trace metadata for final export.
func (e *JSONFileExporter) ExportTrace(ctx context.Context, traceData TraceMetadata) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.closed {
		return nil
	}

	// Store trace metadata for use in Close
	e.traceMeta = &traceData
	return nil
}

// Close exports all collected spans in OTLP JSON format and closes the file.
func (e *JSONFileExporter) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.closed {
		return nil
	}

	e.closed = true

	// Build OTLP JSON structure
	otlpSpans := make([]otlpSpan, 0, len(e.spans))
	for _, span := range e.spans {
		otlpSpan := convertSpanToOTLP(span)
		otlpSpans = append(otlpSpans, otlpSpan)
	}

	// Build resource attributes from trace metadata
	resourceAttrs := make([]otlpAttribute, 0)
	if e.traceMeta != nil {
		resourceAttrs = append(resourceAttrs, otlpAttribute{
			Key: "service.name",
			Value: otlpValue{
				StringValue: stringPtr("bazel-conduit"),
			},
		})
		resourceAttrs = append(resourceAttrs, otlpAttribute{
			Key: "trace.name",
			Value: otlpValue{
				StringValue: stringPtr(e.traceMeta.TraceName),
			},
		})
	}

	otlpTrace := otlpTrace{
		ResourceSpans: []otlpResourceSpan{
			{
				Resource: otlpResource{
					Attributes: resourceAttrs,
				},
				ScopeSpans: []otlpScopeSpan{
					{
						Scope: otlpScope{
							Name:    "bazel-conduit",
							Version: "0.1.0",
						},
						Spans: otlpSpans,
					},
				},
			},
		},
	}

	// Marshal to JSON with indentation for readability
	data, err := json.MarshalIndent(otlpTrace, "", "  ")
	if err != nil {
		return err
	}

	_, err = e.writer.Write(data)
	if err != nil {
		return err
	}

	if err := e.writer.Flush(); err != nil {
		return err
	}

	return e.file.Close()
}

// Helper functions for OTLP conversion

func convertSpanToOTLP(span sdktrace.ReadOnlySpan) otlpSpan {
	otlp := otlpSpan{
		TraceID:           span.SpanContext().TraceID().String(),
		SpanID:            span.SpanContext().SpanID().String(),
		Name:              span.Name(),
		Kind:              int(span.SpanKind()),
		StartTimeUnixNano: timeToNano(span.StartTime()),
		EndTimeUnixNano:   timeToNano(span.EndTime()),
		Attributes:        convertAttributesToOTLP(span.Attributes()),
		Status:            convertStatusToOTLP(span.Status()),
	}

	if span.Parent().HasSpanID() {
		otlp.ParentSpanID = span.Parent().SpanID().String()
	}

	return otlp
}

func convertAttributesToOTLP(attrs []attribute.KeyValue) []otlpAttribute {
	result := make([]otlpAttribute, 0, len(attrs))
	for _, attr := range attrs {
		result = append(result, otlpAttribute{
			Key:   string(attr.Key),
			Value: convertValueToOTLP(attr.Value),
		})
	}
	return result
}

func convertValueToOTLP(v attribute.Value) otlpValue {
	switch v.Type() {
	case attribute.BOOL:
		val := v.AsBool()
		return otlpValue{BoolValue: &val}
	case attribute.INT64:
		val := v.AsInt64()
		return otlpValue{IntValue: &val}
	case attribute.FLOAT64:
		val := v.AsFloat64()
		return otlpValue{DoubleValue: &val}
	case attribute.STRING:
		val := v.AsString()
		return otlpValue{StringValue: &val}
	case attribute.BOOLSLICE:
		vals := v.AsBoolSlice()
		arrayVals := make([]otlpValue, len(vals))
		for i, bv := range vals {
			arrayVals[i] = otlpValue{BoolValue: &bv}
		}
		return otlpValue{ArrayValue: &otlpArrayValue{Values: arrayVals}}
	case attribute.INT64SLICE:
		vals := v.AsInt64Slice()
		arrayVals := make([]otlpValue, len(vals))
		for i, iv := range vals {
			arrayVals[i] = otlpValue{IntValue: &iv}
		}
		return otlpValue{ArrayValue: &otlpArrayValue{Values: arrayVals}}
	case attribute.FLOAT64SLICE:
		vals := v.AsFloat64Slice()
		arrayVals := make([]otlpValue, len(vals))
		for i, fv := range vals {
			arrayVals[i] = otlpValue{DoubleValue: &fv}
		}
		return otlpValue{ArrayValue: &otlpArrayValue{Values: arrayVals}}
	case attribute.STRINGSLICE:
		vals := v.AsStringSlice()
		arrayVals := make([]otlpValue, len(vals))
		for i, sv := range vals {
			arrayVals[i] = otlpValue{StringValue: &sv}
		}
		return otlpValue{ArrayValue: &otlpArrayValue{Values: arrayVals}}
	default:
		val := v.AsString()
		return otlpValue{StringValue: &val}
	}
}

func convertStatusToOTLP(status sdktrace.Status) otlpStatus {
	// OTLP status codes: 0 = Unset, 1 = Ok, 2 = Error
	// Go OTEL codes: 0 = Unset, 1 = Error, 2 = Ok
	code := 0
	switch status.Code {
	case 0: // codes.Unset
		code = 0
	case 1: // codes.Error (in Go SDK)
		code = 2 // OTLP Error
	case 2: // codes.Ok (in Go SDK)
		code = 1 // OTLP Ok
	}

	return otlpStatus{
		Code:    code,
		Message: status.Description,
	}
}

func timeToNano(t time.Time) string {
	// Convert time to nanoseconds since Unix epoch as string
	return fmt.Sprintf("%d", t.UnixNano())
}

func stringPtr(s string) *string {
	return &s
}
