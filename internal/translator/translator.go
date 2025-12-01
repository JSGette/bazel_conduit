package translator

import (
	"crypto/sha256"
	"fmt"
	"log/slog"
	"time"

	build "google.golang.org/genproto/googleapis/devtools/build/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/anypb"

	"go.opentelemetry.io/otel/trace"

	// Import build_event_stream proto to register types
	_ "github.com/JSGette/bazel_conduit/proto/build_event_stream"
)

// SpanData represents an OpenTelemetry span derived from a BEP event.
type SpanData struct {
	TraceID      trace.TraceID
	SpanID       trace.SpanID
	ParentSpanID trace.SpanID // Zero value if root span
	Name         string
	StartTime    time.Time
	EndTime      time.Time
	Attributes   map[string]interface{}
	Status       SpanStatus
	Events       []SpanEvent
}

// SpanStatus represents the status of a span.
type SpanStatus struct {
	Code    StatusCode
	Message string
}

// StatusCode represents span status codes.
type StatusCode int

const (
	StatusCodeUnset StatusCode = iota
	StatusCodeOK
	StatusCodeError
)

// SpanEvent represents a point-in-time event within a span.
type SpanEvent struct {
	Name       string
	Timestamp  time.Time
	Attributes map[string]interface{}
}

// Translator converts BEP events to OpenTelemetry spans.
type Translator struct {
	logger       *slog.Logger
	spanCache    map[string]*SpanData // sequenceNumber -> SpanData
	traceID      trace.TraceID
	buildID      string
	invocationID string
}

// NewTranslator creates a new BEP to OTel translator for a single build.
func NewTranslator(buildID string, invocationID string, logger *slog.Logger) *Translator {
	traceID := generateTraceID(buildID)

	return &Translator{
		logger:       logger.With("build_id", buildID, "trace_id", traceID.String()),
		spanCache:    make(map[string]*SpanData),
		traceID:      traceID,
		buildID:      buildID,
		invocationID: invocationID,
	}
}

// TranslateEvent converts a BEP OrderedBuildEvent to an OTel span.
func (t *Translator) TranslateEvent(event *build.OrderedBuildEvent) (*SpanData, error) {
	if event == nil || event.Event == nil {
		return nil, fmt.Errorf("nil event")
	}

	seqNum := fmt.Sprintf("%d", event.SequenceNumber)

	// Check if we already translated this event
	if span, exists := t.spanCache[seqNum]; exists {
		return span, nil
	}

	span := &SpanData{
		TraceID:    t.traceID,
		SpanID:     generateSpanID(t.buildID, seqNum),
		Attributes: make(map[string]interface{}),
		Status:     SpanStatus{Code: StatusCodeUnset},
		Events:     []SpanEvent{},
	}

	// Extract timing
	if event.Event.EventTime != nil {
		span.StartTime = event.Event.EventTime.AsTime()
		// For now, use event time as both start and end
		// Later events may update the end time
		span.EndTime = span.StartTime
	} else {
		span.StartTime = time.Now()
		span.EndTime = span.StartTime
	}

	// Add common attributes
	span.Attributes["build.id"] = t.buildID
	span.Attributes["build.invocation_id"] = t.invocationID
	span.Attributes["build.sequence_number"] = event.SequenceNumber

	// Extract event-specific data
	if err := t.extractEventData(event.Event, span); err != nil {
		t.logger.Warn("Failed to extract event data",
			"sequence", event.SequenceNumber,
			"error", err)
	}

	// Cache the span
	t.spanCache[seqNum] = span

	t.logger.Debug("Translated event to span",
		"sequence", event.SequenceNumber,
		"span_id", span.SpanID.String(),
		"span_name", span.Name)

	return span, nil
}

// extractEventData extracts data from the BuildEvent based on its type.
func (t *Translator) extractEventData(event *build.BuildEvent, span *SpanData) error {
	// Handle ComponentStreamFinished
	if csf := event.GetComponentStreamFinished(); csf != nil {
		span.Name = "build.stream.finished"
		span.Attributes["build.stream.type"] = csf.Type.String()
		return nil
	}

	// Handle BazelEvent (most common case)
	if bazelEvent := event.GetBazelEvent(); bazelEvent != nil {
		return t.extractBazelEventData(bazelEvent, span)
	}

	// Default span name if not set
	if span.Name == "" {
		span.Name = "build.event.unknown"
	}

	return nil
}

// extractBazelEventData extracts data from the Any-wrapped BazelEvent.
func (t *Translator) extractBazelEventData(bazelEvent *anypb.Any, span *SpanData) error {
	if bazelEvent == nil {
		return fmt.Errorf("nil bazel event")
	}

	// Store the type URL for debugging
	span.Attributes["build.event.type_url"] = bazelEvent.TypeUrl

	// Extract event type from URL
	eventType := extractEventTypeFromURL(bazelEvent.TypeUrl)
	span.Name = fmt.Sprintf("build.%s", eventType)
	span.Attributes["build.event.source"] = "bazel"
	span.Attributes["build.event.type"] = eventType

	// Try to unmarshal the Any type
	dynamicMsg, err := bazelEvent.UnmarshalNew()
	if err != nil {
		t.logger.Debug("Failed to unmarshal BazelEvent",
			"type_url", bazelEvent.TypeUrl,
			"error", err)
		// Not fatal - we still have basic event info
		return nil
	}

	// Extract fields using reflection
	if err := t.extractProtoFields(dynamicMsg, span, "bazel"); err != nil {
		t.logger.Debug("Failed to extract proto fields",
			"type_url", bazelEvent.TypeUrl,
			"error", err)
	}

	return nil
}

// extractProtoFields uses reflection to extract fields from a protobuf message
// and adds them as span attributes.
func (t *Translator) extractProtoFields(msg proto.Message, span *SpanData, prefix string) error {
	if msg == nil {
		return fmt.Errorf("nil message")
	}

	// Get the protobuf reflection descriptor
	protoMsg := msg.ProtoReflect()
	descriptor := protoMsg.Descriptor()
	fields := descriptor.Fields()

	// Iterate through all fields
	for i := 0; i < fields.Len(); i++ {
		field := fields.Get(i)
		fieldName := string(field.Name())

		// Skip if field is not set (for proto3, zero values might be skipped)
		if !protoMsg.Has(field) {
			continue
		}

		value := protoMsg.Get(field)
		attrKey := fmt.Sprintf("%s.%s", prefix, fieldName)

		// Convert the field value to a Go type
		goValue := t.convertProtoValue(value, field)
		if goValue != nil {
			span.Attributes[attrKey] = goValue
		}
	}

	return nil
}

// convertProtoValue converts a protoreflect.Value to a Go type suitable for span attributes.
func (t *Translator) convertProtoValue(value protoreflect.Value, field protoreflect.FieldDescriptor) interface{} {
	// Handle repeated fields (lists)
	if field.IsList() {
		list := value.List()
		if list.Len() == 0 {
			return nil
		}
		// For now, just return the count for repeated fields to avoid complexity
		return fmt.Sprintf("<list: %d items>", list.Len())
	}

	// Handle map fields
	if field.IsMap() {
		mapVal := value.Map()
		return fmt.Sprintf("<map: %d entries>", mapVal.Len())
	}

	switch field.Kind() {
	case protoreflect.BoolKind:
		return value.Bool()
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
		return int32(value.Int())
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		return value.Int()
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		return uint32(value.Uint())
	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		return value.Uint()
	case protoreflect.FloatKind:
		return float32(value.Float())
	case protoreflect.DoubleKind:
		return value.Float()
	case protoreflect.StringKind:
		return value.String()
	case protoreflect.BytesKind:
		// Convert bytes to hex string for readability
		bytes := value.Bytes()
		if len(bytes) > 64 {
			// Truncate very long byte arrays
			return fmt.Sprintf("<bytes: %d bytes>", len(bytes))
		}
		return fmt.Sprintf("%x", bytes)
	case protoreflect.EnumKind:
		enumValue := value.Enum()
		enumDescriptor := field.Enum().Values().ByNumber(enumValue)
		if enumDescriptor != nil {
			return string(enumDescriptor.Name())
		}
		return int32(enumValue)
	case protoreflect.MessageKind:
		// For nested messages, convert to a readable string representation
		msg := value.Message()
		msgDescriptor := field.Message()

		// Special handling for well-known types
		switch msgDescriptor.FullName() {
		case "google.protobuf.Timestamp":
			// Extract timestamp fields
			seconds := msg.Get(msgDescriptor.Fields().ByName("seconds")).Int()
			nanos := msg.Get(msgDescriptor.Fields().ByName("nanos")).Int()
			timestamp := time.Unix(seconds, nanos)
			return timestamp.Format(time.RFC3339Nano)
		case "google.protobuf.Duration":
			// Extract duration fields
			seconds := msg.Get(msgDescriptor.Fields().ByName("seconds")).Int()
			nanos := msg.Get(msgDescriptor.Fields().ByName("nanos")).Int()
			duration := time.Duration(seconds)*time.Second + time.Duration(nanos)*time.Nanosecond
			return duration.String()
		default:
			// For other messages, create a compact representation
			return t.messageToMap(msg)
		}
	case protoreflect.GroupKind:
		// Groups are deprecated, but handle them like messages
		return t.messageToMap(value.Message())
	default:
		return fmt.Sprintf("<unsupported type: %s>", field.Kind())
	}
}

// messageToMap converts a protobuf message to a map for span attributes.
func (t *Translator) messageToMap(msg protoreflect.Message) map[string]interface{} {
	result := make(map[string]interface{})
	descriptor := msg.Descriptor()
	fields := descriptor.Fields()

	for i := 0; i < fields.Len(); i++ {
		field := fields.Get(i)
		if !msg.Has(field) {
			continue
		}

		fieldName := string(field.Name())
		value := msg.Get(field)

		// Simple conversion - just extract primitives
		switch field.Kind() {
		case protoreflect.BoolKind:
			result[fieldName] = value.Bool()
		case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind,
			protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
			result[fieldName] = value.Int()
		case protoreflect.Uint32Kind, protoreflect.Fixed32Kind,
			protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
			result[fieldName] = value.Uint()
		case protoreflect.FloatKind, protoreflect.DoubleKind:
			result[fieldName] = value.Float()
		case protoreflect.StringKind:
			result[fieldName] = value.String()
		case protoreflect.EnumKind:
			enumValue := value.Enum()
			enumDescriptor := field.Enum().Values().ByNumber(enumValue)
			if enumDescriptor != nil {
				result[fieldName] = string(enumDescriptor.Name())
			} else {
				result[fieldName] = int32(enumValue)
			}
		default:
			// For nested structures, just indicate presence
			result[fieldName] = fmt.Sprintf("<%s>", field.Kind())
		}
	}

	return result
}

// GetAllSpans returns all translated spans for this build.
func (t *Translator) GetAllSpans() []*SpanData {
	spans := make([]*SpanData, 0, len(t.spanCache))
	for _, span := range t.spanCache {
		spans = append(spans, span)
	}
	return spans
}

// GetSpanCount returns the number of translated spans.
func (t *Translator) GetSpanCount() int {
	return len(t.spanCache)
}

// generateTraceID creates a deterministic trace ID from a build ID.
func generateTraceID(buildID string) trace.TraceID {
	hash := sha256.Sum256([]byte(buildID))
	var traceID trace.TraceID
	copy(traceID[:], hash[:16]) // Use first 16 bytes (128 bits)
	return traceID
}

// generateSpanID creates a deterministic span ID from build ID and sequence number.
func generateSpanID(buildID string, sequenceNumber string) trace.SpanID {
	data := fmt.Sprintf("%s-%s", buildID, sequenceNumber)
	hash := sha256.Sum256([]byte(data))
	var spanID trace.SpanID
	copy(spanID[:], hash[:8]) // Use first 8 bytes (64 bits)
	return spanID
}

// extractEventTypeFromURL extracts the event type name from a type URL.
// Example: "type.googleapis.com/build_event_stream.BuildEvent" -> "event"
func extractEventTypeFromURL(typeURL string) string {
	// Simple extraction - just take the last part after the last dot
	// e.g., "type.googleapis.com/build_event_stream.BuildStarted" -> "BuildStarted"

	// Find the last slash
	lastSlash := -1
	for i := len(typeURL) - 1; i >= 0; i-- {
		if typeURL[i] == '/' {
			lastSlash = i
			break
		}
	}

	if lastSlash == -1 {
		return "unknown"
	}

	remainder := typeURL[lastSlash+1:]

	// Find the last dot
	lastDot := -1
	for i := len(remainder) - 1; i >= 0; i-- {
		if remainder[i] == '.' {
			lastDot = i
			break
		}
	}

	if lastDot == -1 {
		return remainder
	}

	return remainder[lastDot+1:]
}

// UpdateSpanTiming updates the end time of a span based on a later event.
func (t *Translator) UpdateSpanTiming(sequenceNumber int64, endTime time.Time) error {
	seqNum := fmt.Sprintf("%d", sequenceNumber)
	span, exists := t.spanCache[seqNum]
	if !exists {
		return fmt.Errorf("span not found for sequence %d", sequenceNumber)
	}

	if endTime.After(span.EndTime) {
		span.EndTime = endTime
	}

	return nil
}

// AddSpanEvent adds a timestamped event to a span.
func (t *Translator) AddSpanEvent(sequenceNumber int64, eventName string, timestamp time.Time, attrs map[string]interface{}) error {
	seqNum := fmt.Sprintf("%d", sequenceNumber)
	span, exists := t.spanCache[seqNum]
	if !exists {
		return fmt.Errorf("span not found for sequence %d", sequenceNumber)
	}

	span.Events = append(span.Events, SpanEvent{
		Name:       eventName,
		Timestamp:  timestamp,
		Attributes: attrs,
	})

	return nil
}

// SetSpanStatus sets the status of a span.
func (t *Translator) SetSpanStatus(sequenceNumber int64, code StatusCode, message string) error {
	seqNum := fmt.Sprintf("%d", sequenceNumber)
	span, exists := t.spanCache[seqNum]
	if !exists {
		return fmt.Errorf("span not found for sequence %d", sequenceNumber)
	}

	span.Status = SpanStatus{
		Code:    code,
		Message: message,
	}

	return nil
}

// TraceID returns the trace ID for this build.
func (t *Translator) TraceID() trace.TraceID {
	return t.traceID
}

// BuildID returns the build ID.
func (t *Translator) BuildID() string {
	return t.buildID
}

// InvocationID returns the invocation ID.
func (t *Translator) InvocationID() string {
	return t.invocationID
}
