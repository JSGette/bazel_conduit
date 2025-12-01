package graph

import (
	"fmt"
	"sync"
	"time"

	buildpb "google.golang.org/genproto/googleapis/devtools/build/v1"
	"google.golang.org/protobuf/types/known/anypb"
)

// EventGraph represents an in-memory graph of build events for a single build
// This is our internal state management structure - the actual events use proto types
type EventGraph struct {
	BuildID      string
	InvocationID string
	TraceID      string
	StartTime    time.Time

	// State tracking
	InvocationStarted  bool
	InvocationFinished bool
	BuildFinished      bool

	// Event storage using proto types
	Events       map[string]*StoredEvent // eventID -> StoredEvent
	Spans        []*Span                 // All spans in order (our internal OTel representation)
	PendingSpans map[string]*Span        // Spans waiting for children

	mu sync.RWMutex
}

// StoredEvent wraps a proto BuildEvent with metadata
type StoredEvent struct {
	SequenceNum int64
	Timestamp   time.Time
	Event       *buildpb.BuildEvent // Use proto-generated type
}

// Span represents an OpenTelemetry span
// This is our internal representation - not from proto
type Span struct {
	TraceID      string
	SpanID       string
	ParentSpanID string
	Name         string
	StartTime    time.Time
	EndTime      time.Time
	Attributes   map[string]interface{}
	Status       SpanStatus
	Kind         SpanKind

	// Reference to source event
	SourceEvent *buildpb.BuildEvent
}

// SpanStatus represents the status of a span
type SpanStatus struct {
	Code    StatusCode
	Message string
}

// StatusCode represents span status codes
type StatusCode int

const (
	StatusCodeUnset StatusCode = iota
	StatusCodeOK
	StatusCodeError
)

// SpanKind represents the kind of span
type SpanKind int

const (
	SpanKindUnspecified SpanKind = iota
	SpanKindInternal
	SpanKindServer
	SpanKindClient
	SpanKindProducer
	SpanKindConsumer
)

// AddEvent adds a BuildEvent to the graph
func (g *EventGraph) AddEvent(eventID string, seqNum int64, event *buildpb.BuildEvent) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	if eventID == "" {
		return fmt.Errorf("event ID cannot be empty")
	}

	if event == nil {
		return fmt.Errorf("event cannot be nil")
	}

	g.Events[eventID] = &StoredEvent{
		SequenceNum: seqNum,
		Timestamp:   time.Now(),
		Event:       event,
	}

	return nil
}

// GetEvent retrieves an event by ID
func (g *EventGraph) GetEvent(eventID string) (*StoredEvent, bool) {
	g.mu.RLock()
	defer g.mu.RUnlock()

	event, exists := g.Events[eventID]
	return event, exists
}

// AddSpan adds a span to the graph
func (g *EventGraph) AddSpan(span *Span) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	if span == nil {
		return fmt.Errorf("span cannot be nil")
	}

	g.Spans = append(g.Spans, span)

	// If span doesn't have an end time, it's pending
	if span.EndTime.IsZero() {
		g.PendingSpans[span.SpanID] = span
	}

	return nil
}

// CompleteSpan marks a span as complete by setting its end time
func (g *EventGraph) CompleteSpan(spanID string, endTime time.Time) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	span, exists := g.PendingSpans[spanID]
	if !exists {
		return fmt.Errorf("span %s not found in pending spans", spanID)
	}

	span.EndTime = endTime
	delete(g.PendingSpans, spanID)

	return nil
}

// GetSpanCount returns the total number of spans
func (g *EventGraph) GetSpanCount() int {
	g.mu.RLock()
	defer g.mu.RUnlock()

	return len(g.Spans)
}

// GetPendingSpanCount returns the number of pending spans
func (g *EventGraph) GetPendingSpanCount() int {
	g.mu.RLock()
	defer g.mu.RUnlock()

	return len(g.PendingSpans)
}

// MarkInvocationStarted marks the invocation as started
func (g *EventGraph) MarkInvocationStarted() {
	g.mu.Lock()
	defer g.mu.Unlock()

	g.InvocationStarted = true
}

// MarkInvocationFinished marks the invocation as finished
func (g *EventGraph) MarkInvocationFinished() {
	g.mu.Lock()
	defer g.mu.Unlock()

	g.InvocationFinished = true
}

// MarkBuildFinished marks the build as finished
func (g *EventGraph) MarkBuildFinished() {
	g.mu.Lock()
	defer g.mu.Unlock()

	g.BuildFinished = true
}

// IsComplete returns true if the build is finished
func (g *EventGraph) IsComplete() bool {
	g.mu.RLock()
	defer g.mu.RUnlock()

	return g.BuildFinished
}

// GetAllSpans returns a copy of all spans (thread-safe)
func (g *EventGraph) GetAllSpans() []*Span {
	g.mu.RLock()
	defer g.mu.RUnlock()

	// Create a copy to avoid race conditions
	spans := make([]*Span, len(g.Spans))
	copy(spans, g.Spans)

	return spans
}

// GetEventCount returns the total number of events
func (g *EventGraph) GetEventCount() int {
	g.mu.RLock()
	defer g.mu.RUnlock()

	return len(g.Events)
}

// GetBazelEvent extracts the Bazel-specific event from a BuildEvent
// Returns the unpacked Any if it's a BazelEvent, nil otherwise
func GetBazelEvent(event *buildpb.BuildEvent) *anypb.Any {
	if event == nil {
		return nil
	}

	return event.GetBazelEvent()
}

// GetEventType returns a string representation of the event type
func GetEventType(event *buildpb.BuildEvent) string {
	if event == nil {
		return "unknown"
	}

	switch event.GetEvent().(type) {
	case *buildpb.BuildEvent_InvocationAttemptStarted_:
		return "InvocationAttemptStarted"
	case *buildpb.BuildEvent_InvocationAttemptFinished_:
		return "InvocationAttemptFinished"
	case *buildpb.BuildEvent_BuildEnqueued_:
		return "BuildEnqueued"
	case *buildpb.BuildEvent_BuildFinished_:
		return "BuildFinished"
	case *buildpb.BuildEvent_ConsoleOutput_:
		return "ConsoleOutput"
	case *buildpb.BuildEvent_ComponentStreamFinished:
		return "ComponentStreamFinished"
	case *buildpb.BuildEvent_BazelEvent:
		// This contains the actual detailed BEP events
		// We'd need to unpack the Any to get the specific type
		return "BazelEvent"
	case *buildpb.BuildEvent_BuildExecutionEvent:
		return "BuildExecutionEvent"
	case *buildpb.BuildEvent_SourceFetchEvent:
		return "SourceFetchEvent"
	default:
		return "unknown"
	}
}
