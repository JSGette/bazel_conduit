package graph

import (
	"fmt"
	"testing"
	"time"

	buildpb "google.golang.org/genproto/googleapis/devtools/build/v1"
)

func TestAddEvent(t *testing.T) {
	graph := &EventGraph{
		Events: make(map[string]*StoredEvent),
	}

	event := &buildpb.BuildEvent{
		EventTime: nil,
	}

	err := graph.AddEvent("event-1", 1, event)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	retrieved, exists := graph.GetEvent("event-1")
	if !exists {
		t.Error("Expected event to exist")
	}

	if retrieved.Event != event {
		t.Error("Expected to retrieve the same event")
	}

	if retrieved.SequenceNum != 1 {
		t.Errorf("Expected sequence 1, got %d", retrieved.SequenceNum)
	}
}

func TestAddEventEmptyID(t *testing.T) {
	graph := &EventGraph{
		Events: make(map[string]*StoredEvent),
	}

	event := &buildpb.BuildEvent{}

	err := graph.AddEvent("", 1, event)
	if err == nil {
		t.Error("Expected error for empty event ID")
	}
}

func TestAddEventNilEvent(t *testing.T) {
	graph := &EventGraph{
		Events: make(map[string]*StoredEvent),
	}

	err := graph.AddEvent("event-1", 1, nil)
	if err == nil {
		t.Error("Expected error for nil event")
	}
}

func TestGetEventNotFound(t *testing.T) {
	graph := &EventGraph{
		Events: make(map[string]*StoredEvent),
	}

	_, exists := graph.GetEvent("nonexistent")
	if exists {
		t.Error("Expected event not to exist")
	}
}

func TestAddSpan(t *testing.T) {
	graph := &EventGraph{
		Spans:        make([]*Span, 0),
		PendingSpans: make(map[string]*Span),
	}

	span := &Span{
		SpanID:    "span-1",
		Name:      "Test Span",
		StartTime: time.Now(),
		// No EndTime, so it's pending
	}

	err := graph.AddSpan(span)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	if len(graph.Spans) != 1 {
		t.Errorf("Expected 1 span, got %d", len(graph.Spans))
	}

	// Should be in pending spans
	if _, exists := graph.PendingSpans["span-1"]; !exists {
		t.Error("Expected span to be in pending spans")
	}
}

func TestAddSpanWithEndTime(t *testing.T) {
	graph := &EventGraph{
		Spans:        make([]*Span, 0),
		PendingSpans: make(map[string]*Span),
	}

	span := &Span{
		SpanID:    "span-1",
		Name:      "Test Span",
		StartTime: time.Now(),
		EndTime:   time.Now(), // Has end time
	}

	err := graph.AddSpan(span)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	// Should NOT be in pending spans
	if _, exists := graph.PendingSpans["span-1"]; exists {
		t.Error("Expected span not to be in pending spans")
	}
}

func TestAddSpanNil(t *testing.T) {
	graph := &EventGraph{
		Spans:        make([]*Span, 0),
		PendingSpans: make(map[string]*Span),
	}

	err := graph.AddSpan(nil)
	if err == nil {
		t.Error("Expected error for nil span")
	}
}

func TestCompleteSpan(t *testing.T) {
	graph := &EventGraph{
		Spans:        make([]*Span, 0),
		PendingSpans: make(map[string]*Span),
	}

	span := &Span{
		SpanID:    "span-1",
		StartTime: time.Now(),
	}

	graph.AddSpan(span)

	// Complete the span
	endTime := time.Now().Add(time.Second)
	err := graph.CompleteSpan("span-1", endTime)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	// Should not be in pending spans anymore
	if _, exists := graph.PendingSpans["span-1"]; exists {
		t.Error("Expected span not to be in pending spans after completion")
	}

	// End time should be set
	if !span.EndTime.Equal(endTime) {
		t.Error("Expected end time to be set")
	}
}

func TestCompleteSpanNotFound(t *testing.T) {
	graph := &EventGraph{
		PendingSpans: make(map[string]*Span),
	}

	err := graph.CompleteSpan("nonexistent", time.Now())
	if err == nil {
		t.Error("Expected error for nonexistent span")
	}
}

func TestGetSpanCount(t *testing.T) {
	graph := &EventGraph{
		Spans:        make([]*Span, 0),
		PendingSpans: make(map[string]*Span),
	}

	if graph.GetSpanCount() != 0 {
		t.Error("Expected 0 spans initially")
	}

	graph.AddSpan(&Span{SpanID: "span-1", StartTime: time.Now()})
	graph.AddSpan(&Span{SpanID: "span-2", StartTime: time.Now()})

	if graph.GetSpanCount() != 2 {
		t.Errorf("Expected 2 spans, got %d", graph.GetSpanCount())
	}
}

func TestGetPendingSpanCount(t *testing.T) {
	graph := &EventGraph{
		Spans:        make([]*Span, 0),
		PendingSpans: make(map[string]*Span),
	}

	if graph.GetPendingSpanCount() != 0 {
		t.Error("Expected 0 pending spans initially")
	}

	// Add pending span
	graph.AddSpan(&Span{SpanID: "span-1", StartTime: time.Now()})

	if graph.GetPendingSpanCount() != 1 {
		t.Errorf("Expected 1 pending span, got %d", graph.GetPendingSpanCount())
	}

	// Add completed span
	graph.AddSpan(&Span{SpanID: "span-2", StartTime: time.Now(), EndTime: time.Now()})

	// Should still be 1 pending
	if graph.GetPendingSpanCount() != 1 {
		t.Errorf("Expected 1 pending span, got %d", graph.GetPendingSpanCount())
	}
}

func TestMarkInvocationStarted(t *testing.T) {
	graph := &EventGraph{}

	if graph.InvocationStarted {
		t.Error("Expected InvocationStarted to be false initially")
	}

	graph.MarkInvocationStarted()

	if !graph.InvocationStarted {
		t.Error("Expected InvocationStarted to be true")
	}
}

func TestMarkInvocationFinished(t *testing.T) {
	graph := &EventGraph{}

	if graph.InvocationFinished {
		t.Error("Expected InvocationFinished to be false initially")
	}

	graph.MarkInvocationFinished()

	if !graph.InvocationFinished {
		t.Error("Expected InvocationFinished to be true")
	}
}

func TestMarkBuildFinished(t *testing.T) {
	graph := &EventGraph{}

	if graph.BuildFinished {
		t.Error("Expected BuildFinished to be false initially")
	}

	graph.MarkBuildFinished()

	if !graph.BuildFinished {
		t.Error("Expected BuildFinished to be true")
	}
}

func TestIsComplete(t *testing.T) {
	graph := &EventGraph{}

	if graph.IsComplete() {
		t.Error("Expected build not to be complete initially")
	}

	graph.MarkBuildFinished()

	if !graph.IsComplete() {
		t.Error("Expected build to be complete")
	}
}

func TestGetAllSpans(t *testing.T) {
	graph := &EventGraph{
		Spans:        make([]*Span, 0),
		PendingSpans: make(map[string]*Span),
	}

	span1 := &Span{SpanID: "span-1", StartTime: time.Now()}
	span2 := &Span{SpanID: "span-2", StartTime: time.Now()}

	graph.AddSpan(span1)
	graph.AddSpan(span2)

	spans := graph.GetAllSpans()

	if len(spans) != 2 {
		t.Errorf("Expected 2 spans, got %d", len(spans))
	}

	// Verify it's a copy (modifying returned slice shouldn't affect graph)
	spans[0] = nil

	if graph.Spans[0] == nil {
		t.Error("Expected original spans to be unchanged")
	}
}

func TestGetEventCount(t *testing.T) {
	graph := &EventGraph{
		Events: make(map[string]*StoredEvent),
	}

	if graph.GetEventCount() != 0 {
		t.Error("Expected 0 events initially")
	}

	event1 := &buildpb.BuildEvent{}
	event2 := &buildpb.BuildEvent{}

	graph.AddEvent("event-1", 1, event1)
	graph.AddEvent("event-2", 2, event2)

	if graph.GetEventCount() != 2 {
		t.Errorf("Expected 2 events, got %d", graph.GetEventCount())
	}
}

func TestGetEventType(t *testing.T) {
	tests := []struct {
		name     string
		event    *buildpb.BuildEvent
		expected string
	}{
		{
			name:     "nil event",
			event:    nil,
			expected: "unknown",
		},
		{
			name: "InvocationAttemptStarted",
			event: &buildpb.BuildEvent{
				Event: &buildpb.BuildEvent_InvocationAttemptStarted_{},
			},
			expected: "InvocationAttemptStarted",
		},
		{
			name: "BuildFinished",
			event: &buildpb.BuildEvent{
				Event: &buildpb.BuildEvent_BuildFinished_{},
			},
			expected: "BuildFinished",
		},
		{
			name: "BazelEvent",
			event: &buildpb.BuildEvent{
				Event: &buildpb.BuildEvent_BazelEvent{},
			},
			expected: "BazelEvent",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := GetEventType(tt.event)
			if result != tt.expected {
				t.Errorf("Expected %s, got %s", tt.expected, result)
			}
		})
	}
}

func TestGetBazelEvent(t *testing.T) {
	// Nil event
	result := GetBazelEvent(nil)
	if result != nil {
		t.Error("Expected nil for nil event")
	}

	// Event without BazelEvent
	event := &buildpb.BuildEvent{
		Event: &buildpb.BuildEvent_BuildFinished_{},
	}
	result = GetBazelEvent(event)
	if result != nil {
		t.Error("Expected nil when not a BazelEvent")
	}
}

func TestConcurrentSpanOperations(t *testing.T) {
	graph := &EventGraph{
		Spans:        make([]*Span, 0),
		PendingSpans: make(map[string]*Span),
	}

	done := make(chan bool)

	// Add spans concurrently
	for i := 0; i < 10; i++ {
		go func(i int) {
			span := &Span{
				SpanID:    fmt.Sprintf("span-%d", i),
				StartTime: time.Now(),
			}
			graph.AddSpan(span)
			done <- true
		}(i)
	}

	// Wait for all goroutines
	for i := 0; i < 10; i++ {
		<-done
	}

	if graph.GetSpanCount() != 10 {
		t.Errorf("Expected 10 spans, got %d", graph.GetSpanCount())
	}
}

func TestConcurrentEventOperations(t *testing.T) {
	graph := &EventGraph{
		Events: make(map[string]*StoredEvent),
	}

	done := make(chan bool)

	// Add events concurrently
	for i := 0; i < 10; i++ {
		go func(i int) {
			eventID := fmt.Sprintf("event-%d", i)
			event := &buildpb.BuildEvent{}
			graph.AddEvent(eventID, int64(i), event)
			done <- true
		}(i)
	}

	// Wait for all goroutines
	for i := 0; i < 10; i++ {
		<-done
	}

	if graph.GetEventCount() != 10 {
		t.Errorf("Expected 10 events, got %d", graph.GetEventCount())
	}
}
