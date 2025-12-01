package translator

import (
	"log/slog"
	"os"
	"testing"
	"time"

	build "google.golang.org/genproto/googleapis/devtools/build/v1"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestNewTranslator(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	buildID := "test-build-123"
	invocationID := "test-invocation-456"

	translator := NewTranslator(buildID, invocationID, logger)

	if translator == nil {
		t.Fatal("NewTranslator returned nil")
	}

	if translator.buildID != buildID {
		t.Errorf("Expected buildID %s, got %s", buildID, translator.buildID)
	}

	if translator.invocationID != invocationID {
		t.Errorf("Expected invocationID %s, got %s", invocationID, translator.invocationID)
	}

	if !translator.traceID.IsValid() {
		t.Error("Expected valid trace ID")
	}

	if translator.spanCache == nil {
		t.Error("Expected span cache to be initialized")
	}
}

func TestGenerateTraceID(t *testing.T) {
	buildID1 := "test-build-123"
	buildID2 := "test-build-456"

	traceID1 := generateTraceID(buildID1)
	traceID2 := generateTraceID(buildID2)

	if !traceID1.IsValid() {
		t.Error("Expected valid trace ID for buildID1")
	}

	if !traceID2.IsValid() {
		t.Error("Expected valid trace ID for buildID2")
	}

	// Same build ID should generate same trace ID
	traceID1Again := generateTraceID(buildID1)
	if traceID1 != traceID1Again {
		t.Error("Expected same trace ID for same build ID")
	}

	// Different build IDs should generate different trace IDs
	if traceID1 == traceID2 {
		t.Error("Expected different trace IDs for different build IDs")
	}
}

func TestGenerateSpanID(t *testing.T) {
	buildID := "test-build-123"
	seq1 := "1"
	seq2 := "2"

	spanID1 := generateSpanID(buildID, seq1)
	spanID2 := generateSpanID(buildID, seq2)

	if !spanID1.IsValid() {
		t.Error("Expected valid span ID for seq1")
	}

	if !spanID2.IsValid() {
		t.Error("Expected valid span ID for seq2")
	}

	// Same inputs should generate same span ID
	spanID1Again := generateSpanID(buildID, seq1)
	if spanID1 != spanID1Again {
		t.Error("Expected same span ID for same inputs")
	}

	// Different sequences should generate different span IDs
	if spanID1 == spanID2 {
		t.Error("Expected different span IDs for different sequences")
	}
}

func TestExtractEventTypeFromURL(t *testing.T) {
	tests := []struct {
		name     string
		typeURL  string
		expected string
	}{
		{
			name:     "BuildStarted",
			typeURL:  "type.googleapis.com/build_event_stream.BuildStarted",
			expected: "BuildStarted",
		},
		{
			name:     "BuildFinished",
			typeURL:  "type.googleapis.com/build_event_stream.BuildFinished",
			expected: "BuildFinished",
		},
		{
			name:     "TargetComplete",
			typeURL:  "type.googleapis.com/build_event_stream.TargetComplete",
			expected: "TargetComplete",
		},
		{
			name:     "NoSlash",
			typeURL:  "BuildEvent",
			expected: "unknown",
		},
		{
			name:     "NoDot",
			typeURL:  "type.googleapis.com/buildeventstream",
			expected: "buildeventstream",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := extractEventTypeFromURL(tt.typeURL)
			if result != tt.expected {
				t.Errorf("Expected %s, got %s", tt.expected, result)
			}
		})
	}
}

func TestTranslateEvent_NilEvent(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	translator := NewTranslator("build-123", "inv-456", logger)

	_, err := translator.TranslateEvent(nil)
	if err == nil {
		t.Error("Expected error for nil event")
	}
}

func TestTranslateEvent_BazelEvent(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	buildID := "test-build-123"
	invocationID := "test-invocation-456"
	translator := NewTranslator(buildID, invocationID, logger)

	eventTime := timestamppb.New(time.Now())
	bazelEvent := &anypb.Any{
		TypeUrl: "type.googleapis.com/build_event_stream.BuildStarted",
		Value:   []byte("dummy-data"),
	}

	event := &build.OrderedBuildEvent{
		SequenceNumber: 1,
		Event: &build.BuildEvent{
			EventTime: eventTime,
			Event: &build.BuildEvent_BazelEvent{
				BazelEvent: bazelEvent,
			},
		},
	}

	span, err := translator.TranslateEvent(event)
	if err != nil {
		t.Fatalf("TranslateEvent failed: %v", err)
	}

	if span == nil {
		t.Fatal("Expected non-nil span")
	}

	if span.Name != "build.BuildStarted" {
		t.Errorf("Expected span name 'build.BuildStarted', got '%s'", span.Name)
	}

	if span.TraceID != translator.traceID {
		t.Error("Expected span trace ID to match translator trace ID")
	}

	if !span.SpanID.IsValid() {
		t.Error("Expected valid span ID")
	}

	if span.StartTime != eventTime.AsTime() {
		t.Error("Expected span start time to match event time")
	}

	// Check attributes
	if span.Attributes["build.id"] != buildID {
		t.Errorf("Expected build.id=%s, got %v", buildID, span.Attributes["build.id"])
	}

	if span.Attributes["build.invocation_id"] != invocationID {
		t.Errorf("Expected build.invocation_id=%s, got %v", invocationID, span.Attributes["build.invocation_id"])
	}

	if span.Attributes["build.sequence_number"] != int64(1) {
		t.Errorf("Expected build.sequence_number=1, got %v", span.Attributes["build.sequence_number"])
	}

	if span.Attributes["build.event.type_url"] != bazelEvent.TypeUrl {
		t.Errorf("Expected type_url=%s, got %v", bazelEvent.TypeUrl, span.Attributes["build.event.type_url"])
	}

	if span.Attributes["build.event.source"] != "bazel" {
		t.Errorf("Expected build.event.source=bazel, got %v", span.Attributes["build.event.source"])
	}
}

func TestTranslateEvent_ComponentStreamFinished(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	translator := NewTranslator("build-123", "inv-456", logger)

	event := &build.OrderedBuildEvent{
		SequenceNumber: 10,
		Event: &build.BuildEvent{
			EventTime: timestamppb.Now(),
			Event: &build.BuildEvent_ComponentStreamFinished{
				ComponentStreamFinished: &build.BuildEvent_BuildComponentStreamFinished{
					Type: build.BuildEvent_BuildComponentStreamFinished_FINISHED,
				},
			},
		},
	}

	span, err := translator.TranslateEvent(event)
	if err != nil {
		t.Fatalf("TranslateEvent failed: %v", err)
	}

	if span.Name != "build.stream.finished" {
		t.Errorf("Expected span name 'build.stream.finished', got '%s'", span.Name)
	}

	streamType, ok := span.Attributes["build.stream.type"].(string)
	if !ok {
		t.Error("Expected build.stream.type attribute")
	}

	if streamType != "FINISHED" {
		t.Errorf("Expected stream type 'FINISHED', got '%s'", streamType)
	}
}

func TestTranslateEvent_Caching(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	translator := NewTranslator("build-123", "inv-456", logger)

	event := &build.OrderedBuildEvent{
		SequenceNumber: 1,
		Event: &build.BuildEvent{
			EventTime: timestamppb.Now(),
			Event: &build.BuildEvent_BazelEvent{
				BazelEvent: &anypb.Any{
					TypeUrl: "type.googleapis.com/build_event_stream.BuildStarted",
					Value:   []byte("dummy"),
				},
			},
		},
	}

	// First translation
	span1, err := translator.TranslateEvent(event)
	if err != nil {
		t.Fatalf("First TranslateEvent failed: %v", err)
	}

	// Second translation - should return cached span
	span2, err := translator.TranslateEvent(event)
	if err != nil {
		t.Fatalf("Second TranslateEvent failed: %v", err)
	}

	// Should be the exact same span object (pointer equality)
	if span1 != span2 {
		t.Error("Expected cached span to be returned")
	}

	if translator.GetSpanCount() != 1 {
		t.Errorf("Expected 1 cached span, got %d", translator.GetSpanCount())
	}
}

func TestGetAllSpans(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	translator := NewTranslator("build-123", "inv-456", logger)

	// Create multiple events
	for i := 1; i <= 5; i++ {
		event := &build.OrderedBuildEvent{
			SequenceNumber: int64(i),
			Event: &build.BuildEvent{
				EventTime: timestamppb.Now(),
				Event: &build.BuildEvent_BazelEvent{
					BazelEvent: &anypb.Any{
						TypeUrl: "type.googleapis.com/build_event_stream.BuildEvent",
						Value:   []byte("dummy"),
					},
				},
			},
		}
		_, err := translator.TranslateEvent(event)
		if err != nil {
			t.Fatalf("TranslateEvent failed for sequence %d: %v", i, err)
		}
	}

	spans := translator.GetAllSpans()
	if len(spans) != 5 {
		t.Errorf("Expected 5 spans, got %d", len(spans))
	}

	if translator.GetSpanCount() != 5 {
		t.Errorf("Expected span count 5, got %d", translator.GetSpanCount())
	}
}

func TestUpdateSpanTiming(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	translator := NewTranslator("build-123", "inv-456", logger)

	startTime := time.Now()
	event := &build.OrderedBuildEvent{
		SequenceNumber: 1,
		Event: &build.BuildEvent{
			EventTime: timestamppb.New(startTime),
			Event: &build.BuildEvent_BazelEvent{
				BazelEvent: &anypb.Any{
					TypeUrl: "type.googleapis.com/build_event_stream.BuildStarted",
					Value:   []byte("dummy"),
				},
			},
		},
	}

	span, err := translator.TranslateEvent(event)
	if err != nil {
		t.Fatalf("TranslateEvent failed: %v", err)
	}

	originalEndTime := span.EndTime

	// Update end time to later
	newEndTime := startTime.Add(5 * time.Second)
	err = translator.UpdateSpanTiming(1, newEndTime)
	if err != nil {
		t.Fatalf("UpdateSpanTiming failed: %v", err)
	}

	if span.EndTime != newEndTime {
		t.Errorf("Expected end time to be updated to %v, got %v", newEndTime, span.EndTime)
	}

	// Try to update with earlier time - should not update
	earlierTime := startTime.Add(-1 * time.Second)
	err = translator.UpdateSpanTiming(1, earlierTime)
	if err != nil {
		t.Fatalf("UpdateSpanTiming failed: %v", err)
	}

	if span.EndTime != newEndTime {
		t.Error("End time should not be updated to an earlier time")
	}

	// Try to update non-existent span
	err = translator.UpdateSpanTiming(999, time.Now())
	if err == nil {
		t.Error("Expected error when updating non-existent span")
	}

	_ = originalEndTime // Use variable
}

func TestAddSpanEvent(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	translator := NewTranslator("build-123", "inv-456", logger)

	event := &build.OrderedBuildEvent{
		SequenceNumber: 1,
		Event: &build.BuildEvent{
			EventTime: timestamppb.Now(),
			Event: &build.BuildEvent_BazelEvent{
				BazelEvent: &anypb.Any{
					TypeUrl: "type.googleapis.com/build_event_stream.BuildStarted",
					Value:   []byte("dummy"),
				},
			},
		},
	}

	span, err := translator.TranslateEvent(event)
	if err != nil {
		t.Fatalf("TranslateEvent failed: %v", err)
	}

	// Add span event
	eventTime := time.Now()
	eventAttrs := map[string]interface{}{
		"event_attr": "value",
	}
	err = translator.AddSpanEvent(1, "test.event", eventTime, eventAttrs)
	if err != nil {
		t.Fatalf("AddSpanEvent failed: %v", err)
	}

	if len(span.Events) != 1 {
		t.Fatalf("Expected 1 span event, got %d", len(span.Events))
	}

	spanEvent := span.Events[0]
	if spanEvent.Name != "test.event" {
		t.Errorf("Expected event name 'test.event', got '%s'", spanEvent.Name)
	}

	if spanEvent.Timestamp != eventTime {
		t.Error("Event timestamp mismatch")
	}

	if spanEvent.Attributes["event_attr"] != "value" {
		t.Error("Event attributes mismatch")
	}

	// Try to add event to non-existent span
	err = translator.AddSpanEvent(999, "test", time.Now(), nil)
	if err == nil {
		t.Error("Expected error when adding event to non-existent span")
	}
}

func TestSetSpanStatus(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	translator := NewTranslator("build-123", "inv-456", logger)

	event := &build.OrderedBuildEvent{
		SequenceNumber: 1,
		Event: &build.BuildEvent{
			EventTime: timestamppb.Now(),
			Event: &build.BuildEvent_BazelEvent{
				BazelEvent: &anypb.Any{
					TypeUrl: "type.googleapis.com/build_event_stream.BuildStarted",
					Value:   []byte("dummy"),
				},
			},
		},
	}

	span, err := translator.TranslateEvent(event)
	if err != nil {
		t.Fatalf("TranslateEvent failed: %v", err)
	}

	// Initial status should be unset
	if span.Status.Code != StatusCodeUnset {
		t.Error("Expected initial status to be unset")
	}

	// Set status to OK
	err = translator.SetSpanStatus(1, StatusCodeOK, "Build completed successfully")
	if err != nil {
		t.Fatalf("SetSpanStatus failed: %v", err)
	}

	if span.Status.Code != StatusCodeOK {
		t.Errorf("Expected status code OK, got %v", span.Status.Code)
	}

	if span.Status.Message != "Build completed successfully" {
		t.Errorf("Expected status message 'Build completed successfully', got '%s'", span.Status.Message)
	}

	// Set status to Error
	err = translator.SetSpanStatus(1, StatusCodeError, "Build failed")
	if err != nil {
		t.Fatalf("SetSpanStatus failed: %v", err)
	}

	if span.Status.Code != StatusCodeError {
		t.Errorf("Expected status code Error, got %v", span.Status.Code)
	}

	// Try to set status on non-existent span
	err = translator.SetSpanStatus(999, StatusCodeOK, "test")
	if err == nil {
		t.Error("Expected error when setting status on non-existent span")
	}
}

func TestTranslatorAccessors(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	buildID := "test-build-123"
	invocationID := "test-invocation-456"
	translator := NewTranslator(buildID, invocationID, logger)

	if translator.BuildID() != buildID {
		t.Errorf("Expected BuildID() to return %s, got %s", buildID, translator.BuildID())
	}

	if translator.InvocationID() != invocationID {
		t.Errorf("Expected InvocationID() to return %s, got %s", invocationID, translator.InvocationID())
	}

	if !translator.TraceID().IsValid() {
		t.Error("Expected TraceID() to return valid trace ID")
	}
}

