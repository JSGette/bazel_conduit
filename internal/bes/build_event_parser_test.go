package bes

import (
	"context"
	"log/slog"
	"testing"

	build_event_stream "github.com/JSGette/bazel_conduit/proto/build_event_stream"
	"go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	build "google.golang.org/genproto/googleapis/devtools/build/v1"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestNewBuildEventParser(t *testing.T) {
	tests := []struct {
		name   string
		logger *slog.Logger
	}{
		{
			name:   "parser with logger",
			logger: slog.Default(),
		},
		{
			name:   "parser with nil logger",
			logger: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser := NewBuildEventParser(tt.logger)

			if parser == nil {
				t.Fatal("NewBuildEventParser returned nil")
			}

			if parser.logger == nil {
				t.Error("logger should not be nil")
			}

			if parser.tracer == nil {
				t.Error("tracer should not be nil")
			}
		})
	}
}

func TestExtractEventID(t *testing.T) {
	tests := []struct {
		name     string
		eventID  *build_event_stream.BuildEventId
		expected string
	}{
		{
			name:     "nil event ID",
			eventID:  nil,
			expected: "",
		},
		{
			name: "BuildStarted ID",
			eventID: &build_event_stream.BuildEventId{
				Id: &build_event_stream.BuildEventId_Started{
					Started: &build_event_stream.BuildEventId_BuildStartedId{},
				},
			},
			expected: "BuildStarted",
		},
		{
			name: "BuildFinished ID",
			eventID: &build_event_stream.BuildEventId{
				Id: &build_event_stream.BuildEventId_BuildFinished{
					BuildFinished: &build_event_stream.BuildEventId_BuildFinishedId{},
				},
			},
			expected: "BuildFinished",
		},
		{
			name: "Pattern ID",
			eventID: &build_event_stream.BuildEventId{
				Id: &build_event_stream.BuildEventId_Pattern{
					Pattern: &build_event_stream.BuildEventId_PatternExpandedId{
						Pattern: []string{"//..."},
					},
				},
			},
			expected: "pattern:[//...]",
		},
		{
			name: "TargetConfigured ID",
			eventID: &build_event_stream.BuildEventId{
				Id: &build_event_stream.BuildEventId_TargetConfigured{
					TargetConfigured: &build_event_stream.BuildEventId_TargetConfiguredId{
						Label:  "//my/target:name",
						Aspect: "",
					},
				},
			},
			expected: "target_configured://my/target:name:",
		},
		{
			name: "TargetCompleted ID",
			eventID: &build_event_stream.BuildEventId{
				Id: &build_event_stream.BuildEventId_TargetCompleted{
					TargetCompleted: &build_event_stream.BuildEventId_TargetCompletedId{
						Label:  "//my/target:name",
						Aspect: "",
					},
				},
			},
			expected: "target_completed://my/target:name:",
		},
		{
			name: "TestResult ID",
			eventID: &build_event_stream.BuildEventId{
				Id: &build_event_stream.BuildEventId_TestResult{
					TestResult: &build_event_stream.BuildEventId_TestResultId{
						Label: "//test:target",
						Run:   1,
						Shard: 2,
					},
				},
			},
			expected: "test_result://test:target:run1:shard2:attempt0",
		},
		{
			name: "Progress ID",
			eventID: &build_event_stream.BuildEventId{
				Id: &build_event_stream.BuildEventId_Progress{
					Progress: &build_event_stream.BuildEventId_ProgressId{
						OpaqueCount: 42,
					},
				},
			},
			expected: "progress:42",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := extractEventID(tt.eventID)
			if result != tt.expected {
				t.Errorf("extractEventID() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestGetEventType(t *testing.T) {
	tests := []struct {
		name     string
		event    *build_event_stream.BuildEvent
		expected string
	}{
		{
			name: "BuildStarted",
			event: &build_event_stream.BuildEvent{
				Payload: &build_event_stream.BuildEvent_Started{
					Started: &build_event_stream.BuildStarted{},
				},
			},
			expected: "BuildStarted",
		},
		{
			name: "BuildFinished",
			event: &build_event_stream.BuildEvent{
				Payload: &build_event_stream.BuildEvent_Finished{
					Finished: &build_event_stream.BuildFinished{},
				},
			},
			expected: "BuildFinished",
		},
		{
			name: "Progress",
			event: &build_event_stream.BuildEvent{
				Payload: &build_event_stream.BuildEvent_Progress{
					Progress: &build_event_stream.Progress{},
				},
			},
			expected: "Progress",
		},
		{
			name: "TargetComplete",
			event: &build_event_stream.BuildEvent{
				Payload: &build_event_stream.BuildEvent_Completed{
					Completed: &build_event_stream.TargetComplete{},
				},
			},
			expected: "TargetComplete",
		},
		{
			name: "ActionExecuted",
			event: &build_event_stream.BuildEvent{
				Payload: &build_event_stream.BuildEvent_Action{
					Action: &build_event_stream.ActionExecuted{},
				},
			},
			expected: "ActionExecuted",
		},
		{
			name: "TestResult",
			event: &build_event_stream.BuildEvent{
				Payload: &build_event_stream.BuildEvent_TestResult{
					TestResult: &build_event_stream.TestResult{},
				},
			},
			expected: "TestResult",
		},
		{
			name:     "Unknown",
			event:    &build_event_stream.BuildEvent{},
			expected: "UnknownEvent",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := getEventType(tt.event)
			if result != tt.expected {
				t.Errorf("getEventType() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestShouldEndSpanImmediately(t *testing.T) {
	tests := []struct {
		name     string
		event    *build_event_stream.BuildEvent
		expected bool
	}{
		{
			name: "Progress - should end immediately",
			event: &build_event_stream.BuildEvent{
				Payload: &build_event_stream.BuildEvent_Progress{},
			},
			expected: true,
		},
		{
			name: "BuildStarted - should stay open",
			event: &build_event_stream.BuildEvent{
				Payload: &build_event_stream.BuildEvent_Started{},
			},
			expected: false,
		},
		{
			name: "TargetComplete - should stay open",
			event: &build_event_stream.BuildEvent{
				Payload: &build_event_stream.BuildEvent_Completed{},
			},
			expected: false,
		},
		{
			name: "BuildFinished - should end immediately",
			event: &build_event_stream.BuildEvent{
				Payload: &build_event_stream.BuildEvent_Finished{},
			},
			expected: true,
		},
		{
			name: "OptionsParsed - should end immediately",
			event: &build_event_stream.BuildEvent{
				Payload: &build_event_stream.BuildEvent_OptionsParsed{},
			},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := shouldEndSpanImmediately(tt.event)
			if result != tt.expected {
				t.Errorf("shouldEndSpanImmediately() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestGenerateTraceIDFromInvocationID(t *testing.T) {
	tests := []struct {
		name         string
		invocationID string
	}{
		{
			name:         "simple invocation ID",
			invocationID: "test-invocation-123",
		},
		{
			name:         "UUID invocation ID",
			invocationID: "550e8400-e29b-41d4-a716-446655440000",
		},
		{
			name:         "empty invocation ID",
			invocationID: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			traceID1 := generateTraceIDFromInvocationID(tt.invocationID)
			traceID2 := generateTraceIDFromInvocationID(tt.invocationID)

			// Verify determinism - same invocation ID should produce same trace ID
			if traceID1 != traceID2 {
				t.Errorf("generateTraceIDFromInvocationID() not deterministic: %v != %v", traceID1, traceID2)
			}

			// Verify it's a valid trace ID (non-zero even for empty string)
			if !traceID1.IsValid() {
				t.Errorf("generateTraceIDFromInvocationID() should produce valid trace ID for %q", tt.invocationID)
			}
		})
	}

	// Verify different invocation IDs produce different trace IDs
	t.Run("different invocation IDs produce different trace IDs", func(t *testing.T) {
		traceID1 := generateTraceIDFromInvocationID("inv1")
		traceID2 := generateTraceIDFromInvocationID("inv2")

		if traceID1 == traceID2 {
			t.Error("Different invocation IDs should produce different trace IDs")
		}
	})
}

func TestBuildContext_FindParentContext(t *testing.T) {
	ctx := context.Background()
	bc := &BuildContext{
		rootCtx:       ctx,
		eventContexts: make(map[string]context.Context),
		childToParent: make(map[string]string),
	}

	// Add a parent context
	parentCtx := context.WithValue(ctx, "test", "parent")
	bc.eventContexts["parent-event"] = parentCtx

	// Register child
	bc.childToParent["child-event"] = "parent-event"

	tests := []struct {
		name     string
		eventID  string
		expected context.Context
	}{
		{
			name:     "find existing parent",
			eventID:  "child-event",
			expected: parentCtx,
		},
		{
			name:     "no parent - use root",
			eventID:  "orphan-event",
			expected: bc.rootCtx,
		},
		{
			name:     "parent announced but not yet received",
			eventID:  "early-child",
			expected: bc.rootCtx,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.name == "parent announced but not yet received" {
				bc.childToParent["early-child"] = "missing-parent"
			}

			result := bc.findParentContext(tt.eventID)
			if result != tt.expected {
				t.Errorf("findParentContext() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestBuildContext_RegisterChildren(t *testing.T) {
	bc := &BuildContext{
		childToParent: make(map[string]string),
	}

	children := []*build_event_stream.BuildEventId{
		{
			Id: &build_event_stream.BuildEventId_Progress{
				Progress: &build_event_stream.BuildEventId_ProgressId{OpaqueCount: 1},
			},
		},
		{
			Id: &build_event_stream.BuildEventId_TargetCompleted{
				TargetCompleted: &build_event_stream.BuildEventId_TargetCompletedId{
					Label: "//test:target",
				},
			},
		},
	}

	bc.registerChildren("parent-event", children)

	// Verify children were registered
	if len(bc.childToParent) != 2 {
		t.Errorf("Expected 2 children registered, got %d", len(bc.childToParent))
	}

	// Verify correct parent assignment
	if bc.childToParent["progress:1"] != "parent-event" {
		t.Error("Progress child not correctly registered")
	}

	if bc.childToParent["target_completed://test:target:"] != "parent-event" {
		t.Error("TargetCompleted child not correctly registered")
	}
}

func TestParseBuildEventStreamBuildEvent(t *testing.T) {
	// Create a span recorder for testing
	recorder := tracetest.NewSpanRecorder()
	tp := trace.NewTracerProvider(
		trace.WithSpanProcessor(recorder),
	)

	parser := &BuildEventParser{
		logger: slog.Default(),
		tracer: tp.Tracer("test"),
	}

	tests := []struct {
		name         string
		buildID      string
		invocationID string
		setupEvent   func() *build.BuildEvent
		wantErr      bool
		checkSpan    func(t *testing.T, spans []trace.ReadOnlySpan)
	}{
		{
			name:         "BuildStarted event",
			buildID:      "build-123",
			invocationID: "inv-456",
			setupEvent: func() *build.BuildEvent {
				bazelEvent := &build_event_stream.BuildEvent{
					Id: &build_event_stream.BuildEventId{
						Id: &build_event_stream.BuildEventId_Started{},
					},
					Payload: &build_event_stream.BuildEvent_Started{
						Started: &build_event_stream.BuildStarted{
							Uuid:               "uuid-123",
							Command:            "build",
							WorkspaceDirectory: "/workspace",
							StartTime:          timestamppb.Now(),
						},
					},
				}

				anyEvent, _ := anypb.New(bazelEvent)
				return &build.BuildEvent{
					Event: &build.BuildEvent_BazelEvent{
						BazelEvent: anyEvent,
					},
				}
			},
			wantErr:   false,
			checkSpan: nil, // BuildStarted keeps span open, won't appear in ended spans
		},
		{
			name:         "Progress event - ends immediately",
			buildID:      "build-789",
			invocationID: "inv-012",
			setupEvent: func() *build.BuildEvent {
				bazelEvent := &build_event_stream.BuildEvent{
					Id: &build_event_stream.BuildEventId{
						Id: &build_event_stream.BuildEventId_Progress{
							Progress: &build_event_stream.BuildEventId_ProgressId{
								OpaqueCount: 1,
							},
						},
					},
					Payload: &build_event_stream.BuildEvent_Progress{
						Progress: &build_event_stream.Progress{},
					},
				}

				anyEvent, _ := anypb.New(bazelEvent)
				return &build.BuildEvent{
					Event: &build.BuildEvent_BazelEvent{
						BazelEvent: anyEvent,
					},
				}
			},
			wantErr: false,
			checkSpan: func(t *testing.T, spans []trace.ReadOnlySpan) {
				if len(spans) == 0 {
					t.Fatal("Expected at least one span")
				}
				lastSpan := spans[len(spans)-1]
				if lastSpan.Name() != "Progress" {
					t.Errorf("Expected span name 'Progress', got %s", lastSpan.Name())
				}

				// Verify span ended (has end time)
				if lastSpan.EndTime().IsZero() {
					t.Error("Expected span to have ended")
				}
			},
		},
		{
			name:         "nil BazelEvent - should skip gracefully",
			buildID:      "build-999",
			invocationID: "inv-999",
			setupEvent: func() *build.BuildEvent {
				return &build.BuildEvent{
					Event: &build.BuildEvent_BazelEvent{
						BazelEvent: nil,
					},
				}
			},
			wantErr:   false, // Should not error, just skip
			checkSpan: nil,
		},
		{
			name:         "non-BazelEvent - ComponentStreamFinished",
			buildID:      "build-888",
			invocationID: "inv-888",
			setupEvent: func() *build.BuildEvent {
				// Create an event without BazelEvent field set
				return &build.BuildEvent{
					Event: nil, // No event payload, simulating end-of-stream
				}
			},
			wantErr:   false, // Should not error, just skip
			checkSpan: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Clear recorder for each test
			recorder.Reset()

			ctx := context.Background()
			event := tt.setupEvent()

			err := parser.ParseBuildEventStreamBuildEvent(ctx, tt.buildID, tt.invocationID, event)

			if tt.wantErr {
				if err == nil {
					t.Error("Expected error, got nil")
				}
				return
			}

			if err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}

			if tt.checkSpan != nil {
				// Force flush any pending spans
				tp.ForceFlush(ctx)
				spans := recorder.Ended()
				tt.checkSpan(t, spans)
			}
		})
	}
}

func TestParentChildSpanRelationship(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	tp := trace.NewTracerProvider(
		trace.WithSpanProcessor(recorder),
	)

	parser := &BuildEventParser{
		logger: slog.Default(),
		tracer: tp.Tracer("test"),
	}

	ctx := context.Background()
	buildID := "build-nested"
	invocationID := "inv-nested"

	// Create BuildStarted event that announces children
	buildStartedEvent := &build_event_stream.BuildEvent{
		Id: &build_event_stream.BuildEventId{
			Id: &build_event_stream.BuildEventId_Started{},
		},
		Payload: &build_event_stream.BuildEvent_Started{
			Started: &build_event_stream.BuildStarted{
				Uuid:      "uuid",
				StartTime: timestamppb.Now(),
			},
		},
		Children: []*build_event_stream.BuildEventId{
			{
				Id: &build_event_stream.BuildEventId_Progress{
					Progress: &build_event_stream.BuildEventId_ProgressId{OpaqueCount: 1},
				},
			},
		},
	}

	anyStarted, _ := anypb.New(buildStartedEvent)
	wrappedStarted := &build.BuildEvent{
		Event: &build.BuildEvent_BazelEvent{
			BazelEvent: anyStarted,
		},
	}

	// Parse parent event
	err := parser.ParseBuildEventStreamBuildEvent(ctx, buildID, invocationID, wrappedStarted)
	if err != nil {
		t.Fatalf("Failed to parse parent event: %v", err)
	}

	// Create child Progress event
	progressEvent := &build_event_stream.BuildEvent{
		Id: &build_event_stream.BuildEventId{
			Id: &build_event_stream.BuildEventId_Progress{
				Progress: &build_event_stream.BuildEventId_ProgressId{OpaqueCount: 1},
			},
		},
		Payload: &build_event_stream.BuildEvent_Progress{
			Progress: &build_event_stream.Progress{},
		},
	}

	anyProgress, _ := anypb.New(progressEvent)
	wrappedProgress := &build.BuildEvent{
		Event: &build.BuildEvent_BazelEvent{
			BazelEvent: anyProgress,
		},
	}

	// Parse child event
	err = parser.ParseBuildEventStreamBuildEvent(ctx, buildID, invocationID, wrappedProgress)
	if err != nil {
		t.Fatalf("Failed to parse child event: %v", err)
	}

	// Verify spans were created
	tp.ForceFlush(ctx)
	spans := recorder.Ended()

	// Progress event should have ended immediately
	if len(spans) < 1 {
		t.Fatalf("Expected at least 1 ended span (Progress), got %d", len(spans))
	}

	// Find Progress span
	var progressSpan trace.ReadOnlySpan
	for _, span := range spans {
		if span.Name() == "Progress" {
			progressSpan = span
			break
		}
	}

	if progressSpan == nil {
		t.Fatal("Could not find Progress span")
	}

	// Verify Progress span has a parent (BuildStarted)
	if !progressSpan.Parent().IsValid() {
		t.Error("Progress span should have a valid parent")
	}
}
