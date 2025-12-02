package bes

import (
	"bytes"
	"log/slog"
	"testing"

	build_event_stream "github.com/JSGette/bazel_conduit/proto/build_event_stream"
	build "google.golang.org/genproto/googleapis/devtools/build/v1"
	"google.golang.org/protobuf/types/known/anypb"
)

func TestNewBuildEventParser(t *testing.T) {
	tests := []struct {
		name   string
		logger *slog.Logger
		toJson bool
		toOTel bool
	}{
		{
			name:   "parser with JSON enabled",
			logger: slog.Default(),
			toJson: true,
			toOTel: false,
		},
		{
			name:   "parser with OTel enabled",
			logger: slog.Default(),
			toJson: false,
			toOTel: true,
		},
		{
			name:   "parser with both enabled",
			logger: slog.Default(),
			toJson: true,
			toOTel: true,
		},
		{
			name:   "parser with both disabled",
			logger: slog.Default(),
			toJson: false,
			toOTel: false,
		},
		{
			name:   "parser with nil logger",
			logger: nil,
			toJson: false,
			toOTel: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser := NewBuildEventParser(tt.logger, tt.toJson, tt.toOTel)

			if parser == nil {
				t.Fatal("NewBuildEventParser returned nil")
			}

			if parser.toJson != tt.toJson {
				t.Errorf("toJson = %v, want %v", parser.toJson, tt.toJson)
			}

			if parser.toOTel != tt.toOTel {
				t.Errorf("toOTel = %v, want %v", parser.toOTel, tt.toOTel)
			}

			if parser.logger == nil {
				t.Error("logger should not be nil")
			}
		})
	}
}

func TestParseBuildEvent(t *testing.T) {
	tests := []struct {
		name        string
		setupEvent  func() *build.BuildEvent
		toJson      bool
		toOTel      bool
		wantErr     bool
		errContains string
	}{
		{
			name: "successful parse - BuildStarted event",
			setupEvent: func() *build.BuildEvent {
				bazelEvent := &build_event_stream.BuildEvent{
					Id: &build_event_stream.BuildEventId{
						Id: &build_event_stream.BuildEventId_Started{
							Started: &build_event_stream.BuildEventId_BuildStartedId{},
						},
					},
					Payload: &build_event_stream.BuildEvent_Started{
						Started: &build_event_stream.BuildStarted{
							Uuid:               "test-uuid-123",
							StartTime:          nil,
							BuildToolVersion:   "7.0.0",
							OptionsDescription: "build options",
							Command:            "build",
							WorkingDirectory:   "/workspace",
						},
					},
				}

				anyEvent, err := anypb.New(bazelEvent)
				if err != nil {
					t.Fatalf("Failed to create Any: %v", err)
				}

				return &build.BuildEvent{
					Event: &build.BuildEvent_BazelEvent{
						BazelEvent: anyEvent,
					},
				}
			},
			toJson:  false,
			toOTel:  false,
			wantErr: false,
		},
		{
			name: "successful parse with JSON flag",
			setupEvent: func() *build.BuildEvent {
				bazelEvent := &build_event_stream.BuildEvent{
					Id: &build_event_stream.BuildEventId{
						Id: &build_event_stream.BuildEventId_Started{
							Started: &build_event_stream.BuildEventId_BuildStartedId{},
						},
					},
				}

				anyEvent, err := anypb.New(bazelEvent)
				if err != nil {
					t.Fatalf("Failed to create Any: %v", err)
				}

				return &build.BuildEvent{
					Event: &build.BuildEvent_BazelEvent{
						BazelEvent: anyEvent,
					},
				}
			},
			toJson:  true,
			toOTel:  false,
			wantErr: false,
		},
		{
			name: "successful parse with OTel flag",
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

				anyEvent, err := anypb.New(bazelEvent)
				if err != nil {
					t.Fatalf("Failed to create Any: %v", err)
				}

				return &build.BuildEvent{
					Event: &build.BuildEvent_BazelEvent{
						BazelEvent: anyEvent,
					},
				}
			},
			toJson:  false,
			toOTel:  true,
			wantErr: false,
		},
		{
			name: "successful parse with both flags",
			setupEvent: func() *build.BuildEvent {
				bazelEvent := &build_event_stream.BuildEvent{
					Id: &build_event_stream.BuildEventId{
						Id: &build_event_stream.BuildEventId_BuildFinished{
							BuildFinished: &build_event_stream.BuildEventId_BuildFinishedId{},
						},
					},
					Payload: &build_event_stream.BuildEvent_Finished{
						Finished: &build_event_stream.BuildFinished{
							OverallSuccess: true,
						},
					},
				}

				anyEvent, err := anypb.New(bazelEvent)
				if err != nil {
					t.Fatalf("Failed to create Any: %v", err)
				}

				return &build.BuildEvent{
					Event: &build.BuildEvent_BazelEvent{
						BazelEvent: anyEvent,
					},
				}
			},
			toJson:  true,
			toOTel:  true,
			wantErr: false,
		},
		{
			name: "parse event with TargetComplete",
			setupEvent: func() *build.BuildEvent {
				bazelEvent := &build_event_stream.BuildEvent{
					Id: &build_event_stream.BuildEventId{
						Id: &build_event_stream.BuildEventId_TargetCompleted{
							TargetCompleted: &build_event_stream.BuildEventId_TargetCompletedId{
								Label: "//my/target:name",
							},
						},
					},
					Payload: &build_event_stream.BuildEvent_Completed{
						Completed: &build_event_stream.TargetComplete{
							Success: true,
						},
					},
				}

				anyEvent, err := anypb.New(bazelEvent)
				if err != nil {
					t.Fatalf("Failed to create Any: %v", err)
				}

				return &build.BuildEvent{
					Event: &build.BuildEvent_BazelEvent{
						BazelEvent: anyEvent,
					},
				}
			},
			toJson:  false,
			toOTel:  false,
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a logger that writes to a buffer for testing
			var buf bytes.Buffer
			logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{
				Level: slog.LevelDebug,
			}))

			parser := NewBuildEventParser(logger, tt.toJson, tt.toOTel)
			event := tt.setupEvent()

			result, err := parser.ParseBuildEvent(event)

			if tt.wantErr {
				if err == nil {
					t.Errorf("ParseBuildEvent() expected error, got nil")
				} else if tt.errContains != "" && !bytes.Contains([]byte(err.Error()), []byte(tt.errContains)) {
					t.Errorf("ParseBuildEvent() error = %v, want error containing %v", err, tt.errContains)
				}
				return
			}

			if err != nil {
				t.Errorf("ParseBuildEvent() unexpected error = %v", err)
				return
			}

			if result == nil {
				t.Error("ParseBuildEvent() returned nil result")
				return
			}

			// Verify the result is a valid BuildEvent
			if result.Id == nil {
				t.Error("ParseBuildEvent() result has nil Id")
			}

			// Check that appropriate log messages were generated
			logOutput := buf.String()
			if tt.toJson && !bytes.Contains([]byte(logOutput), []byte("Converting Bazel event to JSON")) {
				t.Error("Expected JSON conversion log message")
			}
			if tt.toOTel && !bytes.Contains([]byte(logOutput), []byte("Converting Bazel event to OTel span")) {
				t.Error("Expected OTel conversion log message")
			}
		})
	}
}

func TestParseBuildEvent_ErrorCases(t *testing.T) {
	tests := []struct {
		name       string
		setupEvent func() *build.BuildEvent
		wantErr    bool
	}{
		{
			name: "nil BazelEvent",
			setupEvent: func() *build.BuildEvent {
				return &build.BuildEvent{
					Event: &build.BuildEvent_BazelEvent{
						BazelEvent: nil,
					},
				}
			},
			wantErr: true,
		},
		{
			name: "invalid Any type",
			setupEvent: func() *build.BuildEvent {
				// Create an Any with a different message type
				wrongMsg := &build.StreamId{
					BuildId:      "test",
					InvocationId: "inv",
				}
				anyEvent, err := anypb.New(wrongMsg)
				if err != nil {
					t.Fatalf("Failed to create Any: %v", err)
				}

				return &build.BuildEvent{
					Event: &build.BuildEvent_BazelEvent{
						BazelEvent: anyEvent,
					},
				}
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{
				Level: slog.LevelDebug,
			}))

			parser := NewBuildEventParser(logger, false, false)
			event := tt.setupEvent()

			result, err := parser.ParseBuildEvent(event)

			if tt.wantErr {
				if err == nil {
					t.Error("ParseBuildEvent() expected error, got nil")
				}
				if result != nil {
					t.Error("ParseBuildEvent() expected nil result on error")
				}

				// Check that error was logged
				logOutput := buf.String()
				if !bytes.Contains([]byte(logOutput), []byte("Failed to unmarshal Bazel event")) {
					t.Error("Expected error log message")
				}
			} else {
				if err != nil {
					t.Errorf("ParseBuildEvent() unexpected error = %v", err)
				}
				if result == nil {
					t.Error("ParseBuildEvent() returned nil result")
				}
			}
		})
	}
}
