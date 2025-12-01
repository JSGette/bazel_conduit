package writer

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	build "google.golang.org/genproto/googleapis/devtools/build/v1"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestNewJSONWriter(t *testing.T) {
	tempDir := t.TempDir()
	
	config := Config{
		OutputDir: tempDir,
		Enabled:   true,
	}
	
	writer, err := NewJSONWriter(config, nil)
	if err != nil {
		t.Fatalf("Failed to create writer: %v", err)
	}
	
	if writer == nil {
		t.Fatal("Expected writer to be created")
	}
	
	if writer.outputDir != tempDir {
		t.Errorf("Expected output_dir=%s, got %s", tempDir, writer.outputDir)
	}
	
	// Cleanup
	writer.Close()
}

func TestNewJSONWriterDisabled(t *testing.T) {
	config := Config{
		OutputDir: "/tmp/test",
		Enabled:   false,
	}
	
	writer, err := NewJSONWriter(config, nil)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	
	if writer != nil {
		t.Error("Expected nil writer when disabled")
	}
}

func TestNewJSONWriterInvalidDir(t *testing.T) {
	config := Config{
		OutputDir: "/invalid/path/that/does/not/exist/and/cannot/be/created\x00",
		Enabled:   true,
	}
	
	_, err := NewJSONWriter(config, nil)
	if err == nil {
		t.Error("Expected error for invalid directory")
	}
}

func TestWriteToolEvent(t *testing.T) {
	tempDir := t.TempDir()
	
	config := Config{
		OutputDir: tempDir,
		Enabled:   true,
	}
	
	writer, err := NewJSONWriter(config, nil)
	if err != nil {
		t.Fatalf("Failed to create writer: %v", err)
	}
	defer writer.Close()
	
	// Create a simple build event
	event := &build.BuildEvent{
		EventTime: timestamppb.Now(),
		Event: &build.BuildEvent_BuildFinished_{
			BuildFinished: &build.BuildEvent_BuildFinished{},
		},
	}
	
	buildID := "test-build-123"
	invocationID := "test-inv-456"
	seqNum := int64(1)
	
	err = writer.WriteToolEvent(buildID, invocationID, seqNum, event)
	if err != nil {
		t.Fatalf("Failed to write event: %v", err)
	}
	
	// Close to flush
	writer.Close()
	
	// Verify file was created
	files, err := filepath.Glob(filepath.Join(tempDir, "build-*.jsonl"))
	if err != nil {
		t.Fatalf("Failed to glob files: %v", err)
	}
	
	if len(files) != 1 {
		t.Fatalf("Expected 1 file, got %d", len(files))
	}
	
	// Read and verify content
	content, err := os.ReadFile(files[0])
	if err != nil {
		t.Fatalf("Failed to read file: %v", err)
	}
	
	var record EventRecord
	if err := json.Unmarshal(content, &record); err != nil {
		t.Fatalf("Failed to unmarshal JSON: %v", err)
	}
	
	if record.BuildID != buildID {
		t.Errorf("Expected build_id=%s, got %s", buildID, record.BuildID)
	}
	
	if record.InvocationID != invocationID {
		t.Errorf("Expected invocation_id=%s, got %s", invocationID, record.InvocationID)
	}
	
	if record.SequenceNumber != seqNum {
		t.Errorf("Expected sequence=%d, got %d", seqNum, record.SequenceNumber)
	}
	
	if record.EventType != "BuildFinished" {
		t.Errorf("Expected event_type=BuildFinished, got %s", record.EventType)
	}
}

func TestWriteMultipleEvents(t *testing.T) {
	tempDir := t.TempDir()
	
	config := Config{
		OutputDir: tempDir,
		Enabled:   true,
	}
	
	writer, err := NewJSONWriter(config, nil)
	if err != nil {
		t.Fatalf("Failed to create writer: %v", err)
	}
	defer writer.Close()
	
	buildID := "test-build-123"
	invocationID := "test-inv-456"
	
	// Write multiple events
	for i := 1; i <= 5; i++ {
		event := &build.BuildEvent{
			EventTime: timestamppb.Now(),
			Event: &build.BuildEvent_ComponentStreamFinished{
				ComponentStreamFinished: &build.BuildEvent_BuildComponentStreamFinished{},
			},
		}
		
		err := writer.WriteToolEvent(buildID, invocationID, int64(i), event)
		if err != nil {
			t.Fatalf("Failed to write event %d: %v", i, err)
		}
	}
	
	writer.Close()
	
	// Verify file has all events
	files, err := filepath.Glob(filepath.Join(tempDir, "build-*.jsonl"))
	if err != nil {
		t.Fatalf("Failed to glob files: %v", err)
	}
	
	if len(files) != 1 {
		t.Fatalf("Expected 1 file, got %d", len(files))
	}
	
	// Count lines in file
	file, err := os.Open(files[0])
	if err != nil {
		t.Fatalf("Failed to open file: %v", err)
	}
	defer file.Close()
	
	scanner := bufio.NewScanner(file)
	lineCount := 0
	for scanner.Scan() {
		lineCount++
		
		var record EventRecord
		if err := json.Unmarshal(scanner.Bytes(), &record); err != nil {
			t.Fatalf("Failed to unmarshal line %d: %v", lineCount, err)
		}
		
		if record.SequenceNumber != int64(lineCount) {
			t.Errorf("Line %d: expected sequence=%d, got %d", lineCount, lineCount, record.SequenceNumber)
		}
	}
	
	if lineCount != 5 {
		t.Errorf("Expected 5 lines, got %d", lineCount)
	}
}

func TestWriteBazelEvent(t *testing.T) {
	tempDir := t.TempDir()
	
	config := Config{
		OutputDir: tempDir,
		Enabled:   true,
	}
	
	writer, err := NewJSONWriter(config, nil)
	if err != nil {
		t.Fatalf("Failed to create writer: %v", err)
	}
	defer writer.Close()
	
	// Create a BazelEvent (which contains Any type)
	anyData := &anypb.Any{
		TypeUrl: "type.googleapis.com/build_event_stream.BuildEvent",
		Value:   []byte{0x01, 0x02, 0x03}, // dummy data
	}
	
	event := &build.BuildEvent{
		EventTime: timestamppb.Now(),
		Event: &build.BuildEvent_BazelEvent{
			BazelEvent: anyData,
		},
	}
	
	buildID := "test-build-123"
	invocationID := "test-inv-456"
	
	// This should not error even though we can't fully marshal the Any
	err = writer.WriteToolEvent(buildID, invocationID, 1, event)
	if err != nil {
		t.Fatalf("Failed to write BazelEvent: %v", err)
	}
	
	writer.Close()
	
	// Verify file was created
	files, err := filepath.Glob(filepath.Join(tempDir, "build-*.jsonl"))
	if err != nil {
		t.Fatalf("Failed to glob files: %v", err)
	}
	
	if len(files) != 1 {
		t.Fatalf("Expected 1 file, got %d", len(files))
	}
	
	// Read and verify content
	content, err := os.ReadFile(files[0])
	if err != nil {
		t.Fatalf("Failed to read file: %v", err)
	}
	
	var record EventRecord
	if err := json.Unmarshal(content, &record); err != nil {
		t.Fatalf("Failed to unmarshal JSON: %v", err)
	}
	
	if record.EventType != "BazelEvent" {
		t.Errorf("Expected event_type=BazelEvent, got %s", record.EventType)
	}
	
	// Verify the event has bazel_event field
	if _, ok := record.Event["bazel_event"]; !ok {
		t.Error("Expected event to have bazel_event field")
	}
}

func TestMultipleBuilds(t *testing.T) {
	tempDir := t.TempDir()
	
	config := Config{
		OutputDir: tempDir,
		Enabled:   true,
	}
	
	writer, err := NewJSONWriter(config, nil)
	if err != nil {
		t.Fatalf("Failed to create writer: %v", err)
	}
	defer writer.Close()
	
	event := &build.BuildEvent{
		EventTime: timestamppb.Now(),
		Event: &build.BuildEvent_BuildFinished_{
			BuildFinished: &build.BuildEvent_BuildFinished{},
		},
	}
	
	// Write events for multiple builds
	buildIDs := []string{"build-1", "build-2", "build-3"}
	for _, buildID := range buildIDs {
		err := writer.WriteToolEvent(buildID, "inv-1", 1, event)
		if err != nil {
			t.Fatalf("Failed to write event for %s: %v", buildID, err)
		}
	}
	
	writer.Close()
	
	// Should have 3 files
	files, err := filepath.Glob(filepath.Join(tempDir, "build-*.jsonl"))
	if err != nil {
		t.Fatalf("Failed to glob files: %v", err)
	}
	
	if len(files) != 3 {
		t.Errorf("Expected 3 files, got %d", len(files))
	}
}

func TestCloseBuild(t *testing.T) {
	tempDir := t.TempDir()
	
	config := Config{
		OutputDir: tempDir,
		Enabled:   true,
	}
	
	writer, err := NewJSONWriter(config, nil)
	if err != nil {
		t.Fatalf("Failed to create writer: %v", err)
	}
	defer writer.Close()
	
	buildID := "test-build-123"
	
	// Write an event (creates the file)
	event := &build.BuildEvent{
		EventTime: timestamppb.Now(),
		Event: &build.BuildEvent_BuildFinished_{
			BuildFinished: &build.BuildEvent_BuildFinished{},
		},
	}
	
	err = writer.WriteToolEvent(buildID, "inv-1", 1, event)
	if err != nil {
		t.Fatalf("Failed to write event: %v", err)
	}
	
	// Close this specific build
	err = writer.CloseBuild(buildID)
	if err != nil {
		t.Fatalf("Failed to close build: %v", err)
	}
	
	// Verify the file is no longer tracked
	writer.mu.Lock()
	_, exists := writer.files[buildID]
	writer.mu.Unlock()
	
	if exists {
		t.Error("Expected file to be removed from tracking")
	}
	
	// Closing again should not error
	err = writer.CloseBuild(buildID)
	if err != nil {
		t.Errorf("Expected no error on second close, got: %v", err)
	}
}

func TestWriteWithNilWriter(t *testing.T) {
	var writer *JSONWriter = nil
	
	event := &build.BuildEvent{
		EventTime: timestamppb.Now(),
		Event: &build.BuildEvent_BuildFinished_{
			BuildFinished: &build.BuildEvent_BuildFinished{},
		},
	}
	
	// Should not panic or error
	err := writer.WriteToolEvent("build-1", "inv-1", 1, event)
	if err != nil {
		t.Errorf("Expected no error with nil writer, got: %v", err)
	}
	
	err = writer.CloseBuild("build-1")
	if err != nil {
		t.Errorf("Expected no error with nil writer, got: %v", err)
	}
	
	err = writer.Close()
	if err != nil {
		t.Errorf("Expected no error with nil writer, got: %v", err)
	}
}

func TestWriteLifecycleEvent(t *testing.T) {
	tempDir := t.TempDir()
	
	config := Config{
		OutputDir: tempDir,
		Enabled:   true,
	}
	
	writer, err := NewJSONWriter(config, nil)
	if err != nil {
		t.Fatalf("Failed to create writer: %v", err)
	}
	defer writer.Close()
	
	// Create a lifecycle event
	orderedEvent := &build.OrderedBuildEvent{
		StreamId: &build.StreamId{
			BuildId:      "build-123",
			InvocationId: "inv-456",
		},
		Event: &build.BuildEvent{
			EventTime: timestamppb.Now(),
			Event: &build.BuildEvent_InvocationAttemptStarted_{
				InvocationAttemptStarted: &build.BuildEvent_InvocationAttemptStarted{},
			},
		},
	}
	
	req := &build.PublishLifecycleEventRequest{
		BuildEvent: orderedEvent,
		ProjectId:  "test-project",
	}
	
	err = writer.WriteLifecycleEvent(req)
	if err != nil {
		t.Fatalf("Failed to write lifecycle event: %v", err)
	}
	
	writer.Close()
	
	// Verify file was created
	files, err := filepath.Glob(filepath.Join(tempDir, "build-*.jsonl"))
	if err != nil {
		t.Fatalf("Failed to glob files: %v", err)
	}
	
	if len(files) != 1 {
		t.Fatalf("Expected 1 file, got %d", len(files))
	}
	
	// Read and verify content
	content, err := os.ReadFile(files[0])
	if err != nil {
		t.Fatalf("Failed to read file: %v", err)
	}
	
	var record EventRecord
	if err := json.Unmarshal(content, &record); err != nil {
		t.Fatalf("Failed to unmarshal JSON: %v", err)
	}
	
	if record.EventType != "InvocationAttemptStarted" {
		t.Errorf("Expected event_type=InvocationAttemptStarted, got %s", record.EventType)
	}
	
	if record.BuildID != "build-123" {
		t.Errorf("Expected build_id=build-123, got %s", record.BuildID)
	}
}

func TestGetEventType(t *testing.T) {
	tests := []struct {
		name     string
		event    *build.BuildEvent
		expected string
	}{
		{
			name:     "nil event",
			event:    nil,
			expected: "unknown",
		},
		{
			name: "BuildFinished",
			event: &build.BuildEvent{
				Event: &build.BuildEvent_BuildFinished_{},
			},
			expected: "BuildFinished",
		},
		{
			name: "InvocationAttemptStarted",
			event: &build.BuildEvent{
				Event: &build.BuildEvent_InvocationAttemptStarted_{},
			},
			expected: "InvocationAttemptStarted",
		},
		{
			name: "BazelEvent",
			event: &build.BuildEvent{
				Event: &build.BuildEvent_BazelEvent{},
			},
			expected: "BazelEvent",
		},
		{
			name: "ConsoleOutput",
			event: &build.BuildEvent{
				Event: &build.BuildEvent_ConsoleOutput_{},
			},
			expected: "ConsoleOutput",
		},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := getEventType(tt.event)
			if result != tt.expected {
				t.Errorf("Expected %s, got %s", tt.expected, result)
			}
		})
	}
}

