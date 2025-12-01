// Package writer handles writing BEP events to files for debugging
package writer

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	build "google.golang.org/genproto/googleapis/devtools/build/v1"
	"google.golang.org/protobuf/encoding/protojson"
)

// JSONWriter writes build events to JSON files
type JSONWriter struct {
	outputDir string
	logger    *slog.Logger

	// Per-build file management
	files map[string]*os.File
	mu    sync.Mutex
}

// Config holds configuration for the JSON writer
type Config struct {
	OutputDir string
	Enabled   bool
}

// EventRecord represents a single event in the JSON file
type EventRecord struct {
	Timestamp      time.Time              `json:"timestamp"`
	BuildID        string                 `json:"build_id,omitempty"`
	InvocationID   string                 `json:"invocation_id,omitempty"`
	SequenceNumber int64                  `json:"sequence_number"`
	EventType      string                 `json:"event_type"`
	Event          map[string]interface{} `json:"event"`
}

// NewJSONWriter creates a new JSON writer
func NewJSONWriter(config Config, logger *slog.Logger) (*JSONWriter, error) {
	if logger == nil {
		logger = slog.Default()
	}

	if !config.Enabled {
		logger.Info("JSON writer disabled")
		return nil, nil
	}

	// Create output directory if it doesn't exist
	if err := os.MkdirAll(config.OutputDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create output directory: %w", err)
	}

	logger.Info("JSON writer initialized",
		"output_dir", config.OutputDir,
	)

	return &JSONWriter{
		outputDir: config.OutputDir,
		logger:    logger,
		files:     make(map[string]*os.File),
	}, nil
}

// WriteLifecycleEvent writes a lifecycle event to JSON
func (w *JSONWriter) WriteLifecycleEvent(req *build.PublishLifecycleEventRequest) error {
	if w == nil {
		return nil
	}

	orderedEvent := req.GetBuildEvent()
	if orderedEvent == nil {
		return nil
	}

	streamID := orderedEvent.GetStreamId()
	event := orderedEvent.GetEvent()

	buildID := ""
	invocationID := ""
	if streamID != nil {
		buildID = streamID.GetBuildId()
		invocationID = streamID.GetInvocationId()
	}

	return w.writeEvent(buildID, invocationID, 0, event)
}

// WriteToolEvent writes a build tool event to JSON
func (w *JSONWriter) WriteToolEvent(
	buildID string,
	invocationID string,
	seqNum int64,
	event *build.BuildEvent,
) error {
	if w == nil {
		return nil
	}

	return w.writeEvent(buildID, invocationID, seqNum, event)
}

// writeEvent is the internal method that writes events to files
func (w *JSONWriter) writeEvent(
	buildID string,
	invocationID string,
	seqNum int64,
	event *build.BuildEvent,
) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Get or create file for this build
	file, err := w.getOrCreateFile(buildID)
	if err != nil {
		return err
	}

	// Determine event type
	eventType := "unknown"
	if event != nil {
		eventType = getEventType(event)
	}

	// Convert proto to JSON
	marshaler := protojson.MarshalOptions{
		Multiline:       false,
		Indent:          "",
		UseProtoNames:   true,
		EmitUnpopulated: false,
	}

	var eventMap map[string]interface{}

	// Special handling for BazelEvent which contains Any types we can't resolve
	if eventType == "BazelEvent" {
		// For BazelEvent, we'll create a simplified representation
		bazelAny := event.GetBazelEvent()
		eventMap = map[string]interface{}{
			"bazel_event": map[string]interface{}{
				"type_url": bazelAny.GetTypeUrl(),
				"note":     "Raw value not decoded - contains build_event_stream.BuildEvent",
			},
		}
		if event.GetEventTime() != nil {
			eventMap["event_time"] = event.GetEventTime().AsTime().Format(time.RFC3339Nano)
		}
	} else {
		// For other event types, try normal marshaling
		eventJSON, err := marshaler.Marshal(event)
		if err != nil {
			w.logger.Warn("Failed to marshal event, writing minimal info",
				"error", err,
				"build_id", buildID,
				"event_type", eventType,
			)
			// Create a minimal record
			eventMap = map[string]interface{}{
				"error":      err.Error(),
				"event_type": eventType,
			}
		} else {
			// Parse to map for our EventRecord
			if err := json.Unmarshal(eventJSON, &eventMap); err != nil {
				return err
			}
		}
	}

	// Create record
	record := EventRecord{
		Timestamp:      time.Now(),
		BuildID:        buildID,
		InvocationID:   invocationID,
		SequenceNumber: seqNum,
		EventType:      eventType,
		Event:          eventMap,
	}

	// Write as single-line JSON (JSONL format)
	encoder := json.NewEncoder(file)
	if err := encoder.Encode(record); err != nil {
		w.logger.Error("Failed to write event",
			"error", err,
			"build_id", buildID,
		)
		return err
	}

	return nil
}

// getOrCreateFile gets or creates a file for a build
func (w *JSONWriter) getOrCreateFile(buildID string) (*os.File, error) {
	// Check if we already have a file open for this build
	if file, exists := w.files[buildID]; exists {
		return file, nil
	}

	// Create new file
	timestamp := time.Now().Format("20060102-150405")

	// Use first 8 chars of buildID, or full buildID if shorter
	buildIDShort := buildID
	if len(buildID) > 8 {
		buildIDShort = buildID[:8]
	}

	filename := fmt.Sprintf("build-%s-%s.jsonl", timestamp, buildIDShort)
	filepath := filepath.Join(w.outputDir, filename)

	file, err := os.OpenFile(filepath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to create file: %w", err)
	}

	w.files[buildID] = file

	w.logger.Info("Created event file",
		"build_id", buildID,
		"file", filepath,
	)

	return file, nil
}

// CloseBuild closes the file for a specific build
func (w *JSONWriter) CloseBuild(buildID string) error {
	if w == nil {
		return nil
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	file, exists := w.files[buildID]
	if !exists {
		return nil
	}

	delete(w.files, buildID)

	if err := file.Close(); err != nil {
		w.logger.Error("Failed to close file",
			"error", err,
			"build_id", buildID,
		)
		return err
	}

	w.logger.Debug("Closed event file",
		"build_id", buildID,
	)

	return nil
}

// Close closes all open files
func (w *JSONWriter) Close() error {
	if w == nil {
		return nil
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	var lastErr error
	for buildID, file := range w.files {
		if err := file.Close(); err != nil {
			w.logger.Error("Failed to close file",
				"error", err,
				"build_id", buildID,
			)
			lastErr = err
		}
	}

	w.files = make(map[string]*os.File)

	w.logger.Info("JSON writer closed")

	return lastErr
}

// Helper function to determine event type
func getEventType(event *build.BuildEvent) string {
	if event == nil {
		return "unknown"
	}

	switch event.GetEvent().(type) {
	case *build.BuildEvent_InvocationAttemptStarted_:
		return "InvocationAttemptStarted"
	case *build.BuildEvent_InvocationAttemptFinished_:
		return "InvocationAttemptFinished"
	case *build.BuildEvent_BuildEnqueued_:
		return "BuildEnqueued"
	case *build.BuildEvent_BuildFinished_:
		return "BuildFinished"
	case *build.BuildEvent_ConsoleOutput_:
		return "ConsoleOutput"
	case *build.BuildEvent_ComponentStreamFinished:
		return "ComponentStreamFinished"
	case *build.BuildEvent_BazelEvent:
		return "BazelEvent"
	case *build.BuildEvent_BuildExecutionEvent:
		return "BuildExecutionEvent"
	case *build.BuildEvent_SourceFetchEvent:
		return "SourceFetchEvent"
	default:
		return "unknown"
	}
}
