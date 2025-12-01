package translator

import (
	"bufio"
	"encoding/json"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"go.opentelemetry.io/otel/trace"
)

func TestNewOTelWriter_Disabled(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	config := &OTelWriterConfig{
		Enabled: false,
	}

	writer := NewOTelWriter(config, logger)
	if writer != nil {
		t.Error("Expected nil writer when disabled")
	}
}

func TestNewOTelWriter_CreatesDirectory(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	tempDir := t.TempDir()
	outputDir := filepath.Join(tempDir, "test-otel-output")

	config := &OTelWriterConfig{
		Enabled:   true,
		OutputDir: outputDir,
	}

	writer := NewOTelWriter(config, logger)
	if writer == nil {
		t.Fatal("Expected non-nil writer")
	}
	defer writer.CloseAll()

	// Check directory was created
	if _, err := os.Stat(outputDir); os.IsNotExist(err) {
		t.Error("Expected output directory to be created")
	}
}

func TestNewOTelWriter_DefaultsBufferSize(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	tempDir := t.TempDir()

	config := &OTelWriterConfig{
		Enabled:    true,
		OutputDir:  tempDir,
		BufferSize: 0, // Should default to 100
	}

	writer := NewOTelWriter(config, logger)
	if writer == nil {
		t.Fatal("Expected non-nil writer")
	}
	defer writer.CloseAll()

	if writer.config.BufferSize != 100 {
		t.Errorf("Expected default buffer size 100, got %d", writer.config.BufferSize)
	}
}

func TestWriteSpan_NilSpan(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	tempDir := t.TempDir()

	config := &OTelWriterConfig{
		Enabled:   true,
		OutputDir: tempDir,
	}

	writer := NewOTelWriter(config, logger)
	if writer == nil {
		t.Fatal("Expected non-nil writer")
	}
	defer writer.CloseAll()

	err := writer.WriteSpan(nil)
	if err != nil {
		t.Errorf("Expected no error for nil span, got: %v", err)
	}
}

func TestWriteSpan_MissingBuildID(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	tempDir := t.TempDir()

	config := &OTelWriterConfig{
		Enabled:   true,
		OutputDir: tempDir,
	}

	writer := NewOTelWriter(config, logger)
	if writer == nil {
		t.Fatal("Expected non-nil writer")
	}
	defer writer.CloseAll()

	span := &SpanData{
		TraceID:    trace.TraceID{1, 2, 3},
		SpanID:     trace.SpanID{4, 5, 6},
		Name:       "test.span",
		Attributes: make(map[string]interface{}),
	}

	err := writer.WriteSpan(span)
	if err == nil {
		t.Error("Expected error for span without build.id attribute")
	}
}

func TestWriteSpan_CreatesFile(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	tempDir := t.TempDir()

	config := &OTelWriterConfig{
		Enabled:    true,
		OutputDir:  tempDir,
		BufferSize: 1, // Flush immediately
	}

	writer := NewOTelWriter(config, logger)
	if writer == nil {
		t.Fatal("Expected non-nil writer")
	}
	defer writer.CloseAll()

	buildID := "test-build-123"
	span := &SpanData{
		TraceID:   trace.TraceID{1, 2, 3},
		SpanID:    trace.SpanID{4, 5, 6},
		Name:      "test.span",
		StartTime: time.Now(),
		EndTime:   time.Now().Add(time.Second),
		Attributes: map[string]interface{}{
			"build.id": buildID,
			"test":     "value",
		},
		Status: SpanStatus{Code: StatusCodeOK},
	}

	err := writer.WriteSpan(span)
	if err != nil {
		t.Fatalf("WriteSpan failed: %v", err)
	}

	// Give time for async flush
	time.Sleep(100 * time.Millisecond)

	// Close to ensure final flush
	writer.CloseWriter(buildID)
	time.Sleep(100 * time.Millisecond)

	// Check file was created
	files, err := os.ReadDir(tempDir)
	if err != nil {
		t.Fatalf("Failed to read output directory: %v", err)
	}

	if len(files) == 0 {
		t.Fatal("Expected at least one file to be created")
	}

	// Read the file and verify content
	filePath := filepath.Join(tempDir, files[0].Name())
	file, err := os.Open(filePath)
	if err != nil {
		t.Fatalf("Failed to open output file: %v", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	lineCount := 0
	for scanner.Scan() {
		lineCount++
		var serialized SerializableSpan
		if err := json.Unmarshal(scanner.Bytes(), &serialized); err != nil {
			t.Fatalf("Failed to parse JSON line: %v", err)
		}

		if serialized.Name != "test.span" {
			t.Errorf("Expected span name 'test.span', got '%s'", serialized.Name)
		}

		if serialized.Attributes["build.id"] != buildID {
			t.Errorf("Expected build.id=%s, got %v", buildID, serialized.Attributes["build.id"])
		}

		if serialized.Status.Code != StatusCodeOK {
			t.Errorf("Expected status OK, got %v", serialized.Status.Code)
		}
	}

	if lineCount == 0 {
		t.Error("Expected at least one line in output file")
	}
}

func TestWriteSpans_Multiple(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	tempDir := t.TempDir()

	config := &OTelWriterConfig{
		Enabled:    true,
		OutputDir:  tempDir,
		BufferSize: 10,
	}

	writer := NewOTelWriter(config, logger)
	if writer == nil {
		t.Fatal("Expected non-nil writer")
	}
	defer writer.CloseAll()

	buildID := "test-build-456"
	spans := make([]*SpanData, 5)
	for i := 0; i < 5; i++ {
		spans[i] = &SpanData{
			TraceID:   trace.TraceID{1, 2, 3},
			SpanID:    trace.SpanID{byte(i), 5, 6},
			Name:      "test.span",
			StartTime: time.Now(),
			EndTime:   time.Now().Add(time.Second),
			Attributes: map[string]interface{}{
				"build.id": buildID,
				"index":    i,
			},
			Status: SpanStatus{Code: StatusCodeOK},
		}
	}

	err := writer.WriteSpans(spans)
	if err != nil {
		t.Fatalf("WriteSpans failed: %v", err)
	}

	// Close to ensure flush
	writer.CloseWriter(buildID)
	time.Sleep(100 * time.Millisecond)

	// Check file was created
	files, err := os.ReadDir(tempDir)
	if err != nil {
		t.Fatalf("Failed to read output directory: %v", err)
	}

	if len(files) == 0 {
		t.Fatal("Expected at least one file to be created")
	}

	// Read the file and count lines
	filePath := filepath.Join(tempDir, files[0].Name())
	file, err := os.Open(filePath)
	if err != nil {
		t.Fatalf("Failed to open output file: %v", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	lineCount := 0
	for scanner.Scan() {
		lineCount++
	}

	if lineCount != 5 {
		t.Errorf("Expected 5 lines in output file, got %d", lineCount)
	}
}

func TestWriteSpan_MultipleBuildsSeparateFiles(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	tempDir := t.TempDir()

	config := &OTelWriterConfig{
		Enabled:    true,
		OutputDir:  tempDir,
		BufferSize: 1,
	}

	writer := NewOTelWriter(config, logger)
	if writer == nil {
		t.Fatal("Expected non-nil writer")
	}
	defer writer.CloseAll()

	buildID1 := "build-1"
	buildID2 := "build-2"

	span1 := &SpanData{
		TraceID:   trace.TraceID{1, 2, 3},
		SpanID:    trace.SpanID{4, 5, 6},
		Name:      "span.build1",
		StartTime: time.Now(),
		EndTime:   time.Now(),
		Attributes: map[string]interface{}{
			"build.id": buildID1,
		},
		Status: SpanStatus{Code: StatusCodeOK},
	}

	span2 := &SpanData{
		TraceID:   trace.TraceID{7, 8, 9},
		SpanID:    trace.SpanID{10, 11, 12},
		Name:      "span.build2",
		StartTime: time.Now(),
		EndTime:   time.Now(),
		Attributes: map[string]interface{}{
			"build.id": buildID2,
		},
		Status: SpanStatus{Code: StatusCodeOK},
	}

	if err := writer.WriteSpan(span1); err != nil {
		t.Fatalf("WriteSpan failed for build1: %v", err)
	}

	if err := writer.WriteSpan(span2); err != nil {
		t.Fatalf("WriteSpan failed for build2: %v", err)
	}

	// Close both
	writer.CloseWriter(buildID1)
	writer.CloseWriter(buildID2)
	time.Sleep(100 * time.Millisecond)

	// Should have 2 files
	files, err := os.ReadDir(tempDir)
	if err != nil {
		t.Fatalf("Failed to read output directory: %v", err)
	}

	if len(files) != 2 {
		t.Errorf("Expected 2 files, got %d", len(files))
	}
}

func TestToSerializableSpan(t *testing.T) {
	startTime := time.Now()
	endTime := startTime.Add(5 * time.Second)

	span := &SpanData{
		TraceID:      trace.TraceID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
		SpanID:       trace.SpanID{1, 2, 3, 4, 5, 6, 7, 8},
		ParentSpanID: trace.SpanID{9, 10, 11, 12, 13, 14, 15, 16},
		Name:         "test.span",
		StartTime:    startTime,
		EndTime:      endTime,
		Attributes: map[string]interface{}{
			"key": "value",
		},
		Status: SpanStatus{
			Code:    StatusCodeOK,
			Message: "Success",
		},
		Events: []SpanEvent{
			{
				Name:      "event.test",
				Timestamp: startTime.Add(time.Second),
				Attributes: map[string]interface{}{
					"event_key": "event_value",
				},
			},
		},
	}

	serializable := toSerializableSpan(span)

	if serializable.TraceID != span.TraceID.String() {
		t.Error("TraceID mismatch")
	}

	if serializable.SpanID != span.SpanID.String() {
		t.Error("SpanID mismatch")
	}

	if serializable.ParentSpanID != span.ParentSpanID.String() {
		t.Error("ParentSpanID mismatch")
	}

	if serializable.Name != "test.span" {
		t.Errorf("Expected name 'test.span', got '%s'", serializable.Name)
	}

	expectedDuration := endTime.Sub(startTime)
	if serializable.Duration != expectedDuration.String() {
		t.Errorf("Expected duration %s, got %s", expectedDuration, serializable.Duration)
	}

	if serializable.Attributes["key"] != "value" {
		t.Error("Attributes mismatch")
	}

	if serializable.Status.Code != StatusCodeOK {
		t.Error("Status code mismatch")
	}

	if len(serializable.Events) != 1 {
		t.Errorf("Expected 1 event, got %d", len(serializable.Events))
	}
}

func TestToSerializableSpan_NoParent(t *testing.T) {
	span := &SpanData{
		TraceID:      trace.TraceID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
		SpanID:       trace.SpanID{1, 2, 3, 4, 5, 6, 7, 8},
		ParentSpanID: trace.SpanID{}, // Zero value (no parent)
		Name:         "root.span",
		StartTime:    time.Now(),
		EndTime:      time.Now(),
		Attributes:   make(map[string]interface{}),
		Status:       SpanStatus{Code: StatusCodeUnset},
	}

	serializable := toSerializableSpan(span)

	if serializable.ParentSpanID != "" {
		t.Errorf("Expected empty ParentSpanID for root span, got '%s'", serializable.ParentSpanID)
	}
}

func TestCloseAll(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	tempDir := t.TempDir()

	config := &OTelWriterConfig{
		Enabled:    true,
		OutputDir:  tempDir,
		BufferSize: 10,
	}

	writer := NewOTelWriter(config, logger)
	if writer == nil {
		t.Fatal("Expected non-nil writer")
	}

	// Write spans for multiple builds
	for i := 1; i <= 3; i++ {
		buildID := "build-" + string(rune('0'+i))
		span := &SpanData{
			TraceID:   trace.TraceID{byte(i)},
			SpanID:    trace.SpanID{byte(i)},
			Name:      "test.span",
			StartTime: time.Now(),
			EndTime:   time.Now(),
			Attributes: map[string]interface{}{
				"build.id": buildID,
			},
			Status: SpanStatus{Code: StatusCodeOK},
		}
		if err := writer.WriteSpan(span); err != nil {
			t.Fatalf("WriteSpan failed: %v", err)
		}
	}

	// Should have 3 writers
	if len(writer.writers) != 3 {
		t.Errorf("Expected 3 writers, got %d", len(writer.writers))
	}

	// Close all
	writer.CloseAll()
	time.Sleep(100 * time.Millisecond)

	// Should have 0 writers
	if len(writer.writers) != 0 {
		t.Errorf("Expected 0 writers after CloseAll, got %d", len(writer.writers))
	}
}

func TestCloseWriter_NonExistent(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	tempDir := t.TempDir()

	config := &OTelWriterConfig{
		Enabled:   true,
		OutputDir: tempDir,
	}

	writer := NewOTelWriter(config, logger)
	if writer == nil {
		t.Fatal("Expected non-nil writer")
	}
	defer writer.CloseAll()

	// Closing non-existent writer should not panic
	writer.CloseWriter("non-existent-build")
}

