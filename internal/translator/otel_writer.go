package translator

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// OTelWriterConfig holds configuration for the OTelWriter.
type OTelWriterConfig struct {
	Enabled    bool
	OutputDir  string
	BufferSize int // Number of spans to buffer before flushing
}

// OTelWriter writes OpenTelemetry spans to JSON Lines files.
type OTelWriter struct {
	config  *OTelWriterConfig
	logger  *slog.Logger
	mu      sync.Mutex
	writers map[string]*spanFileWriter // buildID -> file writer
}

type spanFileWriter struct {
	file      *os.File
	encoder   *json.Encoder
	buffer    []*SpanData
	bufferMu  sync.Mutex
	flushChan chan struct{}
	doneChan  chan struct{}
	logger    *slog.Logger
	buildID   string
}

// SerializableSpan is a JSON-serializable version of SpanData.
type SerializableSpan struct {
	TraceID      string                 `json:"trace_id"`
	SpanID       string                 `json:"span_id"`
	ParentSpanID string                 `json:"parent_span_id,omitempty"`
	Name         string                 `json:"name"`
	StartTime    string                 `json:"start_time"`
	EndTime      string                 `json:"end_time"`
	Duration     string                 `json:"duration"`
	Attributes   map[string]interface{} `json:"attributes"`
	Status       SpanStatus             `json:"status"`
	Events       []SpanEvent            `json:"events,omitempty"`
}

// NewOTelWriter creates a new OpenTelemetry span writer.
func NewOTelWriter(config *OTelWriterConfig, logger *slog.Logger) *OTelWriter {
	if !config.Enabled {
		return nil
	}
	
	if config.OutputDir == "" {
		config.OutputDir = "./otel-spans"
	}
	
	if config.BufferSize <= 0 {
		config.BufferSize = 100
	}

	// Ensure output directory exists
	if err := os.MkdirAll(config.OutputDir, 0755); err != nil {
		logger.Error("Failed to create OTel output directory",
			"error", err,
			"dir", config.OutputDir)
		return nil
	}

	return &OTelWriter{
		config:  config,
		logger:  logger,
		writers: make(map[string]*spanFileWriter),
	}
}

// WriteSpan writes a span to the appropriate file.
func (w *OTelWriter) WriteSpan(span *SpanData) error {
	if !w.config.Enabled || span == nil {
		return nil
	}

	buildID, ok := span.Attributes["build.id"].(string)
	if !ok || buildID == "" {
		return fmt.Errorf("span missing build.id attribute")
	}

	w.mu.Lock()
	writer, ok := w.writers[buildID]
	if !ok {
		var err error
		writer, err = w.newSpanFileWriter(buildID)
		if err != nil {
			w.logger.Error("Failed to create span file writer",
				"error", err,
				"build_id", buildID)
			w.mu.Unlock()
			return err
		}
		w.writers[buildID] = writer
		w.logger.Info("Created OTel span file",
			"build_id", buildID,
			"file", writer.file.Name())
	}
	w.mu.Unlock()

	writer.bufferMu.Lock()
	writer.buffer = append(writer.buffer, span)
	shouldFlush := len(writer.buffer) >= w.config.BufferSize
	writer.bufferMu.Unlock()

	if shouldFlush {
		select {
		case writer.flushChan <- struct{}{}:
		default:
			// Flush already in progress
		}
	}

	return nil
}

// WriteSpans writes multiple spans to the file.
func (w *OTelWriter) WriteSpans(spans []*SpanData) error {
	for _, span := range spans {
		if err := w.WriteSpan(span); err != nil {
			return err
		}
	}
	return nil
}

// CloseWriter closes the file writer for a specific build ID.
func (w *OTelWriter) CloseWriter(buildID string) {
	if !w.config.Enabled {
		return
	}

	w.mu.Lock()
	writer, ok := w.writers[buildID]
	if ok {
		close(writer.doneChan)
		delete(w.writers, buildID)
		w.logger.Debug("Closed OTel span file", "build_id", buildID)
	}
	w.mu.Unlock()
}

// CloseAll closes all open writers.
func (w *OTelWriter) CloseAll() {
	if !w.config.Enabled {
		return
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	for buildID, writer := range w.writers {
		close(writer.doneChan)
		w.logger.Debug("Closed OTel span file", "build_id", buildID)
	}
	w.writers = make(map[string]*spanFileWriter)
}

func (w *OTelWriter) newSpanFileWriter(buildID string) (*spanFileWriter, error) {
	timestamp := time.Now().Format("20060102-150405")
	
	// Use first 8 chars of buildID, or full buildID if shorter
	buildIDShort := buildID
	if len(buildID) > 8 {
		buildIDShort = buildID[:8]
	}
	
	filename := fmt.Sprintf("otel-spans-%s-%s.jsonl", timestamp, buildIDShort)
	filePath := filepath.Join(w.config.OutputDir, filename)

	file, err := os.Create(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to create file %s: %w", filePath, err)
	}

	writer := &spanFileWriter{
		file:      file,
		encoder:   json.NewEncoder(file),
		buffer:    make([]*SpanData, 0, w.config.BufferSize),
		flushChan: make(chan struct{}, 1),
		doneChan:  make(chan struct{}),
		logger:    w.logger.With("build_id", buildID),
		buildID:   buildID,
	}
	
	go writer.flushLoop()
	return writer, nil
}

func (w *spanFileWriter) flushLoop() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	defer w.file.Close()

	for {
		select {
		case <-w.flushChan:
			w.flushBuffer()
		case <-ticker.C:
			w.flushBuffer()
		case <-w.doneChan:
			w.flushBuffer() // Final flush
			return
		}
	}
}

func (w *spanFileWriter) flushBuffer() {
	w.bufferMu.Lock()
	defer w.bufferMu.Unlock()

	if len(w.buffer) == 0 {
		return
	}

	for _, span := range w.buffer {
		serializable := toSerializableSpan(span)
		
		if err := w.encoder.Encode(serializable); err != nil {
			w.logger.Error("Failed to write span to file",
				"error", err,
				"span_id", span.SpanID.String())
		}
	}
	
	w.buffer = w.buffer[:0] // Clear buffer
}

// toSerializableSpan converts a SpanData to a JSON-serializable format.
func toSerializableSpan(span *SpanData) *SerializableSpan {
	duration := span.EndTime.Sub(span.StartTime)
	
	parentSpanID := ""
	if span.ParentSpanID.IsValid() {
		parentSpanID = span.ParentSpanID.String()
	}
	
	return &SerializableSpan{
		TraceID:      span.TraceID.String(),
		SpanID:       span.SpanID.String(),
		ParentSpanID: parentSpanID,
		Name:         span.Name,
		StartTime:    span.StartTime.Format(time.RFC3339Nano),
		EndTime:      span.EndTime.Format(time.RFC3339Nano),
		Duration:     duration.String(),
		Attributes:   span.Attributes,
		Status:       span.Status,
		Events:       span.Events,
	}
}

