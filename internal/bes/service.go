// Package bes implements the Google Build Event Service gRPC interface
package bes

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	build "google.golang.org/genproto/googleapis/devtools/build/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/emptypb"
)

// Service implements the PublishBuildEvent gRPC service
type Service struct {
	build.UnimplementedPublishBuildEventServer

	logger *slog.Logger
	config ServiceConfig
}

// ServiceConfig holds configuration for the BES service
type ServiceConfig struct {
	DumpJSON bool
	DumpOTel bool
	OTelDir  string
}

// NewService creates a new BES service instance
func NewService(logger *slog.Logger, config ServiceConfig) (*Service, error) {
	if logger == nil {
		logger = slog.Default()
	}

	logger.Info("Initializing BES service",
		"dump_json", config.DumpJSON,
		"dump_otel", config.DumpOTel,
		"otel_dir", config.OTelDir,
	)

	// Create output directory if needed
	if config.DumpOTel {
		if err := os.MkdirAll(config.OTelDir, 0755); err != nil {
			return nil, fmt.Errorf("failed to create OTEL output directory: %w", err)
		}
	}

	return &Service{
		logger: logger,
		config: config,
	}, nil
}

// Close cleans up resources held by the service
func (s *Service) Close() error {
	s.logger.Info("Closing BES service")
	return nil
}

// PublishLifecycleEvent handles high-level build lifecycle events
func (s *Service) PublishLifecycleEvent(
	ctx context.Context,
	req *build.PublishLifecycleEventRequest,
) (*emptypb.Empty, error) {

	orderedEvent := req.GetBuildEvent()
	if orderedEvent == nil {
		return nil, fmt.Errorf("build_event is required")
	}

	streamID := orderedEvent.GetStreamId()

	projectID := req.GetProjectId()
	buildID := ""
	invocationID := ""

	if streamID != nil {
		buildID = streamID.GetBuildId()
		invocationID = streamID.GetInvocationId()
	}

	s.logger.Info("Received lifecycle event",
		"project_id", projectID,
		"build_id", buildID,
		"invocation_id", invocationID,
	)

	return &emptypb.Empty{}, nil
}

// PublishBuildToolEventStream handles the bidirectional streaming of build tool events
func (s *Service) PublishBuildToolEventStream(
	stream build.PublishBuildEvent_PublishBuildToolEventStreamServer,
) error {

	var currentBuildID string
	var currentInvocationID string
	var lastSeqNum int64

	s.logger.Info("Build tool event stream opened")

	// Create a new parser and exporter for this build
	var parser *BuildEventParser
	var exporter TraceExporter

	if s.config.DumpOTel {
		// Generate unique filename with timestamp
		timestamp := time.Now().Format("20060102-150405")
		filename := filepath.Join(s.config.OTelDir, fmt.Sprintf("otel-spans-%s.json", timestamp))

		s.logger.Info("Creating JSON exporter for build", "filename", filename)

		var err error
		exporter, err = NewJSONFileExporter(filename)
		if err != nil {
			s.logger.Error("Failed to create JSON exporter", "error", err)
			// Continue without exporter
		}
	}

	if exporter != nil {
		parser = NewBuildEventParserWithExporter(s.logger, exporter)
	} else {
		parser = NewBuildEventParser(s.logger, s.config.DumpJSON, s.config.DumpOTel)
	}

	defer func() {
		if parser != nil {
			if err := parser.Close(); err != nil {
				s.logger.Error("Failed to close parser", "error", err)
			}
		}
	}()

	// Process events from the stream
	for {
		// Receive event from Bazel
		req, err := stream.Recv()
		if err == io.EOF {
			s.logger.Info("Build tool event stream closed by client",
				"build_id", currentBuildID,
				"total_events", lastSeqNum,
			)
			break
		}
		if err != nil {
			s.logger.Error("Error receiving from stream",
				"error", err,
				"build_id", currentBuildID,
			)
			return err
		}

		orderedEvent := req.GetOrderedBuildEvent()
		if orderedEvent == nil {
			s.logger.Warn("Received request without ordered build event")
			continue
		}

		streamID := orderedEvent.GetStreamId()
		seqNum := orderedEvent.GetSequenceNumber()
		event := orderedEvent.GetEvent()

		eventTypes := map[string]string{
			"progress":                      "Progress",
			"aborted":                       "Aborted",
			"started":                       "BuildStarted",
			"unstructuredCommandLine":       "UnstructuredCommandLine",
			"structuredCommandLine":         "StructuredCommandLine",
			"options_parsed":                "OptionsParsed",
			"workspace_status":              "WorkspaceStatus",
			"fetch":                         "Fetch",
			"configuration":                 "Configuration",
			"pattern_expanded":              "PatternExpanded",
			"targetConfigured":              "TargetConfigured",
			"action_executed":               "ActionExecuted",
			"namedSet":                      "NamedSetOfFiles",
			"targetCompleted":               "TargetComplete",
			"testResult":                    "TestResult",
			"testProgress":                  "TestProgress",
			"testSummary":                   "TestSummary",
			"targetSummary":                 "TargetSummary",
			"buildFinished":                 "BuildFinished",
			"buildToolLogs":                 "BuildToolLogs",
			"buildMetrics":                  "BuildMetrics",
			"workspace_info":                "WorkspaceInfo",
			"build_metadata":                "BuildMetadata",
			"convenienceSymlinksIdentified": "ConvenienceSymlinksIdentified",
			"execRequest":                   "ExecRequest",
		}

		s.logger.Debug("Available event types",
			"event_types", eventTypes,
		)

		jsonEvent, err := protojson.Marshal(event)
		s.logger.Debug("RECEIVED BUILD TOOL EVENT",
			"event", string(jsonEvent),
		)

		// First event initializes the stream
		if currentBuildID == "" && streamID != nil {
			currentBuildID = streamID.GetBuildId()
			currentInvocationID = streamID.GetInvocationId()

			s.logger.Info("Initializing build tool event stream",
				"project_id", req.GetProjectId(),
				"build_id", currentBuildID,
				"invocation_id", currentInvocationID,
			)

			// Create or get existing event graph for this build
			if err != nil {
				s.logger.Error("Failed to create event graph",
					"error", err,
					"build_id", currentBuildID,
				)
				return err
			}
		}

		// Validate sequence number
		expectedSeq := lastSeqNum + 1
		if seqNum != expectedSeq {
			s.logger.Warn("Out-of-order sequence number",
				"expected", expectedSeq,
				"received", seqNum,
				"build_id", currentBuildID,
			)
		}

		lastSeqNum = seqNum

		// Parse the event through the BuildEventParser
		if event != nil && parser != nil {
			_, err := parser.ParseBuildEvent(event)
			if err != nil {
				s.logger.Error("Failed to parse build event",
					"error", err,
					"build_id", currentBuildID,
					"sequence", seqNum,
				)
				// Continue processing despite parse errors
			}
		}

		// Send acknowledgment back to Bazel
		resp := &build.PublishBuildToolEventStreamResponse{
			StreamId:       streamID,
			SequenceNumber: seqNum,
		}

		if err := stream.Send(resp); err != nil {
			s.logger.Error("Failed to send acknowledgment",
				"error", err,
				"build_id", currentBuildID,
				"sequence", seqNum,
			)
			return err
		}
	}

	s.logger.Info("Build stream complete")
	// Parser will be closed by the defer function

	return nil
}
