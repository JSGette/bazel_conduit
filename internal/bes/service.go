// Package bes implements the Google Build Event Service gRPC interface
package bes

import (
	"context"
	"fmt"
	"io"
	"log/slog"

	"github.com/JSGette/bazel_conduit/internal/graph"
	build "google.golang.org/genproto/googleapis/devtools/build/v1"
	"google.golang.org/protobuf/types/known/emptypb"
)

// Service implements the PublishBuildEvent gRPC service
type Service struct {
	build.UnimplementedPublishBuildEventServer

	graphManager *graph.Manager
	logger       *slog.Logger
}

// NewService creates a new BES service instance
func NewService(graphManager *graph.Manager, logger *slog.Logger) *Service {
	if logger == nil {
		logger = slog.Default()
	}

	logger.Info("Initializing BES service")

	return &Service{
		graphManager: graphManager,
		logger:       logger,
	}
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
	event := orderedEvent.GetEvent()

	projectID := req.GetProjectId()
	buildID := ""
	invocationID := ""

	if streamID != nil {
		buildID = streamID.GetBuildId()
		invocationID = streamID.GetInvocationId()
	}

	eventType := graph.GetEventType(event)

	s.logger.Info("Received lifecycle event",
		"project_id", projectID,
		"build_id", buildID,
		"invocation_id", invocationID,
		"event_type", eventType,
	)

	// Create or get graph
	if buildID != "" {
		_, err := s.graphManager.GetOrCreateGraph(ctx, buildID, invocationID)
		if err != nil {
			s.logger.Error("Failed to get/create event graph",
				"error", err,
				"build_id", buildID,
			)
			return nil, err
		}
	}

	return &emptypb.Empty{}, nil
}

// PublishBuildToolEventStream handles the bidirectional streaming of build tool events
func (s *Service) PublishBuildToolEventStream(
	stream build.PublishBuildEvent_PublishBuildToolEventStreamServer,
) error {

	ctx := stream.Context()
	var currentBuildID string
	var currentInvocationID string
	var graphObj *graph.EventGraph
	var lastSeqNum int64

	s.logger.Info("Build tool event stream opened")

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
			graphObj, err = s.graphManager.GetOrCreateGraph(ctx, currentBuildID, currentInvocationID)
			if err != nil {
				s.logger.Error("Failed to create event graph",
					"error", err,
					"build_id", currentBuildID,
				)
				return err
			}

			s.logger.Debug("Event graph created/retrieved",
				"build_id", currentBuildID,
				"trace_id", graphObj.TraceID,
			)
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

		eventType := graph.GetEventType(event)

		s.logger.Debug("Processing build event",
			"build_id", currentBuildID,
			"sequence", seqNum,
			"event_type", eventType,
		)

		// Store the event in the graph
		if graphObj != nil {
			eventID := fmt.Sprintf("%s-%d", currentBuildID, seqNum)
			if err := graphObj.AddEvent(eventID, seqNum, event); err != nil {
				s.logger.Error("Failed to add event to graph",
					"error", err,
					"build_id", currentBuildID,
					"sequence", seqNum,
				)
				// Continue processing other events
			}
		}

		lastSeqNum = seqNum

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

	// Stream completed successfully
	if graphObj != nil {
		graphObj.MarkBuildFinished()

		s.logger.Info("Build tool event stream completed",
			"trace_id", graphObj.TraceID,
			"total_events", lastSeqNum,
			"total_stored_events", graphObj.GetEventCount(),
		)

		// Schedule cleanup after 5 minutes
		s.graphManager.ScheduleCleanup(currentBuildID, 5*60*1000000000)
	}

	return nil
}
