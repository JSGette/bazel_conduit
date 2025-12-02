// Package bes implements the Google Build Event Service gRPC interface
package bes

import (
	"context"
	"fmt"
	"io"
	"log/slog"

	build "google.golang.org/genproto/googleapis/devtools/build/v1"
	"google.golang.org/protobuf/types/known/emptypb"
)

// Service implements the PublishBuildEvent gRPC service
type Service struct {
	build.UnimplementedPublishBuildEventServer

	logger *slog.Logger
	parser *BuildEventParser
}

// NewService creates a new BES service instance
func NewService(logger *slog.Logger) *Service {
	if logger == nil {
		logger = slog.Default()
	}

	logger.Info("Initializing BES service")

	parser := NewBuildEventParser(logger)

	return &Service{
		logger: logger,
		parser: parser,
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

	// Determine event type
	var eventType string
	switch event.GetEvent().(type) {
	case *build.BuildEvent_InvocationAttemptStarted_:
		eventType = "InvocationAttemptStarted"
		// Create root span for the invocation
		if err := s.parser.StartInvocationSpan(ctx, buildID, invocationID, eventType); err != nil {
			s.logger.Error("Failed to start invocation span",
				"error", err,
				"invocation_id", invocationID)
		}

	case *build.BuildEvent_InvocationAttemptFinished_:
		eventType = "InvocationAttemptFinished"
		// End root span for the invocation
		finished := event.GetInvocationAttemptFinished()
		// Consider successful if status is not set or result is SUCCESS
		success := finished == nil || 
			finished.InvocationStatus == nil || 
			finished.InvocationStatus.Result == build.BuildStatus_COMMAND_SUCCEEDED

		if err := s.parser.EndInvocationSpan(invocationID, success); err != nil {
			s.logger.Error("Failed to end invocation span",
				"error", err,
				"invocation_id", invocationID)
		}

	case *build.BuildEvent_BuildEnqueued_:
		eventType = "BuildEnqueued"
	case *build.BuildEvent_BuildFinished_:
		eventType = "BuildFinished"
	default:
		eventType = fmt.Sprintf("%T", event.GetEvent())
	}

	s.logger.Info("Received lifecycle event",
		"project_id", projectID,
		"build_id", buildID,
		"invocation_id", invocationID,
		"event_type", eventType,
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
		}

		// Parse the event and create spans
		err = s.parser.ParseBuildEventStreamBuildEvent(
			stream.Context(),
			currentBuildID,
			currentInvocationID,
			event,
		)
		if err != nil {
			s.logger.Error("Failed to parse build event",
				"error", err,
				"build_id", currentBuildID,
				"sequence", seqNum,
			)
			// Continue processing even if one event fails
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

	return nil
}
