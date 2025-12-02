package bes

import (
	"log/slog"

	build_event_stream "github.com/JSGette/bazel_conduit/proto/build_event_stream"
	build "google.golang.org/genproto/googleapis/devtools/build/v1"
)

/*
Parser is supposed to be a singleton inside of the service.
It doesn't care about anything but just converting protobuf messages into something else.
It should never have any other business logic.
*/
type BuildEventParser struct {
	logger *slog.Logger
	toJson bool // Whether to convert to JSON
	toOTel bool // Whether to convert to OTel span
}

func NewBuildEventParser(logger *slog.Logger, toJson bool, toOTel bool) *BuildEventParser {
	if logger == nil {
		logger = slog.Default()
	}

	return &BuildEventParser{
		logger: logger,
		toJson: toJson,
		toOTel: toOTel,
	}
}

func (p *BuildEventParser) ParseBuildEvent(event *build.BuildEvent) (*build_event_stream.BuildEvent, error) {
	/*
		Rough workflow description:
		1. We receive BuildEvent protobuf message (see build_event.proto, which is a part of BEP in googlapis)
		2. BuildEvent contains Event attribute that has yet another BuildEvent protobuf message
		(see build_event_stream.proto, which in contrast a part of bazel's source code). So we extract it
		as it is an object that contains the actual event data.
		3. We can now parse the event data and translate into something else (e.g. JSON or OTel span)

		TODO: Add links to proto files for reference.
		TODO: As soon as parser is functional elaborate on the workflow in more detail.
	*/

	// Extracting stream's BuildEvent from BEP's BuildEvent (AAAAA NAMINGS)
	bazelEvent := &build_event_stream.BuildEvent{}
	err := event.GetBazelEvent().UnmarshalTo(bazelEvent)
	if err != nil {
		p.logger.Error("Failed to unmarshal Bazel event",
			"BuildEventParserError", err,
		)
		return nil, err
	}

	if p.toJson {
		p.logger.Info("Converting Bazel event to JSON...")
		p.logger.Warn("JSON PARSER: NOT YET IMPLEMENTED")
	}

	if p.toOTel {
		p.logger.Info("Converting Bazel event to OTel span...")
		p.logger.Warn("OTEL PARSER: NOT YET IMPLEMENTED")
	}

	return bazelEvent, nil
}
