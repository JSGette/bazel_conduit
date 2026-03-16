//! BEP to OpenTelemetry trace converter library
//!
//! This crate converts Bazel Build Event Protocol (BEP) events into
//! OpenTelemetry traces for observability.

pub mod bep;
pub mod exec_log;
pub mod grpc;
pub mod otel;
pub mod state;

// Re-export BEP proto types
pub use build_event_stream_proto::build_event_stream;

// Re-export gRPC service types (PublishBuildEvent service, request/response types)
pub mod bes_proto {
    pub use publish_build_event_proto::google::devtools::build::v1::*;
}

// Re-export BES event types (BuildEvent, StreamId, etc.)
pub mod bes_events {
    pub use publish_build_event_proto::build_events_proto::google::devtools::build::v1::*;
}

// Re-export well-known protobuf types used by the gRPC service
pub mod proto_types {
    pub use publish_build_event_proto::any_proto::google::protobuf::Any;
    pub use publish_build_event_proto::empty_proto::google::protobuf::Empty;
}
