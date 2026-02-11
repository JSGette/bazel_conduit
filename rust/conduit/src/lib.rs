//! BEP to OpenTelemetry trace converter library
//!
//! This crate converts Bazel Build Event Protocol (BEP) events into
//! OpenTelemetry traces for observability.

pub mod bep;
pub mod grpc;
pub mod state;

// Re-export proto types
pub use build_event_stream_proto::build_event_stream;
