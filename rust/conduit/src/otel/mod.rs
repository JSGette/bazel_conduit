//! OTel mapper – BEP events → OpenTelemetry spans.

pub mod attributes;
pub mod mapper;
pub mod trace_context;

pub use mapper::OtelMapper;
pub use trace_context::{ExportConfig, init_tracer_provider};
