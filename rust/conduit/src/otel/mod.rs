//! OTel mapper – BEP events → OpenTelemetry spans.

pub mod attributes;
pub mod mapper;
pub mod trace_context;

pub use mapper::{ActionCompletedEvent, OtelMapper, TestResultEvent};
pub use trace_context::{
    build_invocation_resource, init_logger_provider, init_tracer_provider,
    init_tracer_provider_with_resource, ExportConfig,
};
