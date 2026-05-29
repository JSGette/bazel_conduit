//! OTel mapper – BEP events → OpenTelemetry spans.

pub mod mapper;
pub mod attributes {
    pub use conduit_otel_attributes::*;
}
pub mod redact {
    pub use conduit_otel_redact::*;
}
pub mod trace_context {
    pub use conduit_otel_trace_context::*;
}

pub use mapper::{ActionCompletedEvent, OtelMapper, TestResultEvent};
pub use redact::{Redactor, DEFAULT_REDACT_PATTERNS};
pub use trace_context::{
    init_logger_provider, init_tracer_provider, ExportConfig,
    DEFAULT_OTLP_MAX_EXPORT_BATCH_SIZE,
};
