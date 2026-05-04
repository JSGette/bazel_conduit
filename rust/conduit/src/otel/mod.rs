//! OTel mapper – BEP events → OpenTelemetry spans.

pub mod attributes;
pub mod mapper;
pub mod redact;
pub mod trace_context;

pub use mapper::{ActionCompletedEvent, OtelMapper, TestResultEvent};
pub use redact::{Redactor, DEFAULT_REDACT_PATTERNS};
pub use trace_context::{
    init_logger_provider, init_tracer_provider, ExportConfig,
    DEFAULT_OTLP_MAX_EXPORT_BATCH_SIZE,
};
