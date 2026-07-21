// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

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
