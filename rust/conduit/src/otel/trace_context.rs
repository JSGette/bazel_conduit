//! TraceContext: UUID→TraceID conversion and TracerProvider initialization.
//!
//! Converts Bazel's invocation UUID into an OTel TraceID so that every span
//! in the trace shares the invocation identity.

use opentelemetry::trace::{SpanContext, SpanId, TraceContextExt, TraceFlags, TraceId, TraceState};
use opentelemetry::{Context, KeyValue};
use opentelemetry_sdk::trace::TracerProvider;
use opentelemetry_sdk::Resource;

/// Export configuration for OTel spans.
#[derive(Debug, Clone)]
pub enum ExportConfig {
    /// No OTel export (BEP analysis only).
    None,
    /// Print spans as JSON to stdout (for development).
    Stdout,
    /// Send spans via OTLP gRPC to an endpoint (Jaeger, Datadog Agent, etc.).
    Otlp { endpoint: String },
}

/// Convert a Bazel invocation UUID string into an OTel TraceID.
///
/// UUID is 128 bits, same as TraceID – we use the raw bytes directly.
pub fn uuid_to_trace_id(uuid_str: &str) -> TraceId {
    let uuid = uuid::Uuid::parse_str(uuid_str).unwrap_or_else(|_| uuid::Uuid::new_v4());
    TraceId::from_bytes(*uuid.as_bytes())
}

/// Create a synthetic parent [`Context`] carrying a remote SpanContext with
/// the given TraceID.  Starting a span "inside" this context causes the SDK
/// to inherit the TraceID while generating a fresh SpanID.
pub fn make_root_context(trace_id: TraceId) -> Context {
    let sc = SpanContext::new(
        trace_id,
        SpanId::INVALID,
        TraceFlags::SAMPLED,
        true, // is_remote
        TraceState::default(),
    );
    Context::new().with_remote_span_context(sc)
}

/// Initialise an OpenTelemetry [`TracerProvider`] according to the export
/// configuration.
///
/// Returns `None` when `config` is [`ExportConfig::None`].
pub fn init_tracer_provider(config: &ExportConfig) -> anyhow::Result<Option<TracerProvider>> {
    let resource = Resource::new(vec![
        KeyValue::new("service.name", "conduit"),
        KeyValue::new("service.version", env!("CARGO_PKG_VERSION")),
    ]);

    match config {
        ExportConfig::None => Ok(None),
        ExportConfig::Stdout => {
            let exporter = opentelemetry_stdout::SpanExporter::default();
            let provider = TracerProvider::builder()
                .with_simple_exporter(exporter)
                .with_resource(resource)
                .build();
            Ok(Some(provider))
        }
        ExportConfig::Otlp { endpoint } => {
            use opentelemetry_otlp::WithExportConfig;
            let exporter = opentelemetry_otlp::SpanExporter::builder()
                .with_tonic()
                .with_endpoint(endpoint)
                .build()?;
            let provider = TracerProvider::builder()
                .with_batch_exporter(exporter, opentelemetry_sdk::runtime::Tokio)
                .with_resource(resource)
                .build();
            Ok(Some(provider))
        }
    }
}
