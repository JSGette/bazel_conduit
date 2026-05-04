//! TraceContext: UUID→TraceID conversion and TracerProvider initialization.
//!
//! Converts Bazel's invocation UUID into an OTel TraceID so that every span
//! in the trace shares the invocation identity.

use opentelemetry::trace::{SpanContext, SpanId, TraceContextExt, TraceFlags, TraceId, TraceState};
use opentelemetry::{Context, KeyValue};
use opentelemetry_sdk::logs::LoggerProvider;
use opentelemetry_sdk::trace::TracerProvider;
use opentelemetry_sdk::Resource;
use std::time::Duration;

/// Default max records per OTLP export RPC. Sized below gRPC's 4 MiB default
/// receive limit so conduit works against off-the-shelf receivers without tuning.
pub const DEFAULT_OTLP_MAX_EXPORT_BATCH_SIZE: usize = 512;

/// Steady-state ticker between OTLP exports. 200 ms keeps trace-list / Datadog
/// APM updates feeling live while keeping per-RPC overhead low.
const SCHEDULED_DELAY: Duration = Duration::from_millis(200);

/// Per-export RPC deadline. With Tokio runtime + a healthy Datadog Agent,
/// 2 s is generous; we'd rather drop a stalled batch than pile pressure on
/// the queue.
const EXPORT_TIMEOUT: Duration = Duration::from_secs(2);

/// Span queue depth. Sized to absorb a 12 k-event bazel test on a slow
/// receiver without dropping spans.
const MAX_QUEUE_SIZE: usize = 65_536;

/// Run two exports in flight so a single slow RPC doesn't stall the queue.
const MAX_CONCURRENT_EXPORTS: usize = 2;

// TODO(transport): once the OTLP endpoint moves off-host, enable HTTP/2 keep-alive
// (`http2_keep_alive_interval`, `keep_alive_while_idle`) and `gzip-tonic`
// compression on both exporters.

/// Export configuration for OTel spans.
#[derive(Debug, Clone)]
pub enum ExportConfig {
    /// No OTel export (BEP analysis only).
    None,
    /// Print spans as JSON to stdout (for development).
    Stdout,
    /// Send spans via OTLP gRPC to an endpoint (Jaeger, Datadog Agent, etc.).
    Otlp {
        endpoint: String,
        /// Applies to the trace exporter only (log exporter uses its own batching).
        max_export_batch_size: usize,
    },
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

fn default_resource() -> Resource {
    Resource::new(vec![
        KeyValue::new("service.name", "bazel"),
        KeyValue::new("telemetry.sdk.name", "conduit"),
        KeyValue::new("telemetry.sdk.version", env!("CARGO_PKG_VERSION")),
    ])
}

fn build_tracer_provider(
    config: &ExportConfig,
    resource: Resource,
) -> anyhow::Result<Option<TracerProvider>> {
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
        ExportConfig::Otlp { endpoint, max_export_batch_size } => {
            use opentelemetry_otlp::WithExportConfig;
            let exporter = opentelemetry_otlp::SpanExporter::builder()
                .with_tonic()
                .with_endpoint(endpoint)
                .with_timeout(EXPORT_TIMEOUT)
                .build()?;
            let batch_config = opentelemetry_sdk::trace::BatchConfigBuilder::default()
                .with_max_queue_size(MAX_QUEUE_SIZE)
                .with_max_export_batch_size(*max_export_batch_size)
                .with_scheduled_delay(SCHEDULED_DELAY)
                .with_max_export_timeout(EXPORT_TIMEOUT)
                .with_max_concurrent_exports(MAX_CONCURRENT_EXPORTS)
                .build();
            let batch_processor =
                opentelemetry_sdk::trace::BatchSpanProcessor::builder(
                    exporter,
                    opentelemetry_sdk::runtime::Tokio,
                )
                .with_batch_config(batch_config)
                .build();
            let provider = TracerProvider::builder()
                .with_span_processor(batch_processor)
                .with_resource(resource)
                .build();
            Ok(Some(provider))
        }
    }
}

fn build_logger_provider(
    config: &ExportConfig,
    resource: Resource,
) -> anyhow::Result<Option<LoggerProvider>> {
    match config {
        ExportConfig::None => Ok(None),
        ExportConfig::Stdout => {
            let exporter = opentelemetry_stdout::LogExporter::default();
            let provider = LoggerProvider::builder()
                .with_simple_exporter(exporter)
                .with_resource(resource)
                .build();
            Ok(Some(provider))
        }
        ExportConfig::Otlp { endpoint, max_export_batch_size } => {
            use opentelemetry_otlp::WithExportConfig;
            let exporter = opentelemetry_otlp::LogExporter::builder()
                .with_tonic()
                .with_endpoint(endpoint)
                .with_timeout(EXPORT_TIMEOUT)
                .build()?;
            let batch_config = opentelemetry_sdk::logs::BatchConfigBuilder::default()
                .with_max_queue_size(MAX_QUEUE_SIZE)
                .with_max_export_batch_size(*max_export_batch_size)
                .with_scheduled_delay(SCHEDULED_DELAY)
                .with_max_export_timeout(EXPORT_TIMEOUT)
                .build();
            let batch_processor = opentelemetry_sdk::logs::BatchLogProcessor::builder(
                exporter,
                opentelemetry_sdk::runtime::Tokio,
            )
            .with_batch_config(batch_config)
            .build();
            let provider = LoggerProvider::builder()
                .with_log_processor(batch_processor)
                .with_resource(resource)
                .build();
            Ok(Some(provider))
        }
    }
}

/// Initialise a [`TracerProvider`] with the default resource.
///
/// Returns `None` when `config` is [`ExportConfig::None`].
pub fn init_tracer_provider(config: &ExportConfig) -> anyhow::Result<Option<TracerProvider>> {
    build_tracer_provider(config, default_resource())
}

/// Initialise an OpenTelemetry [`LoggerProvider`] according to the export
/// configuration.
///
/// Returns `None` when `config` is [`ExportConfig::None`].
pub fn init_logger_provider(config: &ExportConfig) -> anyhow::Result<Option<LoggerProvider>> {
    build_logger_provider(config, default_resource())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uuid_to_trace_id_deterministic() {
        let id = uuid_to_trace_id("87c2c198-4f8e-403d-8068-62a5518167de");
        let bytes = id.to_bytes();
        assert_eq!(bytes[0], 0x87);
        assert_eq!(bytes[1], 0xc2);
        assert_ne!(bytes, [0u8; 16]);

        let id2 = uuid_to_trace_id("87c2c198-4f8e-403d-8068-62a5518167de");
        assert_eq!(id, id2);
    }

    #[test]
    fn uuid_to_trace_id_invalid_falls_back() {
        let id = uuid_to_trace_id("not-a-uuid");
        assert_ne!(id.to_bytes(), [0u8; 16]);
    }

    #[test]
    fn init_tracer_provider_none_returns_none() {
        let result = init_tracer_provider(&ExportConfig::None).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn init_logger_provider_none_returns_none() {
        let result = init_logger_provider(&ExportConfig::None).unwrap();
        assert!(result.is_none());
    }
}
