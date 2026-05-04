//! TraceContext: UUID→TraceID conversion and TracerProvider initialization.
//!
//! Converts Bazel's invocation UUID into an OTel TraceID so that every span
//! in the trace shares the invocation identity.

use opentelemetry::trace::{SpanContext, SpanId, TraceContextExt, TraceFlags, TraceId, TraceState};
use opentelemetry::{Context, KeyValue};
use opentelemetry_sdk::logs::LoggerProvider;
use opentelemetry_sdk::trace::TracerProvider;
use opentelemetry_sdk::Resource;

/// Default max spans per OTLP export RPC. Sized below gRPC's 4 MiB default
/// receive limit so conduit works against off-the-shelf receivers without tuning.
pub const DEFAULT_OTLP_MAX_EXPORT_BATCH_SIZE: usize = 512;

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
                .build()?;
            // 1 s scheduled_delay keeps live streaming snappy in `--serve`
            // mode where the TracerProvider is rebuilt per invocation
            // (Resource carries `bazel.invocation_id`). Default 5 s would
            // delay the first batch noticeably for short builds.
            let batch_config = opentelemetry_sdk::trace::BatchConfigBuilder::default()
                .with_max_queue_size(65536)
                .with_max_export_batch_size(*max_export_batch_size)
                .with_scheduled_delay(std::time::Duration::from_secs(1))
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

/// Build Resource attributes for an invocation (invariant for the trace).
/// Used when creating the TracerProvider so these appear on every span via Resource.
pub fn build_invocation_resource(
    invocation_id: &str,
    command: &str,
    workspace_dir: Option<&str>,
) -> Vec<KeyValue> {
    let mut attrs = vec![
        KeyValue::new("service.name", "bazel"),
        KeyValue::new("telemetry.sdk.name", "conduit"),
        KeyValue::new("telemetry.sdk.version", env!("CARGO_PKG_VERSION")),
        KeyValue::new("bazel.invocation_id", invocation_id.to_string()),
        KeyValue::new("bazel.command", command.to_string()),
    ];
    if let Some(d) = workspace_dir.filter(|s| !s.is_empty()) {
        attrs.push(KeyValue::new("bazel.workspace.directory", d.to_string()));
    }
    attrs
}

/// Initialise a [`TracerProvider`] with the given resource attributes.
pub fn init_tracer_provider_with_resource(
    config: &ExportConfig,
    resource_attrs: Vec<KeyValue>,
) -> anyhow::Result<Option<TracerProvider>> {
    build_tracer_provider(config, Resource::new(resource_attrs))
}

/// Initialise a [`TracerProvider`] with the default resource (no invocation attributes).
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
    let resource = default_resource();
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
        ExportConfig::Otlp { endpoint, .. } => {
            use opentelemetry_otlp::WithExportConfig;
            let exporter = opentelemetry_otlp::LogExporter::builder()
                .with_tonic()
                .with_endpoint(endpoint)
                .build()?;
            let provider = LoggerProvider::builder()
                .with_batch_exporter(exporter, opentelemetry_sdk::runtime::Tokio)
                .with_resource(resource)
                .build();
            Ok(Some(provider))
        }
    }
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
    fn build_invocation_resource_basic() {
        let attrs = build_invocation_resource("abc-123", "build", None);
        assert!(attrs.iter().any(|kv| kv.key.as_str() == "bazel.invocation_id"));
        assert!(attrs.iter().any(|kv| kv.key.as_str() == "bazel.command"));
        assert!(!attrs.iter().any(|kv| kv.key.as_str() == "bazel.workspace.directory"));
    }

    #[test]
    fn build_invocation_resource_with_workspace() {
        let attrs = build_invocation_resource("abc", "test", Some("/home/user/proj"));
        assert!(attrs.iter().any(|kv| kv.key.as_str() == "bazel.workspace.directory"));
    }

    #[test]
    fn init_tracer_provider_none_returns_none() {
        let result = init_tracer_provider(&ExportConfig::None).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn init_tracer_provider_with_resource_none_returns_none() {
        let result = init_tracer_provider_with_resource(&ExportConfig::None, vec![]).unwrap();
        assert!(result.is_none());
    }
}
