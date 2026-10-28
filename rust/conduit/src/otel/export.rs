// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

//! Batching exporter: OTLP gRPC, or OTLP-JSON lines on stdout.
//!
//! Spans and log records are queued as they end. One task batches them and
//! exports with bounded retry; anything that cannot be queued or exported is
//! dropped and counted, never buffered without bound.

use super::span::{
    Item, KeyValue, LogData, Logger, Severity, Sink, SpanData, SpanKind, Status, Tracer, Value,
};
use otlp_proto::opentelemetry::proto as otlp;
use std::fmt::Write as _;
use std::io::Write as _;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, oneshot};
use tracing::{info, warn};

/// Default max records per OTLP export RPC. Sized below gRPC's 4 MiB default
/// receive limit so conduit works against off-the-shelf receivers.
pub const DEFAULT_OTLP_MAX_EXPORT_BATCH_SIZE: usize = 512;

const QUEUE_SIZE: usize = 65_536;
const FLUSH_INTERVAL: Duration = Duration::from_millis(200);
const EXPORT_TIMEOUT: Duration = Duration::from_secs(2);
const EXPORT_ATTEMPTS: u32 = 3;
const RETRY_BACKOFF: Duration = Duration::from_millis(250);
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);
/// W3C trace flags: every span conduit emits is sampled.
const SAMPLED: u32 = 1;

#[derive(Debug, Clone)]
pub enum ExportConfig {
    None,
    /// OTLP-JSON, one export request per line, on stdout.
    Stdout,
    Otlp {
        endpoint: String,
        max_export_batch_size: usize,
    },
}

/// Whatever was submitted but not exported by shutdown counts as dropped.
#[derive(Default)]
struct Stats {
    submitted_spans: AtomicU64,
    submitted_logs: AtomicU64,
    exported_spans: AtomicU64,
    exported_logs: AtomicU64,
}

#[derive(Clone)]
pub(crate) struct Queue {
    tx: mpsc::Sender<Item>,
    stats: Arc<Stats>,
}

impl Queue {
    pub(crate) fn push(&self, item: Item) {
        let submitted = match item {
            Item::Span(_) => &self.stats.submitted_spans,
            Item::Log(_) => &self.stats.submitted_logs,
        };
        submitted.fetch_add(1, Ordering::Relaxed);
        let _ = self.tx.try_send(item);
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct ExportCounts {
    pub exported_spans: u64,
    pub dropped_spans: u64,
    pub exported_logs: u64,
    pub dropped_logs: u64,
}

pub struct Exporter {
    queue: Queue,
    stop: oneshot::Sender<()>,
    task: tokio::task::JoinHandle<()>,
}

impl Exporter {
    /// Starts the export task on the current Tokio runtime. `None` for
    /// [`ExportConfig::None`].
    pub fn start(config: &ExportConfig) -> anyhow::Result<Option<Exporter>> {
        let (backend, batch_size) = match config {
            ExportConfig::None => return Ok(None),
            ExportConfig::Stdout => (Backend::Stdout, DEFAULT_OTLP_MAX_EXPORT_BATCH_SIZE),
            ExportConfig::Otlp {
                endpoint,
                max_export_batch_size,
            } => {
                let channel = tonic::transport::Endpoint::from_shared(endpoint.clone())?
                    .connect_timeout(EXPORT_TIMEOUT)
                    .connect_lazy();
                (Backend::Otlp(channel), (*max_export_batch_size).max(1))
            }
        };
        let (tx, rx) = mpsc::channel(QUEUE_SIZE);
        let stats = Arc::new(Stats::default());
        let (stop, stop_rx) = oneshot::channel();
        let task = tokio::spawn(run(rx, stop_rx, backend, batch_size, stats.clone()));
        Ok(Some(Exporter {
            queue: Queue { tx, stats },
            stop,
            task,
        }))
    }

    pub fn tracer(&self) -> Tracer {
        Tracer::new(Sink::export(self.queue.clone()))
    }

    pub fn logger(&self) -> Logger {
        Logger::new(Sink::export(self.queue.clone()))
    }

    /// Exports everything queued so far, giving up after [`SHUTDOWN_TIMEOUT`].
    /// Later records are dropped.
    pub async fn shutdown(self) -> ExportCounts {
        let _ = self.stop.send(());
        let mut task = self.task;
        if tokio::time::timeout(SHUTDOWN_TIMEOUT, &mut task)
            .await
            .is_err()
        {
            warn!("Exporter did not drain within {SHUTDOWN_TIMEOUT:?}");
            task.abort();
            let _ = task.await;
        }
        let s = &self.queue.stats;
        let load = |c: &AtomicU64| c.load(Ordering::Relaxed);
        let counts = ExportCounts {
            exported_spans: load(&s.exported_spans),
            dropped_spans: load(&s.submitted_spans) - load(&s.exported_spans),
            exported_logs: load(&s.exported_logs),
            dropped_logs: load(&s.submitted_logs) - load(&s.exported_logs),
        };
        info!(
            exported_spans = counts.exported_spans,
            exported_logs = counts.exported_logs,
            dropped_spans = counts.dropped_spans,
            dropped_logs = counts.dropped_logs,
            "Exporter stopped"
        );
        counts
    }
}

enum Backend {
    Stdout,
    Otlp(tonic::transport::Channel),
}

async fn run(
    mut rx: mpsc::Receiver<Item>,
    mut stop: oneshot::Receiver<()>,
    backend: Backend,
    batch_size: usize,
    stats: Arc<Stats>,
) {
    let mut spans = Vec::new();
    let mut logs = Vec::new();
    let mut tick = tokio::time::interval(FLUSH_INTERVAL);
    loop {
        tokio::select! {
            item = rx.recv() => {
                match item {
                    Some(Item::Span(s)) => spans.push(s),
                    Some(Item::Log(l)) => logs.push(l),
                    None => break,
                }
                if spans.len() < batch_size && logs.len() < batch_size {
                    continue;
                }
            }
            _ = tick.tick() => {}
            _ = &mut stop => {
                rx.close();
                while let Some(item) = rx.recv().await {
                    match item {
                        Item::Span(s) => spans.push(s),
                        Item::Log(l) => logs.push(l),
                    }
                }
                break;
            }
        }
        flush(&backend, &mut spans, &mut logs, batch_size, &stats).await;
    }
    flush(&backend, &mut spans, &mut logs, batch_size, &stats).await;
}

async fn flush(
    backend: &Backend,
    spans: &mut Vec<SpanData>,
    logs: &mut Vec<LogData>,
    batch_size: usize,
    stats: &Stats,
) {
    while !spans.is_empty() {
        let batch: Vec<SpanData> = spans.drain(..spans.len().min(batch_size)).collect();
        let exported = match backend {
            Backend::Stdout => {
                if write_line(&spans_json(&batch)) {
                    batch.len() as u64
                } else {
                    0
                }
            }
            Backend::Otlp(channel) => export_spans(channel, batch).await,
        };
        stats.exported_spans.fetch_add(exported, Ordering::Relaxed);
    }
    while !logs.is_empty() {
        let batch: Vec<LogData> = logs.drain(..logs.len().min(batch_size)).collect();
        let exported = match backend {
            Backend::Stdout => {
                if write_line(&logs_json(&batch)) {
                    batch.len() as u64
                } else {
                    0
                }
            }
            Backend::Otlp(channel) => export_logs(channel, batch).await,
        };
        stats.exported_logs.fetch_add(exported, Ordering::Relaxed);
    }
}

fn write_line(line: &str) -> bool {
    let mut out = std::io::stdout().lock();
    out.write_all(line.as_bytes())
        .and_then(|_| out.write_all(b"\n"))
        .and_then(|_| out.flush())
        .is_ok()
}

/// Returns how many spans the receiver accepted.
async fn export_spans(channel: &tonic::transport::Channel, batch: Vec<SpanData>) -> u64 {
    use otlp::collector::trace::v1::ExportTraceServiceRequest;
    use otlp::collector::trace::v1::trace_service_client::TraceServiceClient;
    let n = batch.len() as u64;
    let request = ExportTraceServiceRequest {
        resource_spans: vec![otlp::trace::v1::ResourceSpans {
            resource: Some(resource()),
            scope_spans: vec![otlp::trace::v1::ScopeSpans {
                scope: Some(scope()),
                spans: batch.into_iter().map(span_proto).collect(),
                ..Default::default()
            }],
            ..Default::default()
        }],
    };
    let mut client = TraceServiceClient::new(channel.clone());
    for attempt in 1..=EXPORT_ATTEMPTS {
        let result = tokio::time::timeout(EXPORT_TIMEOUT, client.export(request.clone())).await;
        let result = result.map(|r| {
            r.map(|resp| {
                resp.into_inner()
                    .partial_success
                    .map(|p| (p.rejected_spans, p.error_message))
            })
        });
        if let Some(accepted) = outcome("spans", attempt, n, result) {
            return accepted;
        }
        tokio::time::sleep(RETRY_BACKOFF * 2u32.pow(attempt - 1)).await;
    }
    0
}

/// Returns how many log records the receiver accepted.
async fn export_logs(channel: &tonic::transport::Channel, batch: Vec<LogData>) -> u64 {
    use otlp::collector::logs::v1::ExportLogsServiceRequest;
    use otlp::collector::logs::v1::logs_service_client::LogsServiceClient;
    let n = batch.len() as u64;
    let request = ExportLogsServiceRequest {
        resource_logs: vec![otlp::logs::v1::ResourceLogs {
            resource: Some(resource()),
            scope_logs: vec![otlp::logs::v1::ScopeLogs {
                scope: Some(scope()),
                log_records: batch.into_iter().map(log_proto).collect(),
                ..Default::default()
            }],
            ..Default::default()
        }],
    };
    let mut client = LogsServiceClient::new(channel.clone());
    for attempt in 1..=EXPORT_ATTEMPTS {
        let result = tokio::time::timeout(EXPORT_TIMEOUT, client.export(request.clone())).await;
        let result = result.map(|r| {
            r.map(|resp| {
                resp.into_inner()
                    .partial_success
                    .map(|p| (p.rejected_log_records, p.error_message))
            })
        });
        if let Some(accepted) = outcome("logs", attempt, n, result) {
            return accepted;
        }
        tokio::time::sleep(RETRY_BACKOFF * 2u32.pow(attempt - 1)).await;
    }
    0
}

/// Partial success: (rejected count, receiver message).
type Rejected = Option<(i64, String)>;

/// `Some(accepted)` when done, `None` to retry. Only codes OTLP marks
/// retryable are retried, at most [`EXPORT_ATTEMPTS`] times, so a dead
/// receiver can't stall the queue.
fn outcome(
    what: &str,
    attempt: u32,
    n: u64,
    result: Result<Result<Rejected, tonic::Status>, tokio::time::error::Elapsed>,
) -> Option<u64> {
    use tonic::Code;
    let status = match result {
        Ok(Ok(Some((rejected, message)))) if rejected > 0 => {
            warn!(
                rejected,
                message, "Receiver rejected part of a {what} batch"
            );
            return Some(n.saturating_sub(rejected as u64));
        }
        Ok(Ok(_)) => return Some(n),
        Ok(Err(status)) => status,
        Err(_) => tonic::Status::deadline_exceeded("export timed out"),
    };
    let retryable = matches!(
        status.code(),
        Code::Cancelled
            | Code::DeadlineExceeded
            | Code::ResourceExhausted
            | Code::Aborted
            | Code::OutOfRange
            | Code::Unavailable
            | Code::DataLoss
    );
    if retryable && attempt < EXPORT_ATTEMPTS {
        return None;
    }
    warn!(code = ?status.code(), message = status.message(), attempt, "Dropping {what} batch");
    Some(0)
}

fn unix_nanos(t: SystemTime) -> u64 {
    t.duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos() as u64)
}

fn resource_attrs() -> Vec<KeyValue> {
    vec![
        KeyValue::new("service.name", "bazel"),
        KeyValue::new("telemetry.sdk.name", "conduit"),
        KeyValue::new("telemetry.sdk.version", env!("CARGO_PKG_VERSION")),
    ]
}

fn kind_code(kind: SpanKind) -> i32 {
    match kind {
        SpanKind::Internal => 1,
        SpanKind::Client => 3,
    }
}

fn status_parts(status: &Status) -> (i32, &str) {
    match status {
        Status::Unset => (0, ""),
        Status::Ok => (1, ""),
        Status::Error { description } => (2, description),
    }
}

// ---- OTLP protobuf ----

fn resource() -> otlp::resource::v1::Resource {
    otlp::resource::v1::Resource {
        attributes: resource_attrs().into_iter().map(kv_proto).collect(),
        ..Default::default()
    }
}

fn scope() -> otlp::common::v1::InstrumentationScope {
    otlp::common::v1::InstrumentationScope {
        name: "conduit".into(),
        version: env!("CARGO_PKG_VERSION").into(),
        ..Default::default()
    }
}

fn kv_proto(kv: KeyValue) -> otlp::common::v1::KeyValue {
    use otlp::common::v1::any_value::Value as V;
    let value = match kv.value {
        Value::Bool(v) => V::BoolValue(v),
        Value::I64(v) => V::IntValue(v),
        Value::F64(v) => V::DoubleValue(v),
        Value::String(v) => V::StringValue(v.into_owned()),
    };
    otlp::common::v1::KeyValue {
        key: kv.key.into_owned(),
        value: Some(otlp::common::v1::AnyValue { value: Some(value) }),
        ..Default::default()
    }
}

fn span_proto(s: SpanData) -> otlp::trace::v1::Span {
    use otlp::trace::v1::{Status as PStatus, span::Event};
    let (code, message) = status_parts(&s.status);
    let status = PStatus {
        code,
        message: message.to_string(),
    };
    let parent = s.parent_span_id.to_bytes();
    otlp::trace::v1::Span {
        trace_id: s.span_context.trace_id().to_bytes().to_vec(),
        span_id: s.span_context.span_id().to_bytes().to_vec(),
        parent_span_id: if parent == [0; 8] {
            Vec::new()
        } else {
            parent.to_vec()
        },
        flags: SAMPLED,
        name: s.name.into_owned(),
        kind: kind_code(s.span_kind),
        start_time_unix_nano: unix_nanos(s.start_time),
        end_time_unix_nano: unix_nanos(s.end_time),
        attributes: s.attributes.into_iter().map(kv_proto).collect(),
        events: s
            .events
            .into_iter()
            .map(|e| Event {
                time_unix_nano: unix_nanos(e.timestamp),
                name: e.name.into_owned(),
                attributes: e.attributes.into_iter().map(kv_proto).collect(),
                ..Default::default()
            })
            .collect(),
        status: Some(status),
        ..Default::default()
    }
}

fn log_proto(l: LogData) -> otlp::logs::v1::LogRecord {
    let t = unix_nanos(l.timestamp);
    otlp::logs::v1::LogRecord {
        time_unix_nano: t,
        observed_time_unix_nano: t,
        severity_number: severity_number(l.severity),
        severity_text: severity_text(l.severity).into(),
        body: Some(otlp::common::v1::AnyValue {
            value: Some(otlp::common::v1::any_value::Value::StringValue(l.body)),
        }),
        attributes: l.attributes.into_iter().map(kv_proto).collect(),
        trace_id: l.span_context.trace_id().to_bytes().to_vec(),
        span_id: l.span_context.span_id().to_bytes().to_vec(),
        flags: SAMPLED,
        ..Default::default()
    }
}

// ---- OTLP-JSON (https://opentelemetry.io/docs/specs/otlp/#json-protobuf-encoding) ----

fn json_str(out: &mut String, s: &str) {
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if c < ' ' => {
                let _ = write!(out, "\\u{:04x}", c as u32);
            }
            c => out.push(c),
        }
    }
    out.push('"');
}

fn json_attrs(out: &mut String, attrs: &[KeyValue]) {
    out.push('[');
    for (i, kv) in attrs.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        out.push_str("{\"key\":");
        json_str(out, &kv.key);
        out.push_str(",\"value\":");
        json_value(out, &kv.value);
        out.push('}');
    }
    out.push(']');
}

fn json_value(out: &mut String, value: &Value) {
    match value {
        Value::Bool(v) => {
            let _ = write!(out, "{{\"boolValue\":{v}}}");
        }
        Value::I64(v) => {
            let _ = write!(out, "{{\"intValue\":\"{v}\"}}");
        }
        Value::F64(v) if v.is_finite() => {
            let _ = write!(out, "{{\"doubleValue\":{v}}}");
        }
        Value::F64(v) => {
            let s = if v.is_nan() {
                "NaN"
            } else if *v > 0.0 {
                "Infinity"
            } else {
                "-Infinity"
            };
            let _ = write!(out, "{{\"doubleValue\":\"{s}\"}}");
        }
        Value::String(v) => {
            out.push_str("{\"stringValue\":");
            json_str(out, v);
            out.push('}');
        }
    }
}

fn json_envelope(out: &mut String, resource_key: &str, scope_key: &str, items_key: &str) {
    let _ = write!(
        out,
        "{{\"{resource_key}\":[{{\"resource\":{{\"attributes\":"
    );
    json_attrs(out, &resource_attrs());
    let _ = write!(
        out,
        "}},\"{scope_key}\":[{{\"scope\":{{\"name\":\"conduit\",\"version\":\"{}\"}},\"{items_key}\":[",
        env!("CARGO_PKG_VERSION")
    );
}

fn spans_json(spans: &[SpanData]) -> String {
    let mut out = String::new();
    json_envelope(&mut out, "resourceSpans", "scopeSpans", "spans");
    for (i, s) in spans.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        let _ = write!(
            out,
            "{{\"traceId\":\"{}\",\"spanId\":\"{}\",",
            s.span_context.trace_id(),
            s.span_context.span_id()
        );
        if s.parent_span_id.to_bytes() != [0; 8] {
            let _ = write!(out, "\"parentSpanId\":\"{}\",", s.parent_span_id);
        }
        let _ = write!(out, "\"flags\":{SAMPLED},\"name\":");
        json_str(&mut out, &s.name);
        let _ = write!(
            out,
            ",\"kind\":{},\"startTimeUnixNano\":\"{}\",\"endTimeUnixNano\":\"{}\",\"attributes\":",
            kind_code(s.span_kind),
            unix_nanos(s.start_time),
            unix_nanos(s.end_time)
        );
        json_attrs(&mut out, &s.attributes);
        out.push_str(",\"events\":[");
        for (j, e) in s.events.iter().enumerate() {
            if j > 0 {
                out.push(',');
            }
            let _ = write!(
                out,
                "{{\"timeUnixNano\":\"{}\",\"name\":",
                unix_nanos(e.timestamp)
            );
            json_str(&mut out, &e.name);
            out.push_str(",\"attributes\":");
            json_attrs(&mut out, &e.attributes);
            out.push('}');
        }
        let (code, message) = status_parts(&s.status);
        let _ = write!(out, "],\"status\":{{\"code\":{code},\"message\":");
        json_str(&mut out, message);
        out.push_str("}}");
    }
    out.push_str("]}]}]}");
    out
}

fn severity_number(s: Severity) -> i32 {
    use otlp::logs::v1::SeverityNumber;
    match s {
        Severity::Info => SeverityNumber::Info as i32,
        Severity::Warn => SeverityNumber::Warn as i32,
        Severity::Error => SeverityNumber::Error as i32,
    }
}

fn severity_text(s: Severity) -> &'static str {
    match s {
        Severity::Info => "INFO",
        Severity::Warn => "WARN",
        Severity::Error => "ERROR",
    }
}

fn logs_json(logs: &[LogData]) -> String {
    let mut out = String::new();
    json_envelope(&mut out, "resourceLogs", "scopeLogs", "logRecords");
    for (i, l) in logs.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        let t = unix_nanos(l.timestamp);
        let _ = write!(
            out,
            "{{\"timeUnixNano\":\"{t}\",\"observedTimeUnixNano\":\"{t}\",\"severityNumber\":{},\
             \"severityText\":\"{}\",\"flags\":{SAMPLED},\"traceId\":\"{}\",\"spanId\":\"{}\",\
             \"body\":{{\"stringValue\":",
            severity_number(l.severity),
            severity_text(l.severity),
            l.span_context.trace_id(),
            l.span_context.span_id()
        );
        json_str(&mut out, &l.body);
        out.push_str("},\"attributes\":");
        json_attrs(&mut out, &l.attributes);
        out.push('}');
    }
    out.push_str("]}]}]}");
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::otel::span::{Context, Sink, Tracer};

    #[test]
    fn spans_json_is_otlp_shaped() {
        let (sink, spans) = Sink::memory();
        let tracer = Tracer::new(sink);
        let mut span = tracer.build_with_context(tracer.span_builder("a \"q\"\n"), &Context::new());
        span.set_attribute(KeyValue::new("n", 7_i64));
        span.set_attribute(KeyValue::new("b", true));
        span.set_status(Status::error("boom"));
        span.end();
        let json = spans_json(&spans.snapshot());
        assert!(json.starts_with(
            "{\"resourceSpans\":[{\"resource\":{\"attributes\":[{\"key\":\"service.name\""
        ));
        assert!(json.contains("\"name\":\"a \\\"q\\\"\\n\""));
        assert!(json.contains("{\"key\":\"n\",\"value\":{\"intValue\":\"7\"}}"));
        assert!(json.contains("{\"key\":\"b\",\"value\":{\"boolValue\":true}}"));
        assert!(json.contains("\"status\":{\"code\":2,\"message\":\"boom\"}"));
        assert!(!json.contains("parentSpanId"));
        assert!(json.contains("\"flags\":1,"));
        assert!(json.ends_with("]}]}]}"));
    }

    #[tokio::test]
    async fn unreachable_receiver_drops_and_counts() {
        let config = ExportConfig::Otlp {
            endpoint: "http://127.0.0.1:1".into(),
            max_export_batch_size: 10,
        };
        let exporter = Exporter::start(&config).unwrap().unwrap();
        let tracer = exporter.tracer();
        tracer
            .build_with_context(tracer.span_builder("s"), &Context::new())
            .end();
        let counts = exporter.shutdown().await;
        assert_eq!((counts.exported_spans, counts.dropped_spans), (0, 1));
    }

    #[tokio::test]
    async fn partial_success_counts_rejected_spans_as_dropped() {
        use otlp::collector::trace::v1::trace_service_server::{TraceService, TraceServiceServer};
        use otlp::collector::trace::v1::{
            ExportTracePartialSuccess, ExportTraceServiceRequest, ExportTraceServiceResponse,
        };

        struct RejectBad;
        #[tonic::async_trait]
        impl TraceService for RejectBad {
            async fn export(
                &self,
                request: tonic::Request<ExportTraceServiceRequest>,
            ) -> Result<tonic::Response<ExportTraceServiceResponse>, tonic::Status> {
                let rejected = request.into_inner().resource_spans[0].scope_spans[0]
                    .spans
                    .iter()
                    .filter(|s| s.name == "bad" && s.flags == SAMPLED)
                    .count();
                Ok(tonic::Response::new(ExportTraceServiceResponse {
                    partial_success: Some(ExportTracePartialSuccess {
                        rejected_spans: rejected as i64,
                        error_message: "bad span".into(),
                    }),
                }))
            }
        }

        let addr = std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap();
        tokio::spawn(
            tonic::transport::Server::builder()
                .add_service(TraceServiceServer::new(RejectBad))
                .serve(addr),
        );
        let config = ExportConfig::Otlp {
            endpoint: format!("http://{addr}"),
            max_export_batch_size: 10,
        };
        let exporter = Exporter::start(&config).unwrap().unwrap();
        let tracer = exporter.tracer();
        for name in ["good", "bad", "good"] {
            tracer
                .build_with_context(tracer.span_builder(name), &Context::new())
                .end();
        }
        let counts = exporter.shutdown().await;
        assert_eq!((counts.exported_spans, counts.dropped_spans), (2, 1));
    }

    #[tokio::test]
    async fn hung_receiver_is_abandoned_at_shutdown_timeout() {
        // Accepts TCP but never speaks HTTP/2, so every attempt times out.
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let config = ExportConfig::Otlp {
            endpoint: format!("http://{}", listener.local_addr().unwrap()),
            max_export_batch_size: 10,
        };
        let exporter = Exporter::start(&config).unwrap().unwrap();
        let tracer = exporter.tracer();
        tracer
            .build_with_context(tracer.span_builder("s"), &Context::new())
            .end();
        let started = std::time::Instant::now();
        let counts = exporter.shutdown().await;
        assert!(started.elapsed() < SHUTDOWN_TIMEOUT + Duration::from_secs(1));
        assert_eq!((counts.exported_spans, counts.dropped_spans), (0, 1));
    }
}
