// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

//! Minimal span and log model.
//!
//! Mirrors the small slice of the OpenTelemetry API the mapper uses. Ended
//! spans and emitted log records go to a [`Sink`]; [`super::export`] turns
//! them into OTLP.

use std::borrow::Cow;
use std::fmt;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::SystemTime;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct TraceId([u8; 16]);

impl TraceId {
    pub const INVALID: TraceId = TraceId([0; 16]);

    pub fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }

    pub fn to_bytes(self) -> [u8; 16] {
        self.0
    }
}

impl fmt::Display for TraceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.iter().try_for_each(|b| write!(f, "{b:02x}"))
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct SpanId([u8; 8]);

impl SpanId {
    pub const INVALID: SpanId = SpanId([0; 8]);

    pub fn to_bytes(self) -> [u8; 8] {
        self.0
    }

    fn random() -> Self {
        loop {
            let bytes = uuid::Uuid::new_v4().into_bytes();
            let id: [u8; 8] = bytes[..8].try_into().expect("8 bytes");
            if id != [0; 8] {
                return Self(id);
            }
        }
    }
}

impl fmt::Display for SpanId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.iter().try_for_each(|b| write!(f, "{b:02x}"))
    }
}

/// Bazel invocation UUIDs are 128 bits, like trace IDs: use the bytes as-is.
pub fn uuid_to_trace_id(uuid_str: &str) -> TraceId {
    let uuid = uuid::Uuid::parse_str(uuid_str).unwrap_or_else(|_| uuid::Uuid::new_v4());
    TraceId(*uuid.as_bytes())
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SpanContext {
    trace_id: TraceId,
    span_id: SpanId,
}

impl SpanContext {
    pub fn new(trace_id: TraceId, span_id: SpanId) -> Self {
        Self { trace_id, span_id }
    }

    pub fn trace_id(&self) -> TraceId {
        self.trace_id
    }

    pub fn span_id(&self) -> SpanId {
        self.span_id
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    Bool(bool),
    I64(i64),
    F64(f64),
    String(Cow<'static, str>),
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Bool(v) => write!(f, "{v}"),
            Value::I64(v) => write!(f, "{v}"),
            Value::F64(v) => write!(f, "{v}"),
            Value::String(v) => f.write_str(v),
        }
    }
}

impl From<bool> for Value {
    fn from(v: bool) -> Self {
        Value::Bool(v)
    }
}

impl From<i64> for Value {
    fn from(v: i64) -> Self {
        Value::I64(v)
    }
}

impl From<f64> for Value {
    fn from(v: f64) -> Self {
        Value::F64(v)
    }
}

impl From<&'static str> for Value {
    fn from(v: &'static str) -> Self {
        Value::String(Cow::Borrowed(v))
    }
}

impl From<String> for Value {
    fn from(v: String) -> Self {
        Value::String(Cow::Owned(v))
    }
}

impl From<Cow<'static, str>> for Value {
    fn from(v: Cow<'static, str>) -> Self {
        Value::String(v)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct KeyValue {
    pub key: Cow<'static, str>,
    pub value: Value,
}

impl KeyValue {
    pub fn new(key: impl Into<Cow<'static, str>>, value: impl Into<Value>) -> Self {
        Self {
            key: key.into(),
            value: value.into(),
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub enum Status {
    #[default]
    Unset,
    Ok,
    Error {
        description: Cow<'static, str>,
    },
}

impl Status {
    pub fn error(description: impl Into<Cow<'static, str>>) -> Self {
        Status::Error {
            description: description.into(),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum SpanKind {
    #[default]
    Internal,
    Client,
}

#[derive(Clone, Debug)]
pub struct Event {
    pub name: Cow<'static, str>,
    pub timestamp: SystemTime,
    pub attributes: Vec<KeyValue>,
}

/// An ended span, as handed to the [`Sink`].
#[derive(Clone, Debug)]
pub struct SpanData {
    pub span_context: SpanContext,
    pub parent_span_id: SpanId,
    pub span_kind: SpanKind,
    pub name: Cow<'static, str>,
    pub start_time: SystemTime,
    pub end_time: SystemTime,
    pub attributes: Vec<KeyValue>,
    pub events: Vec<Event>,
    pub status: Status,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Severity {
    Info,
    Warn,
    Error,
}

#[derive(Clone, Debug)]
pub struct LogData {
    pub timestamp: SystemTime,
    pub span_context: SpanContext,
    pub severity: Severity,
    pub body: String,
    pub attributes: Vec<KeyValue>,
}

pub(crate) enum Item {
    Span(SpanData),
    Log(LogData),
}

/// Where ended spans and log records go. Cloned into every span.
#[derive(Clone, Default)]
pub struct Sink(SinkInner);

#[derive(Clone, Default)]
enum SinkInner {
    #[default]
    Discard,
    Export(super::export::Queue),
    Memory(Arc<Mutex<Vec<SpanData>>>),
}

impl Sink {
    pub(crate) fn export(queue: super::export::Queue) -> Self {
        Sink(SinkInner::Export(queue))
    }

    /// Keeps ended spans in memory (tests); log records are discarded.
    pub fn memory() -> (Sink, MemorySpans) {
        let spans = Arc::new(Mutex::new(Vec::new()));
        (Sink(SinkInner::Memory(spans.clone())), MemorySpans(spans))
    }

    fn send(&self, item: Item) {
        match &self.0 {
            SinkInner::Discard => {}
            SinkInner::Export(queue) => queue.push(item),
            SinkInner::Memory(spans) => {
                if let Item::Span(span) = item {
                    spans.lock().unwrap().push(span);
                }
            }
        }
    }
}

#[derive(Clone)]
pub struct MemorySpans(Arc<Mutex<Vec<SpanData>>>);

impl MemorySpans {
    pub fn snapshot(&self) -> Vec<SpanData> {
        self.0.lock().unwrap().clone()
    }
}

/// A live span. Ends (at "now") when dropped without an explicit end.
pub struct Span {
    span_context: SpanContext,
    data: Option<SpanData>,
    sink: Sink,
}

impl Span {
    pub fn span_context(&self) -> SpanContext {
        self.span_context
    }

    /// Replaces an existing attribute with the same key.
    pub fn set_attribute(&mut self, kv: KeyValue) {
        let Some(data) = &mut self.data else { return };
        match data.attributes.iter_mut().find(|a| a.key == kv.key) {
            Some(existing) => existing.value = kv.value,
            None => data.attributes.push(kv),
        }
    }

    pub fn add_event(&mut self, name: impl Into<Cow<'static, str>>, attributes: Vec<KeyValue>) {
        if let Some(data) = &mut self.data {
            data.events.push(Event {
                name: name.into(),
                timestamp: SystemTime::now(),
                attributes,
            });
        }
    }

    pub fn set_status(&mut self, status: Status) {
        if let Some(data) = &mut self.data {
            data.status = status;
        }
    }

    pub fn update_name(&mut self, name: impl Into<Cow<'static, str>>) {
        if let Some(data) = &mut self.data {
            data.name = name.into();
        }
    }

    pub fn end(&mut self) {
        self.end_with_timestamp(SystemTime::now());
    }

    pub fn end_with_timestamp(&mut self, end: SystemTime) {
        if let Some(mut data) = self.data.take() {
            data.end_time = end.max(data.start_time);
            self.sink.send(Item::Span(data));
        }
    }
}

impl Drop for Span {
    fn drop(&mut self) {
        self.end();
    }
}

/// Parent handle for new spans: a live span, a bare span context (an ended
/// parent, or the invocation's trace ID with no parent span), or nothing.
#[derive(Clone, Default)]
pub struct Context {
    span: Option<Arc<Mutex<Span>>>,
    remote: Option<SpanContext>,
}

impl Context {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_span(self, span: Span) -> Self {
        Self {
            span: Some(Arc::new(Mutex::new(span))),
            remote: None,
        }
    }

    pub fn with_remote_span_context(self, span_context: SpanContext) -> Self {
        Self {
            span: None,
            remote: Some(span_context),
        }
    }

    /// The live span, or a no-op stand-in when this context has none.
    pub fn span(&self) -> SpanRef<'_> {
        SpanRef {
            guard: self.span.as_ref().map(|s| s.lock().unwrap()),
            remote: self.remote.unwrap_or_default(),
        }
    }
}

pub struct SpanRef<'a> {
    guard: Option<MutexGuard<'a, Span>>,
    remote: SpanContext,
}

impl SpanRef<'_> {
    pub fn span_context(&self) -> SpanContext {
        self.guard
            .as_ref()
            .map_or(self.remote, |s| s.span_context())
    }

    pub fn set_attribute(&mut self, kv: KeyValue) {
        if let Some(s) = &mut self.guard {
            s.set_attribute(kv);
        }
    }

    pub fn add_event(&mut self, name: impl Into<Cow<'static, str>>, attributes: Vec<KeyValue>) {
        if let Some(s) = &mut self.guard {
            s.add_event(name, attributes);
        }
    }

    pub fn set_status(&mut self, status: Status) {
        if let Some(s) = &mut self.guard {
            s.set_status(status);
        }
    }

    pub fn update_name(&mut self, name: impl Into<Cow<'static, str>>) {
        if let Some(s) = &mut self.guard {
            s.update_name(name);
        }
    }

    pub fn end(&mut self) {
        if let Some(s) = &mut self.guard {
            s.end();
        }
    }

    pub fn end_with_timestamp(&mut self, end: SystemTime) {
        if let Some(s) = &mut self.guard {
            s.end_with_timestamp(end);
        }
    }
}

pub struct SpanBuilder {
    name: Cow<'static, str>,
    kind: SpanKind,
    attributes: Vec<KeyValue>,
    start_time: Option<SystemTime>,
}

impl SpanBuilder {
    pub fn with_kind(mut self, kind: SpanKind) -> Self {
        self.kind = kind;
        self
    }

    pub fn with_attributes(mut self, attributes: Vec<KeyValue>) -> Self {
        self.attributes = attributes;
        self
    }

    pub fn with_start_time(mut self, start: SystemTime) -> Self {
        self.start_time = Some(start);
        self
    }
}

#[derive(Clone, Default)]
pub struct Tracer {
    sink: Sink,
}

impl Tracer {
    pub fn new(sink: Sink) -> Self {
        Self { sink }
    }

    pub fn span_builder(&self, name: impl Into<Cow<'static, str>>) -> SpanBuilder {
        SpanBuilder {
            name: name.into(),
            kind: SpanKind::Internal,
            attributes: Vec::new(),
            start_time: None,
        }
    }

    /// Starts a child of `parent`; a parent without a trace ID starts a new trace.
    pub fn build_with_context(&self, builder: SpanBuilder, parent: &Context) -> Span {
        let parent_cx = parent.span().span_context();
        let trace_id = if parent_cx.trace_id == TraceId::INVALID {
            TraceId(*uuid::Uuid::new_v4().as_bytes())
        } else {
            parent_cx.trace_id
        };
        let span_context = SpanContext::new(trace_id, SpanId::random());
        Span {
            span_context,
            data: Some(SpanData {
                span_context,
                parent_span_id: parent_cx.span_id,
                span_kind: builder.kind,
                name: builder.name,
                start_time: builder.start_time.unwrap_or_else(SystemTime::now),
                end_time: SystemTime::UNIX_EPOCH,
                attributes: builder.attributes,
                events: Vec::new(),
                status: Status::Unset,
            }),
            sink: self.sink.clone(),
        }
    }
}

#[derive(Clone, Default)]
pub struct Logger {
    sink: Sink,
}

impl Logger {
    pub fn new(sink: Sink) -> Self {
        Self { sink }
    }

    pub fn emit(&self, record: LogData) {
        self.sink.send(Item::Log(record));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uuid_to_trace_id_uses_uuid_bytes() {
        let id = uuid_to_trace_id("87c2c198-4f8e-403d-8068-62a5518167de");
        assert_eq!(id.to_string(), "87c2c1984f8e403d806862a5518167de");
        assert_ne!(uuid_to_trace_id("not-a-uuid"), TraceId::INVALID);
    }

    #[test]
    fn child_inherits_trace_and_parent_ids() {
        let (sink, spans) = Sink::memory();
        let tracer = Tracer::new(sink);
        let trace_id = uuid_to_trace_id("87c2c198-4f8e-403d-8068-62a5518167de");
        let root_parent =
            Context::new().with_remote_span_context(SpanContext::new(trace_id, SpanId::INVALID));
        let root = Context::new()
            .with_span(tracer.build_with_context(tracer.span_builder("root"), &root_parent));
        let mut child = tracer.build_with_context(tracer.span_builder("child"), &root);
        child.set_attribute(KeyValue::new("k", 1_i64));
        child.set_attribute(KeyValue::new("k", 2_i64));
        child.end();
        root.span().end();
        // Children of an ended parent stay in its trace.
        let late = tracer.build_with_context(tracer.span_builder("late"), &root);
        drop(late);

        let spans = spans.snapshot();
        let (child, root, late) = (&spans[0], &spans[1], &spans[2]);
        assert_eq!(root.parent_span_id, SpanId::INVALID);
        assert_eq!(child.parent_span_id, root.span_context.span_id());
        assert_eq!(child.span_context.trace_id(), trace_id);
        assert_eq!(late.parent_span_id, root.span_context.span_id());
        assert_eq!(child.attributes, vec![KeyValue::new("k", 2_i64)]);
    }

    #[test]
    fn dropped_span_is_ended_once() {
        let (sink, spans) = Sink::memory();
        let tracer = Tracer::new(sink);
        let mut span = tracer.build_with_context(tracer.span_builder("s"), &Context::new());
        span.end();
        drop(span);
        assert_eq!(spans.snapshot().len(), 1);
    }
}
