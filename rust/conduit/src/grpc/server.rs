//! BES gRPC server implementation
//!
//! Implements the Google Build Event Service (BES) gRPC API.
//! Bazel connects to this server via --bes_backend=grpc://localhost:<port>

use crate::bep::{BepJsonEvent, EventRouter};
use crate::bes_events::build_event::Event as BesEvent;
use crate::bes_proto::publish_build_event_server::{PublishBuildEvent, PublishBuildEventServer};
use crate::bes_proto::{
    PublishBuildToolEventStreamRequest, PublishBuildToolEventStreamResponse,
    PublishLifecycleEventRequest,
};
use crate::proto_types::{Any, Empty};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};
use tracing::{debug, error, info, trace, warn};

/// BES gRPC server implementation
pub struct BesServer {
    router: Arc<Mutex<EventRouter>>,
}

impl BesServer {
    pub fn new() -> Self {
        Self {
            router: Arc::new(Mutex::new(EventRouter::new())),
        }
    }

    /// Get a reference to the shared router (for accessing state after stream ends)
    pub fn router(&self) -> Arc<Mutex<EventRouter>> {
        self.router.clone()
    }
}

impl Default for BesServer {
    fn default() -> Self {
        Self::new()
    }
}

#[tonic::async_trait]
impl PublishBuildEvent for BesServer {
    /// Handle lifecycle events (BuildEnqueued, InvocationAttemptStarted, etc.)
    async fn publish_lifecycle_event(
        &self,
        request: Request<PublishLifecycleEventRequest>,
    ) -> Result<Response<Empty>, Status> {
        let req = request.into_inner();
        info!(
            project_id = %req.project_id,
            "Received lifecycle event"
        );

        if let Some(ordered) = &req.build_event {
            if let Some(bes_event) = &ordered.event {
                if let Some(event) = &bes_event.event {
                    match event {
                        BesEvent::InvocationAttemptStarted(s) => {
                            info!(attempt = s.attempt_number, "Invocation attempt started");
                        }
                        BesEvent::InvocationAttemptFinished(_) => {
                            info!("Invocation attempt finished");
                        }
                        BesEvent::BuildEnqueued(_) => {
                            info!("Build enqueued");
                        }
                        BesEvent::BuildFinished(_) => {
                            info!("Build finished (lifecycle)");
                        }
                        _ => {
                            debug!("Other lifecycle event");
                        }
                    }
                }
            }
        }

        Ok(Response::new(Empty {}))
    }

    /// Server streaming response type for PublishBuildToolEventStream
    type PublishBuildToolEventStreamStream =
        ReceiverStream<Result<PublishBuildToolEventStreamResponse, Status>>;

    /// Handle the bidirectional stream of build tool events
    ///
    /// This is the main method that receives BEP events from Bazel.
    /// Each request contains an OrderedBuildEvent wrapping a BES BuildEvent,
    /// which in turn contains a google.protobuf.Any wrapping a
    /// build_event_stream.BuildEvent.
    async fn publish_build_tool_event_stream(
        &self,
        request: Request<Streaming<PublishBuildToolEventStreamRequest>>,
    ) -> Result<Response<Self::PublishBuildToolEventStreamStream>, Status> {
        let mut stream = request.into_inner();
        let (tx, rx) = mpsc::channel(128);
        let router = self.router.clone();

        info!("Build tool event stream started");

        tokio::spawn(async move {
            let mut event_count: u64 = 0;

            loop {
                match stream.message().await {
                    Ok(Some(req)) => {
                        event_count += 1;

                        let sequence_number = req
                            .ordered_build_event
                            .as_ref()
                            .map(|e| e.sequence_number)
                            .unwrap_or(0);

                        trace!(
                            sequence = sequence_number,
                            event_count,
                            "Received build tool event"
                        );

                        // Extract the BazelEvent Any payload from the BES BuildEvent
                        if let Some(json_event) = extract_bep_event(&req) {
                            let mut router = router.lock().await;
                            if let Err(e) = router.route(&json_event) {
                                warn!(error = %e, "Failed to route BEP event");
                            }
                        }

                        // Send acknowledgement
                        let stream_id = req
                            .ordered_build_event
                            .as_ref()
                            .and_then(|e| e.stream_id.clone());

                        let response = PublishBuildToolEventStreamResponse {
                            stream_id,
                            sequence_number,
                        };

                        if tx.send(Ok(response)).await.is_err() {
                            debug!("Response channel closed");
                            break;
                        }
                    }
                    Ok(None) => {
                        // Stream ended normally
                        let router = router.lock().await;
                        let summary = router.state().summary();
                        info!("\n{}", summary);
                        info!("gRPC stream completed ({event_count} events processed)");
                        break;
                    }
                    Err(e) => {
                        error!(error = %e, "Stream error");
                        let _ = tx
                            .send(Err(Status::internal(format!("Stream error: {e}"))))
                            .await;
                        break;
                    }
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

/// Extract a BEP event from a PublishBuildToolEventStreamRequest.
///
/// The BES protocol wraps events as:
///   PublishBuildToolEventStreamRequest
///     -> ordered_build_event: OrderedBuildEvent
///       -> event: build_events.BuildEvent (BES-level)
///         -> event: BazelEvent(google.protobuf.Any)
///           -> value: serialized build_event_stream.BuildEvent (BEP)
fn extract_bep_event(req: &PublishBuildToolEventStreamRequest) -> Option<BepJsonEvent> {
    let ordered = req.ordered_build_event.as_ref()?;
    let bes_event = ordered.event.as_ref()?;
    let event = bes_event.event.as_ref()?;

    match event {
        BesEvent::BazelEvent(any) => match decode_any_to_bep_json(any) {
            Ok(json_event) => Some(json_event),
            Err(e) => {
                debug!(
                    type_url = %any.type_url,
                    error = %e,
                    "Could not decode BazelEvent Any payload"
                );
                None
            }
        },
        BesEvent::ConsoleOutput(co) => {
            trace!("Console output event (ignored)");
            let _ = co;
            None
        }
        BesEvent::ComponentStreamFinished(_) => {
            debug!("Component stream finished");
            None
        }
        BesEvent::BuildToolLogs(_) => {
            debug!("Build tool logs event");
            None
        }
        _ => {
            debug!("Non-BazelEvent BES event type");
            None
        }
    }
}

/// Decode a google.protobuf.Any payload into a BepJsonEvent
///
/// The Any payload from BES contains a serialized build_event_stream.BuildEvent.
/// We decode it using prost and convert to our JSON-based event format for
/// compatibility with the existing router.
fn decode_any_to_bep_json(any: &Any) -> Result<BepJsonEvent, Box<dyn std::error::Error + Send + Sync>> {
    use prost::Message;

    // Verify the type_url is a BuildEvent
    if !any.type_url.contains("BuildEvent") && !any.type_url.contains("build_event_stream") {
        return Err(format!("Unexpected type_url: {}", any.type_url).into());
    }

    // Decode the protobuf bytes into a BEP BuildEvent
    let build_event = crate::build_event_stream::BuildEvent::decode(any.value.as_ref())?;

    // Convert to JSON map for BepJsonEvent compatibility
    let mut json_map = serde_json::Map::new();

    // Convert the event ID
    if let Some(id) = &build_event.id {
        json_map.insert("id".to_string(), build_event_id_to_json(id));
    } else {
        json_map.insert("id".to_string(), serde_json::json!({}));
    }

    // Convert children
    let children: Vec<serde_json::Value> = build_event
        .children
        .iter()
        .map(build_event_id_to_json)
        .collect();
    if !children.is_empty() {
        json_map.insert("children".to_string(), serde_json::json!(children));
    }

    // Last message flag
    if build_event.last_message {
        json_map.insert("lastMessage".to_string(), serde_json::Value::Bool(true));
    }

    // Convert the payload oneof
    if let Some(payload) = &build_event.payload {
        add_payload_to_json(&mut json_map, payload);
    }

    // Parse the JSON map as BepJsonEvent
    let json_value = serde_json::Value::Object(json_map);
    let event: BepJsonEvent = serde_json::from_value(json_value)?;

    Ok(event)
}

/// Convert a BuildEventId to JSON
fn build_event_id_to_json(id: &crate::build_event_stream::BuildEventId) -> serde_json::Value {
    use crate::build_event_stream::build_event_id::Id;

    let Some(id_inner) = &id.id else {
        return serde_json::json!({});
    };

    match id_inner {
        Id::Started(_) => serde_json::json!({"started": {}}),
        Id::UnstructuredCommandLine(_) => serde_json::json!({"unstructuredCommandLine": {}}),
        Id::StructuredCommandLine(s) => {
            serde_json::json!({"structuredCommandLine": {"commandLineLabel": s.command_line_label}})
        }
        Id::OptionsParsed(_) => serde_json::json!({"optionsParsed": {}}),
        Id::WorkspaceStatus(_) => serde_json::json!({"workspaceStatus": {}}),
        Id::Configuration(c) => serde_json::json!({"configuration": {"id": c.id}}),
        Id::Pattern(p) => {
            serde_json::json!({"pattern": {"pattern": p.pattern}})
        }
        Id::PatternSkipped(p) => {
            serde_json::json!({"patternSkipped": {"pattern": p.pattern}})
        }
        Id::TargetConfigured(t) => {
            serde_json::json!({"targetConfigured": {"label": t.label}})
        }
        Id::TargetCompleted(t) => {
            serde_json::json!({"targetCompleted": {"label": t.label}})
        }
        Id::ActionCompleted(a) => {
            serde_json::json!({"actionCompleted": {
                "label": a.label,
                "primaryOutput": a.primary_output,
            }})
        }
        Id::NamedSet(n) => {
            serde_json::json!({"namedSet": {"id": n.id}})
        }
        Id::TestResult(t) => {
            serde_json::json!({"testResult": {"label": t.label}})
        }
        Id::TestSummary(t) => {
            serde_json::json!({"testSummary": {"label": t.label}})
        }
        Id::BuildFinished(_) => serde_json::json!({"buildFinished": {}}),
        Id::BuildMetrics(_) => serde_json::json!({"buildMetrics": {}}),
        Id::Progress(p) => {
            serde_json::json!({"progress": {"opaqueCount": p.opaque_count}})
        }
        Id::Fetch(f) => {
            serde_json::json!({"fetch": {"url": f.url}})
        }
        Id::Workspace(_) => serde_json::json!({"workspaceInfo": {}}),
        Id::BuildToolLogs(_) => serde_json::json!({"buildToolLogs": {}}),
        Id::BuildMetadata(_) => serde_json::json!({"buildMetadata": {}}),
        Id::ConvenienceSymlinksIdentified(_) => {
            serde_json::json!({"convenienceSymlinksIdentified": {}})
        }
        _ => serde_json::json!({}),
    }
}

/// Convert the payload oneof to JSON fields
#[allow(deprecated)] // start_time_millis and finish_time_millis are deprecated
fn add_payload_to_json(
    map: &mut serde_json::Map<String, serde_json::Value>,
    payload: &crate::build_event_stream::build_event::Payload,
) {
    use crate::build_event_stream::build_event::Payload;
    use crate::build_event_stream::file::File as FileContent;

    match payload {
        Payload::Started(s) => {
            map.insert(
                "started".to_string(),
                serde_json::json!({
                    "uuid": s.uuid,
                    "command": s.command,
                    "startTimeMillis": s.start_time_millis,
                }),
            );
        }
        Payload::UnstructuredCommandLine(u) => {
            map.insert(
                "unstructuredCommandLine".to_string(),
                serde_json::json!({
                    "args": u.args,
                }),
            );
        }
        Payload::OptionsParsed(o) => {
            map.insert(
                "optionsParsed".to_string(),
                serde_json::json!({
                    "startupOptions": o.startup_options,
                    "cmdLine": o.cmd_line,
                    "explicitCmdLine": o.explicit_cmd_line,
                }),
            );
        }
        Payload::WorkspaceStatus(ws) => {
            let items: Vec<serde_json::Value> = ws
                .item
                .iter()
                .map(|i| serde_json::json!({"key": i.key, "value": i.value}))
                .collect();
            map.insert(
                "workspaceStatus".to_string(),
                serde_json::json!({"item": items}),
            );
        }
        Payload::Configuration(c) => {
            map.insert(
                "configuration".to_string(),
                serde_json::json!({
                    "mnemonic": c.mnemonic,
                    "platformName": c.platform_name,
                }),
            );
        }
        Payload::Expanded(_) => {
            map.insert("expanded".to_string(), serde_json::json!({}));
        }
        Payload::Configured(c) => {
            map.insert(
                "configured".to_string(),
                serde_json::json!({
                    "targetKind": c.target_kind,
                }),
            );
        }
        Payload::Completed(c) => {
            map.insert(
                "completed".to_string(),
                serde_json::json!({
                    "success": c.success,
                }),
            );
        }
        Payload::Action(a) => {
            map.insert(
                "action".to_string(),
                serde_json::json!({
                    "success": a.success,
                    "type": a.r#type,
                    "exitCode": a.exit_code,
                }),
            );
        }
        Payload::NamedSetOfFiles(ns) => {
            let files: Vec<serde_json::Value> = ns
                .files
                .iter()
                .map(|f| {
                    // Extract URI from the file oneof
                    let uri = f.file.as_ref().and_then(|content| match content {
                        FileContent::Uri(u) => Some(u.as_str()),
                        _ => None,
                    });
                    serde_json::json!({"name": f.name, "uri": uri})
                })
                .collect();
            map.insert(
                "namedSetOfFiles".to_string(),
                serde_json::json!({"files": files}),
            );
        }
        Payload::Finished(f) => {
            let exit_code = f
                .exit_code
                .as_ref()
                .map(|ec| serde_json::json!({"code": ec.code}));
            map.insert(
                "finished".to_string(),
                serde_json::json!({
                    "exitCode": exit_code,
                    "finishTimeMillis": f.finish_time_millis,
                }),
            );
        }
        Payload::Progress(p) => {
            map.insert(
                "progress".to_string(),
                serde_json::json!({
                    "stdout": p.stdout,
                    "stderr": p.stderr,
                }),
            );
        }
        Payload::BuildMetrics(m) => {
            map.insert(
                "buildMetrics".to_string(),
                serde_json::json!(m.action_summary.as_ref().map(|a| {
                    serde_json::json!({
                        "actionsCreated": a.actions_created,
                        "actionsExecuted": a.actions_executed,
                    })
                })),
            );
        }
        _ => {
            // Other payload types we don't process yet
            trace!("Unhandled payload type in proto-to-JSON conversion");
        }
    }
}

/// Run the BES gRPC server
pub async fn run_server(addr: SocketAddr) -> anyhow::Result<()> {
    let server = BesServer::new();

    info!("Starting BES gRPC server on {}", addr);
    info!(
        "Connect Bazel with: --bes_backend=grpc://localhost:{}",
        addr.port()
    );

    tonic::transport::Server::builder()
        .add_service(PublishBuildEventServer::new(server))
        .serve(addr)
        .await?;

    Ok(())
}
