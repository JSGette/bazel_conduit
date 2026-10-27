// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

//! BES gRPC server implementation
//!
//! Implements the Google Build Event Service (BES) gRPC API.
//! Bazel connects to this server via --bes_backend=grpc://127.0.0.1:<port>

use crate::bep::EventRouter;
use crate::bes_events::build_event::Event as BesEvent;
use crate::bes_proto::publish_build_event_server::{PublishBuildEvent, PublishBuildEventServer};
use crate::bes_proto::{
    PublishBuildToolEventStreamRequest, PublishBuildToolEventStreamResponse,
    PublishLifecycleEventRequest,
};
use crate::proto_types::Empty;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinSet;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};
use tracing::{debug, error, info, trace, warn};

/// (BES sequence number, BEP event, BES event time in nanos)
type RoutedEvent = (i64, crate::build_event_stream::BuildEvent, Option<i64>);

struct Session {
    tx: mpsc::WeakSender<RoutedEvent>,
    resumed: watch::Sender<u64>,
}

/// One routing worker (and [`EventRouter`]) per Bazel invocation, keyed by
/// the full BES `StreamId`. The map holds weak senders:
/// a worker finalizes once every stream feeding it is gone, so no separate
/// reaper.
struct Sessions {
    make_router: Box<dyn Fn() -> EventRouter + Send + Sync>,
    live: Mutex<HashMap<String, Session>>,
    workers: Mutex<JoinSet<()>>,
    stop: watch::Receiver<bool>,
}

impl Sessions {
    fn new(
        make_router: impl Fn() -> EventRouter + Send + Sync + 'static,
        stop: watch::Receiver<bool>,
    ) -> Self {
        Self {
            make_router: Box::new(make_router),
            live: Mutex::new(HashMap::new()),
            workers: Mutex::new(JoinSet::new()),
            stop,
        }
    }

    /// Wait (up to `timeout`) for every routing worker to finalize its trace.
    async fn drain(&self, timeout: Duration) -> Result<usize, tokio::time::error::Elapsed> {
        let mut workers = std::mem::take(&mut *self.workers.lock().unwrap());
        while workers.try_join_next().is_some() {}
        let count = workers.len();
        tokio::time::timeout(timeout, async {
            while workers.join_next().await.is_some() {}
        })
        .await
        .map(|()| count)
    }

    /// Sender into the invocation's worker; a retried stream for a live
    /// invocation resumes it instead of starting a partial trace. The watch
    /// receiver fires when a later stream resumes the same invocation. An
    /// empty key gets a private worker that nothing can resume. `None` once
    /// shutdown has begun.
    fn attach(&self, key: &str) -> Option<(mpsc::Sender<RoutedEvent>, watch::Receiver<u64>)> {
        let mut live = self.live.lock().unwrap();
        live.retain(|_, s| s.tx.strong_count() > 0);
        if let Some(s) = live.get(key) {
            if let Some(tx) = s.tx.upgrade() {
                info!(key, "Resuming BES stream for live invocation");
                s.resumed.send_modify(|n| *n += 1);
                return Some((tx, s.resumed.subscribe()));
            }
        }
        let mut workers = self.workers.lock().unwrap();
        // Checked under the workers lock so `drain` never misses a worker.
        if *self.stop.borrow() {
            return None;
        }
        while workers.try_join_next().is_some() {}
        let (tx, rx) = mpsc::channel(ROUTE_CHANNEL_CAPACITY);
        let worker = route_stream((self.make_router)(), rx, self.stop.clone());
        workers.spawn(async move {
            worker.await;
        });
        let (resumed, resumed_rx) = watch::channel(0);
        if !key.is_empty() {
            let session = Session { tx: tx.downgrade(), resumed };
            live.insert(key.to_string(), session);
        }
        Some((tx, resumed_rx))
    }
}

/// Session key: the full BES `StreamId`, or empty (unresumable) without one.
fn stream_key(ordered: &crate::bes_proto::OrderedBuildEvent) -> String {
    ordered
        .stream_id
        .as_ref()
        .filter(|s| !s.invocation_id.is_empty())
        .map(|s| format!("{}/{}/{}", s.build_id, s.invocation_id, s.component))
        .unwrap_or_default()
}

/// BES gRPC server.
pub struct BesServer {
    sessions: Arc<Sessions>,
}

/// Route one invocation's events in order, then finalize. `stop` ends the
/// invocation early (conduit shutdown). Returns the router so callers
/// (tests) can inspect the per-invocation state.
pub(crate) async fn route_stream(
    mut router: EventRouter,
    mut route_rx: mpsc::Receiver<RoutedEvent>,
    mut stop: watch::Receiver<bool>,
) -> EventRouter {
    let mut routed: u64 = 0;
    let mut last_sequence = None;
    let first_pump = tokio::time::Instant::now() + EXEC_LOG_PUMP_INTERVAL;
    let mut exec_log_tick = tokio::time::interval_at(first_pump, EXEC_LOG_PUMP_INTERVAL);
    exec_log_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            event = route_rx.recv() => {
                let Some(event) = event else { break };
                routed += u64::from(route_event(&mut router, &mut last_sequence, event));
            }
            _ = exec_log_tick.tick() => router.pump_exec_log(),
            Ok(_) = stop.wait_for(|stopped| *stopped) => break,
        }
    }
    // Events already ACKed to Bazel must still reach the trace.
    route_rx.close();
    while let Ok(event) = route_rx.try_recv() {
        routed += u64::from(route_event(&mut router, &mut last_sequence, event));
    }
    // Outside the select: the stream may close in the same instant as stop.
    if *stop.borrow() {
        router.shutdown();
    }
    router.finish();
    info!("\n{}", router.state().summary());
    info!("Routing worker drained ({routed} events routed)");
    router
}

/// Route one event unless it is a retransmission; returns whether it was routed.
fn route_event(
    router: &mut EventRouter,
    last_sequence: &mut Option<i64>,
    (sequence, build_event, event_time_nanos): RoutedEvent,
) -> bool {
    // A resumed stream retransmits events whose ACK was lost in flight.
    if last_sequence.is_some_and(|last| sequence <= last) {
        debug!(sequence, "Skipping retransmitted BES event");
        return false;
    }
    *last_sequence = Some(sequence);
    if let Err(e) = router.route_build_event(&build_event, event_time_nanos) {
        warn!(error = %e, "Failed to route BEP event");
    }
    true
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
    /// Receive loop is decoupled from routing: incoming events are decoded,
    /// queued to the invocation's worker task (which calls into the OTel
    /// mapper serially, preserving order), then ACKed. This keeps Bazel's BES
    /// upload timer bounded by network + decode latency rather than span
    /// emission cost.
    async fn publish_build_tool_event_stream(
        &self,
        request: Request<Streaming<PublishBuildToolEventStreamRequest>>,
    ) -> Result<Response<Self::PublishBuildToolEventStreamStream>, Status> {
        let mut stream = request.into_inner();
        let (ack_tx, ack_rx) = mpsc::channel(128);
        let sessions = self.sessions.clone();

        info!("Build tool event stream started");

        tokio::spawn(async move {
            let mut event_count: u64 = 0;
            let mut session = None;
            let completed = loop {
                match stream.message().await {
                    Ok(Some(req)) => {
                        event_count += 1;
                        let Some(ordered) = req.ordered_build_event.as_ref() else {
                            let status = Status::invalid_argument("missing ordered_build_event");
                            let _ = ack_tx.send(Err(status)).await;
                            break true;
                        };
                        trace!(
                            sequence = ordered.sequence_number,
                            event_count,
                            "Received build tool event"
                        );

                        if session.is_none() {
                            session = sessions.attach(&stream_key(ordered));
                        }
                        let Some((tx, _)) = &session else {
                            let status = Status::unavailable("conduit is shutting down");
                            let _ = ack_tx.send(Err(status)).await;
                            break true;
                        };

                        // Enqueue before ACK: an ACKed event must reach the trace.
                        if let Some((event, time)) = decode_bep_event_from_request(&req) {
                            if tx.send((ordered.sequence_number, event, time)).await.is_err() {
                                warn!("Routing worker channel closed");
                                let status = Status::unavailable("conduit is shutting down");
                                let _ = ack_tx.send(Err(status)).await;
                                break true;
                            }
                        }

                        let response = PublishBuildToolEventStreamResponse {
                            stream_id: ordered.stream_id.clone(),
                            sequence_number: ordered.sequence_number,
                        };
                        if ack_tx.send(Ok(response)).await.is_err() {
                            debug!("ACK channel closed");
                            break false;
                        }
                    }
                    Ok(None) => {
                        info!("gRPC stream completed ({event_count} events received)");
                        break true;
                    }
                    Err(e) => {
                        error!(error = %e, "Stream error");
                        let _ = ack_tx
                            .send(Err(Status::internal(format!("Stream error: {e}"))))
                            .await;
                        break false;
                    }
                }
            };
            // Bazel retries a broken stream under the same invocation_id;
            // hold the worker open until the retry attaches or grace expires.
            if let (false, Some((tx, mut resumed))) = (completed, session) {
                tokio::select! {
                    _ = tokio::time::sleep(STREAM_RESUME_GRACE) => {}
                    _ = resumed.changed() => {}
                }
                drop(tx);
            }
        });

        Ok(Response::new(ReceiverStream::new(ack_rx)))
    }
}

/// Bound on the receive→worker hand-off; sized to match the OTel batch queue
/// so even a fully-cached giant build never backpressures the BES ACK path.
const ROUTE_CHANNEL_CAPACITY: usize = 65536;

/// Compact-log decode runs off-thread; pump completed chunks into spans
/// independently of the BEP event cadence.
const EXEC_LOG_PUMP_INTERVAL: Duration = Duration::from_millis(100);

/// How long an invocation outlives a broken stream. Bazel retransmits only
/// un-ACKed events on retry, backing off 1s * 1.6^n over 4 attempts (~10s).
const STREAM_RESUME_GRACE: Duration = Duration::from_secs(60);

/// Budget for finalizing live invocations on SIGTERM. Leaves room inside a
/// typical 5s+ termination grace for the exporter flush that follows.
const SHUTDOWN_DRAIN_TIMEOUT: Duration = Duration::from_secs(3);

/// Per-message cap for inbound BES events. BEP `BuildMetrics`, action stderr,
/// or fat `OptionsParsed` blobs occasionally reach hundreds of KiB; 8 MiB
/// leaves an order of magnitude of headroom while preventing a single
/// hostile / malformed event from OOM'ing the process via tonic's decode path.
const MAX_INCOMING_MESSAGE_BYTES: usize = 8 * 1024 * 1024;

/// HTTP/2 hard cap on concurrent streams per TCP connection. Bazel uses a
/// single streaming RPC for the whole build plus the occasional lifecycle
/// unary RPC, so 64 covers any realistic workload while bounding fanout
/// from a misbehaving / malicious peer.
const MAX_CONCURRENT_STREAMS: u32 = 64;

/// Tower-level cap on concurrently executing RPCs per connection. Pairs
/// with [`MAX_CONCURRENT_STREAMS`] to also bound work performed by
/// long-running handlers.
const CONCURRENCY_LIMIT_PER_CONNECTION: usize = 32;

/// HTTP/2 keepalive interval. Combined with [`KEEPALIVE_TIMEOUT`], this
/// detects half-open / slow-loris clients without forcing a request-level
/// deadline on the long-lived BES stream (which can legitimately run for
/// hours on big builds).
const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(30);
const KEEPALIVE_TIMEOUT: Duration = Duration::from_secs(20);

/// Extract a BEP event from a PublishBuildToolEventStreamRequest.
///
/// The BES protocol wraps events as:
///   PublishBuildToolEventStreamRequest
///     -> ordered_build_event: OrderedBuildEvent
///       -> event: build_events.BuildEvent (BES-level)
///         -> event: BazelEvent(google.protobuf.Any)
///           -> value: serialized build_event_stream.BuildEvent (BEP)
/// Decode a BEP BuildEvent from the request (gRPC path). Returns the proto
/// and BES-level event time for direct routing without JSON round-trip.
pub(crate) fn decode_bep_event_from_request(
    req: &PublishBuildToolEventStreamRequest,
) -> Option<(crate::build_event_stream::BuildEvent, Option<i64>)> {
    let ordered = req.ordered_build_event.as_ref()?;
    let bes_event = ordered.event.as_ref()?;

    let event_time_nanos = bes_event
        .event_time
        .as_ref()
        .map(|ts| ts.seconds * 1_000_000_000 + ts.nanos as i64);

    let event = bes_event.event.as_ref()?;

    match event {
        BesEvent::BazelEvent(any) => {
            if !any.type_url.contains("BuildEvent")
                && !any.type_url.contains("build_event_stream")
            {
                debug!(type_url = %any.type_url, "Unexpected type_url");
                return None;
            }
            use prost::Message;
            match crate::build_event_stream::BuildEvent::decode(any.value.as_ref()) {
                Ok(build_event) => Some((build_event, event_time_nanos)),
                Err(e) => {
                    debug!(
                        type_url = %any.type_url,
                        error = %e,
                        "Could not decode BuildEvent"
                    );
                    None
                }
            }
        }
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

/// Duration to milliseconds (for JSON). Accepts proto Duration (seconds + nanos).
fn duration_to_ms(seconds: i64, nanos: i32) -> i64 {
    seconds * 1000 + i64::from(nanos) / 1_000_000
}

/// Serialize BuildMetrics proto to JSON (full extraction for OTEL).
pub(crate) fn build_metrics_to_json(
    m: &crate::build_event_stream::BuildMetrics,
) -> serde_json::Value {
    use serde_json::json;

    let action_summary = m.action_summary.as_ref().map(|a| {
        let action_data: Vec<serde_json::Value> = a
            .action_data
            .iter()
            .map(|ad| {
                json!({
                    "mnemonic": ad.mnemonic,
                    "actionsExecuted": ad.actions_executed,
                    "firstStartedMs": ad.first_started_ms,
                    "lastEndedMs": ad.last_ended_ms,
                    "systemTimeNanos": ad.system_time.as_ref().map(|d| d.seconds * 1_000_000_000 + i64::from(d.nanos)),
                    "userTimeNanos": ad.user_time.as_ref().map(|d| d.seconds * 1_000_000_000 + i64::from(d.nanos)),
                    "actionsCreated": ad.actions_created,
                })
            })
            .collect();
        let runner_count: Vec<serde_json::Value> = a
            .runner_count
            .iter()
            .map(|r| json!({"name": r.name, "count": r.count, "execKind": r.exec_kind}))
            .collect();
        let action_cache = a.action_cache_statistics.as_ref().map(|acs| {
            let miss_details: Vec<serde_json::Value> = acs
                .miss_details
                .iter()
                .map(|md| json!({"reason": md.reason, "count": md.count}))
                .collect();
            json!({
                "hits": acs.hits,
                "misses": acs.misses,
                "saveTimeInMs": acs.save_time_in_ms,
                "loadTimeInMs": acs.load_time_in_ms,
                "missDetails": miss_details,
            })
        });
        json!({
            "actionsCreated": a.actions_created,
            "actionsExecuted": a.actions_executed,
            "actionData": action_data,
            "runnerCount": runner_count,
            "actionCacheStatistics": action_cache,
        })
    });

    let timing = m.timing_metrics.as_ref().map(|t| {
        json!({
            "wallTimeInMs": t.wall_time_in_ms,
            "cpuTimeInMs": t.cpu_time_in_ms,
            "analysisPhaseTimeInMs": t.analysis_phase_time_in_ms,
            "executionPhaseTimeInMs": t.execution_phase_time_in_ms,
            "actionsExecutionStartInMs": t.actions_execution_start_in_ms,
            "criticalPathMs": t.critical_path_time.as_ref().map(|d| duration_to_ms(d.seconds, d.nanos)),
        })
    });

    let memory = m.memory_metrics.as_ref().map(|mm| {
        let garbage: Vec<serde_json::Value> = mm
            .garbage_metrics
            .iter()
            .map(|g| json!({"type": g.r#type, "garbageCollected": g.garbage_collected}))
            .collect();
        json!({
            "usedHeapSizePostBuild": mm.used_heap_size_post_build,
            "peakPostGcHeapSize": mm.peak_post_gc_heap_size,
            "garbageMetrics": garbage,
        })
    });

    let target = m.target_metrics.as_ref().map(|tm| {
        json!({
            "targetsConfigured": tm.targets_configured,
            "targetsConfiguredNotIncludingAspects": tm.targets_configured_not_including_aspects,
            "targetsLoaded": tm.targets_loaded,
        })
    });

    let packages = m.package_metrics.as_ref().map(|pm| {
        json!({
            "packagesLoaded": pm.packages_loaded,
        })
    });

    let artifacts = m.artifact_metrics.as_ref().map(|am| {
        let src = am.source_artifacts_read.as_ref().map(|f| json!({"count": f.count, "sizeInBytes": f.size_in_bytes}));
        let out = am.output_artifacts_seen.as_ref().map(|f| json!({"count": f.count, "sizeInBytes": f.size_in_bytes}));
        let cache = am.output_artifacts_from_action_cache.as_ref().map(|f| json!({"count": f.count, "sizeInBytes": f.size_in_bytes}));
        let top = am.top_level_artifacts.as_ref().map(|f| json!({"count": f.count, "sizeInBytes": f.size_in_bytes}));
        json!({
            "sourceArtifactsRead": src,
            "outputArtifactsSeen": out,
            "outputArtifactsFromActionCache": cache,
            "topLevelArtifacts": top,
        })
    });

    let network = m.network_metrics.as_ref().and_then(|nm| {
        nm.system_network_stats.as_ref().map(|sns| {
            json!({
                "bytesSent": sns.bytes_sent,
                "bytesRecv": sns.bytes_recv,
                "packetsSent": sns.packets_sent,
                "packetsRecv": sns.packets_recv,
            })
        })
    });

    let cumulative = m.cumulative_metrics.as_ref().map(|cm| {
        json!({
            "numAnalyses": cm.num_analyses,
            "numBuilds": cm.num_builds,
        })
    });

    let dynamic_exec = m.dynamic_execution_metrics.as_ref().map(|de| {
        let race_stats: Vec<serde_json::Value> = de
            .race_statistics
            .iter()
            .map(|rs| {
                json!({
                    "mnemonic": rs.mnemonic,
                    "localWins": rs.local_wins,
                    "remoteWins": rs.remote_wins,
                })
            })
            .collect();
        json!({ "raceStatistics": race_stats })
    });

    let workers: Vec<serde_json::Value> = m
        .worker_metrics
        .iter()
        .map(|wm| {
            let worker_ids: Vec<i64> = wm.worker_ids.iter().map(|&id| id as i64).collect();
            json!({
                "workerIds": worker_ids,
                "mnemonic": wm.mnemonic,
                "isMultiplex": wm.is_multiplex,
                "isSandbox": wm.is_sandbox,
                "isMeasurable": wm.is_measurable,
            })
        })
        .collect();

    let worker_pools: Vec<serde_json::Value> = m
        .worker_pool_metrics
        .iter()
        .flat_map(|wpm| {
            wpm.worker_pool_stats.iter().map(|wps| {
                json!({
                    "hash": wps.hash,
                    "mnemonic": wps.mnemonic,
                    "createdCount": wps.created_count,
                    "aliveCount": wps.alive_count,
                })
            })
        })
        .collect();

    let build_graph = m.build_graph_metrics.as_ref().map(|bg| {
        json!({
            "actionLookupValueCount": bg.action_lookup_value_count,
            "actionCount": bg.action_count,
            "inputFileConfiguredTargetCount": bg.input_file_configured_target_count,
            "outputFileConfiguredTargetCount": bg.output_file_configured_target_count,
            "otherConfiguredTargetCount": bg.other_configured_target_count,
            "outputArtifactCount": bg.output_artifact_count,
            "postInvocationSkyframeNodeCount": bg.post_invocation_skyframe_node_count,
        })
    });

    serde_json::json!({
        "actionSummary": action_summary,
        "timingMetrics": timing,
        "memoryMetrics": memory,
        "targetMetrics": target,
        "packageMetrics": packages,
        "artifactMetrics": artifacts,
        "networkMetrics": network,
        "cumulativeMetrics": cumulative,
        "dynamicExecutionMetrics": dynamic_exec,
        "workerMetrics": workers,
        "workerPoolMetrics": worker_pools,
        "buildGraphMetrics": build_graph,
    })
}

pub(crate) fn build_metadata_to_json(
    bm: &crate::build_event_stream::BuildMetadata,
) -> serde_json::Value {
    serde_json::json!({ "metadata": bm.metadata })
}


/// Extract URI from a BEP File oneof, if present.
pub(crate) fn bep_file_uri(f: &crate::build_event_stream::File) -> Option<String> {
    use crate::build_event_stream::file::File as FileContent;
    f.file.as_ref().and_then(|c| match c {
        FileContent::Uri(u) => Some(u.clone()),
        _ => None,
    })
}


/// Convert a BEP AbortReason enum to a human-readable string.
pub(crate) fn aborted_reason_to_str(
    reason: crate::build_event_stream::aborted::AbortReason,
) -> &'static str {
    use crate::build_event_stream::aborted::AbortReason;
    match reason {
        AbortReason::Unknown => "UNKNOWN",
        AbortReason::UserInterrupted => "USER_INTERRUPTED",
        AbortReason::NoAnalyze => "NO_ANALYZE",
        AbortReason::NoBuild => "NO_BUILD",
        AbortReason::TimeOut => "TIME_OUT",
        AbortReason::RemoteEnvironmentFailure => "REMOTE_ENVIRONMENT_FAILURE",
        AbortReason::Internal => "INTERNAL",
        AbortReason::LoadingFailure => "LOADING_FAILURE",
        AbortReason::AnalysisFailure => "ANALYSIS_FAILURE",
        AbortReason::Skipped => "SKIPPED",
        AbortReason::Incomplete => "INCOMPLETE",
        AbortReason::OutOfMemory => "OUT_OF_MEMORY",
    }
}

/// Convert a BEP TestStatus enum (i32) to a human-readable string.
pub(crate) fn test_status_to_str(status: i32) -> &'static str {
    match status {
        0 => "NO_STATUS",
        1 => "PASSED",
        2 => "FLAKY",
        3 => "TIMEOUT",
        4 => "FAILED",
        5 => "INCOMPLETE",
        6 => "REMOTE_FAILURE",
        7 => "FAILED_TO_BUILD",
        8 => "TOOL_HALTED_BEFORE_TESTING",
        _ => "UNKNOWN",
    }
}

/// Run the BES gRPC server until `shutdown` resolves, then finalize every
/// live invocation (root span marked as cut short) before returning.
pub async fn run_server(
    addr: SocketAddr,
    make_router: impl Fn() -> EventRouter + Send + Sync + 'static,
    shutdown: impl std::future::Future<Output = ()>,
) -> anyhow::Result<()> {
    let (stop_tx, stop_rx) = watch::channel(false);
    let sessions = Arc::new(Sessions::new(make_router, stop_rx));
    let server = BesServer {
        sessions: sessions.clone(),
    };

    info!("Starting BES gRPC server on {}", addr);
    if !addr.ip().is_unspecified() {
        info!("Connect Bazel with: --bes_backend=grpc://{}", addr);
    }

    let service = PublishBuildEventServer::new(server)
        .max_decoding_message_size(MAX_INCOMING_MESSAGE_BYTES);

    let serve = tonic::transport::Server::builder()
        .http2_keepalive_interval(Some(KEEPALIVE_INTERVAL))
        .http2_keepalive_timeout(Some(KEEPALIVE_TIMEOUT))
        .max_concurrent_streams(Some(MAX_CONCURRENT_STREAMS))
        .concurrency_limit_per_connection(CONCURRENCY_LIMIT_PER_CONNECTION)
        .add_service(service)
        .serve(addr);

    // Not serve_with_shutdown: that waits for open BES streams, which last as
    // long as the build.
    tokio::select! {
        result = serve => return result.map_err(Into::into),
        () = shutdown => {}
    }

    stop_tx.send_replace(true);
    match sessions.drain(SHUTDOWN_DRAIN_TIMEOUT).await {
        Ok(count) => info!("Shutdown: finalized {count} invocation(s)"),
        Err(_) => warn!(
            "Shutdown: invocations still finalizing after {SHUTDOWN_DRAIN_TIMEOUT:?}; their traces may be incomplete"
        ),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::build_event_stream::build_event::Payload;
    use crate::build_event_stream::build_event_id::{BuildStartedId, Id};
    use crate::build_event_stream::{BuildEvent, BuildEventId, BuildStarted};

    fn started(sequence: i64, uuid: &str, command: &str) -> RoutedEvent {
        let event = BuildEvent {
            id: Some(BuildEventId {
                id: Some(Id::Started(BuildStartedId {})),
            }),
            payload: Some(Payload::Started(BuildStarted {
                uuid: uuid.to_string(),
                command: command.to_string(),
                ..Default::default()
            })),
            ..Default::default()
        };
        (sequence, event, None)
    }

    fn no_stop() -> watch::Receiver<bool> {
        watch::channel(false).1
    }

    #[tokio::test]
    async fn concurrent_streams_do_not_share_state() {
        let (tx_a, rx_a) = mpsc::channel(8);
        let (tx_b, rx_b) = mpsc::channel(8);
        let a = tokio::spawn(route_stream(EventRouter::new(), rx_a, no_stop()));
        let b = tokio::spawn(route_stream(EventRouter::new(), rx_b, no_stop()));

        tx_a.send(started(1, "uuid-a", "build")).await.unwrap();
        tx_b.send(started(1, "uuid-b", "test")).await.unwrap();
        drop((tx_a, tx_b));

        let (a, b) = (a.await.unwrap(), b.await.unwrap());
        assert_eq!(a.state().invocation_id(), Some("uuid-a"));
        assert_eq!(a.state().command(), Some("build"));
        assert_eq!(b.state().invocation_id(), Some("uuid-b"));
        assert_eq!(b.state().command(), Some("test"));
    }

    #[tokio::test]
    async fn retransmitted_sequence_is_skipped() {
        let (tx, rx) = mpsc::channel(8);
        let worker = tokio::spawn(route_stream(EventRouter::new(), rx, no_stop()));
        tx.send(started(1, "uuid-a", "build")).await.unwrap();
        tx.send(started(1, "uuid-dup", "test")).await.unwrap();
        drop(tx);

        let router = worker.await.unwrap();
        assert_eq!(router.state().invocation_id(), Some("uuid-a"));
    }

    #[tokio::test]
    async fn retried_stream_resumes_live_invocation() {
        let sessions = Sessions::new(EventRouter::new, no_stop());
        let tx = |key| sessions.attach(key).unwrap().0;
        let (first, first_resumed) = sessions.attach("inv-1").unwrap();
        assert!(!first_resumed.has_changed().unwrap());
        let (resumed, resumed_rx) = sessions.attach("inv-1").unwrap();
        assert!(first.same_channel(&resumed));
        assert!(first_resumed.has_changed().unwrap(), "broken stream must see the resume");
        assert!(!resumed_rx.has_changed().unwrap());
        assert!(!first.same_channel(&tx("inv-2")));
        assert!(!tx("").same_channel(&tx("")));

        let weak = first.downgrade();
        drop((first, resumed));
        assert!(weak.upgrade().is_none(), "worker must finalize once all streams are gone");
        assert_eq!(tx("inv-1").strong_count(), 1);
    }

    #[tokio::test]
    async fn shutdown_routes_already_buffered_events() {
        let (stop_tx, stop_rx) = watch::channel(true);
        let (tx, rx) = mpsc::channel(8);
        tx.send(started(1, "uuid-a", "build")).await.unwrap();

        let router = route_stream(EventRouter::new(), rx, stop_rx).await;
        assert_eq!(router.state().invocation_id(), Some("uuid-a"));
        assert!(tx.is_closed());
        drop(stop_tx);
    }

    #[tokio::test]
    async fn shutdown_finalizes_attached_invocations() {
        let (stop_tx, stop_rx) = watch::channel(false);
        let sessions = Sessions::new(EventRouter::new, stop_rx);
        let (tx, _) = sessions.attach("inv-1").unwrap();
        tx.send(started(1, "uuid-a", "build")).await.unwrap();

        stop_tx.send_replace(true);
        assert!(sessions.attach("inv-2").is_none(), "no new invocations once stopping");
        let drained = sessions.drain(Duration::from_secs(1)).await;
        assert_eq!(drained.ok(), Some(1), "live stream must not block shutdown");
        assert!(tx.is_closed());
    }
}
