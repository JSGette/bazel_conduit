//! BES gRPC server implementation
//!
//! Implements the Google Build Event Service (BES) gRPC API.
//! Bazel connects to this server via --bes_backend=grpc://localhost:<port>

use crate::bep::EventRouter;
use crate::bes_events::build_event::Event as BesEvent;
use crate::bes_proto::publish_build_event_server::{PublishBuildEvent, PublishBuildEventServer};
use crate::bes_proto::{
    PublishBuildToolEventStreamRequest, PublishBuildToolEventStreamResponse,
    PublishLifecycleEventRequest,
};
use crate::proto_types::Empty;
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
    pub fn new(router: EventRouter) -> Self {
        Self {
            router: Arc::new(Mutex::new(router)),
        }
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
    /// Receive loop is decoupled from routing: incoming events are decoded and
    /// ACKed inline, then forwarded to a single worker task that calls into the
    /// OTel mapper serially (preserving order). This keeps Bazel's BES upload
    /// timer bounded by network + decode latency rather than span emission cost.
    async fn publish_build_tool_event_stream(
        &self,
        request: Request<Streaming<PublishBuildToolEventStreamRequest>>,
    ) -> Result<Response<Self::PublishBuildToolEventStreamStream>, Status> {
        let mut stream = request.into_inner();
        let (ack_tx, ack_rx) = mpsc::channel(128);
        let (route_tx, mut route_rx) =
            mpsc::channel::<(crate::build_event_stream::BuildEvent, Option<i64>)>(
                ROUTE_CHANNEL_CAPACITY,
            );
        let router = self.router.clone();

        info!("Build tool event stream started");

        let router_for_worker = router.clone();
        tokio::spawn(async move {
            let mut routed: u64 = 0;
            while let Some((build_event, event_time_nanos)) = route_rx.recv().await {
                let mut router = router_for_worker.lock().await;
                if let Err(e) = router.route_build_event(&build_event, event_time_nanos) {
                    warn!(error = %e, "Failed to route BEP event");
                }
                routed += 1;
            }
            let mut router = router_for_worker.lock().await;
            router.finish();
            let summary = router.state().summary();
            info!("\n{}", summary);
            info!("Routing worker drained ({routed} events routed)");
        });

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
                        let stream_id = req
                            .ordered_build_event
                            .as_ref()
                            .and_then(|e| e.stream_id.clone());

                        trace!(
                            sequence = sequence_number,
                            event_count,
                            "Received build tool event"
                        );

                        let response = PublishBuildToolEventStreamResponse {
                            stream_id,
                            sequence_number,
                        };
                        if ack_tx.send(Ok(response)).await.is_err() {
                            debug!("ACK channel closed");
                            break;
                        }

                        if let Some(decoded) =
                            crate::grpc::server::decode_bep_event_from_request(&req)
                        {
                            if route_tx.send(decoded).await.is_err() {
                                warn!("Routing worker channel closed");
                                break;
                            }
                        }
                    }
                    Ok(None) => {
                        info!("gRPC stream completed ({event_count} events received)");
                        // Dropping route_tx signals the worker to drain and finalize.
                        drop(route_tx);
                        break;
                    }
                    Err(e) => {
                        error!(error = %e, "Stream error");
                        let _ = ack_tx
                            .send(Err(Status::internal(format!("Stream error: {e}"))))
                            .await;
                        drop(route_tx);
                        break;
                    }
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(ack_rx)))
    }
}

/// Bound on the receive→worker hand-off; sized to match the OTel batch queue
/// so even a fully-cached giant build never backpressures the BES ACK path.
const ROUTE_CHANNEL_CAPACITY: usize = 65536;

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

/// Run the BES gRPC server
pub async fn run_server(addr: SocketAddr, router: EventRouter) -> anyhow::Result<()> {
    let server = BesServer::new(router);

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
