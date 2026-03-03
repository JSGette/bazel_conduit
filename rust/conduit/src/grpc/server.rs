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
                        // Stream ended — finalize OTel spans, then print summary.
                        let mut router = router.lock().await;
                        router.finish();
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

    // Extract the BES-level event_time (nanoseconds since epoch)
    // to preserve sub-millisecond precision for span timing.
    let event_time_nanos = bes_event
        .event_time
        .as_ref()
        .map(|ts| ts.seconds * 1_000_000_000 + ts.nanos as i64);

    let event = bes_event.event.as_ref()?;

    match event {
        BesEvent::BazelEvent(any) => match decode_any_to_bep_json(any) {
            Ok(mut json_event) => {
                json_event.event_time_nanos = event_time_nanos;
                Some(json_event)
            }
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

/// Duration to milliseconds (for JSON). Accepts proto Duration (seconds + nanos).
fn duration_to_ms(seconds: i64, nanos: i32) -> i64 {
    seconds * 1000 + i64::from(nanos) / 1_000_000
}

/// Timestamp to nanos since epoch (seconds + nanos).
fn timestamp_to_nanos(seconds: i64, nanos: i32) -> i64 {
    seconds * 1_000_000_000 + i64::from(nanos)
}

/// Serialize BuildMetrics proto to JSON (full extraction for OTEL).
fn build_metrics_to_json(m: &crate::build_event_stream::BuildMetrics) -> serde_json::Value {
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
            let mut obj = serde_json::json!({"label": t.label});
            if !t.aspect.is_empty() {
                obj["aspect"] = serde_json::json!(t.aspect);
            }
            serde_json::json!({"targetConfigured": obj})
        }
        Id::TargetCompleted(t) => {
            let mut obj = serde_json::json!({"label": t.label});
            if let Some(cfg) = &t.configuration {
                obj["configuration"] = serde_json::json!({"id": cfg.id});
            }
            if !t.aspect.is_empty() {
                obj["aspect"] = serde_json::json!(t.aspect);
            }
            serde_json::json!({"targetCompleted": obj})
        }
        Id::ActionCompleted(a) => {
            let mut obj = serde_json::json!({
                "label": a.label,
                "primaryOutput": a.primary_output,
            });
            if let Some(cfg) = &a.configuration {
                obj["configuration"] = serde_json::json!({"id": cfg.id});
            }
            serde_json::json!({"actionCompleted": obj})
        }
        Id::NamedSet(n) => {
            serde_json::json!({"namedSet": {"id": n.id}})
        }
        Id::TestResult(t) => {
            let cfg = t.configuration.as_ref().map(|c| &c.id);
            serde_json::json!({"testResult": {
                "label": t.label,
                "run": t.run,
                "shard": t.shard,
                "attempt": t.attempt,
                "configuration": {"id": cfg},
            }})
        }
        Id::TestSummary(t) => {
            let cfg = t.configuration.as_ref().map(|c| &c.id);
            serde_json::json!({"testSummary": {"label": t.label, "configuration": {"id": cfg}}})
        }
        Id::BuildFinished(_) => serde_json::json!({"buildFinished": {}}),
        Id::BuildMetrics(_) => serde_json::json!({"buildMetrics": {}}),
        Id::TargetSummary(t) => {
            let cfg = t.configuration.as_ref().map(|c| &c.id);
            serde_json::json!({"targetSummary": {"label": t.label, "configuration": {"id": cfg}}})
        }
        Id::Progress(p) => {
            serde_json::json!({"progress": {"opaqueCount": p.opaque_count}})
        }
        Id::Fetch(f) => {
            use crate::build_event_stream::build_event_id::fetch_id::Downloader;
            let dl = match f.downloader() {
                Downloader::Http => "HTTP",
                Downloader::Grpc => "GRPC",
                Downloader::Unknown => "UNKNOWN",
            };
            serde_json::json!({"fetch": {"url": f.url, "downloader": dl}})
        }
        Id::Workspace(_) => serde_json::json!({"workspaceInfo": {}}),
        Id::BuildToolLogs(_) => serde_json::json!({"buildToolLogs": {}}),
        Id::BuildMetadata(_) => serde_json::json!({"buildMetadata": {}}),
        Id::ConvenienceSymlinksIdentified(_) => {
            serde_json::json!({"convenienceSymlinksIdentified": {}})
        }
        Id::UnconfiguredLabel(t) => {
            serde_json::json!({"unconfiguredLabel": {"label": t.label}})
        }
        Id::ConfiguredLabel(t) => {
            let cfg = t.configuration.as_ref().map(|c| &c.id);
            serde_json::json!({"configuredLabel": {"label": t.label, "configuration": {"id": cfg}}})
        }
        Id::TestProgress(t) => {
            let cfg = t.configuration.as_ref().map(|c| &c.id);
            serde_json::json!({"testProgress": {
                "label": t.label,
                "configuration": {"id": cfg},
                "run": t.run,
                "shard": t.shard,
                "attempt": t.attempt,
                "opaqueCount": t.opaque_count,
            }})
        }
        Id::ExecRequest(_) => serde_json::json!({"execRequest": {}}),
        _ => serde_json::json!({}),
    }
}

/// Convert the payload oneof to JSON fields
#[allow(deprecated)] // start_time_millis and finish_time_millis are deprecated
fn add_payload_to_json(
    map: &mut serde_json::Map<String, serde_json::Value>,
    payload: &crate::build_event_stream::build_event::Payload,
) {
    use crate::build_event_stream;
    use crate::build_event_stream::build_event::Payload;
    use crate::build_event_stream::file::File as FileContent;

    match payload {
        Payload::Started(s) => {
            let start_time_nanos = s.start_time.as_ref()
                .map(|ts| ts.seconds * 1_000_000_000 + i64::from(ts.nanos));
            map.insert(
                "started".to_string(),
                serde_json::json!({
                    "uuid": s.uuid,
                    "command": s.command,
                    "startTimeMillis": s.start_time_millis,
                    "startTimeNanos": start_time_nanos,
                    "workspaceDirectory": s.workspace_directory,
                    "workingDirectory": s.working_directory,
                    "serverPid": s.server_pid,
                    "host": s.host,
                    "user": s.user,
                    "buildToolVersion": s.build_tool_version,
                    "optionsDescription": s.options_description,
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
                    "explicitStartupOptions": o.explicit_startup_options,
                    "cmdLine": o.cmd_line,
                    "explicitCmdLine": o.explicit_cmd_line,
                    "toolTag": o.tool_tag,
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
                    "cpu": c.cpu,
                    "isTool": c.is_tool,
                }),
            );
        }
        Payload::Expanded(_) => {
            map.insert("expanded".to_string(), serde_json::json!({}));
        }
        Payload::Configured(c) => {
            let test_size = match c.test_size {
                1 => "SMALL",
                2 => "MEDIUM",
                3 => "LARGE",
                4 => "ENORMOUS",
                _ => "",
            };
            map.insert(
                "configured".to_string(),
                serde_json::json!({
                    "targetKind": c.target_kind,
                    "tag": c.tag,
                    "testSize": test_size,
                }),
            );
        }
        Payload::Completed(c) => {
            let output_groups: Vec<serde_json::Value> = c
                .output_group
                .iter()
                .map(|og| {
                    let file_sets: Vec<serde_json::Value> = og
                        .file_sets
                        .iter()
                        .map(|fs| serde_json::json!({"id": fs.id}))
                        .collect();
                    serde_json::json!({
                        "name": og.name,
                        "fileSets": file_sets,
                    })
                })
                .collect();
            let failure_msg = c.failure_detail.as_ref().map(|fd| &fd.message);
            let test_timeout_ms = c.test_timeout.as_ref().map(|d| duration_to_ms(d.seconds, d.nanos));
            map.insert(
                "completed".to_string(),
                serde_json::json!({
                    "success": c.success,
                    "outputGroup": output_groups,
                    "tag": c.tag,
                    "failureDetail": failure_msg,
                    "testTimeoutMs": test_timeout_ms,
                }),
            );
        }
        Payload::Action(a) => {
            // Extract stdout/stderr/primary_output file URIs.
            let file_uri = |f: &crate::build_event_stream::File| -> Option<String> {
                f.file.as_ref().and_then(|c| match c {
                    FileContent::Uri(u) => Some(u.clone()),
                    _ => None,
                })
            };
            let stdout_uri = a.stdout.as_ref().and_then(file_uri);
            let stderr_uri = a.stderr.as_ref().and_then(file_uri);
            let primary_output_uri = a.primary_output.as_ref().and_then(file_uri);

            // Convert proto Timestamps to nanos-since-epoch.
            let start_time_nanos = a.start_time.as_ref().map(|ts| {
                ts.seconds * 1_000_000_000 + ts.nanos as i64
            });
            let end_time_nanos = a.end_time.as_ref().map(|ts| {
                ts.seconds * 1_000_000_000 + ts.nanos as i64
            });

            let failure_msg = a.failure_detail.as_ref().map(|fd| &fd.message);

            // Decode SpawnExec from strategy_details to get cache/runner/IO info.
            let spawn_info = decode_strategy_details(&a.strategy_details);

            map.insert(
                "action".to_string(),
                serde_json::json!({
                    "success": a.success,
                    "type": a.r#type,
                    "exitCode": a.exit_code,
                    "commandLine": a.command_line,
                    "stdout": stdout_uri,
                    "stderr": stderr_uri,
                    "primaryOutput": primary_output_uri,
                    "startTimeNanos": start_time_nanos,
                    "endTimeNanos": end_time_nanos,
                    "failureDetail": failure_msg,
                    "runner": spawn_info.runner,
                    "cacheHit": spawn_info.cache_hit,
                    "cacheable": spawn_info.cacheable,
                    "remotable": spawn_info.remotable,
                    "remoteCacheable": spawn_info.remote_cacheable,
                    "inputs": spawn_info.inputs,
                    "listedOutputs": spawn_info.listed_outputs,
                    "actualOutputs": spawn_info.actual_outputs,
                    "inputBytes": spawn_info.input_bytes,
                    "inputFiles": spawn_info.input_files,
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
            // Transitive NamedSet references (NamedSetOfFiles.file_sets).
            let child_set_ids: Vec<serde_json::Value> = ns
                .file_sets
                .iter()
                .map(|fs| serde_json::json!({"id": fs.id}))
                .collect();
            map.insert(
                "namedSetOfFiles".to_string(),
                serde_json::json!({"files": files, "fileSets": child_set_ids}),
            );
        }
        Payload::Finished(f) => {
            let exit_code = f
                .exit_code
                .as_ref()
                .map(|ec| serde_json::json!({"code": ec.code, "name": ec.name}));
            let finish_time_nanos = f.finish_time.as_ref()
                .map(|ts| ts.seconds * 1_000_000_000 + i64::from(ts.nanos));
            let failure_msg = f.failure_detail.as_ref().map(|fd| &fd.message);
            map.insert(
                "finished".to_string(),
                serde_json::json!({
                    "exitCode": exit_code,
                    "finishTimeMillis": f.finish_time_millis,
                    "finishTimeNanos": finish_time_nanos,
                    "failureDetail": failure_msg,
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
            let build_metrics_json = build_metrics_to_json(m);
            map.insert("buildMetrics".to_string(), build_metrics_json);
        }
        Payload::Fetch(f) => {
            map.insert(
                "fetch".to_string(),
                serde_json::json!({
                    "success": f.success,
                }),
            );
        }
        Payload::TestResult(tr) => {
            let test_attempt_start_nanos = tr
                .test_attempt_start
                .as_ref()
                .map(|ts| timestamp_to_nanos(ts.seconds, ts.nanos));
            let test_attempt_duration_nanos = tr.test_attempt_duration.as_ref().map(|d| {
                d.seconds * 1_000_000_000 + i64::from(d.nanos)
            });
            map.insert(
                "testResult".to_string(),
                serde_json::json!({
                    "status": test_status_to_str(tr.status),
                    "statusDetails": tr.status_details,
                    "cachedLocally": tr.cached_locally,
                    "testAttemptStartNanos": test_attempt_start_nanos,
                    "testAttemptDurationNanos": test_attempt_duration_nanos,
                    "warning": tr.warning,
                    "executionInfo": tr.execution_info.as_ref().map(|ei| {
                        serde_json::json!({
                            "strategy": ei.strategy,
                            "cachedRemotely": ei.cached_remotely,
                            "exitCode": ei.exit_code,
                            "hostname": ei.hostname,
                        })
                    }),
                }),
            );
        }
        Payload::TestSummary(ts) => {
            let first_start_nanos = ts.first_start_time.as_ref()
                .map(|t| timestamp_to_nanos(t.seconds, t.nanos));
            let last_stop_nanos = ts.last_stop_time.as_ref()
                .map(|t| timestamp_to_nanos(t.seconds, t.nanos));
            let total_run_duration_ms = ts.total_run_duration.as_ref()
                .map(|d| duration_to_ms(d.seconds, d.nanos));
            map.insert(
                "testSummary".to_string(),
                serde_json::json!({
                    "overallStatus": test_status_to_str(ts.overall_status),
                    "totalRunCount": ts.total_run_count,
                    "runCount": ts.run_count,
                    "attemptCount": ts.attempt_count,
                    "shardCount": ts.shard_count,
                    "totalNumCached": ts.total_num_cached,
                    "firstStartTimeNanos": first_start_nanos,
                    "lastStopTimeNanos": last_stop_nanos,
                    "totalRunDurationMs": total_run_duration_ms,
                }),
            );
        }
        Payload::TargetSummary(ts) => {
            map.insert(
                "targetSummary".to_string(),
                serde_json::json!({
                    "overallBuildSuccess": ts.overall_build_success,
                    "overallTestStatus": test_status_to_str(ts.overall_test_status),
                }),
            );
        }
        Payload::BuildMetadata(bm) => {
            map.insert(
                "buildMetadata".to_string(),
                serde_json::json!({
                    "metadata": bm.metadata,
                }),
            );
        }
        Payload::Aborted(a) => {
            let reason = match a.reason() {
                build_event_stream::aborted::AbortReason::Unknown => "UNKNOWN",
                build_event_stream::aborted::AbortReason::UserInterrupted => "USER_INTERRUPTED",
                build_event_stream::aborted::AbortReason::NoAnalyze => "NO_ANALYZE",
                build_event_stream::aborted::AbortReason::NoBuild => "NO_BUILD",
                build_event_stream::aborted::AbortReason::TimeOut => "TIME_OUT",
                build_event_stream::aborted::AbortReason::RemoteEnvironmentFailure => {
                    "REMOTE_ENVIRONMENT_FAILURE"
                }
                build_event_stream::aborted::AbortReason::Internal => "INTERNAL",
                build_event_stream::aborted::AbortReason::LoadingFailure => "LOADING_FAILURE",
                build_event_stream::aborted::AbortReason::AnalysisFailure => "ANALYSIS_FAILURE",
                build_event_stream::aborted::AbortReason::Skipped => "SKIPPED",
                build_event_stream::aborted::AbortReason::Incomplete => "INCOMPLETE",
                build_event_stream::aborted::AbortReason::OutOfMemory => "OUT_OF_MEMORY",
            };
            map.insert(
                "aborted".to_string(),
                serde_json::json!({
                    "reason": reason,
                    "description": a.description,
                }),
            );
        }
        Payload::StructuredCommandLine(_) => {
            // Redundant with optionsParsed; intentionally not converted.
        }
        Payload::TestProgress(tp) => {
            map.insert(
                "testProgress".to_string(),
                serde_json::json!({"uri": tp.uri}),
            );
        }
        Payload::WorkspaceInfo(wc) => {
            map.insert(
                "workspaceInfo".to_string(),
                serde_json::json!({"localExecRoot": wc.local_exec_root}),
            );
        }
        Payload::BuildToolLogs(btl) => {
            let logs: Vec<serde_json::Value> = btl
                .log
                .iter()
                .map(|f| {
                    let uri = f.file.as_ref().and_then(|c| match c {
                        FileContent::Uri(u) => Some(u.as_str()),
                        _ => None,
                    });
                    serde_json::json!({"name": f.name, "uri": uri})
                })
                .collect();
            map.insert(
                "buildToolLogs".to_string(),
                serde_json::json!({"log": logs}),
            );
        }
        Payload::ConvenienceSymlinksIdentified(cs) => {
            let symlinks: Vec<serde_json::Value> = cs
                .convenience_symlinks
                .iter()
                .map(|s| {
                    serde_json::json!({
                        "path": s.path,
                        "action": s.action,
                        "target": s.target,
                    })
                })
                .collect();
            map.insert(
                "convenienceSymlinksIdentified".to_string(),
                serde_json::json!({"convenienceSymlinks": symlinks}),
            );
        }
        Payload::ExecRequest(er) => {
            let argv: Vec<String> = er
                .argv
                .iter()
                .filter_map(|b| String::from_utf8(b.clone()).ok())
                .collect();
            let wd = String::from_utf8(er.working_directory.clone()).unwrap_or_default();
            map.insert(
                "execRequest".to_string(),
                serde_json::json!({
                    "workingDirectory": wd,
                    "argv": argv,
                    "shouldExec": er.should_exec,
                }),
            );
        }
    }
}

/// Extracted SpawnExec data from strategy_details.
#[derive(Default)]
struct SpawnInfo {
    runner: Option<String>,
    cache_hit: Option<bool>,
    cacheable: Option<bool>,
    remotable: Option<bool>,
    remote_cacheable: Option<bool>,
    inputs: Vec<serde_json::Value>,
    listed_outputs: Vec<String>,
    actual_outputs: Vec<serde_json::Value>,
    input_bytes: Option<i64>,
    input_files: Option<i64>,
}

const SPAWN_EXEC_TYPE_URL: &str = "type.googleapis.com/tools.protos.SpawnExec";

/// Try to decode the first SpawnExec from repeated google.protobuf.Any.
/// The `Any` type here comes from the build_event_stream proto (not prost_types).
fn decode_strategy_details(
    details: &[build_event_stream_proto::any_proto::google::protobuf::Any],
) -> SpawnInfo {
    for any in details {
        if any.type_url != SPAWN_EXEC_TYPE_URL {
            continue;
        }
        match prost::Message::decode(any.value.as_ref()) {
            Ok(spawn) => {
                let spawn: crate::spawn_proto::SpawnExec = spawn;
                let inputs: Vec<serde_json::Value> = spawn
                    .inputs
                    .iter()
                    .map(|f| serde_json::json!({"path": f.path, "isTool": f.is_tool}))
                    .collect();
                let actual_outputs: Vec<serde_json::Value> = spawn
                    .actual_outputs
                    .iter()
                    .map(|f| serde_json::json!({"path": f.path}))
                    .collect();
                let (input_bytes, input_files) = spawn.metrics.as_ref().map_or(
                    (None, None),
                    |m| (Some(m.input_bytes), Some(m.input_files)),
                );
                return SpawnInfo {
                    runner: Some(spawn.runner.clone()),
                    cache_hit: Some(spawn.cache_hit),
                    cacheable: Some(spawn.cacheable),
                    remotable: Some(spawn.remotable),
                    remote_cacheable: Some(spawn.remote_cacheable),
                    inputs,
                    listed_outputs: spawn.listed_outputs.clone(),
                    actual_outputs,
                    input_bytes,
                    input_files,
                };
            }
            Err(e) => {
                warn!(?e, "Failed to decode SpawnExec from strategy_details");
            }
        }
    }
    SpawnInfo::default()
}

/// Convert a BEP TestStatus enum (i32) to a human-readable string.
fn test_status_to_str(status: i32) -> &'static str {
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
