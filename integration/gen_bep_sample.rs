// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

//! Writes a minimal binary BEP stream (the `--build_event_binary_file`
//! format) for `//integration:trace_test`: one target with one action.

use build_event_stream_proto::build_event_stream::build_event::Payload;
use build_event_stream_proto::build_event_stream::build_event_id::{
    ActionCompletedId, BuildFinishedId, BuildStartedId, Id, TargetCompletedId, TargetConfiguredId,
};
use build_event_stream_proto::build_event_stream::build_finished::ExitCode;
use build_event_stream_proto::build_event_stream::{
    ActionExecuted, BuildEvent, BuildEventId, BuildFinished, BuildStarted, TargetComplete,
    TargetConfigured,
};
use build_event_stream_proto::timestamp_proto::google::protobuf::Timestamp;
use prost::Message;

const START_SECONDS: i64 = 1_767_225_600; // 2026-01-01T00:00:00Z
const LABEL: &str = "//pkg:lib";

fn at(offset_ms: i64) -> Option<Timestamp> {
    Some(Timestamp {
        seconds: START_SECONDS + offset_ms / 1000,
        nanos: ((offset_ms % 1000) * 1_000_000) as i32,
    })
}

fn event(id: Id, payload: Payload) -> BuildEvent {
    BuildEvent {
        id: Some(BuildEventId { id: Some(id) }),
        payload: Some(payload),
        ..Default::default()
    }
}

fn main() {
    let out = std::env::args().nth(1).expect("usage: gen_bep_sample <out>");
    let mut finished = event(
        Id::BuildFinished(BuildFinishedId {}),
        Payload::Finished(BuildFinished {
            exit_code: Some(ExitCode { name: "SUCCESS".into(), code: 0 }),
            finish_time: at(5_000),
            ..Default::default()
        }),
    );
    finished.last_message = true;

    let events = [
        event(
            Id::Started(BuildStartedId {}),
            Payload::Started(BuildStarted {
                uuid: "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee".into(),
                command: "build".into(),
                start_time: at(0),
                build_tool_version: "9.3.1".into(),
                workspace_directory: "/tmp/ws".into(),
                working_directory: "/tmp/ws".into(),
                ..Default::default()
            }),
        ),
        event(
            Id::TargetConfigured(TargetConfiguredId { label: LABEL.into(), ..Default::default() }),
            Payload::Configured(TargetConfigured {
                target_kind: "rust_library rule".into(),
                ..Default::default()
            }),
        ),
        event(
            Id::ActionCompleted(ActionCompletedId {
                primary_output: "bazel-out/bin/pkg/liblib.rlib".into(),
                label: LABEL.into(),
                ..Default::default()
            }),
            Payload::Action(ActionExecuted {
                success: true,
                r#type: "Rustc".into(),
                start_time: at(1_000),
                end_time: at(3_000),
                ..Default::default()
            }),
        ),
        event(
            Id::TargetCompleted(TargetCompletedId { label: LABEL.into(), ..Default::default() }),
            Payload::Completed(TargetComplete { success: true, ..Default::default() }),
        ),
        finished,
    ];

    let mut buf = Vec::new();
    for e in &events {
        e.encode_length_delimited(&mut buf).unwrap();
    }
    std::fs::write(out, buf).unwrap();
}
