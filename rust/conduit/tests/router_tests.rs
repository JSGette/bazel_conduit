// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

//! Smoke tests for EventRouter proto routing.

use conduit_lib::bep::EventRouter;
use conduit_lib::build_event_stream::build_event::Payload;
use conduit_lib::build_event_stream::build_event_id::{
    BuildFinishedId, BuildStartedId, Id, OptionsParsedId, UnknownBuildEventId,
};
use conduit_lib::build_event_stream::{
    BuildEvent, BuildEventId, BuildFinished, BuildStarted, OptionsParsed,
};

fn event(id: Id, payload: Option<Payload>) -> BuildEvent {
    BuildEvent {
        id: Some(BuildEventId { id: Some(id) }),
        payload,
        ..Default::default()
    }
}

fn started(uuid: &str, command: &str, workspace: &str) -> BuildEvent {
    let started = BuildStarted {
        uuid: uuid.into(),
        command: command.into(),
        workspace_directory: workspace.into(),
        ..Default::default()
    };
    event(Id::Started(BuildStartedId {}), Some(Payload::Started(started)))
}

fn options(explicit_cmd_line: &[&str]) -> BuildEvent {
    let options = OptionsParsed {
        explicit_cmd_line: explicit_cmd_line.iter().map(|s| s.to_string()).collect(),
        ..Default::default()
    };
    event(Id::OptionsParsed(OptionsParsedId {}), Some(Payload::OptionsParsed(options)))
}

#[test]
fn router_started_and_finished_no_panic() {
    let mut router = EventRouter::new();
    router
        .route_build_event(&started("11111111-1111-1111-1111-111111111111", "build", "/tmp/ws"), None)
        .unwrap();
    let finished = BuildFinished {
        exit_code: Some(Default::default()),
        ..Default::default()
    };
    router
        .route_build_event(&event(Id::BuildFinished(BuildFinishedId {}), Some(Payload::Finished(finished))), None)
        .unwrap();
    router.finish();

    let summary = router.state().summary();
    assert_eq!(summary.exit_code, Some(0));
    assert_eq!(summary.command.as_deref(), Some("build"));
}

#[test]
fn router_handles_unknown_event_gracefully() {
    let mut router = EventRouter::new();
    let unknown = event(Id::Unknown(UnknownBuildEventId::default()), None);
    assert!(router.route_build_event(&unknown, None).is_ok());
}

#[test]
fn router_reset_clears_state() {
    let mut router = EventRouter::new();
    router
        .route_build_event(&started("22222222-2222-2222-2222-222222222222", "test", ""), None)
        .unwrap();
    assert_eq!(router.state().command(), Some("test"));
    router
        .route_build_event(&started("33333333-3333-3333-3333-333333333333", "build", ""), None)
        .unwrap();
    assert_eq!(router.state().command(), Some("build"));
}

#[test]
fn exec_log_is_ignored_without_opt_in() {
    let mut router = EventRouter::new();
    router
        .route_build_event(&options(&["--execution_log_compact_file=/tmp/exec.log"]), None)
        .unwrap();
    assert!(router.state().exec_log_path().is_none());
}

#[test]
fn exec_log_path_is_captured_with_opt_in() {
    let mut router = EventRouter::new().with_exec_log(true);
    router
        .route_build_event(&started("44444444-4444-4444-4444-444444444444", "build", "/tmp/ws"), None)
        .unwrap();
    router
        .route_build_event(&options(&["--execution_log_compact_file=exec.log"]), None)
        .unwrap();
    assert_eq!(
        router.state().exec_log_path().map(|p| p.to_string_lossy().to_string()),
        Some("exec.log".to_string())
    );
}
