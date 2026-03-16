//! Smoke tests for EventRouter JSON routing path.

use conduit_lib::bep::{BepJsonEvent, EventRouter};

fn make_event(json: &str) -> BepJsonEvent {
    serde_json::from_str(json).expect("valid BEP JSON event")
}

#[test]
fn router_started_and_finished_no_panic() {
    let mut router = EventRouter::new();

    let started = make_event(r#"{
        "id": {"started": {}},
        "started": {
            "uuid": "11111111-1111-1111-1111-111111111111",
            "command": "build",
            "startTimeMillis": 1700000000000,
            "workspaceDirectory": "/tmp/ws"
        }
    }"#);
    router.route(&started).unwrap();

    let finished = make_event(r#"{
        "id": {"buildFinished": {}},
        "finished": {
            "exitCode": {"code": 0, "name": "SUCCESS"},
            "finishTimeMillis": 1700000001000
        }
    }"#);
    router.route(&finished).unwrap();

    router.finish();

    let summary = router.state().summary();
    assert_eq!(summary.exit_code, Some(0));
    assert_eq!(summary.command.as_deref(), Some("build"));
}

#[test]
fn router_handles_unknown_event_gracefully() {
    let mut router = EventRouter::new();
    let unknown = make_event(r#"{"id": {"someNewEventType": {}}}"#);
    let result = router.route(&unknown);
    assert!(result.is_ok());
}

#[test]
fn router_reset_clears_state() {
    let mut router = EventRouter::new();

    let started = make_event(r#"{
        "id": {"started": {}},
        "started": {
            "uuid": "22222222-2222-2222-2222-222222222222",
            "command": "test"
        }
    }"#);
    router.route(&started).unwrap();
    assert_eq!(router.state().command(), Some("test"));

    let started2 = make_event(r#"{
        "id": {"started": {}},
        "started": {
            "uuid": "33333333-3333-3333-3333-333333333333",
            "command": "build"
        }
    }"#);
    router.route(&started2).unwrap();
    assert_eq!(router.state().command(), Some("build"));
}
