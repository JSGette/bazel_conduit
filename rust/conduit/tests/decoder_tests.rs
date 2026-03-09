use conduit_lib::bep::{BepDecoder, BepJsonEvent};

#[test]
fn test_bep_json_event_type() {
    // Real format from Bazel's --build_event_json_file
    let json = r#"{"id":{"started":{}},"started":{"uuid":"test-uuid","command":"build"}}"#;

    let event: BepJsonEvent = serde_json::from_str(json).unwrap();
    assert_eq!(event.event_type(), Some("started"));
}

#[test]
fn test_last_message_detection() {
    let json =
        r#"{"id":{"buildFinished":{}},"lastMessage":true,"finished":{"exitCode":{"code":0}}}"#;

    let event: BepJsonEvent = serde_json::from_str(json).unwrap();
    assert!(event.is_last_message());
}

#[test]
fn test_ndjson_parsing() {
    let ndjson = r#"{"id":{"started":{}},"started":{"uuid":"abc"}}
{"id":{"buildFinished":{}},"lastMessage":true}"#;

    let decoder = BepDecoder::new();
    let events: Vec<_> = decoder.decode_ndjson(ndjson.as_bytes()).collect();

    assert_eq!(events.len(), 2);
    assert!(events[0].is_ok());
    assert!(events[1].is_ok());

    let first = events[0].as_ref().unwrap();
    assert_eq!(first.event_type(), Some("started"));

    let last = events[1].as_ref().unwrap();
    assert!(last.is_last_message());
}

#[test]
fn test_empty_lines_skipped() {
    let ndjson = r#"{"id":{"started":{}},"started":{"uuid":"abc"}}

{"id":{"buildFinished":{}},"lastMessage":true}
"#;

    let decoder = BepDecoder::new();
    let events: Vec<_> = decoder.decode_ndjson(ndjson.as_bytes()).collect();

    assert_eq!(events.len(), 2);
}

#[test]
fn test_get_payload() {
    let json = r#"{"id":{"started":{}},"started":{"uuid":"abc","command":"build"}}"#;
    let event: BepJsonEvent = serde_json::from_str(json).unwrap();

    let started = event.get_payload("started").unwrap();
    assert_eq!(started.get("uuid").unwrap().as_str().unwrap(), "abc");
    assert_eq!(started.get("command").unwrap().as_str().unwrap(), "build");
}

#[test]
fn test_children_parsing() {
    let json = r#"{"id":{"started":{}},"children":[{"progress":{"opaqueCount":0}},{"pattern":{"pattern":["//..."]}}],"started":{"uuid":"abc"}}"#;
    let event: BepJsonEvent = serde_json::from_str(json).unwrap();

    assert_eq!(event.children.len(), 2);
}

#[test]
fn test_decode_json_file() {
    let ndjson = r#"{"id":{"started":{}},"started":{"uuid":"abc"}}
{"id":{"optionsParsed":{}},"optionsParsed":{"cmdLine":["--foo"]}}
{"id":{"buildFinished":{}},"lastMessage":true,"finished":{"exitCode":{"code":0}}}"#;

    let decoder = BepDecoder::new();
    let events = decoder.decode_json_file(ndjson.as_bytes()).unwrap();

    assert_eq!(events.len(), 3);
    assert_eq!(events[0].event_type(), Some("started"));
    assert_eq!(events[1].event_type(), Some("optionsParsed"));
    assert_eq!(events[2].event_type(), Some("buildFinished"));
    assert!(events[2].is_last_message());
}
