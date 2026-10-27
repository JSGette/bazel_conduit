// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

//! Replay of Bazel's `--build_event_binary_file`: varint length-delimited
//! `BuildEvent` protos.

use super::EventRouter;
use crate::build_event_stream::BuildEvent;
use crate::exec_log::read_message_len;
use prost::Message;
use std::io::{BufReader, Read};
use tracing::warn;

/// Per-event cap; a corrupt length prefix must not drive the allocation.
const MAX_EVENT_BYTES: usize = 64 * 1024 * 1024;

/// Route every event from `reader` until EOF or `last_message`. Returns the
/// number of events read. The file carries no BES event times, so spans
/// timed only by those (targets) are stamped at replay wall-clock.
pub fn replay<R: Read>(reader: R, router: &mut EventRouter) -> anyhow::Result<usize> {
    let mut reader = BufReader::new(reader);
    let mut buf = Vec::new();
    let mut count = 0;
    while let Some(len) = read_message_len(&mut reader, MAX_EVENT_BYTES)? {
        buf.resize(len, 0);
        reader.read_exact(&mut buf)?;
        let event = BuildEvent::decode(buf.as_slice())?;
        count += 1;
        if let Err(e) = router.route_build_event(&event, None) {
            warn!(error = %e, "Failed to route BEP event");
        }
        if event.last_message {
            break;
        }
    }
    Ok(count)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::build_event_stream::build_event::Payload;
    use crate::build_event_stream::build_event_id::{BuildFinishedId, BuildStartedId, Id};
    use crate::build_event_stream::{BuildEventId, BuildFinished, BuildStarted};

    fn encode(events: &[BuildEvent]) -> Vec<u8> {
        let mut out = Vec::new();
        for e in events {
            e.encode_length_delimited(&mut out).unwrap();
        }
        out
    }

    fn event(id: Id, payload: Payload, last_message: bool) -> BuildEvent {
        BuildEvent {
            id: Some(BuildEventId { id: Some(id) }),
            payload: Some(payload),
            last_message,
            ..Default::default()
        }
    }

    fn started() -> BuildEvent {
        let started = BuildStarted {
            uuid: "11111111-1111-1111-1111-111111111111".into(),
            command: "build".into(),
            ..Default::default()
        };
        event(Id::Started(BuildStartedId {}), Payload::Started(started), false)
    }

    fn finished() -> BuildEvent {
        let finished = BuildFinished {
            exit_code: Some(Default::default()),
            ..Default::default()
        };
        event(Id::BuildFinished(BuildFinishedId {}), Payload::Finished(finished), true)
    }

    #[test]
    fn replays_until_last_message() {
        let bytes = encode(&[started(), finished(), started()]);
        let mut router = EventRouter::new();
        assert_eq!(replay(bytes.as_slice(), &mut router).unwrap(), 2);
        assert_eq!(router.summary().command.as_deref(), Some("build"));
        assert_eq!(router.summary().exit_code, Some(0));
    }

    #[test]
    fn truncated_event_is_an_error() {
        let mut bytes = encode(&[started()]);
        bytes.truncate(bytes.len() - 1);
        assert!(replay(bytes.as_slice(), &mut EventRouter::new()).is_err());
    }
}
