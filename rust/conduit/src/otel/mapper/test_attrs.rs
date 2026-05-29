use std::borrow::Cow;

use opentelemetry::trace::Status;
use opentelemetry::KeyValue;

use super::super::attributes::*;
use super::{shorten_label, TestResultEvent};

pub(super) fn build_test_span_name(ev: &TestResultEvent<'_>) -> String {
    match (ev.attempt, ev.run, ev.shard) {
        (Some(a), Some(r), Some(s)) => format!("test {} attempt={a} run={r} shard={s}", ev.label),
        (Some(a), _, _) => format!("test {} attempt={a}", ev.label),
        _ => format!("test {}", ev.label),
    }
}

pub(super) fn test_status(ev: &TestResultEvent<'_>) -> Status {
    if ev.status == Some("PASSED") {
        Status::Ok
    } else {
        Status::Error {
            description: Cow::Owned(format!("test {}", ev.status.unwrap_or("UNKNOWN"))),
        }
    }
}

pub(super) fn build_test_attrs(ev: &TestResultEvent<'_>) -> Vec<KeyValue> {
    let mut attrs = vec![
        KeyValue::new(BAZEL_TARGET_LABEL, ev.label.to_string()),
        KeyValue::new(BAZEL_TARGET_LABEL_SHORT, shorten_label(ev.label).to_string()),
    ];
    if let Some(v) = ev.status {
        attrs.push(KeyValue::new(BAZEL_TEST_STATUS, v.to_string()));
    }
    if let Some(v) = ev.attempt {
        attrs.push(KeyValue::new(BAZEL_TEST_ATTEMPT, i64::from(v)));
    }
    if let Some(v) = ev.run {
        attrs.push(KeyValue::new(BAZEL_TEST_RUN, i64::from(v)));
    }
    if let Some(v) = ev.shard {
        attrs.push(KeyValue::new(BAZEL_TEST_SHARD, i64::from(v)));
    }
    if let Some(v) = ev.cached {
        attrs.push(KeyValue::new(BAZEL_TEST_CACHED, v));
    }
    if let Some(v) = ev.strategy {
        attrs.push(KeyValue::new(BAZEL_TEST_STRATEGY, v.to_string()));
    }
    if let Some(v) = ev.hostname.filter(|s| !s.is_empty()) {
        attrs.push(KeyValue::new(BAZEL_TEST_HOSTNAME, v.to_string()));
    }
    if let Some(v) = ev.cached_remotely {
        attrs.push(KeyValue::new(BAZEL_TEST_CACHED_REMOTELY, v));
    }

    attrs
}

