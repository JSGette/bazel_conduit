// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

use std::borrow::Cow;
use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use spawn_proto::tools::protos::SpawnExec;

/// Strip ANSI escape sequences (colors, cursor movement, etc.) from a string.
///
/// Handles CSI sequences (`ESC[...X`), OSC sequences (`ESC]...BEL/ST`), and
/// bare ESC + single-char sequences.
pub(super) fn strip_ansi(input: &str) -> Cow<'_, str> {
    if !input.contains('\x1b') {
        return Cow::Borrowed(input);
    }
    let mut out = String::with_capacity(input.len());
    let mut chars = input.chars().peekable();

    while let Some(ch) = chars.next() {
        if ch == '\x1b' {
            // ESC - consume the sequence.
            match chars.peek() {
                Some('[') => {
                    // CSI sequence: ESC [ <params> <final byte 0x40-0x7E>
                    chars.next(); // consume '['
                    while let Some(&c) = chars.peek() {
                        if c.is_ascii() && (0x40..=0x7E).contains(&(c as u8)) {
                            chars.next(); // consume final byte
                            break;
                        }
                        chars.next(); // consume parameter/intermediate byte
                    }
                }
                Some(']') => {
                    // OSC sequence: ESC ] ... (terminated by BEL or ST)
                    chars.next(); // consume ']'
                    while let Some(&c) = chars.peek() {
                        chars.next();
                        if c == '\x07' {
                            break; // BEL
                        }
                        if c == '\x1b' {
                            // ST = ESC backslash
                            if chars.peek() == Some(&'\\') {
                                chars.next();
                            }
                            break;
                        }
                    }
                }
                Some(_) => {
                    // Two-character escape (e.g. ESC M, ESC 7)
                    chars.next();
                }
                None => {} // stray ESC at end
            }
        } else {
            out.push(ch);
        }
    }

    Cow::Owned(out)
}

/// Extract (start_nanos, end_nanos) from a `SpawnExec` via its
/// `SpawnMetrics.start_time` + `total_time`. Returns `None` when timing
/// data is absent.
pub(super) fn spawn_time_range(s: &SpawnExec) -> Option<(i64, i64)> {
    let m = s.metrics.as_ref()?;
    let start = m.start_time.as_ref()?;
    let total = m.total_time.as_ref()?;
    let start_nanos = start.seconds * 1_000_000_000 + i64::from(start.nanos);
    let total_nanos = total.seconds * 1_000_000_000 + i64::from(total.nanos);
    Some((start_nanos, start_nanos + total_nanos))
}

/// Clamp a (start, end) time range to fit within (bound_start, bound_end).
/// Ensures the result satisfies start <= end when both are present.
///
/// When the entire range predates `bound_start`, preserve its duration while
/// shifting it inside the parent window. Cached actions commonly replay old
/// execution timestamps; zero-width spans at the parent boundary break
/// flamegraph nesting in Datadog.
pub fn clamp_time_range(
    start: Option<i64>,
    end: Option<i64>,
    bound_start: Option<i64>,
    bound_end: Option<i64>,
) -> (Option<i64>, Option<i64>) {
    if let (Some(s), Some(e), Some(bs)) = (start, end, bound_start) {
        if s < bs && e <= bs {
            let shifted_end = bs.saturating_add(e.saturating_sub(s).max(0));
            return (
                Some(bs),
                Some(bound_end.map_or(shifted_end, |be| shifted_end.min(be)).max(bs)),
            );
        }
    }
    let clamped_start = match (start, bound_start) {
        (Some(s), Some(bs)) => Some(s.max(bs)),
        (s, _) => s,
    };
    let clamped_end = match (end, bound_end) {
        (Some(e), Some(be)) => Some(e.min(be)),
        (e, _) => e,
    };
    match (clamped_start, clamped_end) {
        (Some(s), Some(e)) if s > e => (Some(e), Some(e)),
        _ => (clamped_start, clamped_end),
    }
}

pub(super) const COMMAND_LINE_CAP_BYTES: usize = 4096; // 4 KB


/// Truncate `s` to at most `max_bytes` bytes, stepping back to the nearest
/// UTF-8 boundary and appending `suffix`. Returns the original string when
/// it already fits.
pub fn truncate_to_byte_limit(s: &str, max_bytes: usize, suffix: &str) -> String {
    if s.len() <= max_bytes {
        return s.to_string();
    }
    let mut cut = max_bytes;
    while cut > 0 && !s.is_char_boundary(cut) {
        cut -= 1;
    }
    format!("{}{}", &s[..cut], suffix)
}

/// Resolve a human-readable name for the Bazel workspace producing this
/// trace. Used to populate `VCS_REPOSITORY_NAME` on the root span.
///
/// `module(name = "...")` from `MODULE.bazel`, else the basename of
/// `workspace_dir` (e.g. `/repos/bazel_conduit` -> `bazel_conduit`).
///
/// The file is read once per build at `BuildStarted`. It only exists when
/// conduit shares a filesystem with Bazel; otherwise the basename is used.
pub(super) fn detect_workspace_name(workspace_dir: &Path) -> Option<String> {
    if let Some(name) = read_starlark_name(&workspace_dir.join("MODULE.bazel"), "module") {
        return Some(name);
    }
    let basename = workspace_dir.file_name()?.to_str()?;
    if basename.is_empty() {
        None
    } else {
        Some(basename.to_string())
    }
}

/// Drop `user:token@` from a `scheme://` URL; CI clone URLs (e.g. GitLab's
/// `CI_REPOSITORY_URL`) embed a job token there.
pub(super) fn strip_url_credentials(url: &str) -> String {
    let Some((scheme, rest)) = url.split_once("://") else {
        return url.to_string();
    };
    let authority_end = rest.find('/').unwrap_or(rest.len());
    match rest[..authority_end].rfind('@') {
        Some(at) => format!("{scheme}://{}", &rest[at + 1..]),
        None => url.to_string(),
    }
}

/// `https://github.com/DataDog/datadog-agent.git` and
/// `git@github.com:DataDog/datadog-agent` -> `datadog-agent`.
pub(super) fn repository_name_from_url(url: &str) -> Option<String> {
    let last = url.trim_end_matches('/').rsplit(['/', ':']).next()?;
    let name = last.strip_suffix(".git").unwrap_or(last);
    (!name.is_empty()).then(|| name.to_string())
}

/// Pull `name = "<value>"` out of the first `<func>(...)` block in a
/// MODULE.bazel file. Substring-based and deliberately
/// dumb - Starlark string escaping, commented-out blocks, or unusual
/// formatting (e.g. `module (\n name="x"\n)`) trips it. That's fine for
/// the canonical idioms emitted by `bazel new`, `bazel mod tidy`, and
/// every real-world MODULE.bazel I've seen; degenerate inputs fall
/// through to the basename in `detect_workspace_name`.
pub(super) fn read_starlark_name(path: &Path, func: &str) -> Option<String> {
    let content = std::fs::read_to_string(path).ok()?;
    let needle = format!("{func}(");
    let start = content.find(&needle)?;
    let after = &content[start + needle.len()..];
    // Bound the search to the matching `)` so we don't grab `name=` out
    // of a later `bazel_dep(name = "...")`.
    let mut depth = 1i32;
    let mut end = after.len();
    for (i, c) in after.char_indices() {
        match c {
            '(' => depth += 1,
            ')' => {
                depth -= 1;
                if depth == 0 {
                    end = i;
                    break;
                }
            }
            _ => {}
        }
    }
    let body = &after[..end];
    let key_pos = body.find("name")?;
    let rest = body[key_pos + 4..].trim_start();
    let after_eq = rest.strip_prefix('=')?.trim_start();
    let after_quote = after_eq.strip_prefix('"')?;
    let close = after_quote.find('"')?;
    let name = &after_quote[..close];
    if name.is_empty() {
        None
    } else {
        Some(name.to_string())
    }
}

pub(super) fn nanos_to_system_time(nanos: i64) -> SystemTime {
    if nanos >= 0 {
        UNIX_EPOCH + Duration::from_nanos(nanos as u64)
    } else {
        UNIX_EPOCH
    }
}

/// Current wall-clock time as nanoseconds since the Unix epoch. Used as an
/// upper bound while clamping mid-build, since no event can end in the future.
pub(super) fn wall_clock_now_nanos() -> Option<i64> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|d| i64::try_from(d.as_nanos()).ok())
}
