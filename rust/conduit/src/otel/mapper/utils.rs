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

/// Convert a bytestream URI to a display-friendly form: show the path part (e.g. after
/// the authority, such as `blobs/hash/size`) or leave as-is if no path or not bytestream.
pub(super) fn bytestream_uri_to_display(uri: &str) -> Cow<'_, str> {
    const PREFIX: &str = "bytestream://";
    if !uri.starts_with(PREFIX) {
        return Cow::Borrowed(uri);
    }
    let after_prefix = &uri[PREFIX.len()..];
    if let Some(slash) = after_prefix.find('/') {
        let path = &after_prefix[slash + 1..];
        if !path.is_empty() {
            return Cow::Owned(path.to_string());
        }
    }
    Cow::Borrowed(uri)
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
/// Heuristic: when the entire range predates `bound_start` (both start and end
/// are strictly before the invocation began), treat it as a cache hit replaying
/// original exec timestamps and collapse to zero duration at `bound_start`.
pub fn clamp_time_range(
    start: Option<i64>,
    end: Option<i64>,
    bound_start: Option<i64>,
    bound_end: Option<i64>,
) -> (Option<i64>, Option<i64>) {
    if let (Some(s), Some(e), Some(bs)) = (start, end, bound_start) {
        if s < bs && e <= bs {
            return (Some(bs), Some(bs));
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

/// Normalize label for matching: strip leading `@` so BEP `@@repo//:t` and exec log `@repo//:t` match.
pub(super) fn normalize_label(s: &str) -> &str {
    s.trim_start_matches('@')
}

/// Maximum size in bytes for each of progress_stderr and progress_stdout.
/// Each stream is capped at 1 MB; when appending would exceed the cap, the
/// existing buffer is truncated to keep the tail (recent output) and new
/// content is appended. Enforced in [`crate::otel::mapper::OtelMapper::on_progress`].
pub(super) const PROGRESS_CAP_BYTES: usize = 1024 * 1024; // 1 MB
pub(super) const COMMAND_LINE_CAP_BYTES: usize = 4096; // 4 KB

/// Byte length of the suffix added when we truncate from the front.
pub(super) const PROGRESS_TRUNCATION_SUFFIX: &str = "\n...(truncated)\n";

/// Return the byte offset into `s` such that `s[offset..]` is the last at most
/// `max_tail_bytes` bytes of `s` at a valid UTF-8 character boundary.
pub(super) fn tail_byte_offset(s: &str, max_tail_bytes: usize) -> usize {
    if s.len() <= max_tail_bytes {
        return 0;
    }
    let start = s.len() - max_tail_bytes;
    let mut i = start;
    while i < s.len() && !s.is_char_boundary(i) {
        i += 1;
    }
    i.min(s.len())
}

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

/// Append `new_content` to `buf` while keeping total size <= PROGRESS_CAP_BYTES.
/// When appending would exceed the cap, truncates the existing buffer to keep
/// the tail (recent output), adds a truncation marker, then appends the new content.
pub(super) fn append_progress_capped(buf: &mut String, new_content: &str) {
    let suffix_len = PROGRESS_TRUNCATION_SUFFIX.len();
    let max_total = PROGRESS_CAP_BYTES;

    if buf.len() + new_content.len() <= max_total {
        buf.push_str(new_content);
        return;
    }

    // Need to drop from the front: keep last (max_total - new_content.len() - suffix_len) bytes of buf.
    let max_old_tail = max_total.saturating_sub(new_content.len()).saturating_sub(suffix_len);
    if max_old_tail > 0 {
        let offset = tail_byte_offset(buf, max_old_tail);
        let tail = buf[offset..].to_string();
        buf.clear();
        buf.push_str(&tail);
        buf.push_str(PROGRESS_TRUNCATION_SUFFIX);
    } else {
        buf.clear();
        // New content alone exceeds cap: keep only the tail of new_content.
        let keep_new = max_total.saturating_sub(suffix_len);
        if keep_new > 0 {
            let offset = tail_byte_offset(new_content, keep_new.min(new_content.len()));
            buf.push_str(PROGRESS_TRUNCATION_SUFFIX);
            buf.push_str(&new_content[offset..]);
        }
        return;
    }

    // Append new_content; if we're still over cap, trim from the end of new_content.
    let remaining = max_total.saturating_sub(buf.len());
    if new_content.len() <= remaining {
        buf.push_str(new_content);
    } else {
        let end = new_content.len() - remaining;
        let mut trim_start = end;
        while trim_start < new_content.len() && !new_content.is_char_boundary(trim_start) {
            trim_start += 1;
        }
        buf.push_str(&new_content[trim_start..]);
        buf.push_str("\n...(truncated)");
    }
}

/// Resolve a human-readable name for the Bazel workspace producing this
/// trace. Used to populate `VCS_REPOSITORY_NAME` on the root span.
///
/// Lookup order (first non-empty wins):
///   1. `MODULE.bazel` -> `module(name = "...")` (bzlmod, default since 7.0).
///   2. `WORKSPACE.bazel` / `WORKSPACE` -> `workspace(name = "...")` (legacy).
///   3. Basename of `workspace_dir` (e.g. `/repos/bazel_conduit` -> `bazel_conduit`).
///
/// I/O happens once per build at `BuildStarted` - both candidate files are
/// small (typically <2 KiB) and read synchronously. Failures fall through
/// to the next source rather than propagating, since this attribute is a
/// hint rather than load-bearing.
pub(super) fn detect_workspace_name(workspace_dir: &Path) -> Option<String> {
    if let Some(name) = read_starlark_name(&workspace_dir.join("MODULE.bazel"), "module") {
        return Some(name);
    }
    for candidate in ["WORKSPACE.bazel", "WORKSPACE"] {
        if let Some(name) = read_starlark_name(&workspace_dir.join(candidate), "workspace") {
            return Some(name);
        }
    }
    let basename = workspace_dir.file_name()?.to_str()?;
    if basename.is_empty() {
        None
    } else {
        Some(basename.to_string())
    }
}

/// Pull `name = "<value>"` out of the first `<func>(...)` block in a
/// MODULE.bazel / WORKSPACE.bazel file. Substring-based and deliberately
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
