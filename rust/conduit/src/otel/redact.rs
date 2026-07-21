// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

//! In-process scrubber for sensitive command-line values.
//!
//! Bazel's `--client_env=NAME=VALUE`, `--action_env=NAME=VALUE`,
//! `--test_env=NAME=VALUE`, `--repo_env=NAME=VALUE`,
//! `--host_action_env=NAME=VALUE`, and `--define=NAME=VALUE` flags can
//! carry secrets directly on the command line (e.g.
//! `--client_env=GITLAB_TOKEN=glpat-...` or `--define=db_password=...`).
//! Conduit surfaces these in `bazel.command_line`, `bazel.explicit_cmd_line`,
//! `bazel.action.command_line`, and `bazel.spawn.command` span attributes,
//! which means the raw values leak into whatever backend the OTLP
//! exporter is wired to.
//!
//! Three scrubbing surfaces are exposed:
//!
//! * [`Redactor::scrub_arg`] / [`Redactor::scrub_args`] -- replace the
//!   VALUE half of a `--flag=NAME=VALUE` token with `***` when NAME
//!   matches the sensitive-name list. Used for tokenised command-line
//!   attributes.
//! * [`Redactor::scrub_value_by_name`] -- given a `(name, value)` pair
//!   (e.g. a `workspaceStatus` entry like `STABLE_CI_JOB_TOKEN=abc`),
//!   replaces VALUE with `***` when NAME matches.
//! * [`Redactor::scrub_text`] -- scans free-form text (progress
//!   stdout/stderr) for embedded `--flag=NAME=VALUE` patterns and
//!   rewrites just the VALUE portion in place, preserving whitespace.
//!
//! Forms that do **not** carry a value (e.g. `--client_env=GITLAB_TOKEN`
//! — pass-through from the parent env) are left as-is because no value
//! is on the wire.
//!
//! Defense-in-depth: also configure the Datadog Agent's
//! `apm_config.replace_tags` to scrub the same attributes server-side, so
//! a missed call-site here cannot leak.

use std::borrow::Cow;

const REDACTED: &str = "***";

/// Bazel flags whose `=NAME=VALUE` half can carry a secret.
const SCRUBBED_FLAG_PREFIXES: &[&str] = &[
    "--client_env=",
    "--action_env=",
    "--test_env=",
    "--repo_env=",
    "--host_action_env=",
    "--define=",
];

/// Default case-insensitive substrings that mark an env-var name as sensitive.
///
/// The list is intentionally narrow to avoid false positives (`MONKEY` would
/// match `KEY`). Use `--redact-name-pattern` to extend or replace it.
pub const DEFAULT_REDACT_PATTERNS: &[&str] = &[
    "TOKEN",
    "SECRET",
    "PASSWORD",
    "PASSWD",
    "CREDENTIAL",
    "COOKIE",
    "APIKEY",
    "API_KEY",
    "ACCESS_KEY",
    "PRIVATE_KEY",
    "AUTH",
];

/// Scrubs sensitive values out of Bazel command-line argument vectors.
#[derive(Clone, Debug)]
pub struct Redactor {
    enabled: bool,
    /// Upper-cased substrings — names are matched case-insensitively.
    name_substrings: Vec<String>,
}

impl Redactor {
    /// Enabled redactor with [`DEFAULT_REDACT_PATTERNS`].
    pub fn default_enabled() -> Self {
        Self::new(true, DEFAULT_REDACT_PATTERNS.iter().copied())
    }

    /// No-op redactor (returns inputs unchanged).
    pub fn disabled() -> Self {
        Self {
            enabled: false,
            name_substrings: Vec::new(),
        }
    }

    /// Build a redactor with the given patterns. Empty `patterns` with
    /// `enabled=true` means "match nothing" — equivalent to disabled, but
    /// kept distinct so callers can detect mis-configuration.
    pub fn new<I, S>(enabled: bool, patterns: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let name_substrings = patterns
            .into_iter()
            .map(|p| p.as_ref().to_uppercase())
            .filter(|s| !s.is_empty())
            .collect();
        Self {
            enabled,
            name_substrings,
        }
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled && !self.name_substrings.is_empty()
    }

    /// Scrub a single argument; returns the original slice when no change.
    pub fn scrub_arg<'a>(&self, arg: &'a str) -> Cow<'a, str> {
        if !self.is_enabled() {
            return Cow::Borrowed(arg);
        }
        let Some((flag_prefix, rest)) = SCRUBBED_FLAG_PREFIXES
            .iter()
            .find_map(|p| arg.strip_prefix(p).map(|r| (*p, r)))
        else {
            return Cow::Borrowed(arg);
        };
        // `rest` is `NAME` or `NAME=VALUE`. Without `=`, no value is on the wire.
        let Some(eq) = rest.find('=') else {
            return Cow::Borrowed(arg);
        };
        let name = &rest[..eq];
        if !self.name_matches(name) {
            return Cow::Borrowed(arg);
        }
        Cow::Owned(format!("{flag_prefix}{name}={REDACTED}"))
    }

    /// Scrub a slice of args, allocating a new `Vec<String>` only when
    /// at least one value was redacted.
    pub fn scrub_args(&self, args: &[String]) -> Vec<String> {
        if !self.is_enabled() {
            return args.to_vec();
        }
        args.iter()
            .map(|a| self.scrub_arg(a).into_owned())
            .collect()
    }

    /// Returns `***` when `name` matches the sensitive-name list, otherwise
    /// passes `value` through unchanged. Use for `(name, value)` pairs that
    /// don't share the `--flag=NAME=VALUE` shape -- e.g. `workspaceStatus`
    /// entries or proto map fields.
    pub fn scrub_value_by_name<'a>(&self, name: &str, value: &'a str) -> Cow<'a, str> {
        if self.is_enabled() && self.name_matches(name) {
            Cow::Borrowed(REDACTED)
        } else {
            Cow::Borrowed(value)
        }
    }

    /// Scrub `--flag=NAME=VALUE` patterns embedded in free-form text. Splits
    /// `text` on ASCII whitespace runs and runs each token through
    /// [`Self::scrub_arg`], preserving whitespace exactly. Returns the
    /// original slice when nothing changed so callers pay no allocation on
    /// the common case (progress messages that don't contain Bazel env
    /// flags). Non-ASCII bytes are preserved verbatim -- splitting is safe
    /// because every ASCII-whitespace byte is single-byte in UTF-8.
    pub fn scrub_text<'a>(&self, text: &'a str) -> Cow<'a, str> {
        if !self.is_enabled() {
            return Cow::Borrowed(text);
        }
        let bytes = text.as_bytes();
        let mut out = String::new();
        let mut any_change = false;
        let mut i = 0;
        while i < bytes.len() {
            let tok_start = i;
            while i < bytes.len() && !bytes[i].is_ascii_whitespace() {
                i += 1;
            }
            if tok_start < i {
                let token = &text[tok_start..i];
                match self.scrub_arg(token) {
                    Cow::Borrowed(_) => out.push_str(token),
                    Cow::Owned(s) => {
                        out.push_str(&s);
                        any_change = true;
                    }
                }
            }
            let ws_start = i;
            while i < bytes.len() && bytes[i].is_ascii_whitespace() {
                i += 1;
            }
            if ws_start < i {
                out.push_str(&text[ws_start..i]);
            }
        }
        if any_change {
            Cow::Owned(out)
        } else {
            Cow::Borrowed(text)
        }
    }

    fn name_matches(&self, name: &str) -> bool {
        let upper = name.to_uppercase();
        self.name_substrings.iter().any(|s| upper.contains(s))
    }
}

impl Default for Redactor {
    fn default() -> Self {
        Self::default_enabled()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redacts_client_env_token() {
        let r = Redactor::default_enabled();
        let out = r.scrub_arg("--client_env=GITLAB_TOKEN=glpat-secret");
        assert_eq!(out, "--client_env=GITLAB_TOKEN=***");
    }

    #[test]
    fn redacts_action_env_password_case_insensitive() {
        let r = Redactor::default_enabled();
        let out = r.scrub_arg("--action_env=db_password=hunter2");
        assert_eq!(out, "--action_env=db_password=***");
    }

    #[test]
    fn leaves_non_sensitive_env_alone() {
        let r = Redactor::default_enabled();
        let out = r.scrub_arg("--client_env=PATH=/usr/bin");
        assert_eq!(out, "--client_env=PATH=/usr/bin");
    }

    #[test]
    fn leaves_passthrough_form_untouched() {
        let r = Redactor::default_enabled();
        let out = r.scrub_arg("--client_env=GITLAB_TOKEN");
        assert_eq!(out, "--client_env=GITLAB_TOKEN");
    }

    #[test]
    fn leaves_unrelated_flags_untouched() {
        let r = Redactor::default_enabled();
        let out = r.scrub_arg("--define=FOO=bar");
        assert_eq!(out, "--define=FOO=bar");
        let out = r.scrub_arg("//pkg:target");
        assert_eq!(out, "//pkg:target");
    }

    #[test]
    fn redacts_sensitive_define_value() {
        let r = Redactor::default_enabled();
        assert_eq!(
            r.scrub_arg("--define=db_password=hunter2"),
            "--define=db_password=***"
        );
        assert_eq!(
            r.scrub_arg("--define=GIT_TOKEN=glpat-abc"),
            "--define=GIT_TOKEN=***"
        );
    }

    #[test]
    fn scrub_value_by_name_redacts_sensitive_key() {
        let r = Redactor::default_enabled();
        assert_eq!(
            r.scrub_value_by_name("STABLE_CI_JOB_TOKEN", "abc123"),
            "***"
        );
        assert_eq!(r.scrub_value_by_name("BUILD_USER", "alice"), "alice");
    }

    #[test]
    fn scrub_value_by_name_disabled_is_passthrough() {
        let r = Redactor::disabled();
        assert_eq!(
            r.scrub_value_by_name("STABLE_CI_JOB_TOKEN", "abc123"),
            "abc123"
        );
    }

    #[test]
    fn scrub_text_rewrites_embedded_env_flag() {
        let r = Redactor::default_enabled();
        let input = "ERROR: build failed\n  while calling --client_env=GITLAB_TOKEN=glpat-secret somewhere\n";
        let expected =
            "ERROR: build failed\n  while calling --client_env=GITLAB_TOKEN=*** somewhere\n";
        assert_eq!(r.scrub_text(input), expected);
    }

    #[test]
    fn scrub_text_preserves_whitespace_and_returns_borrow_on_no_change() {
        let r = Redactor::default_enabled();
        let input = "INFO: starting build\n\n  ok\n";
        match r.scrub_text(input) {
            Cow::Borrowed(s) => assert_eq!(s, input),
            Cow::Owned(_) => panic!("expected borrowed return when no changes"),
        }
    }

    #[test]
    fn scrub_text_handles_multiple_hits() {
        let r = Redactor::default_enabled();
        let input =
            "--client_env=TOKEN=a --action_env=PASSWORD=b --define=OTHER=c --client_env=PATH=/bin";
        let expected =
            "--client_env=TOKEN=*** --action_env=PASSWORD=*** --define=OTHER=c --client_env=PATH=/bin";
        assert_eq!(r.scrub_text(input), expected);
    }

    #[test]
    fn keeps_value_when_name_only_partially_matches_unrelated_word() {
        let r = Redactor::default_enabled();
        let out = r.scrub_arg("--client_env=MONKEY_BUSINESS=banana");
        assert_eq!(out, "--client_env=MONKEY_BUSINESS=banana");
    }

    #[test]
    fn handles_value_containing_equals() {
        let r = Redactor::default_enabled();
        let out = r.scrub_arg("--client_env=API_TOKEN=foo=bar=baz");
        assert_eq!(out, "--client_env=API_TOKEN=***");
    }

    #[test]
    fn covers_all_env_flag_variants() {
        let r = Redactor::default_enabled();
        for flag in ["--test_env", "--repo_env", "--host_action_env"] {
            let arg = format!("{flag}=MY_SECRET=abc");
            assert_eq!(r.scrub_arg(&arg), format!("{flag}=MY_SECRET=***"));
        }
    }

    #[test]
    fn disabled_redactor_is_passthrough() {
        let r = Redactor::disabled();
        let out = r.scrub_arg("--client_env=GITLAB_TOKEN=glpat-secret");
        assert_eq!(out, "--client_env=GITLAB_TOKEN=glpat-secret");
    }

    #[test]
    fn empty_patterns_acts_disabled() {
        let r = Redactor::new(true, Vec::<String>::new());
        assert!(!r.is_enabled());
        assert_eq!(
            r.scrub_arg("--client_env=GITLAB_TOKEN=secret"),
            "--client_env=GITLAB_TOKEN=secret"
        );
    }

    #[test]
    fn custom_patterns_override_default() {
        let r = Redactor::new(true, ["JIRA"]);
        assert_eq!(
            r.scrub_arg("--client_env=JIRA_USER=alice"),
            "--client_env=JIRA_USER=***"
        );
        assert_eq!(
            r.scrub_arg("--client_env=GITLAB_TOKEN=secret"),
            "--client_env=GITLAB_TOKEN=secret"
        );
    }

    #[test]
    fn scrub_args_returns_owned_vec_unchanged_when_nothing_to_redact() {
        let r = Redactor::default_enabled();
        let input = vec!["bazel".to_string(), "build".to_string(), "//x:y".to_string()];
        assert_eq!(r.scrub_args(&input), input);
    }

    #[test]
    fn scrub_args_redacts_mixed_input() {
        let r = Redactor::default_enabled();
        let input = vec![
            "build".to_string(),
            "--client_env=GITLAB_TOKEN=glpat".to_string(),
            "--client_env=PATH=/bin".to_string(),
            "--action_env=DD_API_KEY=abc".to_string(),
            "//x:y".to_string(),
        ];
        let expected = vec![
            "build".to_string(),
            "--client_env=GITLAB_TOKEN=***".to_string(),
            "--client_env=PATH=/bin".to_string(),
            "--action_env=DD_API_KEY=***".to_string(),
            "//x:y".to_string(),
        ];
        assert_eq!(r.scrub_args(&input), expected);
    }
}
