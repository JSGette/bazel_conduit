//! In-process scrubber for sensitive command-line values.
//!
//! Bazel's `--client_env=NAME=VALUE`, `--action_env=NAME=VALUE`,
//! `--test_env=NAME=VALUE`, `--repo_env=NAME=VALUE`, and
//! `--host_action_env=NAME=VALUE` flags can carry secrets directly on the
//! command line (e.g. `--client_env=GITLAB_TOKEN=glpat-...`). Conduit
//! surfaces these in the `bazel.command_line`, `bazel.explicit_cmd_line`,
//! and `bazel.action.command_line` span attributes, which means the raw
//! values leak into whatever backend the OTLP exporter is wired to.
//!
//! This module replaces the value half of those flags with `***` whenever
//! the variable name matches any of a configurable list of substrings
//! (case-insensitive). Forms that do **not** carry a value
//! (e.g. `--client_env=GITLAB_TOKEN` — pass-through from the parent env)
//! are left as-is because no value is on the wire.
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
