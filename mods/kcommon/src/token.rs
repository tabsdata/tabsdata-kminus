// Copyright 2026 Tabsdata Inc.

//! Kill tokens: how a process is found again once every structure that
//! used to point at it has broken.
//!
//! A supervisor that wants to stop what it started has three ways to name
//! it, and two of them decay. A PID is a number the operating system
//! reuses — within seconds, on a host churning through short-lived
//! processes — so a remembered one eventually names a stranger. A parent
//! link breaks the moment the parent dies: Unix reparents the orphan to
//! PID 1, Windows leaves the stale parent id in place for whatever claims
//! that number next.
//!
//! An environment variable survives both. It is copied into the new
//! process image on exec and inherited by every descendant, so it outlives
//! `setsid`, `setpgid`, reparenting, and the death of every process in
//! between. It is also exact, so — unlike a recycled PID or a command
//! name — it can never match an unrelated process.
//!
//! # Shape
//!
//! ```text
//! <pid>@<start-stamp>.<millis>[/<scope>...]
//! ```
//!
//! The **root** is the launching process's identity, which is what makes
//! a token verifiable rather than trusted: the run it names is over
//! exactly when no live process has both that pid and that start time, a
//! question [`crate::identity::running_since`] answers with no file in
//! sight. The fixed-width **millis** are mint-time, uniqueness only —
//! never verified (the OS reports start times in seconds, and mint time
//! can even disagree with the OS second) — and exist so two runs landing
//! on the same pid within the same second still have distinct prefixes.
//! **Scopes** nest below, one path segment per level, so a prefix at any
//! depth selects everything under it: one process-table pass can cover a
//! single container, a whole pod, or everything a runner ever started.
//!
//! A launcher that must reclaim across restarts persists its root to a
//! file ([`Run::save`]/[`Run::load`]) and prefix-sweeps it next start;
//! the dead-owner sweep needs no file at all.
//!
//! # Limits
//!
//! Reading another process's environment requires the same privilege as
//! signalling it, so a scan can never reach a process it would not be
//! allowed to kill. What stays invisible: macOS hides the environment of
//! SIP platform binaries even from the same uid (one hop only — their
//! children re-expose it, and a simple `sh -c` command leaves no shell
//! process at all, since the shell execs); a process that sanitises its
//! own environment breaks the chain for everything below it; and on
//! Windows a process can genuinely drop the variable, since the scan
//! reads the live environment block rather than an exec-time copy.

use std::path::Path;
use std::{fs, io};

use crate::identity;

/// Environment variable carrying km's kill token.
pub const KM_KILL_TOKEN_VAR: &str = "TD_KM_KILL_TOKEN";

/// Environment variable carrying pod-runner's kill token.
pub const PR_KILL_TOKEN_VAR: &str = "TD_PR_KILL_TOKEN";

/// Every kill-token variable, for the carve-out below.
///
/// The layers stack: a container carries km's token (inherited through
/// pod-runner) *and* pod-runner's own. Each layer's sweep reads its own
/// variable, so neither may displace the other.
pub const KILL_TOKEN_VARS: &[&str] = &[KM_KILL_TOKEN_VAR, PR_KILL_TOKEN_VAR];

/// Separator between a token's fields.
pub const FIELD_SEP: char = '/';

/// Re-apply the kill-token variables that must survive a deliberate
/// environment clear — a token that fails to reach a process makes it
/// unreachable by the only mechanism that can still find it. An
/// allowlist, not a `TD_` prefix, so nothing else rides along.
///
/// `inherited` reads from the spawner's environment (normally
/// `|k| std::env::var(k).ok()`); injected so tests never mutate global
/// state. Call between `env_clear()` and explicit variables.
pub fn carry_kill_tokens(
    cmd: &mut std::process::Command,
    inherited: impl Fn(&str) -> Option<String>,
) {
    for key in KILL_TOKEN_VARS {
        if let Some(value) = inherited(key) {
            cmd.env(key, value);
        }
    }
}

/// This run's token root: `<pid>@<stamp>.<millis>`, extended per child
/// with [`Run::scope`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Run(String);

impl Run {
    /// Mint this process's run root: its verifiable identity plus
    /// fixed-width mint-time milliseconds for uniqueness.
    pub fn current() -> Self {
        let pid = std::process::id();
        let owner = match identity::process_start_time(pid) {
            Ok(Some(secs)) => identity::owner_identity(pid, secs),
            // Unverifiable on purpose: a token nobody can confirm alive is
            // reclaimed by the next run's sweep, which for an identity
            // that could not even be read is the safe direction.
            Ok(None) | Err(_) => {
                format!("{}{}{}", pid, identity::OWNER_SEP, identity::UNKNOWN_STAMP)
            }
        };
        let millis = chrono::Utc::now().timestamp_subsec_millis();
        Run(format!("{owner}.{millis:03}"))
    }

    /// A run root from persisted text. Rejects anything that could not
    /// have been minted here: no owner separator, whitespace or control
    /// characters, or a scope separator — a persisted root carries no
    /// scopes, and one with a `/` would sweep more than it names.
    pub fn parse(text: &str) -> io::Result<Self> {
        let text = text.trim();
        let valid = text.contains(identity::OWNER_SEP)
            && !text.is_empty()
            && text
                .chars()
                .all(|c| !c.is_whitespace() && !c.is_control() && c != FIELD_SEP);
        if !valid {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("not a valid run token root: {text:?}"),
            ));
        }
        Ok(Run(text.to_string()))
    }

    /// Read the root a previous run persisted. A missing file is `None`;
    /// garbage is an error, not `None` — silently skipping reclamation
    /// would be the wrong reading of it.
    pub fn load(path: &Path) -> io::Result<Option<Self>> {
        match fs::read_to_string(path) {
            Ok(text) => Ok(Some(Self::parse(&text)?)),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Persist this root for the next run to reclaim against. Call before
    /// spawning anything under it.
    pub fn save(&self, path: &Path) -> io::Result<()> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(path, &self.0)
    }

    /// A token one scope below this one: `<self>/<name>`. `name` must not
    /// contain the separator, or it would select more than it names.
    pub fn scope(&self, name: &str) -> Run {
        debug_assert!(!name.contains(FIELD_SEP), "scope must not contain '/'");
        Run(format!("{}{}{}", self.0, FIELD_SEP, name))
    }

    /// Prefix selecting this token and everything scoped below it.
    pub fn as_prefix(&self) -> &str {
        &self.0
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// The owner field of a token: the root before any scopes.
pub fn owner_of(token: &str) -> &str {
    token.split(FIELD_SEP).next().unwrap_or(token)
}

#[cfg(test)]
mod tests;
