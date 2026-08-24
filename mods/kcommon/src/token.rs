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
//! <seed>/<pid>@<start-stamp>[/<scope>...]
//! ```
//!
//! The **seed** is minted once per run of a launcher and persisted by it,
//! so the *next* run can read it back and reclaim whatever the previous
//! one left behind — that is the only state that crosses restarts. The
//! **owner** field is the launching process's identity, which is what
//! makes a token verifiable rather than trusted: the run it names is over
//! exactly when no live process has both that pid and that start time, a
//! question [`crate::identity::running_since`] answers with no file in
//! sight. It is also what protects a live stack if two launchers ever
//! overlap — their tokens differ even if a seed file was misread.
//! **Scopes** nest below, one path segment per level, so a prefix at any
//! depth selects everything under it: one process-table pass can cover a
//! single container, a whole pod, or everything a runner ever started.
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
/// environment clear.
///
/// Giving a child a clean environment is a legitimate thing to ask for,
/// and it is honoured for everything except these: a kill token that
/// fails to reach a process makes that process unreachable by the only
/// mechanism that can still find it once it has left its process group.
///
/// An allowlist, not a prefix match, on purpose: a `TD_` prefix would
/// carry every other such variable into a child that was meant to have
/// none, quietly changing behaviour for anything that reads one.
///
/// `inherited` reads a variable from the spawning process's own
/// environment (normally `|k| std::env::var(k).ok()`). Passed in rather
/// than read here so the sequence can be exercised without mutating
/// global state. Call between `env_clear()` and the caller's explicit
/// variables, so an explicit value still wins.
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

/// A launcher run's seed: the one piece of token state that crosses
/// restarts, by being written to a file the next run reads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Seed(String);

impl Seed {
    /// Mint a fresh seed for this run: a millisecond UTC stamp plus this
    /// process's pid. Readable in a process listing, unique per run
    /// (one process cannot start twice in a millisecond), and free of the
    /// token's separator characters by construction.
    pub fn generate() -> Self {
        let now = chrono::Utc::now().format("%Y%m%dT%H%M%S%.3fZ");
        Seed(format!("{}-{}", now, std::process::id()))
    }

    /// A seed from persisted or configured text.
    ///
    /// Rejects anything that could not have been minted here: an empty
    /// string, whitespace or control characters, or the token separators —
    /// a seed containing `/` would silently become two token fields, and
    /// every prefix match after that would lie.
    pub fn parse(text: &str) -> io::Result<Self> {
        let text = text.trim();
        let valid = !text.is_empty()
            && text
                .chars()
                .all(|c| !c.is_whitespace() && !c.is_control() && c != FIELD_SEP && c != '@');
        if !valid {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("not a valid kill-token seed: {text:?}"),
            ));
        }
        Ok(Seed(text.to_string()))
    }

    /// Read the seed a previous run persisted, if any.
    ///
    /// A missing file is `None` — the first run ever, or a cleaned
    /// instance. An unreadable or invalid file is an error rather than
    /// `None`: it means something wrote garbage where reclamation state
    /// lives, which is worth surfacing over silently skipping cleanup.
    pub fn load(path: &Path) -> io::Result<Option<Self>> {
        match fs::read_to_string(path) {
            Ok(text) => Ok(Some(Self::parse(&text)?)),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Persist this seed for the *next* run to reclaim against. Call
    /// before spawning anything under it, so a launcher that dies
    /// mid-startup still left the trail behind.
    pub fn save(&self, path: &Path) -> io::Result<()> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(path, &self.0)
    }

    /// Token prefix selecting everything any run of this seed started.
    pub fn as_prefix(&self) -> String {
        format!("{}{}", self.0, FIELD_SEP)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// This run's token root: `<seed>/<pid>@<stamp>`, extended per child with
/// [`Run::scope`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Run(String);

impl Run {
    /// Root a run under `seed` with this process's identity.
    pub fn of(seed: &Seed) -> Self {
        let pid = std::process::id();
        let owner = match identity::process_start_time(pid) {
            Some(secs) => identity::owner_identity(pid, secs),
            // Unverifiable on purpose: a token nobody can confirm alive is
            // reclaimed by the next run's sweep, which for an identity
            // that could not even be read is the safe direction.
            None => format!("{}{}{}", pid, identity::OWNER_SEP, identity::UNKNOWN_STAMP),
        };
        Run(format!("{}{}{}", seed.0, FIELD_SEP, owner))
    }

    /// A token one scope below this one: `<self>/<name>`.
    ///
    /// `name` must not contain the separator; a scope with a `/` in it
    /// would select more than it names.
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

/// The owner field of a token: the `<pid>@<stamp>` between the seed and
/// the scopes. `None` for text that does not have one.
pub fn owner_of(token: &str) -> Option<&str> {
    token.split(FIELD_SEP).nth(1)
}

#[cfg(test)]
mod tests;
