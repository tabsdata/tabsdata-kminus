// Copyright 2026 Tabsdata Inc.

//! Finding token-carrying processes, and stopping what a scan found.
//!
//! One [`Scan`] is a snapshot of the whole process table. Loading it is
//! the expensive part — ~950 sequential syscalls on a typical host, 6–8 ms
//! — so a caller stopping many things loads once and passes the snapshot
//! down, rather than paying a scan per container. The staleness that
//! brings is handled where it matters: the force stage re-reads a
//! process's token immediately before SIGKILL, so a signal is never older
//! than its identity check.
//!
//! Synchronous throughout, deliberately: a teardown thread has no runtime
//! to hand a blocking walk to. A caller that has one wraps these in
//! `spawn_blocking` and throttles — that is the async caller's concern,
//! not this module's.

use sysinfo::{Pid, ProcessRefreshKind, ProcessesToUpdate, System, UpdateKind};
use tracing::{debug, warn};

use crate::{identity, signal, token};

/// A process that carries a kill token.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Found {
    pub pid: u32,
    pub token: String,
}

/// A snapshot of the process table, with every readable environment.
///
/// What it cannot see: reading another process's environment takes the
/// same privilege as signalling it, so the scan can never reach a process
/// it would not be allowed to kill. macOS hides the environment of SIP
/// platform binaries even from the same uid — one hop only, their
/// children re-expose it. A process that sanitised its own environment is
/// invisible along with everything below it. And on Windows an elevated
/// process cannot be read by a non-elevated scanner.
pub struct Scan(System);

impl Default for Scan {
    fn default() -> Self {
        Self::load()
    }
}

impl Scan {
    pub fn load() -> Self {
        let mut system = System::new();
        system.refresh_processes_specifics(
            ProcessesToUpdate::All,
            true,
            ProcessRefreshKind::nothing().with_environ(UpdateKind::Always),
        );
        Self(system)
    }

    /// Every process whose `var` token starts with `prefix`, excluding
    /// this process.
    ///
    /// Excluding ourselves matters whenever a supervisor runs underneath
    /// another one and so carries the outer token: a sweep that found the
    /// outer owner dead would otherwise kill the process performing the
    /// sweep.
    pub fn under(&self, var: &str, prefix: &str) -> Vec<Found> {
        self.matching(var, |token| token.starts_with(prefix))
    }

    /// Every process whose `var` token names an owner that is no longer
    /// running, excluding this process.
    ///
    /// A token that cannot be parsed, or whose owner cannot be checked,
    /// is *not* returned: a sweep kills only what it can positively prove
    /// is orphaned, and an unreadable token is not proof of anything.
    pub fn strays_of_dead_owners(&self, var: &str) -> Vec<Found> {
        self.matching(var, |token| !self.owner_alive(token))
    }

    /// Whether the run named by `token`'s owner field is still running,
    /// judged against this snapshot.
    pub fn owner_alive(&self, token: &str) -> bool {
        let Some((pid, stamp)) = token::owner_of(token).and_then(identity::parse_owner) else {
            return true;
        };
        match self.0.process(Pid::from_u32(pid)) {
            Some(process) => identity::owner_stamp(process.start_time()) == stamp,
            None => false,
        }
    }

    fn matching(&self, var: &str, accept: impl Fn(&str) -> bool) -> Vec<Found> {
        let entry_prefix = format!("{var}=");
        let own_pid = std::process::id();
        self.0
            .processes()
            .iter()
            .filter(|(pid, _)| pid.as_u32() != own_pid)
            .filter_map(|(pid, process)| {
                let token = process
                    .environ()
                    .iter()
                    .find_map(|entry| entry.to_str()?.strip_prefix(entry_prefix.as_str()))?;
                accept(token).then(|| Found {
                    pid: pid.as_u32(),
                    token: token.to_string(),
                })
            })
            .collect()
    }
}

/// Whether the process at `pid` still carries exactly this token in `var`
/// — re-read from the live process, not from any snapshot.
///
/// This is what makes killing from an aged snapshot safe: the snapshot
/// found an identity, and this confirms the identity is still at that
/// number immediately before the signal. Costs one single-process
/// environment read.
pub fn still_carries(pid: u32, var: &str, expected: &str) -> bool {
    let mut system = System::new();
    let pid = Pid::from_u32(pid);
    system.refresh_processes_specifics(
        ProcessesToUpdate::Some(&[pid]),
        false,
        ProcessRefreshKind::nothing().with_environ(UpdateKind::Always),
    );
    let Some(process) = system.process(pid) else {
        return false;
    };
    let entry_prefix = format!("{var}=");
    process.environ().iter().any(|entry| {
        entry
            .to_str()
            .and_then(|e| e.strip_prefix(entry_prefix.as_str()))
            == Some(expected)
    })
}

/// Ask each found process to shut down, so it does so inside the same
/// grace period whatever owns it is being given. Returns how many were
/// asked.
///
/// The process alone is signalled, never its group: a process that
/// escaped by calling `setsid` is normally the top of its own tree — a
/// postgres postmaster, not one of its backends — and it knows how to
/// stop what it started. Tearing its children out from under it would
/// turn an orderly shutdown into what looks like a crash.
pub fn terminate_found(found: &[Found]) -> usize {
    let mut asked = 0;
    for f in found {
        if !signal::is_pid_alive(f.pid) {
            continue;
        }
        debug!(
            pid = f.pid,
            token = f.token.as_str(),
            "Asking escapee to stop"
        );
        signal::send_sigterm_pid(f.pid);
        asked += 1;
    }
    asked
}

/// SIGKILL each found process that still carries its token in `var`.
/// Returns how many were killed.
///
/// The re-read before each signal is the staleness defence: `found` may
/// come from a snapshot taken a whole grace period ago, and in that time
/// the operating system may have handed any of these numbers to something
/// else. A process that no longer carries the token is left alone,
/// whoever it is.
///
/// The group is killed where the process leads one: `setsid` made an
/// escapee a group leader, and everything it forked afterwards inherited
/// that group — including children forked after the scan, which no
/// snapshot could contain. Past the grace there is no orderly shutdown
/// left to preserve.
pub fn kill_found(var: &str, found: &[Found]) -> usize {
    let mut killed = 0;
    for f in found {
        if !still_carries(f.pid, var, &f.token) {
            continue;
        }
        warn!(
            pid = f.pid,
            token = f.token.as_str(),
            "Killing process still carrying its kill token",
        );
        if signal::leads_own_group(f.pid) {
            signal::force_kill_group(f.pid);
        } else {
            signal::force_kill(f.pid);
        }
        killed += 1;
    }
    killed
}

/// Fresh-scan-and-kill under `prefix` until a round finds nothing, or
/// `max_rounds` is spent. Returns the total killed.
///
/// The rounds exist for the window between a scan and its signals: a
/// process forked in that window was never in the scan, so one pass can
/// miss it. Assuming processes are well behaved — they stop spawning once
/// asked to shut down — round two finds only that window's stragglers and
/// round three finds nothing. A process ill-behaved enough to keep
/// forking through SIGKILL rounds exhausts `max_rounds` and is *reported*
/// as a leak by the round's kills, never silently absorbed.
///
/// No re-verify inside: each round's scan is its own identity check,
/// microseconds before the signal.
pub fn kill_until_clean(var: &str, prefix: &str, max_rounds: usize) -> usize {
    let mut total = 0;
    for round in 0..max_rounds {
        let found = Scan::load().under(var, prefix);
        if found.is_empty() {
            return total;
        }
        if round > 0 {
            debug!(
                round,
                found = found.len(),
                prefix,
                "Stragglers after previous round"
            );
        }
        // Freshly scanned, so kill without the aged-snapshot re-read.
        for f in &found {
            warn!(
                pid = f.pid,
                token = f.token.as_str(),
                "Killing process still carrying its kill token",
            );
            if signal::leads_own_group(f.pid) {
                signal::force_kill_group(f.pid);
            } else {
                signal::force_kill(f.pid);
            }
        }
        total += found.len();
    }
    warn!(
        prefix,
        max_rounds, "Sweep rounds exhausted with processes still appearing"
    );
    total
}

/// SIGKILL every process carrying a `var` token whose owning run is no
/// longer alive. Returns how many were killed.
///
/// The startup reclamation: a previous run's seed file names *which* seed
/// to sweep, but this does not need it — a dead owner is decidable from
/// the token alone, so this also reclaims runs whose seed file was lost.
/// A token whose owner is still running is left strictly alone, which is
/// what keeps this away from a concurrently live instance's stack.
pub fn kill_strays_of_dead_runs(var: &str) -> usize {
    let scan = Scan::load();
    let strays = scan.strays_of_dead_owners(var);
    if strays.is_empty() {
        return 0;
    }
    let killed = kill_found(var, &strays);
    if killed > 0 {
        warn!(killed, "SIGKILLed processes left behind by previous runs");
    }
    killed
}

#[cfg(test)]
mod tests;
