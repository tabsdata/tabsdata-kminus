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
///
/// Loading fails when the process table cannot be read at all. A sweep
/// then finds nothing and kills nothing: it acts only on what it can see.
pub struct Scan(System);

impl Scan {
    pub fn load() -> Result<Self, sysinfo::Error> {
        let mut system = System::new()?;
        system.refresh_processes_specifics(
            ProcessesToUpdate::All,
            true,
            ProcessRefreshKind::nothing()
                .without_tasks()
                .with_environ(UpdateKind::Always),
        );
        Ok(Self(system))
    }

    /// Every process whose `var` token starts with `prefix` — excluding
    /// this process, so a supervisor carrying an outer token can never
    /// sweep itself.
    pub fn under(&self, var: &str, prefix: &str) -> Vec<Found> {
        self.matching(var, |token| token.starts_with(prefix))
    }

    /// Every process whose `var` token names an owner that is no longer
    /// running. An unparseable token is *not* returned: a sweep kills only
    /// what it can positively prove is orphaned.
    pub fn strays_of_dead_owners(&self, var: &str) -> Vec<Found> {
        self.matching(var, |token| !self.owner_alive(token))
    }

    /// Whether the run named by `token`'s owner field is still running,
    /// judged against this snapshot.
    pub fn owner_alive(&self, token: &str) -> bool {
        let Some((pid, stamp)) = identity::parse_owner(token::owner_of(token)) else {
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
/// — re-read from the live process, which is what makes killing from an
/// aged snapshot safe. One single-process environment read. A process
/// table that cannot be read confirms nothing, so it reads as `false`.
pub fn still_carries(pid: u32, var: &str, expected: &str) -> bool {
    let mut system = match System::new() {
        Ok(system) => system,
        Err(error) => {
            warn!(pid, %error, "Cannot read the process table; not signalling");
            return false;
        }
    };
    let pid = Pid::from_u32(pid);
    system.refresh_processes_specifics(
        ProcessesToUpdate::Some(&[pid]),
        false,
        ProcessRefreshKind::nothing()
            .without_tasks()
            .with_environ(UpdateKind::Always),
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

/// Ask each found process to shut down. The process alone is signalled,
/// never its group: an escapee that called `setsid` is normally the top
/// of its own tree (a postgres postmaster, not a backend) and knows how
/// to stop what it started. Returns how many were asked.
///
/// Asks nobody on Windows: a found process need not lead a process group,
/// and a console event aimed at one that does not reaches everything on
/// its console (see [`signal::send_sigterm_pid`]), so the force stage
/// stops them all.
pub fn terminate_found(found: &[Found]) -> usize {
    if cfg!(windows) {
        if !found.is_empty() {
            debug!(
                count = found.len(),
                "Not asking escapees to stop: Windows cannot single them out"
            );
        }
        return 0;
    }
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

/// SIGKILL each found process that still carries its token in `var` —
/// re-read immediately before each signal, since `found` may be a grace
/// period old and any of these numbers may have been reassigned. Kills
/// the group where the process leads one, reaching children forked after
/// the scan. Returns how many were killed.
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
/// `max_rounds` is spent. The rounds cover processes forked between a
/// scan and its signals; a forker that outlasts the bound is reported,
/// never silently absorbed. Each round's scan is its own identity check.
/// Returns the total killed.
pub fn kill_until_clean(var: &str, prefix: &str, max_rounds: usize) -> usize {
    let mut total = 0;
    for round in 0..max_rounds {
        let found = match Scan::load() {
            Ok(scan) => scan.under(var, prefix),
            Err(error) => {
                warn!(prefix, %error, "Cannot read the process table; sweep stopped");
                return total;
            }
        };
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
/// longer alive. Needs no seed file — a dead owner is decidable from the
/// token alone — and leaves a live owner's processes strictly alone.
/// Returns how many were killed.
pub fn kill_strays_of_dead_runs(var: &str) -> usize {
    let scan = match Scan::load() {
        Ok(scan) => scan,
        Err(error) => {
            warn!(%error, "Cannot read the process table; strays left for the next sweep");
            return 0;
        }
    };
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
