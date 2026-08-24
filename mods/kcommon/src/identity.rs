// Copyright 2026 Tabsdata Inc.

//! Process identity: telling "that process is still running" apart from
//! "something now holds that number".
//!
//! A PID alone cannot make that distinction — the operating system
//! recycles the numbers, within seconds on a host churning through
//! processes. Pairing the PID with the process's start time can: no two
//! processes ever hold the same pid *and* the same start time, so
//! `<pid>@<start-stamp>` names one process, forever. Anything recorded for
//! later — a PID file read after the run that wrote it, a token carried in
//! a child's environment — should record that pair, not the bare number.

use sysinfo::{Pid, ProcessRefreshKind, ProcessesToUpdate, System};

/// Separator between a PID and its start stamp.
pub const OWNER_SEP: char = '@';

/// Fixed-width, filename-safe UTC stamp: `20260822T143205Z`. The trailing
/// `Z` is a literal, correct only because it is always applied to a UTC
/// timestamp. Lexicographic order is chronological order.
const STAMP_FORMAT: &str = "%Y%m%dT%H%M%SZ";

/// Stamp used when a start time cannot be represented. It never equals a
/// real stamp, so an identity carrying it can never be confirmed alive —
/// the safe direction, since acting on an unconfirmable identity means
/// acting on what might be a stranger.
pub const UNKNOWN_STAMP: &str = "00000000T000000Z";

/// Format a process start time (epoch seconds, as reported by `sysinfo`)
/// as a fixed-width UTC stamp.
///
/// Both the writer of an identity and the code verifying one go through
/// this function and compare the results as strings, so the two sides can
/// never disagree about format or timezone.
pub fn owner_stamp(start_time_secs: u64) -> String {
    // Converted rather than cast. `as i64` wraps, and a wrapped negative
    // formats as a plausible stamp just before the epoch instead of
    // falling back — an identity that looks valid and matches nothing is
    // worse than one that declares itself unknown.
    let Ok(secs) = i64::try_from(start_time_secs) else {
        return UNKNOWN_STAMP.to_string();
    };
    match chrono::DateTime::from_timestamp(secs, 0) {
        Some(t) => t.format(STAMP_FORMAT).to_string(),
        None => UNKNOWN_STAMP.to_string(),
    }
}

/// A process identity: `<pid>@<start-stamp>`.
pub fn owner_identity(pid: u32, start_time_secs: u64) -> String {
    format!("{}{}{}", pid, OWNER_SEP, owner_stamp(start_time_secs))
}

/// Split an identity into its pid and start stamp.
///
/// `None` for anything that is not `<pid>@<stamp>`. Callers treat that as
/// "cannot verify" — and an unverifiable identity must never be acted on
/// as though it were confirmed.
pub fn parse_owner(owner: &str) -> Option<(u32, &str)> {
    let (pid, stamp) = owner.split_once(OWNER_SEP)?;
    Some((pid.parse().ok()?, stamp))
}

/// Whether a start stamp is the fallback, i.e. no real start time could be
/// read. Such a stamp matches no live process.
pub fn is_unknown_stamp(stamp: &str) -> bool {
    stamp == UNKNOWN_STAMP
}

/// Start time of `pid` in epoch seconds, if the process can be read.
///
/// Refreshed without any of the per-process detail sysinfo can gather —
/// only the start time is wanted, and the environments and command lines
/// are what make a process-table pass expensive.
pub fn process_start_time(pid: u32) -> Option<u64> {
    let mut system = System::new();
    let pid = Pid::from_u32(pid);
    system.refresh_processes_specifics(
        ProcessesToUpdate::Some(&[pid]),
        false,
        ProcessRefreshKind::nothing(),
    );
    system.process(pid).map(|p| p.start_time())
}

/// Whether a live process holds `pid` and started at `stamp` — the one
/// question a bare liveness check cannot answer.
///
/// A zombie is not running: it has exited and merely awaits its reaper,
/// yet the process table still lists it with its original start time. A
/// caller asking this question wants to know whether there is anything
/// left to wait for or signal, and for a zombie the answer is no.
pub fn running_since(pid: u32, stamp: &str) -> bool {
    let mut system = System::new();
    let pid = Pid::from_u32(pid);
    system.refresh_processes_specifics(
        ProcessesToUpdate::Some(&[pid]),
        false,
        ProcessRefreshKind::nothing(),
    );
    system.process(pid).is_some_and(|p| {
        p.status() != sysinfo::ProcessStatus::Zombie && owner_stamp(p.start_time()) == stamp
    })
}

#[cfg(test)]
mod tests;
