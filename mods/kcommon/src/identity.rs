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

/// Format a process start time (epoch seconds) as a fixed-width UTC
/// stamp. Writer and verifier both come through here, so the two sides
/// can never disagree about format or timezone.
pub fn owner_stamp(start_time_secs: u64) -> String {
    // Converted, not cast: `as i64` wraps, and a wrapped negative formats
    // as a plausible pre-epoch stamp instead of the sentinel.
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

/// Split an identity into its pid and start stamp. `None` for anything
/// that is not `<pid>@<stamp>[.<suffix>]` — unverifiable, which callers
/// must never act on as though confirmed.
///
/// A `.<suffix>` after the stamp is uniqueness data a minter appended
/// (mint-time milliseconds, typically) and is not part of the verifiable
/// identity: the stamp is compared against the OS-reported start time,
/// which has seconds only — and mint-time can even disagree with the OS
/// second, so folding the suffix in would fail live owners.
pub fn parse_owner(owner: &str) -> Option<(u32, &str)> {
    let (pid, stamp) = owner.split_once(OWNER_SEP)?;
    let stamp = stamp.split('.').next().unwrap_or(stamp);
    Some((pid.parse().ok()?, stamp))
}

/// Whether a start stamp is the fallback, i.e. no real start time could be
/// read. Such a stamp matches no live process.
pub fn is_unknown_stamp(stamp: &str) -> bool {
    stamp == UNKNOWN_STAMP
}

/// Start time of `pid` in epoch seconds: `None` when no such process
/// exists, an error when the process table cannot be read at all — which
/// says nothing about the process, so callers must not read it as gone.
/// Refreshed without environments or command lines, which are what make
/// a process-table read expensive.
pub fn process_start_time(pid: u32) -> Result<Option<u64>, sysinfo::Error> {
    let mut system = System::new()?;
    let pid = Pid::from_u32(pid);
    system.refresh_processes_specifics(
        ProcessesToUpdate::Some(&[pid]),
        false,
        ProcessRefreshKind::nothing().without_tasks(),
    );
    Ok(system.process(pid).map(|p| p.start_time()))
}

/// Whether a live process holds `pid` and started at `stamp` — the one
/// question a bare liveness check cannot answer. A zombie reads as gone:
/// it is still listed with its original start time, but nothing can be
/// asked of it or waited for. An error when the process table cannot be
/// read: the answer is then unknown, neither running nor gone.
pub fn running_since(pid: u32, stamp: &str) -> Result<bool, sysinfo::Error> {
    let mut system = System::new()?;
    let pid = Pid::from_u32(pid);
    system.refresh_processes_specifics(
        ProcessesToUpdate::Some(&[pid]),
        false,
        ProcessRefreshKind::nothing().without_tasks(),
    );
    Ok(system.process(pid).is_some_and(|p| {
        p.status() != sysinfo::ProcessStatus::Zombie && owner_stamp(p.start_time()) == stamp
    }))
}

#[cfg(test)]
mod tests;
