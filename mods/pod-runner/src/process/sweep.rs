// Copyright 2026 Tabsdata Inc.

//! Orphan sweep — kill container processes that escaped their process group.
//!
//! # The gap this closes
//!
//! Every container is spawned into its own process group, and every kill
//! goes to that group as `kill(-pgid, ...)` (see
//! [`crate::process::executor`]). A process that calls `setsid()` leaves
//! both that group and the session; if its parent then exits, it is
//! reparented to PID 1. Neither a group signal nor a walk of the parent
//! links can reach it afterwards.
//!
//! `pg_ctl` does exactly this, deliberately: it forks, the child calls
//! `setsid()` so the postmaster is not signalled along with the launching
//! process group, execs the postmaster, and the launcher exits. An
//! embedded postgres started that way survives every signal pod-runner
//! sends and outlives its pod.
//!
//! # How the sweep finds it
//!
//! Every container is spawned with [`KILL_TOKEN_VAR`] in its
//! environment, naming the runner, the pod and the container. An
//! environment is copied into the new process image on `exec` and
//! inherited by every descendant: unlike a process group or a parent
//! link, it survives `setsid`, `setpgid`, reparenting, and the death of
//! every process in between. It is also exact, so — unlike a recycled
//! PID or a command name — it can never match an unrelated process.
//!
//! Three sweeps use it:
//!
//! - [`terminate_orphans`] when a pod kill starts, so an escapee shuts
//!   down within the same grace period as its container.
//! - [`kill_orphans`] once that grace has passed, and again when a pod
//!   reaches a terminal phase however it got there.
//! - [`kill_dead_runner_orphans`] at startup, for escapees left behind by
//!   a previous pod-runner. The token names its owner, so an orphan whose
//!   owner is no longer running is provably stale — which is what the PID
//!   files behind [`crate::process::executor::cleanup_stale_pids`] cannot
//!   tell us, since an escapee was never recorded in one.
//!
//! # Limits
//!
//! Reading another process's environment requires the same privilege as
//! signalling it, so the sweep can never reach a process it would not be
//! allowed to kill. Two cases are invisible to it:
//!
//! - macOS hides the environment of SIP platform binaries even from the
//!   same uid: `/bin/sh` and `/bin/sleep` report no environment at all.
//!   The binaries a container actually runs — tarball postgres, python,
//!   anything cargo-built — are visible.
//! - A process that sanitises its own environment (`env -i`) breaks the
//!   chain for everything below it.

use sysinfo::{Pid, ProcessRefreshKind, ProcessesToUpdate, System, UpdateKind};
use tracing::{debug, warn};

use crate::process::executor::{force_kill, force_kill_group, is_pid_alive, send_sigterm_pid};

/// Environment variable carrying a container's kill token.
///
/// Value: `<runner-pid>@<runner-start>/<pod>/<container-name>`, e.g.
/// `4711@20260822T143205Z/db-pod_8f3c1d2e-.../database`.
pub const KILL_TOKEN_VAR: &str = "TD_PR_KILL_TOKEN";

/// Separator between the token's owner, pod and container fields.
const FIELD_SEP: char = '/';

/// Separator between the owner's PID and its start stamp.
const OWNER_SEP: char = '@';

/// Fixed-width, filename-safe UTC stamp: `20260822T143205Z`. The
/// trailing `Z` is a literal, correct only because it is always applied
/// to a UTC timestamp. Lexicographic order is chronological order.
const STAMP_FORMAT: &str = "%Y%m%dT%H%M%SZ";

/// Stamp used when a start time cannot be represented. It never equals a
/// real stamp, so an owner carrying it can never be confirmed alive.
const UNKNOWN_STAMP: &str = "00000000T000000Z";

/// How many process-table scans may run at once.
///
/// Every scan walks the whole table and allocates a snapshot of it —
/// ~1.2 MB for ~950 processes. Pods tear down independently, so a mass
/// teardown (pod-runner shutting down a full node) starts one scan per
/// pod at the same instant, each redundant with all the others: measured
/// on 16 cores, 128 at once cost 304 ms and 158 MB peak, and tokio's
/// blocking pool would happily run hundreds. A smaller machine has less
/// of both to spare, so the scans are serialised instead.
///
/// The queueing is invisible where it happens: a sweep sits inside a
/// grace period measured in seconds, and ~100 queued scans add well
/// under a second. Each caller still gets its own fresh snapshot, so no
/// staleness is introduced.
const MAX_CONCURRENT_SCANS: usize = 1;

/// Permits for [`MAX_CONCURRENT_SCANS`].
static SCAN_SLOTS: tokio::sync::Semaphore = tokio::sync::Semaphore::const_new(MAX_CONCURRENT_SCANS);

// ---------------------------------------------------------------------------
// Token construction
// ---------------------------------------------------------------------------

/// Format a process start time (epoch seconds, as reported by `sysinfo`)
/// as a fixed-width UTC stamp.
///
/// Both the writer of a token and the code verifying one go through this
/// function and compare the results as strings, so the two sides can
/// never disagree about format or timezone.
fn owner_stamp(start_time_secs: u64) -> String {
    match chrono::DateTime::from_timestamp(start_time_secs as i64, 0) {
        Some(t) => t.format(STAMP_FORMAT).to_string(),
        None => UNKNOWN_STAMP.to_string(),
    }
}

/// Identity of this pod-runner process: `<pid>@<start-stamp>`.
///
/// A bare PID would not do. The startup sweep has to decide whether the
/// runner named by a token is still running, and PIDs are recycled — a
/// restarted pod-runner that landed on its predecessor's PID would read
/// the previous run's orphans as its own and leave them running, which is
/// the exact failure [`kill_dead_runner_orphans`] exists to prevent. The
/// start time makes the identity unreusable.
pub fn runner_id() -> &'static str {
    static RUNNER_ID: std::sync::OnceLock<String> = std::sync::OnceLock::new();
    RUNNER_ID.get_or_init(|| {
        let pid = std::process::id();
        let mut system = System::new();
        system.refresh_processes_specifics(
            ProcessesToUpdate::Some(&[Pid::from_u32(pid)]),
            false,
            ProcessRefreshKind::nothing(),
        );
        let started = match system.process(Pid::from_u32(pid)) {
            Some(process) => process.start_time(),
            None => {
                // Not fatal: tokens stay unique (the PID is ours for as
                // long as we run), they just cannot be verified as ours
                // by a later run's startup sweep.
                warn!(pid, "Own start time unavailable — kill tokens unverifiable");
                0
            }
        };
        format!("{pid}{OWNER_SEP}{}", owner_stamp(started))
    })
}

/// Token prefix shared by every container of one pod:
/// `<runner>/<pod-name>_<pod-uid>/`.
///
/// Matching on the prefix is what lets one sweep cover a whole pod: the
/// process table is walked once per pod kill rather than once per
/// container.
///
/// The pod is identified the same way its log span identifies it, by name
/// *and* uid. The uid alone would collapse every pod of a spec that
/// carries no uid into one prefix, which would let a sweep for one pod
/// kill another's escapees.
pub fn pod_prefix(pod_name: &str, pod_uid: &str) -> String {
    format!("{}{FIELD_SEP}{pod_name}_{pod_uid}{FIELD_SEP}", runner_id())
}

/// The token for one container of the pod identified by `pod_prefix`.
pub fn container_token(pod_prefix: &str, container: &str) -> String {
    format!("{pod_prefix}{container}")
}

/// Token prefix shared by every container of every pod this runner
/// started: `<runner>/`.
///
/// The token's fields nest, so a prefix at any level selects everything
/// below it. That is what lets a runner-wide teardown sweep once instead
/// of once per pod.
pub fn runner_prefix() -> String {
    format!("{}{FIELD_SEP}", runner_id())
}

// ---------------------------------------------------------------------------
// Process table
// ---------------------------------------------------------------------------

/// A process that carries a kill token.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Orphan {
    pid: u32,
    token: String,
}

/// A snapshot of the process table, with every readable environment.
///
/// Loading it costs one pass over the host's processes — 6–8 ms for ~950
/// processes, of which reading the environments is well under a
/// millisecond; the cost is ~950 sequential syscalls, so it tracks
/// per-syscall latency rather than core count. That is paid only on a
/// kill or at startup, never on a steady-state path, and never by more
/// than [`MAX_CONCURRENT_SCANS`] callers at a time.
struct ProcessTable(System);

impl ProcessTable {
    fn load() -> Self {
        let mut system = System::new();
        system.refresh_processes_specifics(
            ProcessesToUpdate::All,
            true,
            ProcessRefreshKind::nothing().with_environ(UpdateKind::Always),
        );
        Self(system)
    }

    /// Every token-carrying process whose token `accept` returns true
    /// for, excluding this process.
    ///
    /// Excluding ourselves matters when pod-runner is itself run as a
    /// container of an outer pod-runner: it then carries the outer
    /// runner's token, and a startup sweep that found the outer runner
    /// dead would otherwise kill the process performing the sweep.
    fn orphans(&self, accept: impl Fn(&str) -> bool) -> Vec<Orphan> {
        let entry_prefix = format!("{KILL_TOKEN_VAR}=");
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
                accept(token).then(|| Orphan {
                    pid: pid.as_u32(),
                    token: token.to_string(),
                })
            })
            .collect()
    }

    /// Whether the pod-runner named by `token`'s owner field is still
    /// running.
    ///
    /// A token that cannot be parsed, or whose owner cannot be checked,
    /// counts as alive: the startup sweep kills only what it can
    /// positively prove is orphaned.
    fn owner_alive(&self, token: &str) -> bool {
        let owner = token.split(FIELD_SEP).next().unwrap_or(token);
        let Some((pid, stamp)) = owner.split_once(OWNER_SEP) else {
            return true;
        };
        let Ok(pid) = pid.parse::<u32>() else {
            return true;
        };
        match self.0.process(Pid::from_u32(pid)) {
            Some(process) => owner_stamp(process.start_time()) == stamp,
            None => false,
        }
    }
}

/// Load the process table off the runtime and select orphans from it.
///
/// The scan is a blocking, syscall-bound walk of every process, so it
/// runs on the blocking pool rather than stalling a runtime worker: the
/// calling task yields for the duration instead of holding a worker.
/// Concurrency is capped at [`MAX_CONCURRENT_SCANS`].
async fn scan<F>(select: F) -> Vec<Orphan>
where
    F: FnOnce(&ProcessTable) -> Vec<Orphan> + Send + 'static,
{
    // A closed semaphore is impossible for a static that is never
    // closed; if it somehow were, scanning unthrottled beats not
    // sweeping at all.
    let _slot = SCAN_SLOTS.acquire().await.ok();

    match tokio::task::spawn_blocking(move || select(&ProcessTable::load())).await {
        Ok(orphans) => orphans,
        Err(e) => {
            warn!(error = %e, "Orphan sweep scan failed");
            Vec::new()
        }
    }
}

// ---------------------------------------------------------------------------
// Sweeps
// ---------------------------------------------------------------------------

/// The escaped processes selected by one scan.
///
/// A kill signals the same set twice — SIGTERM, then SIGKILL for whatever
/// outlived the grace period — so the set is found once and reused rather
/// than rescanned. A process that escapes *during* the grace is missed
/// here and caught by the sweep at the end of the pod's orchestrator,
/// which scans afresh.
pub struct Orphans(Vec<Orphan>);

impl Orphans {
    /// Scan for every escaped process whose token starts with `prefix` —
    /// a pod's [`pod_prefix`] for one pod, [`runner_prefix`] for all of
    /// them.
    pub async fn find(prefix: &str) -> Self {
        let prefix = prefix.to_string();
        Self(scan(move |table| table.orphans(|token| token.starts_with(&prefix))).await)
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// SIGTERM each one, so it shuts down inside the same grace period
    /// its container gets. Returns how many were signalled.
    pub fn terminate(&self) -> usize {
        let signalled = signal_orphans(&self.0, Escalation::Graceful);
        if signalled > 0 {
            debug!(
                signalled,
                "SIGTERM sent to processes that escaped their container's process group",
            );
        }
        signalled
    }

    /// SIGKILL whichever are still alive. Returns how many were killed.
    ///
    /// Anything still here outlived its container, so it is reported at
    /// warning level: a container leaked a process, and that is worth
    /// seeing rather than silently patching over.
    pub fn force_kill(&self) -> usize {
        let killed = signal_orphans(&self.0, Escalation::Force);
        if killed > 0 {
            warn!(
                killed,
                "SIGKILLed processes that escaped their container's process group",
            );
        }
        killed
    }
}

/// Scan once and SIGTERM everything under `prefix`.
///
/// Used where a teardown begins and the escapees are not signalled again
/// by the same caller: the runner-wide SIGTERM at the start of a
/// shutdown. A pod kill uses [`Orphans`] directly so its two stages share
/// one scan.
pub async fn terminate_orphans(prefix: &str) -> usize {
    Orphans::find(prefix).await.terminate()
}

/// Scan once and SIGKILL everything under `prefix`.
///
/// Used at the end of a pod's lifetime however it ended, at the end of a
/// runner-wide drain, and when a terminal pod is deleted.
pub async fn kill_orphans(prefix: &str) -> usize {
    Orphans::find(prefix).await.force_kill()
}

/// SIGKILL every token-carrying process whose owning pod-runner is no
/// longer running. Returns how many were killed.
///
/// Run at startup. An escaped process was never recorded in a PID file,
/// so this is the only thing that reclaims one after the pod-runner that
/// started it exited — including the case where that pod-runner was
/// itself SIGKILLed mid-drain and never got to clean up.
pub async fn kill_dead_runner_orphans() -> usize {
    let orphans = Orphans(scan(|table| table.orphans(|token| !table.owner_alive(token))).await);
    let killed = signal_orphans(&orphans.0, Escalation::Force);
    if killed > 0 {
        warn!(
            killed,
            "SIGKILLed escaped container processes left by a previous run",
        );
    }
    killed
}

/// PIDs currently carrying a token under `pod_prefix`.
///
/// Test-only: the sweeps act on exactly this set, and an end-to-end test
/// has to observe an escapee before the sweep reaps it.
#[cfg(test)]
pub(crate) async fn orphan_pids(pod_prefix: &str) -> Vec<u32> {
    Orphans::find(pod_prefix)
        .await
        .0
        .into_iter()
        .map(|orphan| orphan.pid)
        .collect()
}

/// Which signal a sweep sends.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Escalation {
    /// SIGTERM: let the process shut itself down within the grace period.
    Graceful,
    /// SIGKILL: the grace period is over, or the owner is long gone.
    Force,
}

/// Signal each orphan. Returns how many were signalled.
///
/// The two stages address different things on purpose.
///
/// The graceful stage signals the **process only**. A process that
/// escaped by calling `setsid` is normally the top of its own tree — a
/// postgres postmaster, not one of its backends — and it knows how to
/// stop what it started. Sending SIGTERM to the whole group instead would
/// tear its children out from under it, which for postgres turns an
/// orderly shutdown into what looks like a crash, with recovery on the
/// next start.
///
/// The force stage signals the **process group** it leads, if it leads
/// one. POSIX does not propagate a signal from a parent to its children,
/// and a child forked after the scan was never in the orphan set at all;
/// but `setsid` made the escapee a group leader, and everything it forked
/// afterwards inherited that group. Signalling the group therefore
/// reaches the children the scan never saw — postgres backends started
/// while the grace period ran, for instance — without relying on the
/// application noticing its parent is gone. Past the grace there is no
/// orderly shutdown left to preserve, so the bluntness costs nothing.
///
/// A process that leads no group is signalled alone: the group it sits in
/// belongs to something else, most likely the container that is already
/// being killed through its own path.
fn signal_orphans(orphans: &[Orphan], escalation: Escalation) -> usize {
    let mut signalled = 0;
    for orphan in orphans {
        if !is_pid_alive(orphan.pid) {
            continue;
        }
        warn!(
            pid = orphan.pid,
            token = orphan.token.as_str(),
            signal = ?escalation,
            "Container process escaped its process group",
        );
        match escalation {
            Escalation::Graceful => send_sigterm_pid(orphan.pid),
            // Racy in principle — the escapee could exit between the
            // liveness check above and here, and something else could
            // claim its PID and lead a group by then — but the window is
            // microseconds, against a leak that lasts until reboot.
            Escalation::Force if leads_own_group(orphan.pid) => force_kill_group(orphan.pid),
            Escalation::Force => force_kill(orphan.pid),
        }
        signalled += 1;
    }
    signalled
}

/// Whether `pid` is the leader of its own process group, i.e. the group
/// contains it and its descendants and nothing else.
fn leads_own_group(pid: u32) -> bool {
    #[cfg(unix)]
    {
        matches!(
            nix::unistd::getpgid(Some(nix::unistd::Pid::from_raw(pid as i32))),
            Ok(pgid) if pgid.as_raw() as u32 == pid,
        )
    }
    #[cfg(windows)]
    {
        // Windows has no process groups; `force_kill` already terminates
        // the tree via `taskkill /T`.
        let _ = pid;
        false
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    use std::time::Duration;

    use serial_test::serial;
    use tokio::sync::watch;

    use crate::process::executor::{DevNullOutput, ExecutorHandle, ProcessExecutor};
    use crate::testutils::{HELPER_IGNORE_SIGTERM, HELPER_SLEEP, HELPER_VAR, helper_command};

    /// Bounded polling for a process appearing in, or leaving, the table.
    const POLL_INTERVAL: Duration = Duration::from_millis(50);
    const POLL_ATTEMPTS: u32 = 100;

    /// A PID far above any platform's `pid_max`, so its owner can never
    /// be found alive.
    const UNUSED_PID: u32 = 2_000_000_000;

    /// Shutdown receiver for spawns that never need to signal shutdown.
    /// The sender is leaked so `changed()` never fires.
    fn shutdown() -> watch::Receiver<()> {
        let (tx, rx) = watch::channel(());
        std::mem::forget(tx);
        rx
    }

    /// Spawn a helper carrying `token`, detached into its own session —
    /// the same position a daemonised process reaches by calling
    /// `setsid()`, and therefore outside any process group pod-runner
    /// would signal.
    async fn spawn_helper(token: &str, mode: &str) -> ExecutorHandle {
        let argv = helper_command();
        ProcessExecutor::new(&argv[0])
            .args(&argv[1..])
            .env(HELPER_VAR, mode)
            .env(KILL_TOKEN_VAR, token)
            .die_with_parent(false)
            .output(DevNullOutput)
            .spawn(shutdown())
            .await
            .expect("spawn helper")
    }

    /// Wait until the sweep can see a token under `prefix`.
    async fn wait_visible(prefix: &str) -> bool {
        for _ in 0..POLL_ATTEMPTS {
            if !Orphans::find(prefix).await.is_empty() {
                return true;
            }
            tokio::time::sleep(POLL_INTERVAL).await;
        }
        false
    }

    async fn wait_dead(pid: u32) -> bool {
        for _ in 0..POLL_ATTEMPTS {
            if !is_pid_alive(pid) {
                return true;
            }
            tokio::time::sleep(POLL_INTERVAL).await;
        }
        false
    }

    // -- token construction --------------------------------------------------

    #[test]
    fn owner_stamp_is_fixed_width_utc() {
        // 2026-08-22T14:32:05Z
        assert_eq!(owner_stamp(1_787_409_125), "20260822T143205Z");
        assert_eq!(owner_stamp(0), "19700101T000000Z");
        assert_eq!(owner_stamp(0).len(), UNKNOWN_STAMP.len());
    }

    #[test]
    fn owner_stamp_sorts_chronologically() {
        assert!(owner_stamp(1_787_409_125) > owner_stamp(1_787_409_124));
        assert!(owner_stamp(0) < owner_stamp(1));
    }

    #[test]
    fn runner_id_is_stable_and_names_this_process() {
        let id = runner_id();
        assert_eq!(id, runner_id(), "runner id must not change");
        let (pid, stamp) = id.split_once(OWNER_SEP).expect("pid@stamp");
        assert_eq!(pid, std::process::id().to_string());
        assert_eq!(stamp.len(), UNKNOWN_STAMP.len());
        assert_ne!(stamp, UNKNOWN_STAMP, "own start time must be readable");
    }

    #[test]
    fn container_token_extends_its_pod_prefix() {
        let prefix = pod_prefix("db", "uid-1");
        let token = container_token(&prefix, "postgres");
        assert!(token.starts_with(&prefix), "{token} must extend {prefix}");
        assert_eq!(token, format!("{}/db_uid-1/postgres", runner_id()));
    }

    #[test]
    fn runner_prefix_covers_every_pod_of_this_runner() {
        // The invariant the runner-wide shutdown sweep rests on: one
        // prefix selects every container of every pod.
        for (name, uid) in [("db", "uid-1"), ("api", "uid-2"), ("no-uid", "")] {
            let token = container_token(&pod_prefix(name, uid), "main");
            assert!(
                token.starts_with(&runner_prefix()),
                "{token} must be covered by {}",
                runner_prefix(),
            );
        }
    }

    #[test]
    fn pod_prefix_separates_pods_that_share_a_uid() {
        // A spec with no uid must not collapse into one prefix, or a
        // sweep for one pod would kill another pod's escapees.
        assert_ne!(pod_prefix("a", ""), pod_prefix("b", ""));
        assert_ne!(pod_prefix("same", "uid-1"), pod_prefix("same", "uid-2"));
    }

    // -- owner liveness ------------------------------------------------------

    #[test]
    fn owner_alive_recognises_this_runner() {
        let table = ProcessTable::load();
        assert!(table.owner_alive(&container_token(&pod_prefix("p", "u"), "c")));
    }

    #[test]
    fn owner_alive_rejects_a_vanished_runner() {
        let table = ProcessTable::load();
        let token = format!("{UNUSED_PID}{OWNER_SEP}20260822T143205Z{FIELD_SEP}p_u{FIELD_SEP}c");
        assert!(!table.owner_alive(&token));
    }

    #[test]
    fn owner_alive_rejects_a_recycled_pid() {
        // Our own PID, but not our start time: exactly what a restarted
        // pod-runner that landed on its predecessor's PID looks like.
        let table = ProcessTable::load();
        let token = format!(
            "{}{OWNER_SEP}{UNKNOWN_STAMP}{FIELD_SEP}p_u{FIELD_SEP}c",
            std::process::id()
        );
        assert!(!table.owner_alive(&token));
    }

    #[test]
    fn owner_alive_keeps_unverifiable_tokens() {
        // The sweep kills only what it can prove is orphaned.
        let table = ProcessTable::load();
        for token in ["no-owner-separator/p_u/c", "notapid@20260822T143205Z/p_u/c"] {
            assert!(table.owner_alive(token), "{token} must be kept");
        }
    }

    // -- sweeps --------------------------------------------------------------
    //
    // These spawn a real detached process, so they are Unix-only: the
    // escape they model (`setsid`, reparenting to PID 1) is a POSIX
    // mechanism, and Windows needs a job object rather than a sweep.
    //
    // Each of these spawns a real helper process, and the runner-wide
    // sweeps below reach *every* helper alive in this process — so they
    // all run serially with respect to each other. A pod-scoped sweep is
    // isolated by its own prefix; a runner-wide one cannot be.

    #[cfg(unix)]
    #[tokio::test]
    #[serial]
    async fn kill_orphans_kills_a_process_that_escaped_its_group() {
        use nix::unistd::{Pid as NixPid, getpgid};

        let prefix = pod_prefix("escapee-pod", "uid-1");
        let handle = spawn_helper(&container_token(&prefix, "main"), HELPER_SLEEP).await;
        let pid = handle.pid();
        assert!(wait_visible(&prefix).await, "token must be visible");

        // The helper leads its own group, so no group signal aimed at
        // whoever spawned it can reach it. Only the token can.
        let own = getpgid(None).expect("own pgid");
        let helper = getpgid(Some(NixPid::from_raw(pid as i32))).expect("helper pgid");
        assert_ne!(helper, own, "helper must have escaped the spawner's group");

        assert_eq!(kill_orphans(&prefix).await, 1);
        assert!(wait_dead(pid).await, "escapee must be dead");
    }

    #[cfg(unix)]
    #[tokio::test]
    #[serial]
    async fn queued_scans_all_see_the_same_orphan() {
        // Scans are serialised, so a mass teardown queues them. Every
        // caller must still get a snapshot of its own that contains the
        // escapee, and the queue must not deadlock.
        let prefix = pod_prefix("queued-pod", "uid-1");
        let handle = spawn_helper(&container_token(&prefix, "main"), HELPER_SLEEP).await;
        let pid = handle.pid();
        assert!(wait_visible(&prefix).await, "token must be visible");

        let scans = (0..8).map(|_| {
            let prefix = prefix.clone();
            tokio::spawn(async move { orphan_pids(&prefix).await })
        });
        for scan in scans {
            assert_eq!(scan.await.expect("scan task"), vec![pid]);
        }

        force_kill(pid);
        assert!(wait_dead(pid).await);
    }

    #[cfg(unix)]
    #[tokio::test]
    #[serial]
    async fn one_runner_wide_sweep_reaps_every_pod() {
        // What a shutdown does instead of sweeping per pod: a single scan
        // reaching the escapees of every pod this runner started.
        let first = spawn_helper(
            &container_token(&pod_prefix("pod-one", "uid-1"), "main"),
            HELPER_SLEEP,
        )
        .await;
        let second = spawn_helper(
            &container_token(&pod_prefix("pod-two", "uid-2"), "main"),
            HELPER_SLEEP,
        )
        .await;
        let prefix = runner_prefix();
        assert!(wait_visible(&prefix).await, "tokens must be visible");

        let orphans = Orphans::find(&prefix).await;
        let mut found: Vec<u32> = orphans.0.iter().map(|o| o.pid).collect();
        found.sort_unstable();
        let mut expected = vec![first.pid(), second.pid()];
        expected.sort_unstable();
        assert_eq!(found, expected, "one scan must see both pods");

        assert_eq!(orphans.force_kill(), 2);
        assert!(wait_dead(first.pid()).await);
        assert!(wait_dead(second.pid()).await);
    }

    #[cfg(unix)]
    #[tokio::test]
    #[serial]
    async fn force_kill_reaches_children_the_scan_never_saw() {
        // The corner case the single scan per kill leaves open: the
        // escapee forks after the scan, so the new child is in no orphan
        // set. It is in the escapee's *group*, though — the postgres
        // shape, where the postmaster detached and its backends did not —
        // so the force stage reaches it anyway.
        use crate::testutils::{HELPER_FORK_CHILD, HELPER_PID_FILE_VAR};

        let dir = crate::testutils::make_test_dir("process/sweep/tests/force_kill_children");
        let pid_file = dir.join("child.pid");
        let _ = std::fs::remove_file(&pid_file);

        let prefix = pod_prefix("forking-pod", "uid-1");
        let token = container_token(&prefix, "main");
        let argv = helper_command();
        let escapee = ProcessExecutor::new(&argv[0])
            .args(&argv[1..])
            .env(HELPER_VAR, HELPER_FORK_CHILD)
            .env(HELPER_PID_FILE_VAR, pid_file.display().to_string())
            .env(KILL_TOKEN_VAR, &token)
            .die_with_parent(false)
            .output(DevNullOutput)
            .spawn(shutdown())
            .await
            .expect("spawn escapee");

        let mut child_pid = None;
        for _ in 0..POLL_ATTEMPTS {
            if let Ok(text) = std::fs::read_to_string(&pid_file)
                && let Ok(pid) = text.trim().parse::<u32>()
            {
                child_pid = Some(pid);
                break;
            }
            tokio::time::sleep(POLL_INTERVAL).await;
        }
        let child_pid = child_pid.expect("escapee should have recorded its child");

        // Hand the force stage only the escapee, as a scan taken before
        // the fork would have.
        let found = Orphans::find(&prefix).await;
        let escaped = found
            .0
            .into_iter()
            .find(|orphan| orphan.pid == escapee.pid())
            .expect("escapee must carry the token");
        let only_the_parent = Orphans(vec![escaped]);

        assert_eq!(only_the_parent.force_kill(), 1);
        assert!(wait_dead(escapee.pid()).await, "escapee");
        assert!(
            wait_dead(child_pid).await,
            "child {child_pid} was never in the orphan set and must still die with its group",
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    #[serial]
    async fn kill_orphans_leaves_other_pods_alone() {
        let mine = pod_prefix("pod-a", "uid-a");
        let other = pod_prefix("pod-b", "uid-b");
        let handle = spawn_helper(&container_token(&mine, "main"), HELPER_SLEEP).await;
        let pid = handle.pid();
        assert!(wait_visible(&mine).await, "token must be visible");

        assert_eq!(kill_orphans(&other).await, 0);
        assert!(is_pid_alive(pid), "another pod's sweep must not kill it");

        force_kill(pid);
        assert!(wait_dead(pid).await);
    }

    #[cfg(unix)]
    #[tokio::test]
    #[serial]
    async fn terminate_orphans_is_graceful_and_kill_orphans_escalates() {
        let prefix = pod_prefix("stubborn-pod", "uid-1");
        let handle = spawn_helper(&container_token(&prefix, "main"), HELPER_IGNORE_SIGTERM).await;
        let pid = handle.pid();
        assert!(wait_visible(&prefix).await, "token must be visible");

        // A process that ignores SIGTERM proves the first stage really
        // sends SIGTERM and not SIGKILL.
        assert_eq!(terminate_orphans(&prefix).await, 1);
        tokio::time::sleep(POLL_INTERVAL * 4).await;
        assert!(is_pid_alive(pid), "SIGTERM is ignored by this helper");

        assert_eq!(kill_orphans(&prefix).await, 1);
        assert!(wait_dead(pid).await, "SIGKILL must end it");
    }

    #[cfg(unix)]
    #[tokio::test]
    #[serial]
    async fn boot_sweep_kills_orphans_of_dead_runners_only() {
        // Sweeps the whole host, so it also reclaims escapees left by
        // earlier runs — which is the point of it.
        let live_prefix = pod_prefix("live-pod", "uid-1");
        let live = spawn_helper(&container_token(&live_prefix, "main"), HELPER_SLEEP).await;
        let vanished = spawn_helper(
            &format!("{UNUSED_PID}{OWNER_SEP}20260822T143205Z{FIELD_SEP}gone_uid{FIELD_SEP}main"),
            HELPER_SLEEP,
        )
        .await;
        let recycled = spawn_helper(
            &format!(
                "{}{OWNER_SEP}{UNKNOWN_STAMP}{FIELD_SEP}stale_uid{FIELD_SEP}main",
                std::process::id()
            ),
            HELPER_SLEEP,
        )
        .await;
        assert!(wait_visible(&live_prefix).await, "token must be visible");

        let killed = kill_dead_runner_orphans().await;
        assert!(killed >= 2, "both stale owners must be swept, got {killed}");
        assert!(wait_dead(vanished.pid()).await, "vanished owner");
        assert!(wait_dead(recycled.pid()).await, "recycled pid");
        assert!(
            is_pid_alive(live.pid()),
            "this runner's own containers must survive"
        );

        force_kill(live.pid());
        assert!(wait_dead(live.pid()).await);
    }
}
