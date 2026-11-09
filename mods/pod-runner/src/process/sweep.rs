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
//! Every container is spawned with [`PR_KILL_TOKEN_VAR`] in its
//! environment. The grammar, the scan and the signalling are
//! [`kcommon::token`] and [`kcommon::scan`]'s; what is pod-runner's is the
//! scope layering below the owner — `<runner>/<pod>/<container>` —
//! and the async throttling around a scan, which exists because a mass
//! teardown starts one sweep per pod and a runtime is on hand to keep
//! them from all walking the process table at once.
//!
//! No run-token file is persisted: reclamation after a dead pod-runner
//! rides on the owner field alone ([`kill_dead_runner_orphans`]), which
//! needs no file — a run is over exactly when no live process has its pid
//! *and* start time.
//!
//! Three sweeps:
//!
//! - [`terminate_orphans`] when a pod kill starts, so an escapee shuts
//!   down within the same grace period as its container.
//! - [`kill_orphans`] once that grace has passed, and again when a pod
//!   reaches a terminal phase however it got there.
//! - [`kill_dead_runner_orphans`] at startup, for escapees left behind by
//!   a previous pod-runner. An escapee was never recorded in a PID file,
//!   so a dead owner's token is the only thing that still names it.

use std::sync::OnceLock;
use std::time::Duration;

use kcommon::scan::{Found, Scan, kill_found, kill_strays_of_dead_runs, terminate_found};
use kcommon::signal::is_pid_alive;
pub use kcommon::token::PR_KILL_TOKEN_VAR;
use kcommon::token::Run;
use tracing::{debug, warn};

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

/// How often [`Orphans::await_exit`] rechecks the set it is waiting on.
const ORPHAN_EXIT_POLL: Duration = Duration::from_millis(50);

/// Permits for [`MAX_CONCURRENT_SCANS`].
static SCAN_SLOTS: tokio::sync::Semaphore = tokio::sync::Semaphore::const_new(MAX_CONCURRENT_SCANS);

/// This pod-runner's token root: `<pid>@<start-stamp>.<millis>`. The
/// start stamp is what keeps a restarted runner that landed on its
/// predecessor's PID from reading that run's orphans as its own.
/// Resolved once, so every token this process writes agrees.
fn runner_run() -> &'static Run {
    static RUNNER: OnceLock<Run> = OnceLock::new();
    RUNNER.get_or_init(Run::current)
}

/// Token prefix shared by every container of one pod:
/// `<runner-run>/<pod-name>_<pod-uid>/`.
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
    format!("{}/{pod_name}_{pod_uid}/", runner_run().as_str())
}

/// The token for one container of the pod identified by `pod_prefix`.
pub fn container_token(pod_prefix: &str, container: &str) -> String {
    format!("{pod_prefix}{container}")
}

/// Token prefix shared by every container of every pod this runner
/// started: `<runner-run>/`.
///
/// The token's fields nest, so a prefix at any level selects everything
/// below it. That is what lets a runner-wide teardown sweep once instead
/// of once per pod.
pub fn runner_prefix() -> String {
    format!("{}/", runner_run().as_str())
}

/// Run a blocking process-table operation off the runtime.
///
/// The scan is a syscall-bound walk of every process, so it runs on the
/// blocking pool rather than stalling a runtime worker; concurrency is
/// capped at [`MAX_CONCURRENT_SCANS`].
async fn on_scan_slot<T, F>(op: F) -> Option<T>
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    // A closed semaphore is impossible for a static that is never
    // closed; if it somehow were, scanning unthrottled beats not
    // sweeping at all.
    let _slot = SCAN_SLOTS.acquire().await.ok();

    match tokio::task::spawn_blocking(op).await {
        Ok(result) => Some(result),
        Err(e) => {
            warn!(error = %e, "Orphan sweep scan failed");
            None
        }
    }
}

/// The escaped processes selected by one scan.
///
/// A kill signals the same set twice — SIGTERM, then SIGKILL for whatever
/// outlived the grace period — so the set is found once and reused rather
/// than rescanned. A process that escapes *during* the grace is missed
/// here and caught by the sweep at the end of the pod's orchestrator,
/// which scans afresh.
pub struct Orphans {
    found: Vec<Found>,
    /// When [`Self::terminate`] signalled the set, so [`Self::await_exit`]
    /// can count the grace from there rather than from its own call.
    terminated_at: Option<tokio::time::Instant>,
}

impl Orphans {
    /// Scan for every escaped process whose token starts with `prefix` —
    /// a pod's [`pod_prefix`] for one pod, [`runner_prefix`] for all of
    /// them.
    pub async fn find(prefix: &str) -> Self {
        let prefix = prefix.to_string();
        Self {
            found: on_scan_slot(move || match Scan::load() {
                Ok(scan) => scan.under(PR_KILL_TOKEN_VAR, &prefix),
                Err(error) => {
                    warn!(prefix, %error, "Cannot read the process table; no orphans found");
                    Vec::new()
                }
            })
            .await
            .unwrap_or_default(),
            terminated_at: None,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.found.is_empty()
    }

    /// Wait until every process in the set is gone, or its `grace` since
    /// [`Self::terminate`] runs out.
    ///
    /// The stage between [`Self::terminate`] and [`Self::force_kill`]: an
    /// escapee is not the container's main process, so nothing else waits
    /// for it. A server that detaches to own its own shutdown — `pg_ctl`
    /// daemonizing an embedded postgres — needs that wait to finish, or
    /// the force stage arrives before it has released anything.
    ///
    /// The grace is counted from the SIGTERM, not from this call: the
    /// containers are killed in between, and an escapee that has already
    /// outlived its grace while that happened does not get a second one.
    pub async fn await_exit(&self, grace: Duration) {
        if self.found.is_empty() {
            return;
        }
        let Some(deadline) = self.terminated_at.map(|at| at + grace) else {
            return;
        };
        while self.found.iter().any(|found| is_pid_alive(found.pid)) {
            if tokio::time::Instant::now() >= deadline {
                return;
            }
            tokio::time::sleep(ORPHAN_EXIT_POLL).await;
        }
    }

    /// SIGTERM each one, so it shuts down inside the same grace period
    /// its container gets. Returns how many were signalled.
    pub fn terminate(&mut self) -> usize {
        self.terminated_at = Some(tokio::time::Instant::now());
        let signalled = terminate_found(&self.found);
        if signalled > 0 {
            debug!(
                signalled,
                "SIGTERM sent to processes that escaped their container's process group",
            );
        }
        signalled
    }

    /// SIGKILL whichever still carry the token this set recorded for them
    /// — re-verified per process, the set being a grace period old by
    /// now. Reported at warning level: a container leaked a process, and
    /// that is worth seeing. Returns how many were killed.
    pub fn force_kill(&self) -> usize {
        let killed = kill_found(PR_KILL_TOKEN_VAR, &self.found);
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
    on_scan_slot(|| kill_strays_of_dead_runs(PR_KILL_TOKEN_VAR))
        .await
        .unwrap_or(0)
}

/// PIDs currently carrying a token under `pod_prefix`.
///
/// Test-only: the sweeps act on exactly this set, and an end-to-end test
/// has to observe an escapee before the sweep reaps it.
#[cfg(all(test, unix))]
pub(crate) async fn orphan_pids(pod_prefix: &str) -> Vec<u32> {
    Orphans::find(pod_prefix)
        .await
        .found
        .into_iter()
        .map(|found| found.pid)
        .collect()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(unix)]
    use std::time::Duration;

    use kcommon::identity::{OWNER_SEP, UNKNOWN_STAMP};
    #[cfg(unix)]
    use kcommon::signal::{force_kill, is_pid_alive};
    #[cfg(unix)]
    use serial_test::serial;
    #[cfg(unix)]
    use tokio::sync::watch;

    #[cfg(unix)]
    use crate::process::executor::{DevNullOutput, ExecutorHandle, ProcessExecutor};
    #[cfg(unix)]
    use crate::testutils::{HELPER_IGNORE_STOP, HELPER_SLEEP, HELPER_VAR, helper_command};

    /// Bounded polling for a process appearing in, or leaving, the table.
    #[cfg(unix)]
    const POLL_INTERVAL: Duration = Duration::from_millis(50);
    #[cfg(unix)]
    const POLL_ATTEMPTS: u32 = 100;

    /// A PID far above any platform's `pid_max`, so its owner can never
    /// be found alive.
    const UNUSED_PID: u32 = 2_000_000_000;

    /// A full container token under an owner that can never be found
    /// running — the shape a previous, dead run's token has.
    fn dead_runner_token(pod: &str, container: &str) -> String {
        format!("{UNUSED_PID}{OWNER_SEP}20260822T143205Z.000/{pod}/{container}")
    }

    /// Shutdown receiver for spawns that never need to signal shutdown.
    /// The sender is leaked so `changed()` never fires.
    #[cfg(unix)]
    fn shutdown() -> watch::Receiver<()> {
        let (tx, rx) = watch::channel(());
        std::mem::forget(tx);
        rx
    }

    /// Spawn a helper carrying `token`, detached into its own session —
    /// the same position a daemonised process reaches by calling
    /// `setsid()`, and therefore outside any process group pod-runner
    /// would signal.
    #[cfg(unix)]
    async fn spawn_helper(token: &str, mode: &str) -> ExecutorHandle {
        let argv = helper_command();
        ProcessExecutor::new(&argv[0])
            .args(&argv[1..])
            .env(HELPER_VAR, mode)
            .env(PR_KILL_TOKEN_VAR, token)
            .die_with_parent(false)
            .output(DevNullOutput)
            .spawn(shutdown())
            .await
            .expect("spawn helper")
    }

    /// Wait until the sweep can see a token under `prefix`.
    #[cfg(unix)]
    async fn wait_visible(prefix: &str) -> bool {
        for _ in 0..POLL_ATTEMPTS {
            if !Orphans::find(prefix).await.is_empty() {
                return true;
            }
            tokio::time::sleep(POLL_INTERVAL).await;
        }
        false
    }

    #[cfg(unix)]
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
    fn runner_run_is_stable_and_names_this_process() {
        // The grammar itself is kcommon's contract and is tested there;
        // what matters here is that a runner's tokens all carry one
        // identity, and that it is this process's.
        let run = runner_run();
        assert_eq!(run.as_str(), runner_run().as_str(), "must not change");
        let owner = kcommon::token::owner_of(run.as_str());
        let (pid, stamp) = kcommon::identity::parse_owner(owner).expect("pid@stamp");
        assert_eq!(pid, std::process::id());
        assert!(
            !kcommon::identity::is_unknown_stamp(stamp),
            "own start time must be readable"
        );
    }

    #[test]
    fn container_token_extends_its_pod_prefix() {
        let prefix = pod_prefix("db", "uid-1");
        let token = container_token(&prefix, "postgres");
        assert!(token.starts_with(&prefix), "{token} must extend {prefix}");
        assert_eq!(
            token,
            format!("{}/db_uid-1/postgres", runner_run().as_str())
        );
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
    fn owner_alive_recognises_this_runner_and_rejects_dead_ones() {
        let scan = Scan::load().unwrap();
        assert!(scan.owner_alive(&container_token(&pod_prefix("p", "u"), "c")));
        assert!(!scan.owner_alive(&dead_runner_token("p_u", "c")));
        // Our own PID, but not our start time: exactly what a restarted
        // pod-runner that landed on its predecessor's PID looks like.
        let recycled = format!("{}{OWNER_SEP}{UNKNOWN_STAMP}.000/p_u/c", std::process::id());
        assert!(!scan.owner_alive(&recycled));
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
        let mut found: Vec<u32> = orphans.found.iter().map(|o| o.pid).collect();
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
            .env(PR_KILL_TOKEN_VAR, &token)
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
            .found
            .into_iter()
            .find(|orphan| orphan.pid == escapee.pid())
            .expect("escapee must carry the token");
        let only_the_parent = Orphans {
            found: vec![escaped],
            terminated_at: None,
        };

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
        let handle = spawn_helper(&container_token(&prefix, "main"), HELPER_IGNORE_STOP).await;
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
    async fn stale_snapshot_entries_are_reverified_not_signalled() {
        // The force stage acts on a set found a grace period earlier. A
        // process that exited in between may have handed its number to
        // something else, so an entry that no longer carries its token
        // must be left alone, whoever holds the number now.
        let prefix = pod_prefix("stale-pod", "uid-1");
        let handle = spawn_helper(&container_token(&prefix, "main"), HELPER_SLEEP).await;
        let pid = handle.pid();
        assert!(wait_visible(&prefix).await, "token must be visible");

        let snapshot = Orphans::find(&prefix).await;
        assert!(!snapshot.is_empty());

        // The escapee dies between the snapshot and the kill.
        force_kill(pid);
        assert!(wait_dead(pid).await);

        assert_eq!(
            snapshot.force_kill(),
            0,
            "a snapshot entry that no longer verifies must not be signalled",
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    #[serial]
    async fn boot_sweep_kills_orphans_of_dead_runners_only() {
        // Sweeps the whole host, so it also reclaims escapees left by
        // earlier runs — which is the point of it.
        let live_prefix = pod_prefix("live-pod", "uid-1");
        let live = spawn_helper(&container_token(&live_prefix, "main"), HELPER_SLEEP).await;
        let vanished = spawn_helper(&dead_runner_token("gone_uid", "main"), HELPER_SLEEP).await;
        let recycled = spawn_helper(
            &format!(
                "{}{OWNER_SEP}{UNKNOWN_STAMP}.000/stale_uid/main",
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
