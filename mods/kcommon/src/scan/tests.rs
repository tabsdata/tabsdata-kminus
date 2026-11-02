// Copyright 2026 Tabsdata Inc.

use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use serial_test::serial;

use super::*;
use crate::token::{KM_KILL_TOKEN_VAR, Run};

/// Environment variable selecting [`scan_test_helper_process`] when the
/// test binary is re-executed.
///
/// The helper is this binary rather than a shell because macOS hides the
/// environment of SIP platform binaries even from the same uid: a token
/// passed to `/bin/sh` is invisible to the very scan under test. A
/// cargo-built binary is visible, like the binaries a real component runs.
const HELPER_VAR: &str = "TD_KCOMMON_SCAN_TEST_HELPER";

/// Substring selecting the helper test when the binary is re-executed.
const HELPER_FILTER: &str = "scan_test_helper_process";

/// How long a helper lives if the test that spawned it fails to kill it.
const HELPER_TTL: Duration = Duration::from_secs(30);

const POLL_INTERVAL: Duration = Duration::from_millis(20);
const POLL_TIMEOUT: Duration = Duration::from_secs(5);

/// A PID far above any platform's `pid_max`, so its owner can never be
/// found alive.
const UNUSED_PID: u32 = 2_000_000_000;

/// The helper process. Without [`HELPER_VAR`] this is a no-op, so an
/// ordinary run of the suite passes through it instantly.
#[test]
fn scan_test_helper_process() {
    if std::env::var(HELPER_VAR).is_err() {
        return;
    }
    std::thread::sleep(HELPER_TTL);
}

fn spawn_helper(token: &str) -> Child {
    let exe = std::env::current_exe().expect("test binary path");
    Command::new(exe)
        .arg(HELPER_FILTER)
        .env(HELPER_VAR, "1")
        .env(KM_KILL_TOKEN_VAR, token)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn helper")
}

/// Wait until a fresh scan can see exactly `token`.
fn wait_visible(token: &str) -> bool {
    let deadline = Instant::now() + POLL_TIMEOUT;
    while Instant::now() < deadline {
        if !Scan::load()
            .unwrap()
            .under(KM_KILL_TOKEN_VAR, token)
            .is_empty()
        {
            return true;
        }
        std::thread::sleep(POLL_INTERVAL);
    }
    false
}

/// Reap `child` and report whether it was killed rather than exiting on
/// its own terms. Asserted on the wait status: until reaped, an exited
/// child is a zombie and still holds its number.
fn killed(child: &mut Child) -> bool {
    let status = child.wait().expect("reap helper");
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;
        status.signal().is_some()
    }
    #[cfg(windows)]
    {
        !status.success()
    }
}

/// A run token rooted in a live owner — this process.
fn own_run() -> Run {
    Run::current()
}

/// A token whose owner can never be found running.
fn dead_owner_token(scope: &str) -> String {
    format!("{UNUSED_PID}@20260823T173004Z.000/{scope}")
}

// The sweeps read and signal across the whole host, so tests spawning
// token-carrying helpers cannot run alongside them. None asserts on how
// many a sweep killed — that count is the host's, not the test's; each
// test owns the fate of its own helper.

#[test]
#[serial(kcommon_sweep)]
fn under_selects_by_prefix_and_excludes_wider_scopes() {
    let run = own_run();
    let pod_a = run.scope("pod-a").as_str().to_string();
    let pod_b = run.scope("pod-b").as_str().to_string();
    let mut a = spawn_helper(&pod_a);
    let mut b = spawn_helper(&pod_b);
    assert!(wait_visible(&pod_a) && wait_visible(&pod_b));

    let scan = Scan::load().unwrap();
    let under_a = scan.under(KM_KILL_TOKEN_VAR, run.scope("pod-a").as_prefix());
    assert!(under_a.iter().any(|f| f.pid == a.id()));
    assert!(
        !under_a.iter().any(|f| f.pid == b.id()),
        "a pod prefix must not select a sibling pod",
    );

    // The run prefix selects both.
    let under_run = scan.under(KM_KILL_TOKEN_VAR, run.as_prefix());
    for pid in [a.id(), b.id()] {
        assert!(under_run.iter().any(|f| f.pid == pid));
    }

    let _ = a.kill();
    let _ = a.wait();
    let _ = b.kill();
    let _ = b.wait();
}

#[test]
#[serial(kcommon_sweep)]
fn owner_liveness_separates_this_run_from_a_dead_one() {
    let scan_before = Scan::load().unwrap();
    assert!(scan_before.owner_alive(own_run().as_str()));
    assert!(!scan_before.owner_alive(&dead_owner_token("x")));
    // Unparseable is "cannot prove stale", never "stale".
    assert!(scan_before.owner_alive("garbage-with-no-owner"));
}

#[test]
#[serial(kcommon_sweep)]
fn strays_of_dead_owners_takes_the_dead_and_spares_the_living() {
    let dead_token = dead_owner_token("main");
    let live_token = own_run().scope("main").as_str().to_string();
    let mut stray = spawn_helper(&dead_token);
    let mut live = spawn_helper(&live_token);
    assert!(wait_visible(&dead_token) && wait_visible(&live_token));

    kill_strays_of_dead_runs(KM_KILL_TOKEN_VAR);

    assert!(killed(&mut stray), "a dead run's stray must not survive");
    assert!(
        signal::is_pid_alive(live.id()),
        "a live owner's process is not a stray",
    );
    let _ = live.kill();
    let _ = live.wait();
}

#[test]
#[serial(kcommon_sweep)]
fn kill_found_reverifies_before_signalling() {
    let token = dead_owner_token("reverify");
    let mut helper = spawn_helper(&token);
    assert!(wait_visible(&token));
    let snapshot = Scan::load().unwrap().under(KM_KILL_TOKEN_VAR, &token);
    assert_eq!(snapshot.len(), 1);

    // The helper dies between the snapshot and the kill; its number is now
    // the OS's to reuse. The re-read must notice and signal nothing.
    let _ = helper.kill();
    assert!(killed(&mut helper));
    assert_eq!(
        kill_found(KM_KILL_TOKEN_VAR, &snapshot),
        0,
        "an aged snapshot entry that no longer verifies must not be signalled",
    );
}

#[test]
#[serial(kcommon_sweep)]
fn kill_until_clean_reaps_everything_under_a_prefix() {
    let run = own_run();
    let token = run.scope("until-clean").as_str().to_string();
    let mut helper = spawn_helper(&token);
    assert!(wait_visible(&token));

    let swept = kill_until_clean(KM_KILL_TOKEN_VAR, run.scope("until-clean").as_prefix(), 3);
    assert!(swept >= 1);
    assert!(killed(&mut helper));

    // Converged: a further round finds nothing.
    assert_eq!(
        kill_until_clean(KM_KILL_TOKEN_VAR, run.scope("until-clean").as_prefix(), 3),
        0
    );
}

#[test]
#[serial(kcommon_sweep)]
fn still_carries_confirms_the_exact_token_and_only_that() {
    let token = own_run().scope("carrier").as_str().to_string();
    let mut helper = spawn_helper(&token);
    assert!(wait_visible(&token));

    assert!(still_carries(helper.id(), KM_KILL_TOKEN_VAR, &token));
    assert!(
        !still_carries(helper.id(), KM_KILL_TOKEN_VAR, "some-other-token"),
        "a different token is a different identity",
    );
    assert!(!still_carries(UNUSED_PID, KM_KILL_TOKEN_VAR, &token));

    let _ = helper.kill();
    let _ = helper.wait();
}

#[test]
#[serial(kcommon_sweep)]
fn terminate_found_asks_gracefully() {
    let token = dead_owner_token("graceful");
    let mut helper = spawn_helper(&token);
    assert!(wait_visible(&token));
    let found = Scan::load().unwrap().under(KM_KILL_TOKEN_VAR, &token);

    assert_eq!(terminate_found(&found), 1);

    // SIGTERM's default disposition terminates the helper; the wait status
    // shows it went by signal, not by its own exit.
    #[cfg(unix)]
    assert!(killed(&mut helper), "SIGTERM should have ended the helper");
    #[cfg(windows)]
    {
        let _ = helper.kill();
        let _ = helper.wait();
    }
}
