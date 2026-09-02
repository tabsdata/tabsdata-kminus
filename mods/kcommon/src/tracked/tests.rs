// Copyright 2026 Tabsdata Inc.

use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use super::*;
use crate::token::{KM_KILL_TOKEN_VAR, Run};

/// Environment variable selecting [`tracked_test_helper_process`] when the
/// test binary is re-executed. A cargo-built helper rather than a shell,
/// so its environment stays readable on macOS (SIP hides a platform
/// shell's) and its signal handling is ours to script.
const HELPER_VAR: &str = "TD_KCOMMON_TRACKED_TEST_HELPER";

/// Helper mode: sleep, dying on SIGTERM like any ordinary process.
const MODE_SLEEP: &str = "sleep";

/// Helper mode: ignore SIGTERM, so only the force stage ends it.
const MODE_IGNORE_SIGTERM: &str = "ignore-sigterm";

/// Helper mode: write the kill-token variable's value to the file named
/// by [`OUT_VAR`], then exit 0.
const MODE_PRINT_TOKEN: &str = "print-token";

/// Helper mode: exit with code 7 immediately.
const MODE_EXIT_7: &str = "exit-7";

const OUT_VAR: &str = "TD_KCOMMON_TRACKED_TEST_OUT";
const HELPER_FILTER: &str = "tracked_test_helper_process";
const HELPER_TTL: Duration = Duration::from_secs(30);

/// The helper process. Without [`HELPER_VAR`] this is a no-op, so an
/// ordinary run of the suite passes through it instantly.
#[test]
fn tracked_test_helper_process() {
    let Ok(mode) = std::env::var(HELPER_VAR) else {
        return;
    };
    match mode.as_str() {
        MODE_EXIT_7 => std::process::exit(7),
        MODE_PRINT_TOKEN => {
            let token = std::env::var(KM_KILL_TOKEN_VAR).unwrap_or_default();
            let out = std::env::var(OUT_VAR).expect("out file");
            std::fs::write(out, token).expect("write token");
            std::process::exit(0);
        }
        MODE_IGNORE_SIGTERM => {
            #[cfg(unix)]
            // SAFETY: set before anything in this process relies on
            // SIGTERM; the helper does nothing but sleep.
            unsafe {
                nix::sys::signal::signal(
                    nix::sys::signal::Signal::SIGTERM,
                    nix::sys::signal::SigHandler::SigIgn,
                )
                .expect("ignore SIGTERM");
            }
            // Only now is the helper actually deaf to SIGTERM: a stop
            // issued before this point would land on the default
            // disposition and end the test's premise, not its subject.
            if let Ok(out) = std::env::var(OUT_VAR) {
                std::fs::write(out, "deaf").expect("write ready marker");
            }
            std::thread::sleep(HELPER_TTL);
        }
        _ => std::thread::sleep(HELPER_TTL),
    }
}

fn helper_command(mode: &str) -> Command {
    let exe = std::env::current_exe().expect("test binary path");
    let mut cmd = Command::new(exe);
    cmd.arg(HELPER_FILTER)
        .env(HELPER_VAR, mode)
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    cmd
}

fn run() -> Run {
    // Rooted in this live process, so the host-wide dead-owner sweeps in
    // other tests can never mistake these children for strays.
    Run::current()
}

fn spawn(mode: &str, scope: &str) -> Tracked {
    Tracked::spawn(helper_command(mode), KM_KILL_TOKEN_VAR, &run().scope(scope))
        .expect("spawn tracked helper")
}

#[test]
fn a_child_that_exits_settles_with_its_code() {
    let tracked = spawn(MODE_EXIT_7, "exits");

    assert_eq!(tracked.wait(), Exit::Code(7));
    assert_eq!(tracked.state(), State::Gone(Exit::Code(7)));
    assert!(tracked.has_exited());
    // Settled: asking again cannot change the answer.
    assert_eq!(tracked.wait(), Exit::Code(7));
}

#[test]
fn a_running_child_reads_as_running() {
    let tracked = spawn(MODE_SLEEP, "runs");
    assert_eq!(tracked.state(), State::Running);
    assert!(!tracked.has_exited());
    assert_eq!(tracked.wait_timeout(Duration::from_millis(100)), None);

    tracked.stop(Duration::ZERO);
}

#[test]
fn the_token_reaches_the_child_environment() {
    let out = std::env::temp_dir().join(format!("tracked-token-{}", std::process::id()));
    let mut cmd = helper_command(MODE_PRINT_TOKEN);
    cmd.env(OUT_VAR, &out);
    let token = run().scope("prints");
    let tracked = Tracked::spawn(cmd, KM_KILL_TOKEN_VAR, &token).expect("spawn");

    assert_eq!(tracked.wait(), Exit::Code(0));
    assert_eq!(
        std::fs::read_to_string(&out).expect("token file"),
        token.as_str(),
        "the child must carry exactly the token it was spawned under",
    );
    assert_eq!(tracked.token(), token.as_str());
    std::fs::remove_file(&out).ok();
}

#[test]
fn stop_asks_first_and_a_cooperative_child_goes_gracefully() {
    let tracked = spawn(MODE_SLEEP, "graceful");
    // The graceful ask is delivered on Windows by re-exec'ing the current
    // binary as a console-event helper (see signal::ctrl_break_helper_hook).
    // libtest owns a test binary's main, so the hook never runs here and the
    // stop can only reach the force stage: the grace is burned by design, not
    // by a regression. Leave none to burn.
    #[cfg(windows)]
    let grace = Duration::ZERO;
    #[cfg(not(windows))]
    let grace = Duration::from_secs(30);

    let started = Instant::now();
    let exit = tracked.stop(grace);

    // Died to the request, not to the grace expiring.
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "a cooperative child must not burn its grace, took {:?}",
        started.elapsed()
    );
    #[cfg(unix)]
    assert_eq!(exit, Exit::Signal(15), "ended by the SIGTERM it was sent");
    #[cfg(windows)]
    assert!(matches!(exit, Exit::Code(_)));
}

#[cfg(unix)]
#[test]
fn stop_escalates_when_the_grace_is_ignored() {
    let ready = std::env::temp_dir().join(format!("tracked-deaf-{}", std::process::id()));
    std::fs::remove_file(&ready).ok();
    let mut cmd = helper_command(MODE_IGNORE_SIGTERM);
    cmd.env(OUT_VAR, &ready);
    let tracked = Tracked::spawn(cmd, KM_KILL_TOKEN_VAR, &run().scope("stubborn")).expect("spawn");

    // Not until the helper is provably deaf: a SIGTERM sent while it is
    // still starting up would meet the default disposition instead.
    let deadline = Instant::now() + Duration::from_secs(5);
    while !ready.exists() {
        assert!(Instant::now() < deadline, "helper never became deaf");
        std::thread::sleep(Duration::from_millis(10));
    }
    std::fs::remove_file(&ready).ok();

    let grace = Duration::from_millis(300);

    let started = Instant::now();
    let exit = tracked.stop(grace);

    assert!(
        started.elapsed() >= grace,
        "the child ignores SIGTERM, so the grace must run out first",
    );
    assert_eq!(exit, Exit::Signal(9), "ended by the escalation");
}

#[test]
fn stop_with_zero_grace_kills_outright() {
    let tracked = spawn(MODE_SLEEP, "no-grace");
    let started = Instant::now();
    let exit = tracked.stop(Duration::ZERO);

    assert!(started.elapsed() < Duration::from_secs(5));
    #[cfg(unix)]
    assert_eq!(exit, Exit::Signal(9), "zero grace skips the asking");
    #[cfg(windows)]
    assert!(matches!(exit, Exit::Code(_)));
}

#[test]
fn stop_on_a_settled_child_returns_at_once_and_changes_nothing() {
    let tracked = spawn(MODE_EXIT_7, "settled");
    assert_eq!(tracked.wait(), Exit::Code(7));

    // Its number is the OS's to reuse by now; a stop must neither wait a
    // grace out nor signal anything — the settled outcome is the answer.
    let started = Instant::now();
    assert_eq!(tracked.stop(Duration::from_secs(90)), Exit::Code(7));
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "took {:?}",
        started.elapsed()
    );
}

#[test]
fn every_clone_sees_the_same_outcome() {
    let tracked = spawn(MODE_SLEEP, "cloned");
    let view = tracked.clone();

    let waiter = std::thread::spawn(move || view.wait());
    let exit = tracked.stop(Duration::ZERO);

    assert_eq!(
        waiter.join().expect("waiter thread"),
        exit,
        "a view waiting elsewhere observes exactly the stop's outcome",
    );
}

#[test]
fn repeated_stops_do_not_restart_the_clock() {
    let tracked = spawn(MODE_SLEEP, "idempotent");
    let view = tracked.clone();

    // Two stops from two views: one clock, one outcome, no panic, no
    // second signal to a number that may no longer be the child's.
    let first = tracked.stop(Duration::ZERO);
    let second = view.stop(Duration::from_secs(90));
    assert_eq!(first, second);
}
