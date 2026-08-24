// Copyright 2026 Tabsdata Inc.

use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use kcommon::token::{KM_KILL_TOKEN_VAR, Run};
use kcommon::tracked::{Exit, Tracked};

use super::*;
use crate::system::pid::write_pid;

// No environment scanning happens here, so plain shell children are fine
// (the SIP readback limitation only affects scans).

fn run() -> Run {
    // Rooted in this live process, so host-wide dead-owner sweeps in other
    // tests can never mistake these children for strays.
    Run::current()
}

fn track(script: &str, scope: &str) -> Tracked {
    let mut cmd = Command::new("sh");
    cmd.args(["-c", script])
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    Tracked::spawn(cmd, KM_KILL_TOKEN_VAR, &run().scope(scope)).expect("spawn tracked")
}

fn wait_for_failure(components: &Components) -> Option<(&'static str, Exit)> {
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        if let Some(failure) = components.first_failed() {
            return Some(failure);
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    None
}

fn tempdir() -> std::path::PathBuf {
    static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!("km-components-{}-{}", std::process::id(), n));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

#[test]
fn nothing_registered_reports_nothing() {
    assert_eq!(Components::new().first_failed(), None);
}

#[test]
fn no_failure_while_registered_components_run() {
    let components = Components::new();
    components.register("alpha", track("sleep 30", "alpha"));
    assert_eq!(components.first_failed(), None);
    components.teardown(std::path::Path::new("/nonexistent/km/pids"));
}

#[test]
fn a_dead_component_is_reported_with_its_exit() {
    let components = Components::new();
    components.register("beta", track("exit 3", "beta"));

    let (name, exit) = wait_for_failure(&components).expect("failure should be reported");
    assert_eq!(name, "beta");
    // The outcome is the point of holding the view: a PID check could only
    // say the process is absent, never how it ended.
    assert_eq!(exit, Exit::Code(3));
}

#[test]
fn the_first_dead_component_in_start_order_is_reported() {
    // Both dead, so the answer turns purely on registration order: the
    // component started first is the one the others depend on, and naming
    // it keeps a cascade from being reported against the wrong component.
    let components = Components::new();
    components.register("first", track("exit 1", "first"));
    components.register("second", track("exit 2", "second"));

    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        match components.first_failed() {
            Some(("first", exit)) => {
                assert_eq!(exit, Exit::Code(1));
                break;
            }
            // The second may settle first; keep polling until order wins.
            Some(("second", _)) | None if Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(20))
            }
            other => panic!("unexpected report: {other:?}"),
        }
    }
}

#[test]
fn pid_names_the_registered_component() {
    let components = Components::new();
    let tracked = track("sleep 30", "gamma");
    let pid = tracked.pid();
    components.register("gamma", tracked);

    assert_eq!(components.pid("gamma"), Some(pid));
    assert_eq!(components.pid("absent"), None);

    components.teardown(std::path::Path::new("/nonexistent/km/pids"));
}

#[test]
fn teardown_stops_everything_reports_names_and_empties_the_registry() {
    let dir = tempdir();
    let components = Components::new();
    let one = track("sleep 30", "one");
    let two = track("sleep 30", "two");
    write_pid(&dir, "one", one.pid());
    write_pid(&dir, "two", two.pid());
    components.register("one", one.clone());
    components.register("two", two.clone());

    let handled = components.teardown(&dir);

    // Reverse start order, so nothing outlives what it depends on.
    assert_eq!(handled, vec!["two", "one"]);
    assert!(one.has_exited() && two.has_exited());
    assert!(!dir.join(format!("{}.one", one.pid())).exists());
    assert!(!dir.join(format!("{}.two", two.pid())).exists());
    // Emptied, so a later round has nothing left to report — a torn-down
    // component must not read as a liveness failure.
    assert_eq!(components.first_failed(), None);
    std::fs::remove_dir_all(&dir).ok();
}
