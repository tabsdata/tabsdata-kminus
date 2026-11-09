// Copyright 2026 Tabsdata Inc.

use std::process::{Command, Stdio};

use super::*;

/// A PID high enough that no live process can hold it.
const UNUSED_PID: u32 = u32::MAX - 1;

/// A child that outlives the assertion made about it.
fn long_running() -> std::process::Child {
    #[cfg(unix)]
    let mut c = {
        let mut c = Command::new("sh");
        c.args(["-c", "sleep 30"]);
        c
    };
    #[cfg(windows)]
    let mut c = {
        let mut c = Command::new("cmd");
        c.args(["/C", "ping -n 31 127.0.0.1"]);
        c
    };
    c.stdout(Stdio::null()).stderr(Stdio::null());
    c.spawn().expect("spawn child")
}

/// Reap `child` and report whether it was killed rather than exiting on
/// its own terms.
///
/// Asserted on the wait status, not on the PID disappearing: until a
/// parent reaps it, an exited child is a zombie and still holds its
/// number, so `is_pid_alive` would keep saying yes however dead it is.
fn killed(child: &mut std::process::Child) -> bool {
    let status = child.wait().expect("reap child");
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

#[test]
fn is_pid_alive_sees_this_process_and_not_an_unused_number() {
    assert!(is_pid_alive(std::process::id()));
    assert!(!is_pid_alive(UNUSED_PID));
}

/// PID 1 always runs and belongs to root: an unprivileged caller gets
/// `EPERM` asking about it, which must still read as alive.
#[cfg(unix)]
#[test]
fn is_pid_alive_sees_a_process_it_may_not_signal() {
    assert!(is_pid_alive(1));
}

#[test]
fn force_kill_stops_a_running_process() {
    let mut child = long_running();
    let pid = child.id();
    assert!(is_pid_alive(pid));

    force_kill(pid);

    assert!(killed(&mut child), "force_kill must not be survivable");
}

#[test]
fn signalling_an_unused_pid_is_harmless() {
    // Every one of these can be reached with a number the caller could not
    // prove was still theirs, so none may panic or abort.
    force_kill(UNUSED_PID);
    send_sigterm_pid(UNUSED_PID);
    send_sigterm_group(UNUSED_PID);
    force_kill_group(UNUSED_PID);
    assert!(!is_pid_alive(UNUSED_PID));
}

#[cfg(unix)]
#[test]
fn leads_own_group_recognises_a_group_leader() {
    use std::os::unix::process::CommandExt;

    let mut command = Command::new("sh");
    command.args(["-c", "sleep 30"]);
    command.stdout(Stdio::null()).stderr(Stdio::null());
    command.process_group(0);
    let mut child = command.spawn().expect("spawn group leader");
    let pid = child.id();

    assert!(
        leads_own_group(pid),
        "a child put in its own group leads it, so signalling that group \
         reaches only it and its descendants",
    );

    force_kill_group(pid);
    assert!(killed(&mut child), "the group kill must reach its leader");
}

#[cfg(unix)]
#[test]
fn leads_own_group_rejects_a_mere_group_member() {
    // Spawned without its own group, so it inherits this process's and
    // must not be reported as leading one — signalling that group would
    // reach the test runner and everything else in it.
    let mut child = long_running();
    let pid = child.id();

    assert!(!leads_own_group(pid));

    force_kill(pid);
    assert!(killed(&mut child));
}

#[cfg(windows)]
#[test]
fn leads_own_group_is_always_false_on_windows() {
    // No leadership query exists on Windows; the tree kill covers
    // descendants on the force path instead.
    assert!(!leads_own_group(std::process::id()));
}

/// When set, [`sleeper_entry`] sleeps instead of passing at once.
#[cfg(windows)]
const SLEEPER_VAR: &str = "KCOMMON_TEST_SLEEPER";

/// When the env var is set, this test is a sleeper with default console
/// event handling, so a CTRL_BREAK ends it. Without it, it passes at once.
#[cfg(windows)]
#[test]
fn sleeper_entry() {
    if std::env::var_os(SLEEPER_VAR).is_some() {
        std::thread::sleep(std::time::Duration::from_secs(60));
    }
}

/// A sleeper started the way km starts a component: in a hidden console
/// of its own, leading its own process group.
#[cfg(windows)]
fn own_console_group_sleeper() -> crate::console::OwnConsoleChild {
    let mut c = Command::new(std::env::current_exe().expect("test exe"));
    c.args(["signal::tests::sleeper_entry", "--exact"]);
    c.env(SLEEPER_VAR, "1");
    crate::console::OwnConsoleChild::spawn(&c, None).expect("spawn sleeper")
}

/// Hosts the console-event helper in this test binary: the re-exec that
/// [`request_graceful`] runs selects this test by its name.
#[cfg(windows)]
#[test]
fn kcommon_ctrl_break_helper() {
    ctrl_break_helper_hook();
}

#[cfg(windows)]
#[test]
fn ctrl_break_reaches_a_child_in_its_own_console() {
    let mut child = own_console_group_sleeper();
    let pid = child.id();
    assert!(is_pid_alive(pid));

    request_graceful(pid);

    // CTRL_BREAK's default handler exits the process.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        if child.try_wait().expect("try_wait").is_some() {
            break;
        }
        if std::time::Instant::now() > deadline {
            let _ = child.kill();
            panic!("CTRL_BREAK did not stop the target within 10s");
        }
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
}
