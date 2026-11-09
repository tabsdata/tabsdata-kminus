// Copyright 2026 Tabsdata Inc.

use std::env;
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use super::*;

fn tempdir() -> PathBuf {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let dir = env::temp_dir().join(format!("km-pid-{}-{}", std::process::id(), n));
    fs::create_dir_all(&dir).unwrap();
    dir
}

#[test]
fn pids_dir_under_data_bin_pids() {
    let workdir = PathBuf::from("/tmp/foo");
    let p = pids_dir(&workdir, BIN_KM);
    assert_eq!(p, PathBuf::from("/tmp/foo/data/km/pids"));
}

#[test]
fn write_pid_records_the_processs_identity() {
    let dir = tempdir();
    let own = std::process::id();
    write_pid(&dir, "alpha", own);

    let path = dir.join(format!("{own}.alpha"));
    assert!(path.exists());

    // The number is in the filename; the content is what makes it
    // verifiable later.
    let content = fs::read_to_string(&path).unwrap();
    let (pid, stamp) = kcommon::identity::parse_owner(&content).expect("pid@stamp");
    assert_eq!(pid, own);
    assert!(
        !kcommon::identity::is_unknown_stamp(stamp),
        "a running process has a readable start time",
    );
    fs::remove_dir_all(&dir).ok();
}

#[test]
fn write_pid_creates_missing_dir() {
    let dir = tempdir().join("nested").join("deeper");
    write_pid(&dir, "beta", 42);
    assert!(dir.join("42.beta").exists());
    fs::remove_dir_all(dir.parent().unwrap().parent().unwrap()).ok();
}

#[test]
fn remove_pid_removes_matching_file() {
    let dir = tempdir();
    write_pid(&dir, "comp", 100);
    write_pid(&dir, "other", 200);
    remove_pid(&dir, "comp");
    assert!(!dir.join("100.comp").exists());
    assert!(dir.join("200.other").exists());
    fs::remove_dir_all(&dir).ok();
}

#[test]
fn remove_pid_no_op_when_missing() {
    let dir = tempdir();
    remove_pid(&dir, "nonexistent");
    fs::remove_dir_all(&dir).ok();
}

#[test]
fn recorded_confirms_a_process_that_is_still_itself() {
    let dir = tempdir();
    let own = std::process::id();
    write_pid(&dir, "alpha", own);
    assert_eq!(recorded(&dir, "alpha"), Recorded::Live(own));
    fs::remove_dir_all(&dir).ok();
}

#[test]
fn recorded_absent_when_no_file_or_no_dir() {
    let dir = tempdir();
    write_pid(&dir, "alpha", std::process::id());
    assert_eq!(recorded(&dir, "nonexistent"), Recorded::Absent);
    assert_eq!(
        recorded(Path::new("/nonexistent/km/pids"), "alpha"),
        Recorded::Absent
    );
    fs::remove_dir_all(&dir).ok();
}

#[test]
fn recorded_is_stale_when_the_subject_is_gone() {
    let dir = tempdir();
    let dead = u32::MAX - 1;
    write_pid(&dir, "alpha", dead);
    assert_eq!(recorded(&dir, "alpha"), Recorded::Stale(dead));
    fs::remove_dir_all(&dir).ok();
}

#[test]
fn recorded_is_stale_when_the_number_was_reused() {
    // Our own PID, recorded against a start time that is not ours. This is
    // the case a bare liveness check cannot see and the whole reason the
    // stamp is written: `km stop` would otherwise signal a stranger, and
    // `km start` would refuse to run because one was holding the number.
    let dir = tempdir();
    let own = std::process::id();
    fs::create_dir_all(&dir).unwrap();
    fs::write(
        dir.join(format!("{own}.alpha")),
        format!("{own}@19700101T000000Z"),
    )
    .unwrap();
    assert_eq!(recorded(&dir, "alpha"), Recorded::Stale(own));
    fs::remove_dir_all(&dir).ok();
}

#[test]
fn recorded_is_stale_for_content_it_cannot_verify() {
    let dir = tempdir();
    let own = std::process::id();
    fs::create_dir_all(&dir).unwrap();
    for content in [
        // A file in the old format, carrying the number alone.
        own.to_string(),
        // Content disagreeing with the filename.
        format!("{}@19700101T000000Z", own + 1),
        String::new(),
        "nonsense".to_string(),
    ] {
        fs::write(dir.join(format!("{own}.alpha")), &content).unwrap();
        assert_eq!(
            recorded(&dir, "alpha"),
            Recorded::Stale(own),
            "unverifiable content {content:?} must not read as running",
        );
    }
    fs::remove_dir_all(&dir).ok();
}

#[cfg(target_os = "macos")]
#[test]
fn kill_descendants_kills_a_child() {
    use std::process::{Command, Stdio};

    let child = Command::new("sleep")
        .arg("30")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn sleep");
    assert!(kcommon::signal::is_pid_alive(child.id()));

    // Kill everything parented under this test process.
    kill_descendants(std::process::id());

    // Judged on the wait status: until reaped, an exited child is a
    // zombie and still holds its number.
    let mut child = child;
    let status = child.wait().expect("reap child");
    use std::os::unix::process::ExitStatusExt;
    assert!(
        status.signal().is_some(),
        "child should have been killed by kill_descendants",
    );
}

#[cfg(target_os = "macos")]
#[test]
fn kill_descendants_no_op_without_children() {
    // A PID very unlikely to have children; must not panic.
    kill_descendants(999_999);
}

#[test]
fn cleanup_stale_pids_removes_files_for_dead_processes() {
    let dir = tempdir();
    let dead_pid: u32 = 999_999;
    let path = dir.join(format!("{}.zombie", dead_pid));
    fs::write(&path, dead_pid.to_string()).unwrap();
    cleanup_stale_pids(&dir);
    assert!(
        !path.exists(),
        "pid file for dead process should be removed"
    );
    fs::remove_dir_all(&dir).ok();
}

#[test]
fn cleanup_stale_pids_ignores_malformed_filenames() {
    let dir = tempdir();
    fs::write(dir.join("not-a-pid-file"), "junk").unwrap();
    fs::write(dir.join("abc.notapid"), "junk").unwrap();
    cleanup_stale_pids(&dir);
    fs::remove_dir_all(&dir).ok();
}

#[test]
fn cleanup_stale_pids_no_op_when_dir_missing() {
    cleanup_stale_pids(Path::new("/nonexistent/km/pids"));
}

#[cfg(unix)]
#[test]
fn apply_subprocess_flags_puts_child_in_own_group() {
    use nix::sys::signal::{self, Signal};
    use nix::unistd::{Pid as NixPid, getpgid};
    use std::process::{Command, Stdio};

    let mut command = Command::new("sleep");
    command
        .arg("30")
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    apply_subprocess_flags(&mut command);
    let mut child = command.spawn().expect("spawn sleep");
    let pid = child.id();

    // The child must lead its own process group (pgid == pid), i.e. it
    // is detached from the test runner's foreground group so Ctrl+C
    // reaches km alone. Without process_group(0) this would equal the
    // runner's pgid, not the child's pid.
    let pgid = getpgid(Some(NixPid::from_raw(pid as i32))).expect("getpgid");
    assert_eq!(
        pgid.as_raw() as u32,
        pid,
        "child should be its own process group leader"
    );

    let _ = signal::kill(NixPid::from_raw(pid as i32), Signal::SIGKILL);
    let _ = child.wait();
}

// --- stderr capture ---------------------------------------------------------

/// A command that writes `text` to its standard error and exits.
fn stderr_writer(text: &str) -> Command {
    #[cfg(unix)]
    {
        let mut c = Command::new("sh");
        c.args(["-c", &format!("echo {} 1>&2", text)]);
        c
    }
    #[cfg(windows)]
    {
        let mut c = Command::new("cmd");
        c.args(["/C", &format!("echo {} 1>&2", text)]);
        c
    }
}

#[test]
fn stderr_log_names_file_after_bin() {
    let p = stderr_log(Path::new("/tmp/logs"), "pod-runner");
    assert_eq!(p, PathBuf::from("/tmp/logs/pod-runner.err"));
}

/// Start `text`'s writer the way km starts a component, and wait for it.
fn run_component(text: &str, logdir: &Path, bin: &str) -> kcommon::tracked::Exit {
    let run = kcommon::token::Run::current();
    spawn_component(
        stderr_writer(text),
        logdir,
        bin,
        kcommon::token::KM_KILL_TOKEN_VAR,
        &run.scope(bin),
    )
    .expect("spawn")
    .wait()
}

#[test]
fn spawn_component_writes_child_stderr_to_file() {
    let logdir = tempdir();
    assert_eq!(
        run_component("boom", &logdir, "alpha"),
        kcommon::tracked::Exit::Code(0)
    );

    let captured = fs::read_to_string(stderr_log(&logdir, "alpha")).expect("read capture");
    assert!(
        captured.contains("boom"),
        "stderr should reach the file, got {:?}",
        captured
    );
    fs::remove_dir_all(&logdir).ok();
}

#[test]
fn spawn_component_creates_missing_logdir() {
    let logdir = tempdir().join("nested").join("deeper");
    run_component("hello", &logdir, "beta");

    assert!(stderr_log(&logdir, "beta").exists());
    fs::remove_dir_all(&logdir).ok();
}

#[test]
fn spawn_component_truncates_previous_run() {
    let logdir = tempdir();
    let path = stderr_log(&logdir, "gamma");
    fs::create_dir_all(&logdir).unwrap();
    fs::write(&path, "stale output from an earlier run\n").unwrap();

    // The file describes one run, so a fresh spawn must not append to
    // whatever the previous one left behind.
    run_component("fresh", &logdir, "gamma");

    let captured = fs::read_to_string(&path).expect("read capture");
    assert!(!captured.contains("stale"), "got {:?}", captured);
    assert!(captured.contains("fresh"), "got {:?}", captured);
    fs::remove_dir_all(&logdir).ok();
}

#[test]
fn spawn_component_falls_back_when_logdir_unusable() {
    // A path whose parent is a regular file can be neither created nor
    // opened, which must leave the spawn working rather than failing it.
    let dir = tempdir();
    let blocker = dir.join("not-a-dir");
    fs::write(&blocker, "x").unwrap();
    let logdir = blocker.join("logs");

    run_component("ignored", &logdir, "delta");

    assert!(!stderr_log(&logdir, "delta").exists());
    fs::remove_dir_all(&dir).ok();
}

// --- stopping by record -------------------------------------------------------

/// A child that outlives any grace a test gives it.
fn sleeper() -> std::process::Child {
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
    c.stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null());
    c.spawn().expect("spawn sleeper")
}

/// Reap `child` and report whether it was killed rather than exiting on
/// its own terms.
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
fn stop_recorded_returns_at_once_when_there_is_nothing_to_stop() {
    let dir = tempdir();
    // Absent, and stale: neither may burn the grace or signal anything.
    write_pid(&dir, "gone", u32::MAX - 1);

    let started = std::time::Instant::now();
    stop_recorded(&dir, "absent", Duration::from_secs(90));
    stop_recorded(&dir, "gone", Duration::from_secs(90));
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "took {:?}",
        started.elapsed()
    );
    fs::remove_dir_all(&dir).ok();
}

#[test]
fn stop_recorded_stops_a_live_verified_subject() {
    let dir = tempdir();
    let mut child = sleeper();
    write_pid(&dir, "sleeper", child.id());

    // The graceful ask is delivered on Windows by re-exec'ing the current
    // binary as a console-event helper (see kcommon::signal). libtest owns a
    // test binary's main, so the hook never runs here and the stop can only
    // reach the force stage: the grace is burned by design, not by a
    // regression. Leave none to burn.
    #[cfg(windows)]
    let grace = Duration::ZERO;
    #[cfg(not(windows))]
    let grace = Duration::from_secs(30);

    let started = std::time::Instant::now();
    stop_recorded(&dir, "sleeper", grace);

    // Died to the graceful request, not to the grace expiring.
    assert!(
        started.elapsed() < Duration::from_secs(10),
        "took {:?}",
        started.elapsed()
    );
    assert!(killed(&mut child));
    fs::remove_dir_all(&dir).ok();
}

#[test]
fn cleanup_stale_pids_spares_a_pid_reused_by_something_else() {
    let dir = tempdir();
    // This process is alive and holds this PID, but the file records a
    // different start time — what a recycled number looks like after a
    // previous run ended. Signalling it would signal a stranger.
    let own = std::process::id();
    fs::create_dir_all(&dir).unwrap();
    fs::write(
        dir.join(format!("{own}.control-plane")),
        format!("{own}@19700101T000000Z"),
    )
    .unwrap();

    let started = std::time::Instant::now();
    cleanup_stale_pids(&dir);

    assert!(
        started.elapsed() < Duration::from_secs(2),
        "an unverified entry must not be waited on, took {:?}",
        started.elapsed()
    );
    assert!(
        !dir.join(format!("{own}.control-plane")).exists(),
        "the stale file goes either way",
    );
    fs::remove_dir_all(&dir).ok();
}

#[test]
fn cleanup_stale_pids_stops_a_verified_survivor() {
    let dir = tempdir();
    // A component from "the previous run" that is genuinely still itself:
    // the one case cleanup exists to stop.
    let mut child = sleeper();
    write_pid(&dir, "sleeper", child.id());

    cleanup_stale_pids(&dir);

    assert!(killed(&mut child), "the recorded survivor must be stopped");
    assert!(!dir.join(format!("{}.sleeper", child.id())).exists());
    fs::remove_dir_all(&dir).ok();
}

// --- best-effort removals ------------------------------------------------------

#[test]
fn best_effort_removals_are_silent_only_about_absence() {
    // Absence is the goal, so a missing target is success, not a failure
    // to report — and crucially, not a panic on any platform.
    remove_file_best_effort(Path::new("/nonexistent/km/pids/1.alpha"), "PID file");
    remove_dir_all_best_effort(Path::new("/nonexistent/km/pids"), "PID directory");
}

#[test]
fn write_pid_survives_an_unwritable_destination() {
    // The parent is a file, so neither the directory nor the PID file can
    // be created. The write is best-effort — a warn, never a panic — and
    // leaves nothing behind that `recorded` could misread.
    let dir = tempdir();
    let blocker = dir.join("not-a-dir");
    fs::write(&blocker, "x").unwrap();
    let pids = blocker.join("pids");

    write_pid(&pids, "alpha", std::process::id());

    assert_eq!(recorded(&pids, "alpha"), Recorded::Absent);
    fs::remove_dir_all(&dir).ok();
}

/// Hosts kcommon's console-event helper in this test binary, so a graceful
/// stop a test runs on Windows reaches its target (see
/// `kcommon::signal::ctrl_break_helper_hook`).
#[cfg(windows)]
#[test]
fn kcommon_ctrl_break_helper() {
    kcommon::signal::ctrl_break_helper_hook();
}
