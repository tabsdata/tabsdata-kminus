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
fn write_pid_creates_named_file() {
    let dir = tempdir();
    write_pid(&dir, "alpha", 1234);
    let path = dir.join("1234.alpha");
    assert!(path.exists());
    let content = fs::read_to_string(&path).unwrap();
    assert_eq!(content, "1234");
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
fn read_pid_returns_recorded_pid() {
    let dir = tempdir();
    write_pid(&dir, "alpha", 4321);
    write_pid(&dir, "beta", 5678);
    assert_eq!(read_pid(&dir, "alpha"), Some(4321));
    assert_eq!(read_pid(&dir, "beta"), Some(5678));
    fs::remove_dir_all(&dir).ok();
}

#[test]
fn read_pid_none_when_missing_or_dir_absent() {
    let dir = tempdir();
    write_pid(&dir, "alpha", 1);
    assert_eq!(read_pid(&dir, "nonexistent"), None);
    assert_eq!(read_pid(Path::new("/nonexistent/km/pids"), "alpha"), None);
    fs::remove_dir_all(&dir).ok();
}

#[cfg(unix)]
#[test]
fn kill_descendants_kills_a_child() {
    use std::process::{Command, Stdio};

    let child = Command::new("sleep")
        .arg("30")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn sleep");
    let pid = child.id();
    assert!(is_alive(pid));

    // Kill everything parented under this test process.
    kill_descendants(std::process::id());

    // The child should die promptly.
    let mut dead = false;
    for _ in 0..50 {
        if !is_alive(pid) {
            dead = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    // Reap the zombie so it does not linger.
    let mut child = child;
    let _ = child.wait();
    assert!(dead, "child should have been killed by kill_descendants");
}

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

#[test]
fn is_alive_true_for_self() {
    assert!(is_alive(std::process::id()));
}

#[test]
fn is_alive_false_for_unlikely_pid() {
    assert!(!is_alive(999_999));
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
