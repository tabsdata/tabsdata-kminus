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
fn cleanup_stale_pids_removes_files_for_dead_processes() {
    let dir = tempdir();
    // Use a PID guaranteed not to exist (very high number).
    let dead_pid: i32 = 999_999;
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
    // Files without "<pid>.<name>" pattern are left alone (the
    // function only removes entries it could parse and act on).
    // We just verify no panic.
    fs::remove_dir_all(&dir).ok();
}

#[test]
fn cleanup_stale_pids_no_op_when_dir_missing() {
    // Should not panic when the dir does not exist.
    cleanup_stale_pids(Path::new("/nonexistent/km/pids"));
}
