// Copyright 2026 Tabsdata Inc.

use std::env;
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use super::*;

fn tempdir() -> PathBuf {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let dir = env::temp_dir().join(format!("km-file-{}-{}", std::process::id(), n));
    fs::create_dir_all(&dir).unwrap();
    dir
}

#[test]
fn wait_for_file_returns_true_when_present() {
    let dir = tempdir();
    let path = dir.join("ready");
    fs::write(&path, b"ok").unwrap();
    assert!(wait_for_file(&path, Duration::from_millis(100)));
    fs::remove_dir_all(&dir).ok();
}

#[test]
fn wait_for_file_times_out_when_missing() {
    let dir = tempdir();
    let path = dir.join("never");
    let start = Instant::now();
    assert!(!wait_for_file(&path, Duration::from_millis(200)));
    assert!(start.elapsed() >= Duration::from_millis(200));
    fs::remove_dir_all(&dir).ok();
}

#[test]
fn resolve_bin_dir_returns_a_path() {
    let bin = resolve_bin_dir();
    assert!(bin.is_absolute());
}
