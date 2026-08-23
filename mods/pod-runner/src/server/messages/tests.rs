// Copyright 2026 Tabsdata Inc.

use super::*;
use serde::Deserialize;
use std::fs;

#[derive(Debug, Deserialize)]
struct Read {
    time: String,
    binary: String,
    level: String,
    message: String,
}

fn tempdir() -> std::path::PathBuf {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!("pr-msg-{}-{}", std::process::id(), n));
    fs::create_dir_all(dir.join(MESSAGES_DIR)).unwrap();
    dir
}

fn lines(workdir: &Path, phase: &str) -> Vec<Read> {
    let path = workdir
        .join(MESSAGES_DIR)
        .join(format!("{phase}{BINARY}.json"));
    fs::read_to_string(path)
        .unwrap()
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| serde_json::from_str(l).expect("each line must be valid JSON"))
        .collect()
}

#[test]
fn write_emits_one_ndjson_record_per_call() {
    let dir = tempdir();
    write(&dir, START_PHASE, LEVEL_INFO, "first");
    write(&dir, START_PHASE, LEVEL_WARN, "second");

    let got = lines(&dir, START_PHASE);
    assert_eq!(got.len(), 2);
    assert_eq!(got[0].message, "first");
    assert_eq!(got[0].level, LEVEL_INFO);
    assert_eq!(got[0].binary, BINARY);
    assert!(!got[0].time.is_empty(), "km needs a time to order by");
    assert_eq!(got[1].level, LEVEL_WARN);
    fs::remove_dir_all(&dir).ok();
}

// The two sets must land in different files, or km would reprint the startup
// messages as the stack comes down.
#[test]
fn write_keeps_the_phases_in_separate_files() {
    let dir = tempdir();
    write(&dir, START_PHASE, LEVEL_INFO, "up");
    write(&dir, STOP_PHASE, LEVEL_INFO, "down");

    assert_eq!(lines(&dir, START_PHASE)[0].message, "up");
    assert_eq!(lines(&dir, STOP_PHASE)[0].message, "down");
    fs::remove_dir_all(&dir).ok();
}

// km sorts these as plain strings, so the stamp must be fixed width, fixed
// precision and UTC, or the ordering silently breaks.
#[test]
fn timestamps_are_fixed_width_utc() {
    let dir = tempdir();
    write(&dir, START_PHASE, LEVEL_INFO, "a");
    write(&dir, START_PHASE, LEVEL_INFO, "b");

    let got = lines(&dir, START_PHASE);
    assert_eq!(
        got[0].time.len(),
        got[1].time.len(),
        "widths differ: {:?} vs {:?}",
        got[0].time,
        got[1].time
    );
    assert!(got[0].time.ends_with('Z'), "{:?} is not UTC", got[0].time);
    // '2026-08-22T10:00:00.000Z'
    assert_eq!(got[0].time.len(), 24, "{:?}", got[0].time);
    assert!(got[0].time <= got[1].time, "stamps went backwards");
    fs::remove_dir_all(&dir).ok();
}

// Reporting must never be able to stop the pod-runner from serving.
#[test]
fn write_is_silent_when_there_is_no_messages_dir() {
    let dir = std::env::temp_dir().join(format!("pr-msg-none-{}", std::process::id()));
    fs::create_dir_all(&dir).unwrap();
    write(&dir, START_PHASE, LEVEL_INFO, "nowhere to go");
    assert!(!dir.join(MESSAGES_DIR).exists());
    fs::remove_dir_all(&dir).ok();
}

// The pod ceiling is the number this whole channel exists to surface.
#[test]
fn report_sizing_names_the_pod_ceiling_and_its_setting() {
    let dir = tempdir();
    report_sizing(&dir, 8, 16, 24);

    let got = lines(&dir, START_PHASE);
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].level, LEVEL_INFO);
    for want in [
        "24 pods",
        "8 cpu",
        "16GB",
        "prsysconfig.yaml",
        "ProviderFailed",
    ] {
        assert!(
            got[0].message.contains(want),
            "sizing message {:?} does not mention {want:?}",
            got[0].message
        );
    }
    fs::remove_dir_all(&dir).ok();
}
