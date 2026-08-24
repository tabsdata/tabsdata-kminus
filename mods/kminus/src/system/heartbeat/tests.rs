// Copyright 2026 Tabsdata Inc.

use std::thread::sleep;

use super::*;

fn tempdir() -> PathBuf {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!("km-hb-{}-{}", std::process::id(), n));
    fs::create_dir_all(&dir).unwrap();
    dir
}

fn write_beat(dir: &Path, status: &str, detail: &str) -> PathBuf {
    let path = dir.join(HEARTBEAT_FILE);
    fs::write(
        &path,
        format!(r#"{{"ts":1,"status":"{status}","detail":"{detail}"}}"#),
    )
    .unwrap();
    path
}

#[test]
fn heartbeat_file_sits_under_data_component() {
    let p = heartbeat_file(Path::new("/tmp/wd"), "pod-runner");
    assert_eq!(
        p,
        Path::new("/tmp/wd")
            .join(DATA_DIR)
            .join("pod-runner")
            .join(HEARTBEAT_FILE)
    );
}

#[test]
fn stale_after_adds_grace_to_the_interval() {
    assert_eq!(
        stale_after(Duration::from_secs(15)),
        Duration::from_secs(15) + HEARTBEAT_GRACE
    );
}

#[test]
fn read_none_when_missing_or_unparseable() {
    let dir = tempdir();
    assert!(read(&dir.join(HEARTBEAT_FILE)).is_none());

    let garbage = dir.join(HEARTBEAT_FILE);
    fs::write(&garbage, "not: [json").unwrap();
    assert!(read(&garbage).is_none());
    fs::remove_dir_all(&dir).ok();
}

#[test]
fn read_returns_status_and_detail() {
    let dir = tempdir();
    let path = write_beat(&dir, "degraded", "kcm /healthz refused");
    let (age, beat) = read(&path).unwrap();
    assert!(age < Duration::from_secs(5));
    assert_eq!(beat.status, "degraded");
    assert_eq!(beat.detail, "kcm /healthz refused");
    fs::remove_dir_all(&dir).ok();
}

#[test]
fn is_ready_only_for_ready_status() {
    let dir = tempdir();
    assert!(!is_ready(&dir.join(HEARTBEAT_FILE)));
    let path = write_beat(&dir, STATUS_READY, "");
    assert!(is_ready(&path));
    write_beat(&dir, "degraded", "self-probe failed");
    assert!(!is_ready(&path));
    fs::remove_dir_all(&dir).ok();
}

#[test]
fn monitor_reports_healthy_on_a_fresh_ready_beat() {
    let dir = tempdir();
    let path = write_beat(&dir, STATUS_READY, "");
    let mut m = HeartbeatMonitor::new("test", path, Duration::from_secs(15), 4);
    assert_eq!(m.poll(), Beat::Healthy);
    fs::remove_dir_all(&dir).ok();
}

#[test]
fn monitor_counts_missing_beats_up_to_the_limit() {
    let dir = tempdir();
    let mut m = HeartbeatMonitor::new("test", dir.join(HEARTBEAT_FILE), Duration::from_secs(15), 3);
    assert_eq!(m.poll(), Beat::Missed { misses: 1 });
    assert_eq!(m.poll(), Beat::Missed { misses: 2 });
    // The final poll carries what it observed, so the failure can name a
    // cause and not just a count.
    let Beat::Failed { misses, reason } = m.poll() else {
        panic!("the third miss must fail");
    };
    assert_eq!(misses, 3);
    assert!(reason.contains("no readable heartbeat"));
    fs::remove_dir_all(&dir).ok();
}

#[test]
fn monitor_resets_the_count_after_a_good_beat() {
    let dir = tempdir();
    let path = dir.join(HEARTBEAT_FILE);
    let mut m = HeartbeatMonitor::new("test", path.clone(), Duration::from_secs(15), 3);
    assert_eq!(m.poll(), Beat::Missed { misses: 1 });
    write_beat(&dir, STATUS_READY, "");
    assert_eq!(m.poll(), Beat::Healthy);
    fs::remove_file(&path).unwrap();
    assert_eq!(m.poll(), Beat::Missed { misses: 1 });
    fs::remove_dir_all(&dir).ok();
}

#[test]
fn monitor_counts_a_stale_beat_as_missed() {
    let dir = tempdir();
    let path = write_beat(&dir, STATUS_READY, "");
    // Zero staleness budget: any age at all is over the limit.
    let mut m = HeartbeatMonitor::new("test", path, Duration::ZERO, 2);
    sleep(Duration::from_millis(20));
    assert_eq!(m.poll(), Beat::Missed { misses: 1 });
    fs::remove_dir_all(&dir).ok();
}

#[test]
fn monitor_counts_a_degraded_beat_as_missed() {
    let dir = tempdir();
    let path = write_beat(&dir, "degraded", "etcd /health failed");
    let mut m = HeartbeatMonitor::new("test", path, Duration::from_secs(15), 2);
    assert_eq!(m.poll(), Beat::Missed { misses: 1 });
    fs::remove_dir_all(&dir).ok();
}

#[test]
fn a_failure_carries_the_last_observed_reason() {
    let dir = tempdir();
    let path = dir.join("absent.json");
    let mut m = HeartbeatMonitor::new("test", path, Duration::from_secs(15), 2);

    assert_eq!(m.poll(), Beat::Missed { misses: 1 });
    let Beat::Failed { reason, .. } = m.poll() else {
        panic!("the second miss must fail");
    };
    assert!(
        reason.contains("no readable heartbeat"),
        "reason = {reason}, want what the poll observed"
    );
    fs::remove_dir_all(&dir).ok();
}

#[test]
fn a_self_reported_degraded_beat_outlasts_a_silent_one() {
    // A component publishing 'degraded' is running and able to report, so it
    // gets DEGRADED_TOLERANCE_FACTOR times the patience of one saying nothing.
    let dir = tempdir();
    let path = write_beat(&dir, "degraded", "pod status sync stalled");
    let mut m = HeartbeatMonitor::new("test", path, Duration::from_secs(15), 2);

    let limit = 2 * DEGRADED_TOLERANCE_FACTOR;
    for miss in 1..limit {
        assert_eq!(
            m.poll(),
            Beat::Missed { misses: miss },
            "a degraded component must not be failed at {miss} of {limit}"
        );
    }
    let Beat::Failed { reason, .. } = m.poll() else {
        panic!("the degraded limit must eventually fail");
    };
    assert!(
        reason.contains("pod status sync stalled"),
        "reason = {reason}, want the component's own detail"
    );
    fs::remove_dir_all(&dir).ok();
}
