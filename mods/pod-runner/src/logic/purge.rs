// Copyright 2026 Tabsdata Inc.

//! Background purge daemon for expired pod work directories and archives.
//!
//! Scans the work directory for pod dirs containing a [`FINISHED_SENTINEL`]
//! file and the archive directory for `.tgz` files. Removes entries whose
//! modification time exceeds the configured TTL.
//!
//! The daemon calculates an optimal sleep duration based on the earliest
//! expiry among remaining files, adding a 15-minute buffer to batch
//! multiple expirations into a single scan.

use std::fs;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use tokio::sync::watch;
use tracing::{debug, info, info_span, warn, Instrument};

use super::cleanup::CleanupConfig;
use crate::spec::constants::FINISHED_SENTINEL;
use crate::store::{PodId, PodStore};

/// 15-minute buffer added to sleep durations to batch expirations.
const BATCH_BUFFER: Duration = Duration::from_secs(15 * 60);

/// Delay between individual file/directory deletions to avoid disk I/O spikes.
const THROTTLE_DELAY: Duration = Duration::from_millis(50);

/// Initial delay before the first scan to let the system stabilize.
const INITIAL_DELAY: Duration = Duration::from_secs(60);

/// Spawn the purge daemon as a background tokio task.
///
/// The daemon wakes periodically to scan for expired work directories
/// and archives. It shuts down cleanly when the shutdown signal fires.
pub fn spawn_purge_daemon(
    config: CleanupConfig,
    store: Arc<dyn PodStore>,
    shutdown_rx: watch::Receiver<()>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(
        async move {
            purge_loop(config, store, shutdown_rx).await;
        }
        .instrument(info_span!("purge_daemon")),
    )
}

async fn purge_loop(
    config: CleanupConfig,
    store: Arc<dyn PodStore>,
    mut shutdown_rx: watch::Receiver<()>,
) {
    info!("Purge daemon started");

    // Initial delay
    tokio::select! {
        _ = tokio::time::sleep(INITIAL_DELAY) => {},
        _ = shutdown_rx.changed() => {
            info!("Shutdown before first scan");
            return;
        }
    }

    loop {
        debug!("Starting purge scan");
        let next_expiry = purge_scan(&config, &store, &mut shutdown_rx).await;

        // If shutdown was signalled during scan, exit
        if shutdown_rx.has_changed().unwrap_or(true) {
            info!("Shutdown signal received, purge daemon exiting");
            return;
        }

        // Sleep calculation: next_expiry + 15min buffer, or idle_ttl + 15min
        let sleep_duration = match next_expiry {
            Some(d) => d + BATCH_BUFFER,
            None => idle_sleep_duration(&config) + BATCH_BUFFER,
        };

        debug!(sleep_secs = sleep_duration.as_secs(), "Purge scan complete, sleeping");

        tokio::select! {
            _ = tokio::time::sleep(sleep_duration) => {},
            _ = shutdown_rx.changed() => {
                info!("Shutdown signal received, purge daemon exiting");
                return;
            }
        }
    }
}

/// Default sleep duration when nothing is pending: the shorter of the
/// two configured TTLs.
fn idle_sleep_duration(config: &CleanupConfig) -> Duration {
    let work_ttl = Duration::from_secs(config.deleted_pod_dir_ttl_hours * 3600);
    let archive_ttl = Duration::from_secs(config.pod_archive_ttl_days * 86400);

    match (work_ttl.is_zero(), archive_ttl.is_zero()) {
        (true, true) => Duration::from_secs(3600), // both disabled, check hourly
        (true, false) => archive_ttl,
        (false, true) => work_ttl,
        (false, false) => work_ttl.min(archive_ttl),
    }
}

/// Perform one purge scan. Returns the duration until the earliest
/// remaining file expires, or `None` if nothing is pending.
///
/// Scans rotated run directories (`run.0/pods/`, `run.1/pods/`, ...)
/// for finished pods whose FINISHED sentinel has expired. Archives
/// inside pod dirs are cleaned up when the pod dir is removed.
async fn purge_scan(
    config: &CleanupConfig,
    store: &Arc<dyn PodStore>,
    shutdown_rx: &mut watch::Receiver<()>,
) -> Option<Duration> {
    use crate::server::config::{PODS_SUBDIR, PREV_RUN_DIRS_TO_KEEP};

    let mut earliest_remaining: Option<Duration> = None;
    let now = SystemTime::now();
    let mut dirs_purged: u32 = 0;

    let work_ttl = Duration::from_secs(config.deleted_pod_dir_ttl_hours * 3600);
    if !work_ttl.is_zero() {
        // Scan the LIVE run/pods/ directory — finished pods whose
        // FINISHED sentinel is older than the TTL get purged. This
        // handles pods that clients never explicitly /deletePod'd
        // (e.g. async K8s Job deletion that didn't propagate). These pods
        // are still tracked in the store, so pass it: reaping the dir also
        // evicts the lingering store entry (self-heal) rather than leaking
        // it until restart.
        dirs_purged += scan_work_dirs(
            &config.pods_dir,
            work_ttl,
            now,
            &mut earliest_remaining,
            shutdown_rx,
            Some(store),
        )
        .await;

        // Also scan the rotated run.0/, run.1/, ... directories.
        if let Some(workdir) = config.pods_dir.parent().and_then(|r| r.parent()) {
            for i in 0..PREV_RUN_DIRS_TO_KEEP {
                let run_pods = workdir.join(format!("run.{}", i)).join(PODS_SUBDIR);
                // Rotated run dirs belong to previous incarnations and are
                // not tracked in the current store, so no store eviction.
                dirs_purged += scan_work_dirs(
                    &run_pods,
                    work_ttl,
                    now,
                    &mut earliest_remaining,
                    shutdown_rx,
                    None,
                )
                .await;
            }
        }
    }

    if dirs_purged > 0 {
        info!(dirs_purged, "Purge scan completed");
    } else {
        debug!("Purge scan completed, nothing to purge");
    }

    earliest_remaining
}

async fn scan_work_dirs(
    work_dir: &std::path::Path,
    ttl: Duration,
    now: SystemTime,
    earliest: &mut Option<Duration>,
    shutdown_rx: &mut watch::Receiver<()>,
    store: Option<&Arc<dyn PodStore>>,
) -> u32 {
    let mut count: u32 = 0;
    let entries = match fs::read_dir(work_dir) {
        Ok(rd) => rd,
        Err(_) => return 0,
    };

    for entry in entries.filter_map(|e| e.ok()) {
        if shutdown_rx.has_changed().unwrap_or(true) {
            return count;
        }

        let path = entry.path();
        if !path.is_dir() {
            continue;
        }

        let sentinel = path.join(FINISHED_SENTINEL);
        if !sentinel.exists() {
            continue;
        }

        let mtime = match sentinel.metadata().and_then(|m| m.modified()) {
            Ok(t) => t,
            Err(_) => continue,
        };

        let age = now.duration_since(mtime).unwrap_or(Duration::ZERO);
        if age >= ttl {
            debug!(dir = ?path, age_hours = age.as_secs() / 3600, "Purging expired work dir");
            if let Err(e) = fs::remove_dir_all(&path) {
                warn!(error = %e, dir = ?path, "Failed to purge work dir");
            } else {
                count += 1;
            }
            // Self-heal: a finished pod still in the store past the TTL was
            // never deleted by the control plane (delete dropped or never
            // sent). Evict the lingering entry so it stops occupying memory.
            // `remove` is a no-op if it was already deleted normally — not a
            // failure (the dir can outlive the entry via keep_*_pod_dir).
            if let Some(store) = store
                && let Some(id) =
                    path.file_name().and_then(|n| n.to_str()).and_then(PodId::from_full)
            {
                store.remove(&id).await;
            }
            tokio::time::sleep(THROTTLE_DELAY).await;
        } else {
            let remaining = ttl - age;
            update_earliest(earliest, remaining);
        }
    }
    count
}

fn update_earliest(earliest: &mut Option<Duration>, remaining: Duration) {
    *earliest = Some(match *earliest {
        Some(e) => e.min(remaining),
        None => remaining,
    });
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    /// Test config with `pods_dir = <tmp>/run/pods`.
    /// purge_scan derives workdir as pods_dir.parent.parent = <tmp>,
    /// then scans `<tmp>/run.0/pods/`, `<tmp>/run.1/pods/`.
    fn test_config(tmp: &std::path::Path) -> CleanupConfig {
        CleanupConfig {
            keep_deleted_pod_dir: false,
            keep_failed_pod_dir: true,
            deleted_pod_dir_ttl_hours: 1,
            pod_archive_ttl_days: 1,
            pods_dir: tmp.join("run").join("pods"),
        }
    }

    fn make_tmp_dir() -> PathBuf {
        let dir = std::env::temp_dir().join(format!("pr-purge-test-{}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    /// An empty store for tests that only exercise dir reaping.
    fn dummy_store() -> Arc<dyn PodStore> {
        Arc::new(crate::store::memory::MemoryStore::new())
    }

    #[tokio::test]
    async fn purge_removes_expired_work_dir() {
        let tmp = make_tmp_dir().join("expired-work");
        let config = test_config(&tmp);

        // Create a pod dir under run.0/pods/ (simulating a previous run)
        let run0_pods = tmp.join("run.0").join("pods");
        fs::create_dir_all(&run0_pods).unwrap();
        let pod_dir = run0_pods.join("default_app_uid-1");
        fs::create_dir_all(&pod_dir).unwrap();
        let sentinel = pod_dir.join(FINISHED_SENTINEL);
        fs::write(&sentinel, "done").unwrap();
        // Set mtime to 2 hours ago (TTL is 1 hour)
        let old = std::time::SystemTime::now() - Duration::from_secs(7200);
        filetime::set_file_mtime(&sentinel, filetime::FileTime::from_system_time(old)).unwrap();

        let (_tx, mut rx) = watch::channel(());
        let result = purge_scan(&config, &dummy_store(), &mut rx).await;

        assert!(!pod_dir.exists(), "expired work dir should be removed");
        assert!(result.is_none(), "nothing remaining");

        let _ = fs::remove_dir_all(&tmp);
    }

    #[tokio::test]
    async fn purge_keeps_fresh_work_dir() {
        let tmp = make_tmp_dir().join("fresh-work");
        let config = test_config(&tmp);

        let run0_pods = tmp.join("run.0").join("pods");
        fs::create_dir_all(&run0_pods).unwrap();
        let pod_dir = run0_pods.join("default_app_uid-2");
        fs::create_dir_all(&pod_dir).unwrap();
        fs::write(pod_dir.join(FINISHED_SENTINEL), "done").unwrap();
        // mtime is now — well within TTL

        let (_tx, mut rx) = watch::channel(());
        let result = purge_scan(&config, &dummy_store(), &mut rx).await;

        assert!(pod_dir.exists(), "fresh work dir should survive");
        assert!(result.is_some(), "should have remaining TTL");

        let _ = fs::remove_dir_all(&tmp);
    }

    #[tokio::test]
    async fn purge_ignores_dirs_without_finished() {
        let tmp = make_tmp_dir().join("no-sentinel");
        let config = test_config(&tmp);

        let run0_pods = tmp.join("run.0").join("pods");
        fs::create_dir_all(&run0_pods).unwrap();
        let pod_dir = run0_pods.join("default_app_uid-3");
        fs::create_dir_all(&pod_dir).unwrap();
        // No FINISHED file

        let (_tx, mut rx) = watch::channel(());
        purge_scan(&config, &dummy_store(), &mut rx).await;

        assert!(pod_dir.exists(), "dir without FINISHED should survive");

        let _ = fs::remove_dir_all(&tmp);
    }

    #[tokio::test]
    async fn purge_evicts_store_entry_for_expired_live_pod() {
        use crate::spec::pod::PodSpec;
        use crate::spec::status::PodStatus;
        use crate::store::memory::MemoryStore;

        let tmp = make_tmp_dir().join("evict-live");
        let config = test_config(&tmp);

        // A finished pod in the LIVE run/pods dir whose sentinel has expired
        // and was never /deletePod'd — its store entry would otherwise leak.
        let pod_dir = config.pods_dir.join("default_app_uid-1");
        fs::create_dir_all(&pod_dir).unwrap();
        let sentinel = pod_dir.join(FINISHED_SENTINEL);
        fs::write(&sentinel, "done").unwrap();
        let old = SystemTime::now() - Duration::from_secs(7200); // TTL is 1h
        filetime::set_file_mtime(&sentinel, filetime::FileTime::from_system_time(old)).unwrap();

        let store: Arc<dyn PodStore> = Arc::new(MemoryStore::new());
        let id = PodId::new("default", "app", "uid-1");
        let spec = Arc::new(PodSpec::default());
        store
            .put(id.clone(), Arc::clone(&spec), spec, PodStatus::default())
            .await;
        assert!(store.contains(&id).await);

        let (_tx, mut rx) = watch::channel(());
        purge_scan(&config, &store, &mut rx).await;

        assert!(!pod_dir.exists(), "expired live pod dir should be removed");
        assert!(
            !store.contains(&id).await,
            "lingering store entry should be evicted",
        );

        let _ = fs::remove_dir_all(&tmp);
    }

    // Archive tests removed — archives now live inside pod dirs and
    // are cleaned up when the pod dir is purged. No separate archive
    // scan is needed.

    // -- idle_sleep_duration --

    #[test]
    fn idle_sleep_both_zero() {
        let config = CleanupConfig {
            deleted_pod_dir_ttl_hours: 0,
            pod_archive_ttl_days: 0,
            ..test_config(std::path::Path::new("/tmp"))
        };
        assert_eq!(idle_sleep_duration(&config), Duration::from_secs(3600));
    }

    #[test]
    fn idle_sleep_only_work_ttl() {
        let config = CleanupConfig {
            deleted_pod_dir_ttl_hours: 2,
            pod_archive_ttl_days: 0,
            ..test_config(std::path::Path::new("/tmp"))
        };
        assert_eq!(idle_sleep_duration(&config), Duration::from_secs(2 * 3600));
    }

    #[test]
    fn idle_sleep_only_archive_ttl() {
        let config = CleanupConfig {
            deleted_pod_dir_ttl_hours: 0,
            pod_archive_ttl_days: 3,
            ..test_config(std::path::Path::new("/tmp"))
        };
        assert_eq!(idle_sleep_duration(&config), Duration::from_secs(3 * 86400));
    }

    #[test]
    fn idle_sleep_picks_shorter_ttl() {
        let config = CleanupConfig {
            deleted_pod_dir_ttl_hours: 2,
            pod_archive_ttl_days: 3,
            ..test_config(std::path::Path::new("/tmp"))
        };
        // 2 hours < 3 days
        assert_eq!(idle_sleep_duration(&config), Duration::from_secs(2 * 3600));
    }

    // -- shutdown --

    #[tokio::test]
    async fn purge_daemon_shuts_down_on_signal() {
        let tmp = make_tmp_dir().join("shutdown");
        let config = test_config(&tmp);

        let (tx, rx) = watch::channel(());
        let handle = spawn_purge_daemon(config, dummy_store(), rx);

        // Send shutdown immediately
        tx.send(()).unwrap();
        let result = tokio::time::timeout(Duration::from_secs(5), handle).await;
        assert!(result.is_ok(), "daemon should exit on shutdown");

        let _ = fs::remove_dir_all(&tmp);
    }
}
