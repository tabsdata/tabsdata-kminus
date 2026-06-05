// Copyright 2026 Tabsdata Inc.

//! Pod cleanup and archival.
//!
//! Handles pod directory retention decisions, tgz archive creation,
//! and the coordinated cleanup sequence on pod deletion.

use std::collections::HashMap;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use flate2::write::GzEncoder;
use flate2::Compression;
use tracing::{debug, warn};

use super::ExecutorProvider;
use crate::utils::dir_size::SIZE_FILE;
use crate::spec::constants::{
    ANN_KEEP_DELETED_POD_DIR, ANN_KEEP_FAILED_POD_DIR, FINISHED_SENTINEL, PodPhase,
};
use crate::store::{PodId, PodStore, SpecView};

// ---------------------------------------------------------------------------
// CleanupConfig
// ---------------------------------------------------------------------------

/// Configuration for pod directory cleanup, archival, and purging.
#[derive(Debug, Clone)]
pub struct CleanupConfig {
    /// Keep work dir when a pod is deleted (default: false).
    pub keep_deleted_pod_dir: bool,
    /// Keep work dir if the pod failed (default: true).
    pub keep_failed_pod_dir: bool,
    /// TTL in hours for kept work dirs before purge (default: 24).
    pub deleted_pod_dir_ttl_hours: u64,
    /// TTL in days for archive tgz files before purge (default: 7).
    pub pod_archive_ttl_days: u64,
    /// Pods directory (`<workdir>/run/pods/`) — used by purge daemon
    /// to scan rotated run dirs for finished pods.
    pub pods_dir: PathBuf,
}

impl Default for CleanupConfig {
    fn default() -> Self {
        use crate::server::config::{PODS_SUBDIR, RUN_DIR};
        Self {
            keep_deleted_pod_dir: false,
            keep_failed_pod_dir: true,
            deleted_pod_dir_ttl_hours: 24,
            pod_archive_ttl_days: 7,
            pods_dir: PathBuf::from(RUN_DIR).join(PODS_SUBDIR),
        }
    }
}

// ---------------------------------------------------------------------------
// Annotation helpers
// ---------------------------------------------------------------------------

/// Parse a bool annotation ("true"/"false"), returning `None` if absent
/// or not a valid bool string.
pub fn parse_bool_annotation(annotations: &HashMap<String, String>, key: &str) -> Option<bool> {
    annotations.get(key).and_then(|v| match v.as_str() {
        "true" => Some(true),
        "false" => Some(false),
        _ => {
            warn!(key, value = v.as_str(), "Invalid bool annotation, ignoring");
            None
        }
    })
}

/// Determine whether the work dir should be kept for this pod.
///
/// Checks pod annotations first, falls back to global config values.
pub fn should_keep_work_dir(
    phase: PodPhase,
    annotations: &HashMap<String, String>,
    keep_deleted_pod_dir: bool,
    keep_failed_pod_dir: bool,
) -> bool {
    let keep_failed = parse_bool_annotation(annotations, ANN_KEEP_FAILED_POD_DIR)
        .unwrap_or(keep_failed_pod_dir);
    let keep_deleted = parse_bool_annotation(annotations, ANN_KEEP_DELETED_POD_DIR)
        .unwrap_or(keep_deleted_pod_dir);

    if keep_failed && phase == PodPhase::Failed {
        return true;
    }
    keep_deleted
}

// ---------------------------------------------------------------------------
// Archive creation
// ---------------------------------------------------------------------------

/// Create a `.tgz` archive containing the original pod spec and all log files.
///
/// The archive layout:
/// ```text
/// original-spec.json
/// logs/<container>/<n>.log
/// ```
///
/// Returns the archive path on success.
pub async fn create_pod_archive(
    pod_dir: &Path,
    id: &PodId,
    store: &dyn PodStore,
    log_dir: Option<&Path>,
) -> io::Result<PathBuf> {
    let archive_path = pod_dir.join("archive.tgz");
    let file = fs::File::create(&archive_path)?;
    let enc = GzEncoder::new(file, Compression::default());
    let mut tar = tar::Builder::new(enc);

    // Add original spec
    if let Some(snap) = store.get(id, SpecView::Original).await {
        let json = serde_json::to_vec_pretty(&*snap.spec)
            .map_err(|e| io::Error::other(e))?;
        let mut header = tar::Header::new_gnu();
        header.set_size(json.len() as u64);
        header.set_mode(0o644);
        header.set_cksum();
        tar.append_data(&mut header, "original-spec.json", json.as_slice())?;
    } else {
        warn!(pod = %id, "No original spec found for archival");
    }

    // Add log files
    if let Some(log_dir) = log_dir
        && log_dir.is_dir() {
            append_dir_recursive(&mut tar, log_dir, Path::new("logs"))?;
        }

    tar.into_inner()?.finish()?;
    debug!(pod = %id, path = ?archive_path, "Pod archive created");
    Ok(archive_path)
}

/// Recursively append all files under `src_dir` into the tar archive
/// under `archive_prefix`.
fn append_dir_recursive<W: io::Write>(
    tar: &mut tar::Builder<W>,
    src_dir: &Path,
    archive_prefix: &Path,
) -> io::Result<()> {
    for entry in fs::read_dir(src_dir)? {
        let entry = entry?;
        let path = entry.path();
        let name = entry.file_name();
        let archive_path = archive_prefix.join(&name);

        if path.is_dir() {
            append_dir_recursive(tar, &path, &archive_path)?;
        } else if path.is_file() {
            tar.append_path_with_name(&path, &archive_path)?;
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Pod cleanup
// ---------------------------------------------------------------------------

/// Perform full cleanup for a completed/deleted pod.
///
/// 1. Read the original spec from the store (for phase + annotations)
/// 2. Optionally create a tgz archive (before store removal)
/// 3. Remove store entry
/// 4. Delete log directory (after archiving)
/// 5. Conditionally delete or keep the work directory
pub async fn cleanup_pod(
    id: &PodId,
    store: &dyn PodStore,
    executor_provider: &dyn ExecutorProvider,
    config: &CleanupConfig,
) {
    let snap = match store.get(id, SpecView::Original).await {
        Some(s) => s,
        None => {
            warn!(pod = %id, "No snapshot found during cleanup");
            store.remove(id).await;
            return;
        }
    };

    let phase = snap.status.phase;
    let annotations = &snap.spec.metadata.annotations;
    let keep_work = should_keep_work_dir(
        phase,
        annotations,
        config.keep_deleted_pod_dir,
        config.keep_failed_pod_dir,
    );

    debug!(pod = %id, %phase, keep_work, "Cleanup decision");

    // Archive before store removal (needs original spec).
    // Archive is written inside the pod dir: <pod_dir>/archive.tgz
    let pod_dir = executor_provider.work_dir(id);
    if config.pod_archive_ttl_days > 0 {
        let log_dir = executor_provider.log_dir(id);
        if let Err(e) = create_pod_archive(
            &pod_dir,
            id,
            store,
            log_dir.as_deref(),
        ).await {
            warn!(pod = %id, error = %e, "Failed to create pod archive, continuing cleanup");
        }
        // Remove the disk-usage cache file: the value is stale post-pod
        // and meaningless to anyone scanning a kept pod_dir later.
        // Idempotent — silently no-op if it never existed.
        let _ = fs::remove_file(pod_dir.join(SIZE_FILE));
    }

    // Remove store entry
    store.remove(id).await;

    // Delete or keep pod dir
    let work_dir = pod_dir;
    if !keep_work {
        if work_dir.exists() {
            if let Err(e) = fs::remove_dir_all(&work_dir) {
                warn!(error = %e, "Failed to remove work directory");
            } else {
                debug!("Work directory removed");
            }
        }
    } else {
        // FINISHED sentinel should already exist from monitor_pod.
        // Ensure it exists for the terminal-delete path (pod may not have
        // gone through monitor_pod completion yet).
        let sentinel = work_dir.join(FINISHED_SENTINEL);
        if !sentinel.exists()
            && let Err(e) = fs::write(&sentinel, chrono::Utc::now().to_rfc3339()) {
                warn!(error = %e, "Failed to write FINISHED sentinel");
            }
        debug!(%phase, "Work directory kept");
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn parse_bool_annotation_true() {
        let mut ann = HashMap::new();
        ann.insert("key".to_string(), "true".to_string());
        assert_eq!(parse_bool_annotation(&ann, "key"), Some(true));
    }

    #[tokio::test]
    async fn parse_bool_annotation_false() {
        let mut ann = HashMap::new();
        ann.insert("key".to_string(), "false".to_string());
        assert_eq!(parse_bool_annotation(&ann, "key"), Some(false));
    }

    #[tokio::test]
    async fn parse_bool_annotation_absent() {
        let ann = HashMap::new();
        assert_eq!(parse_bool_annotation(&ann, "key"), None);
    }

    #[tokio::test]
    async fn parse_bool_annotation_invalid() {
        let mut ann = HashMap::new();
        ann.insert("key".to_string(), "yes".to_string());
        assert_eq!(parse_bool_annotation(&ann, "key"), None);
    }

    #[tokio::test]
    async fn keep_work_dir_failed_with_keep_failed() {
        let ann = HashMap::new();
        assert!(should_keep_work_dir(PodPhase::Failed, &ann, false, true));
    }

    #[tokio::test]
    async fn keep_work_dir_failed_without_keep_failed() {
        let ann = HashMap::new();
        assert!(!should_keep_work_dir(PodPhase::Failed, &ann, false, false));
    }

    #[tokio::test]
    async fn keep_work_dir_succeeded_with_keep_deleted() {
        let ann = HashMap::new();
        assert!(should_keep_work_dir(PodPhase::Succeeded, &ann, true, false));
    }

    #[tokio::test]
    async fn keep_work_dir_succeeded_without_keep() {
        let ann = HashMap::new();
        assert!(!should_keep_work_dir(PodPhase::Succeeded, &ann, false, false));
    }

    #[tokio::test]
    async fn keep_work_dir_annotation_overrides_config() {
        // Config says don't keep failed, but annotation says keep
        let mut ann = HashMap::new();
        ann.insert(ANN_KEEP_FAILED_POD_DIR.to_string(), "true".to_string());
        assert!(should_keep_work_dir(PodPhase::Failed, &ann, false, false));
    }

    #[tokio::test]
    async fn keep_work_dir_annotation_disables_keep() {
        // Config says keep failed, but annotation says don't
        let mut ann = HashMap::new();
        ann.insert(ANN_KEEP_FAILED_POD_DIR.to_string(), "false".to_string());
        assert!(!should_keep_work_dir(PodPhase::Failed, &ann, false, true));
    }

    // -- create_pod_archive --

    use crate::spec::pod::PodSpec;
    use crate::spec::status::PodStatus;
    use crate::store::memory::MemoryStore;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};

    static TEST_COUNTER: AtomicU64 = AtomicU64::new(0);

    fn test_tmp() -> PathBuf {
        let n = TEST_COUNTER.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!(
            "pr-cleanup-test-{}-{}",
            std::process::id(),
            n,
        ));
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    async fn make_store_with_pod(id: &PodId, phase: PodPhase) -> MemoryStore {
        let store = MemoryStore::new();
        let spec = Arc::new(PodSpec::default());
        let mut status = PodStatus::default();
        status.phase = phase;
        store.put(id.clone(), Arc::clone(&spec), Arc::clone(&spec), status).await;
        store
    }

    /// Stub executor provider that uses temp dirs.
    /// Container logs go inside the pod work dir at `log/`.
    struct StubProvider {
        work_dir: PathBuf,
    }

    impl StubProvider {
        fn new(base: &Path) -> Self {
            Self {
                work_dir: base.join("work"),
            }
        }
    }

    impl super::super::ExecutorProvider for StubProvider {
        fn create(
            &self,
            _pod: Arc<PodSpec>,
            _id: &PodId,
        ) -> crate::pod::executor::PodExecutor {
            unimplemented!("stub")
        }
        fn work_dir(&self, id: &PodId) -> PathBuf {
            self.work_dir.join(id.full())
        }
        fn log_dir(&self, id: &PodId) -> Option<PathBuf> {
            Some(self.work_dir.join(id.full()).join("log"))
        }
    }

    #[tokio::test]
    async fn archive_contains_spec_and_logs() {
        let tmp = test_tmp();
        let store = MemoryStore::new();
        let id = PodId::new("default", "app", "uid-1");
        let spec = Arc::new(PodSpec::default());
        store.put(id.clone(), Arc::clone(&spec), Arc::clone(&spec), PodStatus::default()).await;

        let pod_dir = tmp.join("pod-dir");
        fs::create_dir_all(&pod_dir).unwrap();
        let log_dir = tmp.join("logs");

        let container_log_dir = log_dir.join("app");
        fs::create_dir_all(&container_log_dir).unwrap();
        fs::write(container_log_dir.join("0.log"), "log line 1\n").unwrap();

        let result = create_pod_archive(&pod_dir, &id, &store, Some(&log_dir)).await;
        assert!(result.is_ok(), "archive creation failed: {:?}", result.err());

        let archive_path = result.unwrap();
        assert!(archive_path.exists());

        let file = fs::File::open(&archive_path).unwrap();
        let dec = flate2::read::GzDecoder::new(file);
        let mut archive = tar::Archive::new(dec);
        let entries: Vec<String> = archive
            .entries()
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.path().unwrap().to_string_lossy().to_string())
            .collect();

        assert!(entries.contains(&"original-spec.json".to_string()));
        assert!(entries.contains(&"logs/app/0.log".to_string()));

        let _ = fs::remove_dir_all(&tmp);
    }

    #[tokio::test]
    async fn archive_spec_only_no_logs() {
        let tmp = test_tmp();
        let store = MemoryStore::new();
        let id = PodId::new("default", "app", "uid-2");
        let spec = Arc::new(PodSpec::default());
        store.put(id.clone(), Arc::clone(&spec), Arc::clone(&spec), PodStatus::default()).await;

        let pod_dir = tmp.join("pod-dir");
        fs::create_dir_all(&pod_dir).unwrap();
        let result = create_pod_archive(&pod_dir, &id, &store, None).await;
        assert!(result.is_ok());

        let file = fs::File::open(result.unwrap()).unwrap();
        let dec = flate2::read::GzDecoder::new(file);
        let mut archive = tar::Archive::new(dec);
        let entries: Vec<String> = archive
            .entries()
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.path().unwrap().to_string_lossy().to_string())
            .collect();

        assert!(entries.contains(&"original-spec.json".to_string()));
        assert_eq!(entries.len(), 1);

        let _ = fs::remove_dir_all(&tmp);
    }

    // -- cleanup_pod --

    #[tokio::test]
    async fn cleanup_deletes_work_dir_when_not_kept() {
        let tmp = test_tmp();
        let id = PodId::new("default", "app", "uid-1");
        let store = make_store_with_pod(&id, PodPhase::Succeeded).await;
        let provider = StubProvider::new(&tmp);

        // Create work and log dirs
        let work = provider.work_dir(&id);
        let log = provider.log_dir(&id).unwrap();
        fs::create_dir_all(&work).unwrap();
        fs::create_dir_all(&log).unwrap();

        let config = CleanupConfig {
            keep_deleted_pod_dir: false,
            keep_failed_pod_dir: false,
            pod_archive_ttl_days: 0,
            ..CleanupConfig::default()
        };

        cleanup_pod(&id, &store, &provider, &config).await;

        assert!(!work.exists(), "work dir should be deleted");
        assert!(!log.exists(), "log dir should be deleted");
        assert!(store.get(&id, SpecView::Original).await.is_none(), "store entry should be removed");

        let _ = fs::remove_dir_all(&tmp);
    }

    #[tokio::test]
    async fn cleanup_keeps_work_dir_for_failed_pod() {
        let tmp = test_tmp();
        let id = PodId::new("default", "app", "uid-2");
        let store = make_store_with_pod(&id, PodPhase::Failed).await;
        let provider = StubProvider::new(&tmp);

        let work = provider.work_dir(&id);
        let log = provider.log_dir(&id).unwrap();
        fs::create_dir_all(&work).unwrap();
        fs::create_dir_all(&log).unwrap();

        let config = CleanupConfig {
            keep_deleted_pod_dir: false,
            keep_failed_pod_dir: true,
            pod_archive_ttl_days: 0,
            ..CleanupConfig::default()
        };

        cleanup_pod(&id, &store, &provider, &config).await;

        assert!(work.exists(), "work dir should be kept for failed pod");
        assert!(work.join(FINISHED_SENTINEL).exists(), "FINISHED sentinel should exist");
        assert!(log.exists(), "log dir should be kept (inside work dir)");

        let _ = fs::remove_dir_all(&tmp);
    }

    #[tokio::test]
    async fn cleanup_creates_archive_when_configured() {
        let tmp = test_tmp();
        let id = PodId::new("default", "app", "uid-3");
        let store = make_store_with_pod(&id, PodPhase::Succeeded).await;
        let provider = StubProvider::new(&tmp);

        let work = provider.work_dir(&id);
        let log = provider.log_dir(&id).unwrap();
        fs::create_dir_all(&work).unwrap();
        let container_log = log.join("app");
        fs::create_dir_all(&container_log).unwrap();
        fs::write(container_log.join("0.log"), "hello").unwrap();

        let config = CleanupConfig {
            keep_deleted_pod_dir: true,  // keep so we can inspect the archive
            keep_failed_pod_dir: true,
            pod_archive_ttl_days: 7,
            pods_dir: tmp.join("work"),
            ..CleanupConfig::default()
        };

        cleanup_pod(&id, &store, &provider, &config).await;

        // Archive is inside the pod dir: <work>/<pod_id>/archive.tgz
        let archive = work.join("archive.tgz");
        assert!(archive.exists(), "archive should be created");

        let _ = fs::remove_dir_all(&tmp);
    }

    #[tokio::test]
    async fn cleanup_removes_size_file_on_archival() {
        let tmp = test_tmp();
        let id = PodId::new("default", "app", "uid-archive-size");
        let store = make_store_with_pod(&id, PodPhase::Succeeded).await;
        let provider = StubProvider::new(&tmp);

        let work = provider.work_dir(&id);
        let log = provider.log_dir(&id).unwrap();
        fs::create_dir_all(&work).unwrap();
        fs::create_dir_all(&log).unwrap();
        // Plant a stale _size cache file.
        fs::write(work.join(SIZE_FILE), "12345\n").unwrap();

        let config = CleanupConfig {
            keep_deleted_pod_dir: true,
            keep_failed_pod_dir: true,
            pod_archive_ttl_days: 7,
            pods_dir: tmp.join("work"),
            ..CleanupConfig::default()
        };

        cleanup_pod(&id, &store, &provider, &config).await;

        assert!(work.join("archive.tgz").exists(), "archive should be created");
        assert!(
            !work.join(SIZE_FILE).exists(),
            "_size file should be removed on archival",
        );

        let _ = fs::remove_dir_all(&tmp);
    }

    #[tokio::test]
    async fn cleanup_removes_size_file_idempotent_when_absent() {
        let tmp = test_tmp();
        let id = PodId::new("default", "app", "uid-archive-no-size");
        let store = make_store_with_pod(&id, PodPhase::Succeeded).await;
        let provider = StubProvider::new(&tmp);

        let work = provider.work_dir(&id);
        let log = provider.log_dir(&id).unwrap();
        fs::create_dir_all(&work).unwrap();
        fs::create_dir_all(&log).unwrap();
        // Note: no _size file planted.

        let config = CleanupConfig {
            keep_deleted_pod_dir: true,
            keep_failed_pod_dir: true,
            pod_archive_ttl_days: 7,
            pods_dir: tmp.join("work"),
            ..CleanupConfig::default()
        };

        // Should not panic even though _size doesn't exist.
        cleanup_pod(&id, &store, &provider, &config).await;
        assert!(work.join("archive.tgz").exists());

        let _ = fs::remove_dir_all(&tmp);
    }

    #[tokio::test]
    async fn cleanup_skips_archive_when_ttl_zero() {
        let tmp = test_tmp();
        let id = PodId::new("default", "app", "uid-4");
        let store = make_store_with_pod(&id, PodPhase::Succeeded).await;
        let provider = StubProvider::new(&tmp);

        let work = provider.work_dir(&id);
        fs::create_dir_all(&work).unwrap();

        let config = CleanupConfig {
            pod_archive_ttl_days: 0,
            pods_dir: tmp.join("work"),
            ..CleanupConfig::default()
        };

        cleanup_pod(&id, &store, &provider, &config).await;

        assert!(!work.join("archive.tgz").exists(), "archive should not be created");

        let _ = fs::remove_dir_all(&tmp);
    }

    #[tokio::test]
    async fn cleanup_continues_after_archive_failure() {
        let tmp = test_tmp();
        let id = PodId::new("default", "app", "uid-5");
        let store = make_store_with_pod(&id, PodPhase::Succeeded).await;
        let provider = StubProvider::new(&tmp);

        let work = provider.work_dir(&id);
        // Don't create work dir — archive will fail because it can't write archive.tgz

        let config = CleanupConfig {
            keep_deleted_pod_dir: false,
            keep_failed_pod_dir: false,
            pod_archive_ttl_days: 7,
            pods_dir: tmp.join("work"),
            ..CleanupConfig::default()
        };

        // Should not panic — archive failure is logged and cleanup continues
        cleanup_pod(&id, &store, &provider, &config).await;

        assert!(!work.exists(), "work dir should still not exist");
        assert!(store.get(&id, SpecView::Original).await.is_none(), "store should be removed");

        let _ = fs::remove_dir_all(&tmp);
    }
}
