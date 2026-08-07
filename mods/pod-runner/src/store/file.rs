// Copyright 2026 Tabsdata Inc.

//! File-based [`PodStore`] implementation.
//!
//! Persists pod specs and statuses to disk. On creation, a timestamped
//! directory is created under the root. Each pod gets its own
//! subdirectory containing spec files and timestamped status snapshots.
//!
//! # Directory layout
//!
//! ```text
//! root/
//!   20260407T194530/                    # store session (start time)
//!     default_app_uid-1/                # pod dir (PodId::full())
//!       original-spec.json
//!       running-spec.json               # optional (see FileStoreConfig)
//!       status/
//!         20260407T194530123.json        # timestamped status snapshots
//!         20260407T194531456.json
//!     prod_web_uid-2/
//!       ...
//! ```
//!
//! # Logging
//!
//! The store directory is logged at INFO on creation. All other
//! operations only log on error (WARN).
//!
//! # Blocking vs non-blocking I/O
//!
//! This implementation uses synchronous `std::fs` under an async
//! trait. That's a deliberate choice given the observed workload:
//!
//! - Writes are small (≤2 KB status JSON files) — sub-ms on SSD.
//! - The hot path is `update_status`, which at 500 concurrent pods
//!   peaks at ~80 writes/sec — ~10 ms of aggregate blocking time
//!   per second across the tokio blocking pool.
//! - Reads (`get`, `list`, `get_by_name`) are usually served from
//!   [`MemoryStore`](super::memory::MemoryStore) via
//!   [`MultiplexStore`](super::multiplex::MultiplexStore); `FileStore`
//!   reads are predominantly test-only.
//!
//! Under those conditions the blocking cost is negligible and the
//! `std::fs` + `serde_json` code stays simple.
//!
//! If this store is deployed on slower storage (network FS, remote
//! block device) where individual writes aren't sub-ms, wrap each
//! hot-path method body in [`tokio::task::spawn_blocking`] to move
//! the I/O off the tokio worker threads. The trait is already async,
//! so callers need no changes — only this file does.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use tracing::{debug, debug_span, warn};

use crate::spec::pod::PodSpec;
use crate::spec::status::PodStatus;

use super::{PodId, PodSnapshot, PodStore, SpecView};

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Configuration for the file store.
pub struct FileStoreConfig {
    /// Root directory for the store. A timestamped session directory
    /// is created under this path.
    pub root_dir: PathBuf,
    /// Whether to write the running (prepared) spec to disk.
    ///
    /// Set to `false` if the running spec may contain resolved secrets
    /// in environment variables that should not be persisted.
    pub write_running_spec: bool,
}

impl Default for FileStoreConfig {
    fn default() -> Self {
        Self {
            root_dir: PathBuf::from("run").join("pods"),
            write_running_spec: false,
        }
    }
}

// ---------------------------------------------------------------------------
// FileStore
// ---------------------------------------------------------------------------

/// File-based pod store. Persists specs and statuses under each pod's
/// `store/` subdirectory: `<pods_dir>/<pod_id>/store/`.
///
/// Run rotation (see `rotate_run_dir`) provides session isolation —
/// each pod-runner restart writes to a fresh `run/pods/` tree.
pub struct FileStore {
    /// Pods directory: `<workdir>/run/pods/`.
    pods_dir: PathBuf,
    config: FileStoreConfig,
}

impl FileStore {
    /// Create a new file store.
    ///
    /// `config.root_dir` points at the pods directory
    /// (`<workdir>/run/pods/`). No session subdirectory is created —
    /// run rotation handles isolation between restarts.
    pub fn new(config: impl Into<FileStoreConfig>) -> Self {
        let config = config.into();
        let pods_dir = config.root_dir.clone();
        if let Err(e) = fs::create_dir_all(&pods_dir) {
            warn!(error = %e, dir = ?pods_dir, "Failed to create pods directory");
        }
        debug!(dir = ?pods_dir, "File store initialized");
        Self { pods_dir, config }
    }

    /// The pods directory path.
    pub fn pods_dir(&self) -> &Path {
        &self.pods_dir
    }

    // -- internal helpers --------------------------------------------------

    fn pod_dir(&self, id: &PodId) -> PathBuf {
        self.pods_dir.join(id.full())
    }

    /// Per-pod store directory: `<pods_dir>/<pod_id>/store/`.
    fn store_dir(&self, id: &PodId) -> PathBuf {
        self.pod_dir(id).join("store")
    }

    fn status_dir(&self, id: &PodId) -> PathBuf {
        self.store_dir(id).join("status")
    }

    fn write_json<T: serde::Serialize>(&self, path: &Path, value: &T) {
        if let Some(parent) = path.parent()
            && let Err(e) = fs::create_dir_all(parent)
        {
            warn!(error = %e, path = ?path, "Failed to create directory");
            return;
        }
        match serde_json::to_string_pretty(value) {
            Ok(json) => {
                if let Err(e) = fs::write(path, json) {
                    warn!(error = %e, path = ?path, "Failed to write file");
                }
            }
            Err(e) => {
                warn!(error = %e, path = ?path, "Failed to serialize");
            }
        }
    }

    fn read_json<T: serde::de::DeserializeOwned>(&self, path: &Path) -> Option<T> {
        let json = match fs::read_to_string(path) {
            Ok(j) => j,
            Err(_) => return None,
        };
        match serde_json::from_str(&json) {
            Ok(value) => Some(value),
            Err(e) => {
                warn!(error = %e, path = ?path, "Failed to deserialize");
                None
            }
        }
    }

    fn write_status(&self, id: &PodId, status: &PodStatus) {
        let ts = chrono::Utc::now().format("%Y%m%dT%H%M%S%3f");
        let path = self.status_dir(id).join(format!("{}.json", ts));
        self.write_json(&path, status);
    }

    fn read_latest_status(&self, id: &PodId) -> Option<PodStatus> {
        let status_dir = self.status_dir(id);
        let mut entries: Vec<PathBuf> = fs::read_dir(&status_dir)
            .ok()?
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.extension().is_some_and(|ext| ext == "json"))
            .collect();
        entries.sort();
        entries.last().and_then(|path| self.read_json(path))
    }

    fn read_spec(&self, id: &PodId, view: SpecView) -> Option<Arc<PodSpec>> {
        let store = self.store_dir(id);
        let path = match view {
            SpecView::Running => {
                let running = store.join("running-spec.json");
                if running.exists() {
                    running
                } else {
                    store.join("original-spec.json")
                }
            }
            SpecView::Original => store.join("original-spec.json"),
        };
        self.read_json::<PodSpec>(&path).map(Arc::new)
    }

    fn read_snapshot(&self, id: &PodId, view: SpecView) -> Option<PodSnapshot> {
        let spec = self.read_spec(id, view)?;
        let status = self.read_latest_status(id)?;
        Some(PodSnapshot {
            id: id.clone(),
            spec,
            status,
        })
    }

    fn list_pod_ids(&self) -> Vec<PodId> {
        let entries = match fs::read_dir(&self.pods_dir) {
            Ok(rd) => rd,
            Err(_) => return Vec::new(),
        };
        entries
            .filter_map(|e| e.ok())
            .filter(|e| e.path().is_dir())
            .filter_map(|e| {
                let spec_path = e.path().join("store").join("original-spec.json");
                let spec: PodSpec = self.read_json(&spec_path)?;
                Some(PodId::from_spec(&spec))
            })
            .collect()
    }
}

// ---------------------------------------------------------------------------
// PodStore implementation
// ---------------------------------------------------------------------------

#[async_trait]
impl PodStore for FileStore {
    async fn put(
        &self,
        id: PodId,
        original_spec: Arc<PodSpec>,
        running_spec: Arc<PodSpec>,
        status: PodStatus,
    ) {
        let _span = debug_span!("file_store", pod = %id).entered();
        let store = self.store_dir(&id);
        if let Err(e) = fs::create_dir_all(&store) {
            warn!(error = %e, "Failed to create pod store directory");
            return;
        }

        self.write_json(&store.join("original-spec.json"), original_spec.as_ref());

        if self.config.write_running_spec {
            self.write_json(&store.join("running-spec.json"), running_spec.as_ref());
        }

        self.write_status(&id, &status);
    }

    async fn update_status(&self, id: &PodId, status: PodStatus) {
        let _span = debug_span!("file_store", pod = %id).entered();
        if self.pod_dir(id).exists() {
            self.write_status(id, &status);
        }
    }

    async fn get(&self, id: &PodId, view: SpecView) -> Option<PodSnapshot> {
        self.read_snapshot(id, view)
    }

    async fn get_by_name(
        &self,
        namespace: &str,
        name: &str,
        view: SpecView,
    ) -> Option<PodSnapshot> {
        let prefix = format!("{}_{}_", namespace, name);
        let entries = match fs::read_dir(&self.pods_dir) {
            Ok(rd) => rd,
            Err(_) => return None,
        };
        for entry in entries.filter_map(|e| e.ok()) {
            let dir_name = entry.file_name();
            let dir_name = dir_name.to_string_lossy();
            if dir_name.starts_with(&prefix) && entry.path().is_dir() {
                let spec_path = entry.path().join("store").join("original-spec.json");
                if let Some(spec) = self.read_json::<PodSpec>(&spec_path) {
                    let id = PodId::from_spec(&spec);
                    if id.namespace == namespace && id.name == name {
                        return self.read_snapshot(&id, view);
                    }
                }
            }
        }
        None
    }

    async fn list(&self, view: SpecView) -> Vec<PodSnapshot> {
        self.list_pod_ids()
            .iter()
            .filter_map(|id| self.read_snapshot(id, view))
            .collect()
    }

    async fn list_in_namespace(&self, namespace: &str, view: SpecView) -> Vec<PodSnapshot> {
        self.list_pod_ids()
            .iter()
            .filter(|id| id.namespace == namespace)
            .filter_map(|id| self.read_snapshot(id, view))
            .collect()
    }

    async fn remove(&self, id: &PodId) -> Option<PodSnapshot> {
        let _span = debug_span!("file_store", pod = %id).entered();
        let snap = self.read_snapshot(id, SpecView::Running);
        // Remove only the store/ subdir — the pod dir may contain
        // work/ and log/ that the reaper manages separately.
        let store = self.store_dir(id);
        if store.exists()
            && let Err(e) = fs::remove_dir_all(&store)
        {
            warn!(error = %e, "Failed to remove pod store directory");
        }
        snap
    }

    /// Reads the snapshot back but leaves `store/` on disk: the specs and
    /// statuses are the pod's troubleshooting record, and the reaper's
    /// delete deadline decides when they go.
    async fn evict(&self, id: &PodId) -> Option<PodSnapshot> {
        let _span = debug_span!("file_store", pod = %id).entered();
        self.read_snapshot(id, SpecView::Running)
    }

    async fn len(&self) -> usize {
        match fs::read_dir(&self.pods_dir) {
            Ok(rd) => rd
                .filter_map(|e| e.ok())
                .filter(|e| e.path().join("store").is_dir())
                .count(),
            Err(_) => 0,
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::constants::PodPhase;
    use crate::spec::pod::{PodMetadata, PodSpecBody};

    fn temp_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "pod-runner-rs2-test-{}-{}",
            name,
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&dir);
        dir
    }

    fn make_store(name: &str) -> FileStore {
        FileStore::new(FileStoreConfig {
            root_dir: temp_dir(name),
            write_running_spec: true,
        })
    }

    fn make_spec(namespace: &str, name: &str, uid: &str) -> Arc<PodSpec> {
        Arc::new(PodSpec {
            metadata: PodMetadata {
                namespace: namespace.to_string(),
                name: name.to_string(),
                uid: uid.to_string(),
                ..Default::default()
            },
            spec: PodSpecBody::default(),
            ..Default::default()
        })
    }

    fn make_running_spec(namespace: &str, name: &str, uid: &str) -> Arc<PodSpec> {
        make_spec(namespace, name, &format!("{}_running", uid))
    }

    fn make_status(phase: PodPhase) -> PodStatus {
        PodStatus {
            phase,
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn creates_session_directory() {
        let store = make_store("session-dir");
        assert!(store.pods_dir().exists());
        assert!(store.pods_dir().is_dir());
    }

    #[tokio::test]
    async fn put_and_get() {
        let store = make_store("put-get");
        let original = make_spec("default", "app", "uid-1");
        let running = make_running_spec("default", "app", "uid-1");
        let id = PodId::from_spec(&original);

        store
            .put(
                id.clone(),
                original,
                running,
                make_status(PodPhase::Pending),
            )
            .await;

        // Running view returns running spec
        let snap = store.get(&id, SpecView::Running).await.unwrap();
        assert_eq!(snap.id.name, "app");
        assert_eq!(snap.spec.metadata.uid, "uid-1_running");
        assert_eq!(snap.status.phase, PodPhase::Pending);

        // Original view returns original spec
        let snap = store.get(&id, SpecView::Original).await.unwrap();
        assert_eq!(snap.spec.metadata.uid, "uid-1");
    }

    #[tokio::test]
    async fn put_without_running_spec() {
        let config = FileStoreConfig {
            root_dir: temp_dir("no-running"),
            write_running_spec: false,
        };
        let store = FileStore::new(config);
        let original = make_spec("default", "app", "uid-1");
        let running = make_running_spec("default", "app", "uid-1");
        let id = PodId::from_spec(&original);

        store
            .put(
                id.clone(),
                original,
                running,
                make_status(PodPhase::Pending),
            )
            .await;

        // Running view falls back to original when running-spec.json absent
        let snap = store.get(&id, SpecView::Running).await.unwrap();
        assert_eq!(snap.spec.metadata.uid, "uid-1");

        // No running-spec.json on disk (specs are under store/)
        assert!(!store.store_dir(&id).join("running-spec.json").exists());
        assert!(store.store_dir(&id).join("original-spec.json").exists());
    }

    #[tokio::test]
    async fn update_status_writes_new_file() {
        let store = make_store("update-status");
        let spec = make_spec("default", "app", "uid-1");
        let id = PodId::from_spec(&spec);

        store
            .put(
                id.clone(),
                Arc::clone(&spec),
                spec,
                make_status(PodPhase::Pending),
            )
            .await;

        // Small delay so the timestamp differs
        std::thread::sleep(std::time::Duration::from_millis(5));
        store
            .update_status(&id, make_status(PodPhase::Running))
            .await;

        let snap = store.get(&id, SpecView::Running).await.unwrap();
        assert_eq!(snap.status.phase, PodPhase::Running);

        // Two status files on disk
        let status_dir = store.status_dir(&id);
        let count = fs::read_dir(&status_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .count();
        assert_eq!(count, 2);
    }

    #[tokio::test]
    async fn update_status_nonexistent_is_noop() {
        let store = make_store("update-noop");
        let id = PodId::new("default", "nope", "uid");
        store
            .update_status(&id, make_status(PodPhase::Running))
            .await;
        assert!(store.get(&id, SpecView::Running).await.is_none());
    }

    #[tokio::test]
    async fn get_by_name() {
        let store = make_store("get-by-name");
        let spec = make_spec("prod", "web", "uid-3");
        let running = make_running_spec("prod", "web", "uid-3");
        let id = PodId::from_spec(&spec);
        store
            .put(id, spec, running, make_status(PodPhase::Running))
            .await;

        let snap = store
            .get_by_name("prod", "web", SpecView::Running)
            .await
            .unwrap();
        assert_eq!(snap.id.uid, "uid-3");
        assert_eq!(snap.spec.metadata.uid, "uid-3_running");

        let snap = store
            .get_by_name("prod", "web", SpecView::Original)
            .await
            .unwrap();
        assert_eq!(snap.spec.metadata.uid, "uid-3");

        assert!(
            store
                .get_by_name("prod", "nope", SpecView::Running)
                .await
                .is_none()
        );
        assert!(
            store
                .get_by_name("staging", "web", SpecView::Running)
                .await
                .is_none()
        );
    }

    #[tokio::test]
    async fn list_and_len() {
        let store = make_store("list");
        assert!(store.is_empty().await);

        for i in 0..3 {
            let original = make_spec("default", &format!("pod-{}", i), &format!("uid-{}", i));
            let running =
                make_running_spec("default", &format!("pod-{}", i), &format!("uid-{}", i));
            let id = PodId::from_spec(&original);
            store
                .put(id, original, running, make_status(PodPhase::Pending))
                .await;
        }

        assert_eq!(store.len().await, 3);
        assert!(!store.is_empty().await);

        let pods = store.list(SpecView::Running).await;
        assert_eq!(pods.len(), 3);
        assert!(
            pods.iter()
                .all(|s| s.spec.metadata.uid.ends_with("_running"))
        );

        let pods = store.list(SpecView::Original).await;
        assert_eq!(pods.len(), 3);
        assert!(
            pods.iter()
                .all(|s| !s.spec.metadata.uid.ends_with("_running"))
        );
    }

    #[tokio::test]
    async fn list_in_namespace() {
        let store = make_store("list-ns");
        store
            .put(
                PodId::new("ns1", "a", "1"),
                make_spec("ns1", "a", "1"),
                make_running_spec("ns1", "a", "1"),
                make_status(PodPhase::Running),
            )
            .await;
        store
            .put(
                PodId::new("ns1", "b", "2"),
                make_spec("ns1", "b", "2"),
                make_running_spec("ns1", "b", "2"),
                make_status(PodPhase::Running),
            )
            .await;
        store
            .put(
                PodId::new("ns2", "c", "3"),
                make_spec("ns2", "c", "3"),
                make_running_spec("ns2", "c", "3"),
                make_status(PodPhase::Running),
            )
            .await;

        assert_eq!(
            store
                .list_in_namespace("ns1", SpecView::Running)
                .await
                .len(),
            2
        );
        assert_eq!(
            store
                .list_in_namespace("ns2", SpecView::Running)
                .await
                .len(),
            1
        );
        assert_eq!(
            store
                .list_in_namespace("ns3", SpecView::Running)
                .await
                .len(),
            0
        );
    }

    #[tokio::test]
    async fn remove_deletes_directory() {
        let store = make_store("remove");
        let spec = make_spec("default", "app", "uid-1");
        let id = PodId::from_spec(&spec);
        store
            .put(
                id.clone(),
                Arc::clone(&spec),
                spec,
                make_status(PodPhase::Pending),
            )
            .await;

        assert!(store.contains(&id).await);
        let snap = store.remove(&id).await.unwrap();
        assert_eq!(snap.id.name, "app");
        assert!(!store.contains(&id).await);
        assert!(!store.store_dir(&id).exists());
        assert_eq!(store.len().await, 0);
    }

    #[tokio::test]
    async fn evict_keeps_the_directory() {
        let store = make_store("evict");
        let spec = make_spec("default", "app", "uid-1");
        let id = PodId::from_spec(&spec);
        store
            .put(
                id.clone(),
                Arc::clone(&spec),
                spec,
                make_status(PodPhase::Failed),
            )
            .await;

        let snap = store.evict(&id).await.unwrap();
        assert_eq!(snap.id.name, "app");
        assert!(
            store.store_dir(&id).exists(),
            "the specs and statuses are the pod's record — evicting a pod \
             from the API must not destroy them",
        );
        assert!(
            store.store_dir(&id).join("original-spec.json").exists(),
            "the spec survives",
        );
    }

    #[tokio::test]
    async fn evict_nonexistent() {
        let store = make_store("evict-missing");
        let id = PodId::new("default", "ghost", "uid-x");
        assert!(store.evict(&id).await.is_none());
    }

    #[tokio::test]
    async fn remove_nonexistent() {
        let store = make_store("remove-nope");
        let id = PodId::new("default", "nope", "uid");
        assert!(store.remove(&id).await.is_none());
    }

    #[tokio::test]
    async fn status_reads_latest() {
        let store = make_store("status-latest");
        let spec = make_spec("default", "app", "uid-1");
        let id = PodId::from_spec(&spec);

        store
            .put(
                id.clone(),
                Arc::clone(&spec),
                spec,
                make_status(PodPhase::Pending),
            )
            .await;

        for phase in [PodPhase::Running, PodPhase::Succeeded] {
            std::thread::sleep(std::time::Duration::from_millis(5));
            store.update_status(&id, make_status(phase)).await;
        }

        let snap = store.get(&id, SpecView::Running).await.unwrap();
        assert_eq!(snap.status.phase, PodPhase::Succeeded);

        // Three status files total
        let count = fs::read_dir(store.status_dir(&id))
            .unwrap()
            .filter_map(|e| e.ok())
            .count();
        assert_eq!(count, 3);
    }

    #[tokio::test]
    async fn put_replaces_pod() {
        let store = make_store("put-replace");
        let original = make_spec("default", "app", "uid-1");
        let running = make_running_spec("default", "app", "uid-1");
        let id = PodId::from_spec(&original);

        store
            .put(
                id.clone(),
                Arc::clone(&original),
                Arc::clone(&running),
                make_status(PodPhase::Pending),
            )
            .await;

        std::thread::sleep(std::time::Duration::from_millis(5));
        store
            .put(
                id.clone(),
                original,
                running,
                make_status(PodPhase::Running),
            )
            .await;

        let snap = store.get(&id, SpecView::Running).await.unwrap();
        assert_eq!(snap.status.phase, PodPhase::Running);
        assert_eq!(store.len().await, 1);
    }
}
