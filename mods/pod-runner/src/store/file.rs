// Copyright 2026 Tabsdata Inc.

//! File-based [`PodStore`] implementation.
//!
//! Persists pod specs and statuses to disk, in the `store/` subdir of each
//! pod incarnation's state dir (see [`PodLayout`]).
//!
//! # Directory layout
//!
//! ```text
//! pod-state/
//!   3f/                                 # bucket (layout::state_bucket)
//!     app/                              # pod name
//!       20260407T194530Z/               # incarnation (creation time)
//!         store/
//!           original-spec.json          # env values masked
//!           running-spec.json           # optional (see FileStoreConfig)
//!           status/
//!             20260407T194530123.json   # timestamped status snapshots
//!             20260407T194531456.json
//! ```
//!
//! # Write-only in production
//!
//! The server only ever writes through this store. It sits behind a
//! [`MultiplexStore`](super::multiplex::MultiplexStore) that sends every
//! read to the [`MemoryStore`](super::memory::MemoryStore), so `get`,
//! `get_by_name`, `list`, `list_in_namespace` and `len` here are never
//! called outside tests. They are kept so the store is a complete
//! [`PodStore`] and its writes can be checked by reading them back. The
//! on-disk record is read by people and tools, not through this API.
//!
//! The state dir is a function of the spec, so it is resolved once, at
//! [`put`](PodStore::put), and remembered per pod id for the calls that
//! only carry the id. The reads therefore cover the pods put into this
//! store — those of this run; the dirs earlier runs left are records on
//! disk, not pods it serves.
//!
//! State dirs outlive a restart, and a pod replayed after one keeps its
//! creation time, so it writes into the dir its previous attempt left: the
//! spec is overwritten and the status history carries on, latest snapshot
//! first.
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
//! - Reads are never served from here in production (see above).
//!
//! Under those conditions the blocking cost is negligible and the
//! `std::fs` + `serde_json` code stays simple.
//!
//! If this store is deployed on slower storage (network FS, remote
//! block device) where individual writes aren't sub-ms, wrap each
//! hot-path method body in [`tokio::task::spawn_blocking`] to move
//! the I/O off the tokio worker threads. The trait is already async,
//! so callers need no changes — only this file does.

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use tracing::{debug, debug_span, warn};

use crate::spec::pod::PodSpec;
use crate::spec::status::PodStatus;

use super::layout::PodLayout;
use super::{PodId, PodSnapshot, PodStore, SpecView};

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Configuration for the file store.
#[derive(Default)]
pub struct FileStoreConfig {
    /// Pod layout; the store writes under each pod's state dir.
    pub layout: PodLayout,
    /// Whether to write the running (prepared) spec to disk.
    ///
    /// Set to `false` if the running spec may contain resolved secrets
    /// in environment variables that should not be persisted.
    pub write_running_spec: bool,
}

// ---------------------------------------------------------------------------
// FileStore
// ---------------------------------------------------------------------------

/// File-based pod store. Persists specs and statuses under each pod
/// incarnation's `store/` subdirectory:
/// `<pod-state>/<bucket>/<pod_name>/<stamp>/store/`.
pub struct FileStore {
    config: FileStoreConfig,
    /// Store dir of every pod put into this store, by pod id.
    store_dirs: RwLock<HashMap<PodId, PathBuf>>,
}

impl FileStore {
    /// Create a new file store rooted at `config.layout`'s state root.
    pub fn new(config: impl Into<FileStoreConfig>) -> Self {
        let config = config.into();
        let root = &config.layout.state_root;
        if let Err(e) = fs::create_dir_all(root) {
            warn!(error = %e, dir = ?root, "Failed to create pod state directory");
        }
        debug!(dir = ?root, "File store initialized");
        Self {
            config,
            store_dirs: RwLock::new(HashMap::new()),
        }
    }

    /// The pod-state root directory.
    pub fn state_root(&self) -> &Path {
        &self.config.layout.state_root
    }

    // -- internal helpers --------------------------------------------------

    /// The store dir of a pod put into this store: `<state_dir>/store/`.
    fn store_dir(&self, id: &PodId) -> Option<PathBuf> {
        self.store_dirs
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .get(id)
            .cloned()
    }

    /// Every pod put into this store.
    fn pod_ids(&self) -> Vec<PodId> {
        self.store_dirs
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .keys()
            .cloned()
            .collect()
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

    fn write_status(&self, store: &Path, status: &PodStatus) {
        let ts = chrono::Utc::now().format("%Y%m%dT%H%M%S%3f");
        let path = store.join("status").join(format!("{}.json", ts));
        self.write_json(&path, status);
    }

    fn read_latest_status(&self, store: &Path) -> Option<PodStatus> {
        let status_dir = store.join("status");
        let mut entries: Vec<PathBuf> = fs::read_dir(&status_dir)
            .ok()?
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.extension().is_some_and(|ext| ext == "json"))
            .collect();
        entries.sort();
        entries.last().and_then(|path| self.read_json(path))
    }

    fn read_spec(&self, store: &Path, view: SpecView) -> Option<Arc<PodSpec>> {
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
        let store = self.store_dir(id)?;
        let spec = self.read_spec(&store, view)?;
        let status = self.read_latest_status(&store)?;
        Some(PodSnapshot {
            id: id.clone(),
            spec,
            status,
        })
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
        let store = self.config.layout.store_dir(&original_spec);
        if let Err(e) = fs::create_dir_all(&store) {
            warn!(error = %e, "Failed to create pod store directory");
            return;
        }

        self.write_json(
            &store.join("original-spec.json"),
            &original_spec.with_env_values_masked(),
        );

        if self.config.write_running_spec {
            self.write_json(&store.join("running-spec.json"), running_spec.as_ref());
        }

        self.write_status(&store, &status);
        self.store_dirs
            .write()
            .unwrap_or_else(|p| p.into_inner())
            .insert(id, store);
    }

    async fn update_status(&self, id: &PodId, status: PodStatus) {
        let _span = debug_span!("file_store", pod = %id).entered();
        // A state dir its delete deadline already took stays gone.
        if let Some(store) = self.store_dir(id)
            && store.parent().is_some_and(Path::exists)
        {
            self.write_status(&store, &status);
        }
    }

    // -- reads: test-only, the server reads from the memory store ---------

    async fn get(&self, id: &PodId, view: SpecView) -> Option<PodSnapshot> {
        self.read_snapshot(id, view)
    }

    async fn get_by_name(
        &self,
        namespace: &str,
        name: &str,
        view: SpecView,
    ) -> Option<PodSnapshot> {
        self.pod_ids()
            .iter()
            .filter(|id| id.namespace == namespace && id.name == name)
            .find_map(|id| self.read_snapshot(id, view))
    }

    async fn list(&self, view: SpecView) -> Vec<PodSnapshot> {
        self.pod_ids()
            .iter()
            .filter_map(|id| self.read_snapshot(id, view))
            .collect()
    }

    async fn list_in_namespace(&self, namespace: &str, view: SpecView) -> Vec<PodSnapshot> {
        self.pod_ids()
            .iter()
            .filter(|id| id.namespace == namespace)
            .filter_map(|id| self.read_snapshot(id, view))
            .collect()
    }

    async fn remove(&self, id: &PodId) -> Option<PodSnapshot> {
        let _span = debug_span!("file_store", pod = %id).entered();
        let snap = self.read_snapshot(id, SpecView::Running);
        // Remove only the store/ subdir — the state dir also holds log/
        // and the deadline files, which the reaper manages.
        let store = self
            .store_dirs
            .write()
            .unwrap_or_else(|p| p.into_inner())
            .remove(id);
        if let Some(store) = store
            && store.exists()
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
        self.pod_ids()
            .iter()
            .filter(|id| self.store_dir(id).is_some_and(|d| d.is_dir()))
            .count()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::constants::PodPhase;
    use crate::spec::pod::{Container, EnvVar, MASKED_ENV_VALUE, PodMetadata, PodSpecBody};

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
            layout: PodLayout::new(&temp_dir(name)),
            write_running_spec: true,
        })
    }

    fn make_spec(namespace: &str, name: &str, uid: &str) -> Arc<PodSpec> {
        Arc::new(PodSpec {
            metadata: PodMetadata {
                namespace: namespace.to_string(),
                name: name.to_string(),
                uid: uid.to_string(),
                creation_timestamp: Some("2026-10-19T14:30:05Z".to_string()),
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
        assert!(store.state_root().exists());
        assert!(store.state_root().is_dir());
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
            layout: PodLayout::new(&temp_dir("no-running")),
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
        assert!(
            !store
                .store_dir(&id)
                .unwrap()
                .join("running-spec.json")
                .exists()
        );
        assert!(
            store
                .store_dir(&id)
                .unwrap()
                .join("original-spec.json")
                .exists()
        );
    }

    #[tokio::test]
    async fn original_spec_on_disk_has_env_values_masked() {
        let store = make_store("masked-env");
        let mut original = (*make_spec("default", "app", "uid-1")).clone();
        original.spec.containers.push(Container {
            name: "app".to_string(),
            env: vec![EnvVar {
                name: "PASSWORD".to_string(),
                value: Some("s3cr3t".to_string()),
                value_from: None,
            }],
            ..Default::default()
        });
        let original = Arc::new(original);
        let id = PodId::from_spec(&original);

        store
            .put(
                id.clone(),
                Arc::clone(&original),
                Arc::clone(&original),
                make_status(PodPhase::Pending),
            )
            .await;

        let dir = store.store_dir(&id).unwrap();
        let on_disk = fs::read_to_string(dir.join("original-spec.json")).unwrap();
        assert!(!on_disk.contains("s3cr3t"), "{on_disk}");
        assert!(on_disk.contains("PASSWORD"), "{on_disk}");
        assert!(on_disk.contains(MASKED_ENV_VALUE), "{on_disk}");

        let running = fs::read_to_string(dir.join("running-spec.json")).unwrap();
        assert!(running.contains("s3cr3t"), "{running}");
        assert_eq!(
            original.spec.containers[0].env[0].value.as_deref(),
            Some("s3cr3t")
        );
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
        let status_dir = store.store_dir(&id).unwrap().join("status");
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
        let dir = store.store_dir(&id).unwrap();
        let snap = store.remove(&id).await.unwrap();
        assert_eq!(snap.id.name, "app");
        assert!(!store.contains(&id).await);
        assert!(!dir.exists());
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
        let dir = store.store_dir(&id).unwrap();
        assert!(
            dir.exists(),
            "the specs and statuses are the pod's record — evicting a pod \
             from the API must not destroy them",
        );
        assert!(dir.join("original-spec.json").exists(), "the spec survives");
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
        let count = fs::read_dir(store.store_dir(&id).unwrap().join("status"))
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

    #[tokio::test]
    async fn writes_into_the_incarnation_state_dir() {
        let store = make_store("incarnations");
        let first = make_spec("default", "app", "uid-1");
        let mut second = (*make_spec("default", "app", "uid-2")).clone();
        second.metadata.creation_timestamp = Some("2026-10-19T15:00:00Z".to_string());
        let second = Arc::new(second);

        for spec in [&first, &second] {
            store
                .put(
                    PodId::from_spec(spec),
                    Arc::clone(spec),
                    Arc::clone(spec),
                    make_status(PodPhase::Pending),
                )
                .await;
        }

        let layout = &store.config.layout;
        for spec in [&first, &second] {
            assert_eq!(
                store.store_dir(&PodId::from_spec(spec)),
                Some(layout.store_dir(spec)),
            );
            assert!(layout.store_dir(spec).join("original-spec.json").exists());
        }
        assert_ne!(layout.store_dir(&first), layout.store_dir(&second));
        assert_eq!(store.len().await, 2);
    }

    #[tokio::test]
    async fn update_status_does_not_recreate_a_deleted_state_dir() {
        let store = make_store("deleted-state");
        let spec = make_spec("default", "app", "uid-1");
        let id = PodId::from_spec(&spec);
        store
            .put(
                id.clone(),
                Arc::clone(&spec),
                Arc::clone(&spec),
                make_status(PodPhase::Failed),
            )
            .await;
        let state_dir = store.config.layout.state_dir(&spec);
        fs::remove_dir_all(&state_dir).unwrap();

        store
            .update_status(&id, make_status(PodPhase::Failed))
            .await;

        assert!(!state_dir.exists(), "a late status must not resurrect it");
    }
}
