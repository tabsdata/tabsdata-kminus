// Copyright 2026 Tabsdata Inc.

//! Pod store — tracks pod specs and statuses for the logic layer.
//!
//! The store is the interface between the API/logic layer and the pod
//! data. It holds submitted pod specs (original and running) and
//! current statuses.
//!
//! The [`PodStore`] trait defines the storage API — implementations can
//! be in-memory ([`MemoryStore`]) or file-based.
//!
//! # Example
//!
//! ```rust,no_run
//! use std::sync::Arc;
//! use pod_runner::store::{PodStore, PodId, SpecView};
//! use pod_runner::store::memory::MemoryStore;
//! use pod_runner::spec::pod::PodSpec;
//! use pod_runner::spec::status::PodStatus;
//!
//! # async fn example() {
//! let store = MemoryStore::new();
//! let spec: PodSpec = todo!();
//! let id = PodId::from_spec(&spec);
//! let original = Arc::new(spec.clone());
//! let running = Arc::new(spec);
//! store.put(id.clone(), original, running, PodStatus::default()).await;
//!
//! // Get with the running (prepared) spec
//! if let Some(snap) = store.get(&id, SpecView::Running).await {
//!     println!("Phase: {:?}", snap.status.phase);
//! }
//!
//! // Get with the original (as-submitted) spec
//! if let Some(snap) = store.get(&id, SpecView::Original).await {
//!     println!("Original: {:?}", snap.spec.metadata.name);
//! }
//! # }
//! ```

pub mod file;
pub mod layout;
pub mod memory;
pub mod multiplex;

use std::sync::Arc;

use async_trait::async_trait;

use crate::spec::pod::PodSpec;
use crate::spec::status::PodStatus;

// ---------------------------------------------------------------------------
// PodId
// ---------------------------------------------------------------------------

/// Pod identifier: namespace + name + uid.
///
/// Used as the key for the store. Derived from a [`PodSpec`] via
/// [`PodId::from_spec`].
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PodId {
    pub namespace: String,
    pub name: String,
    pub uid: String,
}

impl PodId {
    pub fn new(
        namespace: impl Into<String>,
        name: impl Into<String>,
        uid: impl Into<String>,
    ) -> Self {
        Self {
            namespace: namespace.into(),
            name: name.into(),
            uid: uid.into(),
        }
    }

    /// Create a PodId from a pod spec. Uses "default" if namespace is empty.
    pub fn from_spec(spec: &PodSpec) -> Self {
        Self {
            namespace: if spec.metadata.namespace.is_empty() {
                "default".to_string()
            } else {
                spec.metadata.namespace.clone()
            },
            name: spec.metadata.name.clone(),
            uid: spec.metadata.uid.clone(),
        }
    }

    /// `namespace/name` format for namespace-qualified display.
    pub fn short(&self) -> String {
        format!("{}/{}", self.namespace, self.name)
    }

    /// `namespace_name_uid` format for file paths and unique keys.
    pub fn full(&self) -> String {
        format!("{}_{}_{}", self.namespace, self.name, self.uid)
    }

    /// Inverse of [`full`](Self::full): parse a `namespace_name_uid` key
    /// (e.g. a pod directory name) back into a `PodId`. Returns `None` if
    /// the key isn't three non-empty underscore-separated parts. Safe to
    /// split on `_` because K8s namespaces and names are DNS-1123 labels
    /// (no underscore) and UIDs are UUIDs (no underscore), so only the
    /// three separators we wrote are present.
    pub fn from_full(full: &str) -> Option<PodId> {
        let mut parts = full.splitn(3, '_');
        let namespace = parts.next()?;
        let name = parts.next()?;
        let uid = parts.next()?;
        if namespace.is_empty() || name.is_empty() || uid.is_empty() {
            return None;
        }
        Some(PodId::new(namespace, name, uid))
    }

    /// `namespace_name` format for name-based lookups.
    pub fn lookup_key(&self) -> String {
        format!("{}_{}", self.namespace, self.name)
    }
}

// Logging format: the k8s pod identity, `name_uid`. The uid
// distinguishes incarnations of a reused pod name, and the pair is the
// trailing two segments of the `full()` pod directory name, so log
// lines correlate directly with on-disk pod dirs. Namespace is omitted
// — pod-runner logs are already per-node and namespace adds no
// disambiguation.
impl std::fmt::Display for PodId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}_{}", self.name, self.uid)
    }
}

// ---------------------------------------------------------------------------
// SpecView — which spec variant to include in snapshots
// ---------------------------------------------------------------------------

/// Selects which spec variant to include in [`PodSnapshot`]s returned
/// by `get()`, `get_by_name()`, `list()`, and `list_in_namespace()`.
///
/// The store holds two copies of each pod spec:
/// - **Original**: the spec as submitted by the caller.
/// - **Running**: the prepared spec with paths rewritten, defaults
///   applied, etc.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpecView {
    /// The spec as originally submitted.
    Original,
    /// The prepared/running spec (paths rewritten, defaults applied).
    Running,
}

// ---------------------------------------------------------------------------
// PodSnapshot — read-only view of a pod entry
// ---------------------------------------------------------------------------

/// Read-only snapshot of a pod, returned by `get()` and `list()`.
///
/// Contains cloned/Arc'd data so the caller does not hold any lock.
/// The `spec` field contains whichever variant was requested via
/// [`SpecView`].
#[derive(Debug, Clone)]
pub struct PodSnapshot {
    pub id: PodId,
    pub spec: Arc<PodSpec>,
    pub status: PodStatus,
}

// ---------------------------------------------------------------------------
// PodStore trait
// ---------------------------------------------------------------------------

/// Storage API for pod specs and statuses.
///
/// All methods take `&self` — implementations are expected to handle
/// interior mutability (e.g. `RwLock` for in-memory, file locks for
/// file-based stores).
///
/// The trait is async so implementations can use non-blocking I/O
/// (e.g. `tokio::fs` for file-backed stores). Callers must `.await`
/// every method. `#[async_trait]` boxes the returned futures to keep
/// the trait dyn-compatible (`Arc<dyn PodStore>` is used widely).
///
/// Each pod stores two spec variants: the original (as-submitted) and
/// the running (prepared). Query methods accept a [`SpecView`] to
/// select which variant appears in the returned [`PodSnapshot`].
#[async_trait]
pub trait PodStore: Send + Sync {
    /// Insert or replace a pod entry with both spec variants.
    async fn put(
        &self,
        id: PodId,
        original_spec: Arc<PodSpec>,
        running_spec: Arc<PodSpec>,
        status: PodStatus,
    );

    /// Update the status for a pod. No-op if the pod doesn't exist.
    async fn update_status(&self, id: &PodId, status: PodStatus);

    /// Get a pod by id.
    async fn get(&self, id: &PodId, view: SpecView) -> Option<PodSnapshot>;

    /// Look up a pod by namespace and name (ignoring uid).
    async fn get_by_name(&self, namespace: &str, name: &str, view: SpecView)
    -> Option<PodSnapshot>;

    /// List all pods.
    async fn list(&self, view: SpecView) -> Vec<PodSnapshot>;

    /// List pods in a specific namespace.
    async fn list_in_namespace(&self, namespace: &str, view: SpecView) -> Vec<PodSnapshot>;

    /// Remove a pod from the store, discarding any durable record of it.
    /// Returns the snapshot (with the running spec) if it existed.
    ///
    /// Use this only when the pod's on-disk dir is going away too. To drop
    /// a pod from the API while keeping its record for troubleshooting, use
    /// [`evict`](Self::evict).
    async fn remove(&self, id: &PodId) -> Option<PodSnapshot>;

    /// Drop a pod from the store index while leaving its durable record in
    /// place. Returns the snapshot (with the running spec) if it existed.
    ///
    /// This is what pod deletion uses. The specs and statuses on disk are
    /// the only record of why a pod ended the way it did — for a pod that
    /// failed before any container started, they are the *whole* record —
    /// and their lifetime belongs to the reaper's delete deadline, not to
    /// the API. Stores with no durable record (memory) evict by removing.
    async fn evict(&self, id: &PodId) -> Option<PodSnapshot>;

    /// Number of pods in the store.
    async fn len(&self) -> usize;

    /// Whether the store is empty.
    async fn is_empty(&self) -> bool {
        self.len().await == 0
    }

    /// Number of non-terminal (Pending/Running) pods.
    ///
    /// This is the count that matters for capacity admission: terminal
    /// (Succeeded/Failed) pods hold no resources, so they must not consume
    /// the capacity budget. Terminal pods can pile up in the store between
    /// completion and the control plane's delete, so [`len`](Self::len)
    /// would over-count and wrongly reject new pods.
    ///
    /// The default lists every snapshot and filters; in-memory stores
    /// override this to count in place without cloning, since it runs on
    /// the hot admission path.
    async fn count_non_terminal(&self) -> usize {
        self.list(SpecView::Running)
            .await
            .into_iter()
            .filter(|snap| !snap.status.phase.is_terminal())
            .count()
    }

    /// Check if a pod exists.
    async fn contains(&self, id: &PodId) -> bool {
        self.get(id, SpecView::Running).await.is_some()
    }
}

// ---------------------------------------------------------------------------
// Tests (trait-level, using MemoryStore as the concrete impl)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::file::{FileStore, FileStoreConfig};
    use super::memory::MemoryStore;
    use super::multiplex::MultiplexStore;
    use super::*;
    use crate::spec::constants::PodPhase;
    use crate::spec::pod::{PodMetadata, PodSpecBody};
    use std::sync::Arc as StdArc;

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

    /// Make a "running" spec that differs from the original by having
    /// a `_running` suffix on the uid, so tests can verify which
    /// variant is returned.
    fn make_running_spec(namespace: &str, name: &str, uid: &str) -> Arc<PodSpec> {
        make_spec(namespace, name, &format!("{}_running", uid))
    }

    fn make_status(phase: PodPhase) -> PodStatus {
        PodStatus {
            phase,
            ..Default::default()
        }
    }

    /// Run the full CRUD test suite against any PodStore implementation.
    async fn run_store_tests(store: &dyn PodStore) {
        // -- put and get (running view) --
        let original = make_spec("default", "my-pod", "uid-1");
        let running = make_running_spec("default", "my-pod", "uid-1");
        let id = PodId::from_spec(&original);
        store
            .put(
                id.clone(),
                original,
                running,
                make_status(PodPhase::Pending),
            )
            .await;

        let snap = store.get(&id, SpecView::Running).await.unwrap();
        assert_eq!(snap.id.name, "my-pod");
        assert_eq!(snap.spec.metadata.uid, "uid-1_running");
        assert_eq!(snap.status.phase, PodPhase::Pending);

        // -- get (original view) --
        let snap = store.get(&id, SpecView::Original).await.unwrap();
        assert_eq!(snap.spec.metadata.uid, "uid-1");

        // -- get nonexistent --
        let nope = PodId::new("default", "nope", "uid");
        assert!(store.get(&nope, SpecView::Running).await.is_none());

        // -- update status --
        store
            .update_status(&id, make_status(PodPhase::Running))
            .await;
        let snap = store.get(&id, SpecView::Running).await.unwrap();
        assert_eq!(snap.status.phase, PodPhase::Running);

        // -- get by name --
        let snap = store
            .get_by_name("default", "my-pod", SpecView::Running)
            .await
            .unwrap();
        assert_eq!(snap.spec.metadata.uid, "uid-1_running");
        let snap = store
            .get_by_name("default", "my-pod", SpecView::Original)
            .await
            .unwrap();
        assert_eq!(snap.spec.metadata.uid, "uid-1");
        assert!(
            store
                .get_by_name("default", "nope", SpecView::Running)
                .await
                .is_none()
        );
        assert!(
            store
                .get_by_name("staging", "my-pod", SpecView::Running)
                .await
                .is_none()
        );

        // -- put replaces --
        let original2 = make_spec("default", "my-pod", "uid-1");
        let running2 = make_running_spec("default", "my-pod", "uid-1");
        store
            .put(
                id.clone(),
                original2,
                running2,
                make_status(PodPhase::Succeeded),
            )
            .await;
        let snap = store.get(&id, SpecView::Running).await.unwrap();
        assert_eq!(snap.status.phase, PodPhase::Succeeded);
        assert_eq!(store.len().await, 1);

        // -- remove --
        let removed = store.remove(&id).await;
        assert!(removed.is_some());
        assert!(!store.contains(&id).await);
        assert!(store.get(&id, SpecView::Running).await.is_none());

        // -- remove nonexistent --
        assert!(store.remove(&nope).await.is_none());
    }

    async fn run_list_tests(store: &dyn PodStore) {
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

        // -- list with running view --
        let pods = store.list(SpecView::Running).await;
        assert_eq!(pods.len(), 3);
        assert!(
            pods.iter()
                .all(|s| s.spec.metadata.uid.ends_with("_running"))
        );

        // -- list with original view --
        let pods = store.list(SpecView::Original).await;
        assert_eq!(pods.len(), 3);
        assert!(
            pods.iter()
                .all(|s| !s.spec.metadata.uid.ends_with("_running"))
        );

        assert_eq!(store.len().await, 3);
        assert!(!store.is_empty().await);
    }

    async fn run_namespace_tests(store: &dyn PodStore) {
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

        let ns1 = store.list_in_namespace("ns1", SpecView::Running).await;
        assert_eq!(ns1.len(), 2);
        assert!(
            ns1.iter()
                .all(|s| s.spec.metadata.uid.ends_with("_running"))
        );

        let ns1_orig = store.list_in_namespace("ns1", SpecView::Original).await;
        assert_eq!(ns1_orig.len(), 2);
        assert!(
            ns1_orig
                .iter()
                .all(|s| !s.spec.metadata.uid.ends_with("_running"))
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

    // -- PodId unit tests (independent of store impl) --

    #[test]
    fn test_pod_id_from_spec_default_namespace() {
        let spec = PodSpec {
            metadata: PodMetadata {
                namespace: String::new(),
                name: "app".to_string(),
                uid: "123".to_string(),
                ..Default::default()
            },
            ..Default::default()
        };
        let id = PodId::from_spec(&spec);
        assert_eq!(id.namespace, "default");
    }

    #[test]
    fn test_pod_id_display() {
        let id = PodId::new("prod", "web", "uid-1");
        assert_eq!(id.to_string(), "web_uid-1");
        assert_eq!(id.short(), "prod/web");
        assert_eq!(id.full(), "prod_web_uid-1");
        assert_eq!(id.lookup_key(), "prod_web");
    }

    #[test]
    fn test_pod_id_from_full_roundtrips() {
        let id = PodId::new("prod", "web", "550e8400-e29b-41d4-a716-446655440000");
        assert_eq!(PodId::from_full(&id.full()), Some(id));
        // Dashed uid (the pod-runner test convention) survives the split.
        assert_eq!(
            PodId::from_full("default_app_uid-app"),
            Some(PodId::new("default", "app", "uid-app")),
        );
        // Malformed keys yield None rather than a bogus id.
        assert_eq!(PodId::from_full("default_app"), None);
        assert_eq!(PodId::from_full("nounderscores"), None);
        assert_eq!(PodId::from_full("default__uid"), None);
    }

    // -- MemoryStore tests via the shared test suite --

    #[tokio::test]
    async fn memory_store_crud() {
        run_store_tests(&MemoryStore::new()).await;
    }

    #[tokio::test]
    async fn memory_store_list() {
        run_list_tests(&MemoryStore::new()).await;
    }

    #[tokio::test]
    async fn memory_store_namespaces() {
        run_namespace_tests(&MemoryStore::new()).await;
    }

    #[tokio::test]
    async fn memory_store_default() {
        let store = MemoryStore::default();
        assert!(store.is_empty().await);
    }

    #[tokio::test]
    async fn memory_store_concurrent_reads() {
        use std::sync::Arc as StdArc;

        let store = StdArc::new(MemoryStore::new());
        for i in 0..10 {
            let original = make_spec("default", &format!("pod-{}", i), &format!("uid-{}", i));
            let running =
                make_running_spec("default", &format!("pod-{}", i), &format!("uid-{}", i));
            store
                .put(
                    PodId::from_spec(&original),
                    original,
                    running,
                    make_status(PodPhase::Running),
                )
                .await;
        }

        let handles: Vec<_> = (0..5)
            .map(|_| {
                let store = StdArc::clone(&store);
                tokio::spawn(async move {
                    for _ in 0..100 {
                        let pods = store.list(SpecView::Running).await;
                        assert_eq!(pods.len(), 10);
                    }
                })
            })
            .collect();

        for h in handles {
            h.await.unwrap();
        }
    }

    // -- FileStore tests via the shared test suite --

    fn temp_dir(name: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "pod-runner-rs2-test-{}-{}",
            name,
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        dir
    }

    fn make_file_store(name: &str) -> FileStore {
        FileStore::new(FileStoreConfig {
            layout: layout::PodLayout::new(&temp_dir(name)),
            write_running_spec: true,
        })
    }

    #[tokio::test]
    async fn file_store_crud() {
        run_store_tests(&make_file_store("shared-crud")).await;
    }

    #[tokio::test]
    async fn file_store_list() {
        run_list_tests(&make_file_store("shared-list")).await;
    }

    #[tokio::test]
    async fn file_store_namespaces() {
        run_namespace_tests(&make_file_store("shared-ns")).await;
    }

    // -- MultiplexStore tests via the shared test suite --

    fn make_multiplex_store(name: &str) -> MultiplexStore {
        let primary = StdArc::new(MemoryStore::new());
        let secondary = StdArc::new(make_file_store(name));
        MultiplexStore::new(
            primary as StdArc<dyn PodStore>,
            secondary as StdArc<dyn PodStore>,
        )
    }

    #[tokio::test]
    async fn multiplex_store_crud() {
        run_store_tests(&make_multiplex_store("mpx-crud")).await;
    }

    #[tokio::test]
    async fn multiplex_store_list() {
        run_list_tests(&make_multiplex_store("mpx-list")).await;
    }

    #[tokio::test]
    async fn multiplex_store_namespaces() {
        run_namespace_tests(&make_multiplex_store("mpx-ns")).await;
    }
}
