// Copyright 2026 Tabsdata Inc.

//! Multiplexing [`PodStore`] — writes to two stores, reads from one.
//!
//! Wraps a primary and secondary store. All write operations (`put`,
//! `update_status`, `remove`) are forwarded to both stores. Read
//! operations (`get`, `list`, etc.) use the primary store only.
//!
//! Typical usage: primary = [`MemoryStore`](super::memory::MemoryStore)
//! for fast reads, secondary = [`FileStore`](super::file::FileStore)
//! for persistence.

use std::sync::Arc;

use async_trait::async_trait;

use crate::spec::pod::PodSpec;
use crate::spec::status::PodStatus;

use super::{PodId, PodSnapshot, PodStore, SpecView};

// ---------------------------------------------------------------------------
// MultiplexStore
// ---------------------------------------------------------------------------

/// A store that writes to two backends and reads from the primary.
pub struct MultiplexStore {
    primary: Arc<dyn PodStore>,
    secondary: Arc<dyn PodStore>,
}

impl MultiplexStore {
    pub fn new(primary: Arc<dyn PodStore>, secondary: Arc<dyn PodStore>) -> Self {
        Self { primary, secondary }
    }
}

#[async_trait]
impl PodStore for MultiplexStore {
    async fn put(
        &self,
        id: PodId,
        original_spec: Arc<PodSpec>,
        running_spec: Arc<PodSpec>,
        status: PodStatus,
    ) {
        self.primary
            .put(
                id.clone(),
                Arc::clone(&original_spec),
                Arc::clone(&running_spec),
                status.clone(),
            )
            .await;
        self.secondary
            .put(id, original_spec, running_spec, status)
            .await;
    }

    async fn update_status(&self, id: &PodId, status: PodStatus) {
        self.primary.update_status(id, status.clone()).await;
        self.secondary.update_status(id, status).await;
    }

    async fn get(&self, id: &PodId, view: SpecView) -> Option<PodSnapshot> {
        self.primary.get(id, view).await
    }

    async fn get_by_name(
        &self,
        namespace: &str,
        name: &str,
        view: SpecView,
    ) -> Option<PodSnapshot> {
        self.primary.get_by_name(namespace, name, view).await
    }

    async fn list(&self, view: SpecView) -> Vec<PodSnapshot> {
        self.primary.list(view).await
    }

    async fn list_in_namespace(&self, namespace: &str, view: SpecView) -> Vec<PodSnapshot> {
        self.primary.list_in_namespace(namespace, view).await
    }

    async fn remove(&self, id: &PodId) -> Option<PodSnapshot> {
        let snap = self.primary.remove(id).await;
        self.secondary.remove(id).await;
        snap
    }

    async fn len(&self) -> usize {
        self.primary.len().await
    }

    async fn count_non_terminal(&self) -> usize {
        self.primary.count_non_terminal().await
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
    use crate::store::memory::MemoryStore;

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

    fn make_stores() -> (MultiplexStore, Arc<MemoryStore>, Arc<MemoryStore>) {
        let primary = Arc::new(MemoryStore::new());
        let secondary = Arc::new(MemoryStore::new());
        let multiplex = MultiplexStore::new(
            Arc::clone(&primary) as Arc<dyn PodStore>,
            Arc::clone(&secondary) as Arc<dyn PodStore>,
        );
        (multiplex, primary, secondary)
    }

    #[tokio::test]
    async fn put_writes_to_both() {
        let (mpx, primary, secondary) = make_stores();
        let id = PodId::new("default", "app", "uid-1");

        mpx.put(
            id.clone(),
            make_spec("default", "app", "uid-1"),
            make_running_spec("default", "app", "uid-1"),
            make_status(PodPhase::Pending),
        )
        .await;

        assert!(primary.contains(&id).await);
        assert!(secondary.contains(&id).await);
    }

    #[tokio::test]
    async fn update_status_writes_to_both() {
        let (mpx, primary, secondary) = make_stores();
        let id = PodId::new("default", "app", "uid-1");

        mpx.put(
            id.clone(),
            make_spec("default", "app", "uid-1"),
            make_running_spec("default", "app", "uid-1"),
            make_status(PodPhase::Pending),
        )
        .await;
        mpx.update_status(&id, make_status(PodPhase::Running)).await;

        let p = primary.get(&id, SpecView::Running).await.unwrap();
        let s = secondary.get(&id, SpecView::Running).await.unwrap();
        assert_eq!(p.status.phase, PodPhase::Running);
        assert_eq!(s.status.phase, PodPhase::Running);
    }

    #[tokio::test]
    async fn reads_from_primary_only() {
        let (mpx, primary, secondary) = make_stores();
        let id = PodId::new("default", "app", "uid-1");

        // Put only in primary (bypass multiplex)
        primary
            .put(
                id.clone(),
                make_spec("default", "app", "uid-1"),
                make_running_spec("default", "app", "uid-1"),
                make_status(PodPhase::Pending),
            )
            .await;

        // Multiplex reads from primary
        assert!(mpx.get(&id, SpecView::Running).await.is_some());
        assert_eq!(mpx.len().await, 1);
        assert!(!mpx.is_empty().await);
        assert!(mpx.contains(&id).await);
        assert!(
            mpx.get_by_name("default", "app", SpecView::Running)
                .await
                .is_some()
        );
        assert_eq!(mpx.list(SpecView::Running).await.len(), 1);
        assert_eq!(
            mpx.list_in_namespace("default", SpecView::Running)
                .await
                .len(),
            1
        );

        // Secondary is empty — doesn't affect reads
        assert_eq!(secondary.len().await, 0);
    }

    #[tokio::test]
    async fn remove_removes_from_both() {
        let (mpx, primary, secondary) = make_stores();
        let id = PodId::new("default", "app", "uid-1");

        mpx.put(
            id.clone(),
            make_spec("default", "app", "uid-1"),
            make_running_spec("default", "app", "uid-1"),
            make_status(PodPhase::Pending),
        )
        .await;

        let snap = mpx.remove(&id).await.unwrap();
        assert_eq!(snap.id.name, "app");
        assert!(!primary.contains(&id).await);
        assert!(!secondary.contains(&id).await);
    }

    #[tokio::test]
    async fn remove_returns_primary_snapshot() {
        let (mpx, primary, _secondary) = make_stores();
        let id = PodId::new("default", "app", "uid-1");

        // Put via multiplex
        mpx.put(
            id.clone(),
            make_spec("default", "app", "uid-1"),
            make_running_spec("default", "app", "uid-1"),
            make_status(PodPhase::Pending),
        )
        .await;

        // Update only primary status (bypass multiplex)
        primary
            .update_status(&id, make_status(PodPhase::Running))
            .await;

        // Remove returns primary's snapshot (Running), not secondary's (Pending)
        let snap = mpx.remove(&id).await.unwrap();
        assert_eq!(snap.status.phase, PodPhase::Running);
    }

    #[tokio::test]
    async fn spec_views_work_through_multiplex() {
        let (mpx, _, _) = make_stores();
        let id = PodId::new("default", "app", "uid-1");

        mpx.put(
            id.clone(),
            make_spec("default", "app", "uid-1"),
            make_running_spec("default", "app", "uid-1"),
            make_status(PodPhase::Pending),
        )
        .await;

        let original = mpx.get(&id, SpecView::Original).await.unwrap();
        let running = mpx.get(&id, SpecView::Running).await.unwrap();
        assert_eq!(original.spec.metadata.uid, "uid-1");
        assert_eq!(running.spec.metadata.uid, "uid-1_running");
    }
}
