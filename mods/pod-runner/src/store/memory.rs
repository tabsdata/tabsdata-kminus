// Copyright 2026 Tabsdata Inc.

//! In-memory [`PodStore`] implementation.
//!
//! Uses `RwLock<HashMap>` for concurrent read access from API handlers
//! and exclusive write access from the executor event loop.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use tracing::{debug, debug_span};

use crate::spec::pod::PodSpec;
use crate::spec::status::PodStatus;

use super::{PodId, PodSnapshot, PodStore, SpecView};

// ---------------------------------------------------------------------------
// PodEntry — internal storage record
// ---------------------------------------------------------------------------

struct PodEntry {
    id: PodId,
    original_spec: Arc<PodSpec>,
    running_spec: Arc<PodSpec>,
    status: PodStatus,
}

impl PodEntry {
    fn snapshot(&self, view: SpecView) -> PodSnapshot {
        PodSnapshot {
            id: self.id.clone(),
            spec: match view {
                SpecView::Original => Arc::clone(&self.original_spec),
                SpecView::Running => Arc::clone(&self.running_spec),
            },
            status: self.status.clone(),
        }
    }
}

// ---------------------------------------------------------------------------
// MemoryStore
// ---------------------------------------------------------------------------

/// In-memory pod store backed by `RwLock<HashMap>`.
pub struct MemoryStore {
    pods: RwLock<HashMap<PodId, PodEntry>>,
}

impl MemoryStore {
    pub fn new() -> Self {
        Self {
            pods: RwLock::new(HashMap::new()),
        }
    }
}

impl Default for MemoryStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl PodStore for MemoryStore {
    async fn put(
        &self,
        id: PodId,
        original_spec: Arc<PodSpec>,
        running_spec: Arc<PodSpec>,
        status: PodStatus,
    ) {
        let _span = debug_span!("store", pod = %id).entered();
        debug!(phase = ?status.phase, "Pod added");
        let mut pods = self.pods.write().unwrap();
        pods.insert(
            id.clone(),
            PodEntry {
                id,
                original_spec,
                running_spec,
                status,
            },
        );
    }

    async fn update_status(&self, id: &PodId, status: PodStatus) {
        let _span = debug_span!("store", pod = %id).entered();
        debug!(phase = ?status.phase, "Status updated");
        let mut pods = self.pods.write().unwrap();
        if let Some(entry) = pods.get_mut(id) {
            entry.status = status;
        }
    }

    async fn get(&self, id: &PodId, view: SpecView) -> Option<PodSnapshot> {
        let pods = self.pods.read().unwrap();
        pods.get(id).map(|e| e.snapshot(view))
    }

    async fn get_by_name(
        &self,
        namespace: &str,
        name: &str,
        view: SpecView,
    ) -> Option<PodSnapshot> {
        let pods = self.pods.read().unwrap();
        pods.values()
            .find(|e| e.id.namespace == namespace && e.id.name == name)
            .map(|e| e.snapshot(view))
    }

    async fn list(&self, view: SpecView) -> Vec<PodSnapshot> {
        let pods = self.pods.read().unwrap();
        pods.values().map(|e| e.snapshot(view)).collect()
    }

    async fn list_in_namespace(&self, namespace: &str, view: SpecView) -> Vec<PodSnapshot> {
        let pods = self.pods.read().unwrap();
        pods.values()
            .filter(|e| e.id.namespace == namespace)
            .map(|e| e.snapshot(view))
            .collect()
    }

    async fn remove(&self, id: &PodId) -> Option<PodSnapshot> {
        let _span = debug_span!("store", pod = %id).entered();
        debug!("Pod removed");
        let mut pods = self.pods.write().unwrap();
        pods.remove(id).map(|e| e.snapshot(SpecView::Running))
    }

    async fn len(&self) -> usize {
        let pods = self.pods.read().unwrap();
        pods.len()
    }

    async fn count_non_terminal(&self) -> usize {
        let pods = self.pods.read().unwrap();
        pods.values()
            .filter(|e| !e.status.phase.is_terminal())
            .count()
    }
}
