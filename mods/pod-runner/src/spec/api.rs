// Copyright 2026 Tabsdata Inc.

//! K8s-style API request and response types.

use serde::{Deserialize, Serialize};

use super::pod::{PodMetadata, PodSpecBody};
use super::status::PodStatus;

// ---------------------------------------------------------------------------
// Pod object — full pod representation for API responses
// ---------------------------------------------------------------------------

/// Full pod object returned by getPod and getPods.
///
/// Mirrors the K8s Pod resource structure with metadata, spec, and status
/// at the top level.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PodObject {
    pub metadata: PodMetadata,
    pub spec: PodSpecBody,
    pub status: PodStatus,
}

// ---------------------------------------------------------------------------
// Response types
// ---------------------------------------------------------------------------

/// Response for POST /createPod.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreatePodResponse {
    pub pod_id: String,
    pub phase: String,
}

/// Response for DELETE /deletePod.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeletePodResponse {
    pub deleted: bool,
}

/// Entry in the GET /getAllPodStatuses response.
///
/// Contains namespace, name, phase, and the full pod status — designed
/// for the virtual-kubelet's sync loop. One bulk call replaces N
/// individual getPodStatus calls.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PodStatusEntry {
    pub namespace: String,
    pub name: String,
    pub phase: String,
    pub status: PodStatus,
}

/// Error response body.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorResponse {
    pub error: String,
}
