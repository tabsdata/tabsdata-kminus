// Copyright 2026 Tabsdata Inc.

//! Node-level types for K8s virtual kubelet compatibility.

use serde::{Deserialize, Serialize};

/// Node capacity reported to the K8s API server.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeCapacity {
    pub cpu: String,
    pub memory: String,
    pub pods: String,
}

/// Node condition (e.g. Ready, MemoryPressure, DiskPressure).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NodeCondition {
    #[serde(rename = "type")]
    pub condition_type: String,
    pub status: String,
    pub last_heartbeat_time: String,
    pub last_transition_time: String,
    pub reason: String,
    pub message: String,
}

/// Node address (e.g. InternalIP).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeAddress {
    #[serde(rename = "type")]
    pub address_type: String,
    pub address: String,
}
