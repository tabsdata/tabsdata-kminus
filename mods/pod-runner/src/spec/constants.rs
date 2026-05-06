// Copyright 2026 Tabsdata Inc.

use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Pod phases
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PodPhase {
    Pending,
    Running,
    Succeeded,
    Failed,
}

impl Default for PodPhase {
    fn default() -> Self {
        Self::Pending
    }
}

impl PodPhase {
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Succeeded | Self::Failed)
    }
}

impl std::fmt::Display for PodPhase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Pending => write!(f, "Pending"),
            Self::Running => write!(f, "Running"),
            Self::Succeeded => write!(f, "Succeeded"),
            Self::Failed => write!(f, "Failed"),
        }
    }
}

// ---------------------------------------------------------------------------
// Pod conditions
// ---------------------------------------------------------------------------

pub const CONDITION_POD_SCHEDULED: &str = "PodScheduled";
pub const CONDITION_INITIALIZED: &str = "Initialized";
pub const CONDITION_CONTAINERS_READY: &str = "ContainersReady";
pub const CONDITION_READY: &str = "Ready";
pub const CONDITION_DISRUPTION_TARGET: &str = "DisruptionTarget";

pub const CONDITION_TRUE: &str = "True";
pub const CONDITION_FALSE: &str = "False";

// ---------------------------------------------------------------------------
// Condition / status reasons
// ---------------------------------------------------------------------------

pub const REASON_POD_SCHEDULED: &str = "PodScheduled";
pub const REASON_INITIALIZED: &str = "Initialized";
pub const REASON_INIT_CONTAINERS_RUNNING: &str = "InitContainersRunning";
pub const REASON_CONTAINERS_READY: &str = "ContainersReady";
pub const REASON_CONTAINERS_NOT_READY: &str = "ContainersNotReady";
pub const REASON_CONTAINERS_STARTING: &str = "ContainersStarting";
pub const REASON_CONTAINERS_CREATING: &str = "ContainersCreating";
pub const REASON_CONTAINERS_FAILED: &str = "ContainersFailed";
pub const REASON_POD_READY: &str = "PodReady";
pub const REASON_POD_COMPLETED: &str = "PodCompleted";
pub const REASON_DELETION_TRIGGERED: &str = "DeletionTriggered";

// ---------------------------------------------------------------------------
// Container state reasons
// ---------------------------------------------------------------------------

pub const REASON_COMPLETED: &str = "Completed";
pub const REASON_ERROR: &str = "Error";
pub const REASON_KILLED: &str = "Killed";
pub const REASON_CONTAINER_NOT_FOUND: &str = "ContainerNotFound";
pub const REASON_POD_INITIALIZING: &str = "PodInitializing";

// ---------------------------------------------------------------------------
// Container state serialization keys
// ---------------------------------------------------------------------------
//
// The ContainerStatus.state field is serialized as `{"<key>": {...}}` with
// exactly one key at a time. These constants are the canonical key strings.

pub const STATE_KEY_WAITING: &str = "waiting";
pub const STATE_KEY_RUNNING: &str = "running";
pub const STATE_KEY_TERMINATED: &str = "terminated";

// ---------------------------------------------------------------------------
// Restart policies
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RestartPolicy {
    Never,
    OnFailure,
    Always,
}

impl Default for RestartPolicy {
    fn default() -> Self {
        Self::Never
    }
}

// ---------------------------------------------------------------------------
// Probe types
// ---------------------------------------------------------------------------

pub const PROBE_STARTUP: &str = "startup";
pub const PROBE_LIVENESS: &str = "liveness";
pub const PROBE_READINESS: &str = "readiness";

// ---------------------------------------------------------------------------
// Probe phases
// ---------------------------------------------------------------------------

pub const PROBE_PHASE_RUNNING: &str = "running";
pub const PROBE_PHASE_SUCCEEDED: &str = "succeeded";
pub const PROBE_PHASE_FAILED: &str = "failed";

// ---------------------------------------------------------------------------
// Probe results
// ---------------------------------------------------------------------------

pub const PROBE_RESULT_SUCCESS: &str = "success";
pub const PROBE_RESULT_FAILURE: &str = "failure";

// ---------------------------------------------------------------------------
// Node condition reasons
// ---------------------------------------------------------------------------

pub const REASON_KUBELET_READY: &str = "KubeletReady";
pub const REASON_KUBELET_SUFFICIENT_MEMORY: &str = "KubeletHasSufficientMemory";
pub const REASON_KUBELET_NO_DISK_PRESSURE: &str = "KubeletHasNoDiskPressure";

// ---------------------------------------------------------------------------
// Pod runner annotation keys
// ---------------------------------------------------------------------------

/// Annotation to override `keep_deleted_pod_dir` per pod ("true"/"false").
pub const ANN_KEEP_DELETED_POD_DIR: &str = "prKeepDeletedPodDir";

/// Annotation to override `keep_failed_pod_dir` per pod ("true"/"false").
pub const ANN_KEEP_FAILED_POD_DIR: &str = "prKeepFailedPodDir";

// ---------------------------------------------------------------------------
// Sentinel files
// ---------------------------------------------------------------------------

/// Written to the pod work dir when the pod reaches a terminal phase.
pub const FINISHED_SENTINEL: &str = "FINISHED";
