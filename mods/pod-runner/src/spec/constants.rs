// Copyright 2026 Tabsdata Inc.

use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Pod phases
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum PodPhase {
    #[default]
    Pending,
    Running,
    Succeeded,
    Failed,
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
// Pod-preparation failure reasons (set when prepare_pod fails before the
// executor is spawned — e.g. missing configmap/secret times out).
// ---------------------------------------------------------------------------

/// `activeDeadlineSeconds` elapsed while the preparer was waiting for a
/// referenced configmap or secret to appear. Matches the k8s convention.
pub const REASON_DEADLINE_EXCEEDED: &str = "DeadlineExceeded";

/// Pod preparation failed for a non-timeout reason — e.g. an unrecoverable
/// resolver error, executor spawn failure, or filesystem error setting up
/// the pod work directory.
pub const REASON_POD_PREPARATION_FAILED: &str = "PodPreparationFailed";

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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum RestartPolicy {
    #[default]
    Never,
    OnFailure,
    Always,
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

/// Annotation to override `pod_reap_ttl_on_success` per pod.
pub const ANN_POD_REAP_TTL_ON_SUCCESS: &str = "prPodReapTtlOnSuccess";

/// Annotation to override `pod_reap_ttl_on_error` per pod.
pub const ANN_POD_REAP_TTL_ON_ERROR: &str = "prPodReapTtlOnError";

/// Annotation to override `pod_delete_ttl_on_success` per pod.
pub const ANN_POD_DELETE_TTL_ON_SUCCESS: &str = "prPodDeleteTtlOnSuccess";

/// Annotation to override `pod_delete_ttl_on_error` per pod.
pub const ANN_POD_DELETE_TTL_ON_ERROR: &str = "prPodDeleteTtlOnError";

// ---------------------------------------------------------------------------
// Cleanup TTL defaults
// ---------------------------------------------------------------------------
//
// A TTL is `<number><unit>` with unit `s`, `m` or `h`, or the literal
// `never`. Reap TTLs run from pod completion; delete TTLs run from the
// moment the pod is removed from the pod runner.

/// Default TTL before a succeeded pod's data dirs are reaped.
pub const DEFAULT_POD_REAP_TTL_ON_SUCCESS: &str = "0s";

/// Default TTL before a failed pod's data dirs are reaped.
pub const DEFAULT_POD_REAP_TTL_ON_ERROR: &str = "1h";

/// Default TTL before a succeeded pod's dir is deleted.
pub const DEFAULT_POD_DELETE_TTL_ON_SUCCESS: &str = "1h";

/// Default TTL before a failed pod's dir is deleted.
pub const DEFAULT_POD_DELETE_TTL_ON_ERROR: &str = "24h";

/// TTL value meaning "no deadline" — nothing is ever reaped or deleted.
pub const TTL_NEVER: &str = "never";

// ---------------------------------------------------------------------------
// Pod dir marker and deadline files
// ---------------------------------------------------------------------------

/// Per-pod file holding the resolved cleanup TTLs, written at pod start.
pub const CLEANUP_TTLS_FILE: &str = "_cleanup_ttls.json";

/// Marker written to the pod dir when the pod completes successfully.
/// Content is the RFC3339 completion time — the reap clock origin.
pub const SUCCEEDED_MARKER: &str = "_succeeded";

/// Marker written to the pod dir when the pod completes with a failure.
/// Content is the RFC3339 completion time — the reap clock origin.
pub const FAILED_MARKER: &str = "_failed";

/// Deadline file for reaping the pod's data dirs (`work/`, `volumes/`).
/// Content is an RFC3339 instant or [`TTL_NEVER`].
pub const REAP_ON_FILE: &str = "_reap_on";

/// Deadline file for deleting the whole pod dir. Written when the pod is
/// removed from the pod runner. Content is an RFC3339 instant or
/// [`TTL_NEVER`]; absence means the pod has not been removed yet.
pub const DELETE_ON_FILE: &str = "_delete_on";

/// Marker written once the pod's data dirs have been reaped. Content is
/// the RFC3339 time of the reap.
pub const REAPED_MARKER: &str = "_reaped";
