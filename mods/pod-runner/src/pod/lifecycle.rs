// Copyright 2026 Tabsdata Inc.

//! Pod and container lifecycle state machines.
//!
//! Centralizes all pod/container state transitions with explicit guards.
//! Invalid transitions return [`TransitionError`] instead of silently
//! corrupting state. All transitions are logged at `debug` level.
//!
//! # Design
//!
//! - [`ContainerState`] — typed replacement for the stringly-typed
//!   `HashMap<String, Value>` container state. Enforces
//!   `Waiting → Running → Terminated` one-way flow.
//! - [`ProbePhase`] — tracks consecutive success/failure counts for a
//!   single probe kind, replacing ad-hoc counters in `probe.rs`.
//! - [`ContainerLifecycle`] — per-container aggregate of state, readiness,
//!   and probe phases.
//! - [`PodLifecycle`] — pod-level phase (`Pending`/`Running`/terminal),
//!   conditions, and owned container lifecycles.
//!
//! # Integration
//!
//! The module is synchronous and testable without an async runtime.
//! Timestamps are passed in as `&str` by callers (usually
//! `chrono::Utc::now().to_rfc3339()`) so tests can use deterministic values.
//!
//! [`PodLifecycle::to_pod_status`] produces the existing [`PodStatus`]
//! struct so the store/API/logic layers remain unchanged during
//! incremental adoption.

use std::collections::HashMap;

use tracing::debug;

use crate::pod::executor::ContainerRole;
use crate::spec::constants::*;
use crate::spec::status::{
    ContainerStateRunning, ContainerStateTerminated, ContainerStateWaiting, ContainerStatus,
    PodCondition, PodStatus,
};

// ---------------------------------------------------------------------------
// Local constants — display/error strings for internal state machine names
// ---------------------------------------------------------------------------
//
// These are used in transition error messages and debug logs. They are the
// typed-enum variant names — distinct from the lowercase serialization keys
// (STATE_KEY_*) which are part of the JSON API contract.

// Container lifecycle state names
const STATE_NAME_WAITING: &str = "Waiting";
const STATE_NAME_RUNNING: &str = "Running";
const STATE_NAME_TERMINATED: &str = "Terminated";

// Probe phase names
const PROBE_PHASE_NAME_PENDING: &str = "Pending";
const PROBE_PHASE_NAME_PASSING: &str = "Passing";
const PROBE_PHASE_NAME_FAILING: &str = "Failing";

// Pod lifecycle transition targets (used in error messages)
const POD_TARGET_SCHEDULED: &str = "Scheduled";
const POD_TARGET_INITIALIZED: &str = "Initialized";
const POD_TARGET_NOT_INITIALIZED: &str = "NotInitialized";
const POD_TARGET_RUNNING: &str = "Running";
const POD_TARGET_SUCCEEDED: &str = "Succeeded";
const POD_TARGET_FAILED: &str = "Failed";

// State machine identifiers in TransitionError.machine
const MACHINE_CONTAINER: &str = "ContainerLifecycle";
const MACHINE_POD: &str = "PodLifecycle";

// ---------------------------------------------------------------------------
// TransitionError
// ---------------------------------------------------------------------------

/// Error returned when a state transition is rejected by a guard.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{machine}: invalid transition {from} -> {to} ({reason})")]
pub struct TransitionError {
    pub machine: &'static str,
    pub from: String,
    pub to: String,
    pub reason: String,
}

// ---------------------------------------------------------------------------
// WaitingReason / TerminatedReason
// ---------------------------------------------------------------------------

/// Why a container is in the `Waiting` state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WaitingReason {
    PodInitializing,
    ContainerCreating,
    CrashLoopBackOff,
    Custom(String),
}

impl WaitingReason {
    pub fn as_str(&self) -> &str {
        match self {
            Self::PodInitializing => REASON_POD_INITIALIZING,
            Self::ContainerCreating => REASON_CONTAINERS_CREATING,
            Self::CrashLoopBackOff => REASON_CRASH_LOOP_BACK_OFF,
            Self::Custom(s) => s.as_str(),
        }
    }
}

/// Why a container terminated.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TerminatedReason {
    Completed,
    Error,
    Killed,
    Custom(String),
}

impl TerminatedReason {
    pub fn as_str(&self) -> &str {
        match self {
            Self::Completed => REASON_COMPLETED,
            Self::Error => REASON_ERROR,
            Self::Killed => REASON_KILLED,
            Self::Custom(s) => s.as_str(),
        }
    }

    /// Pick the canonical reason for a given exit code.
    pub fn from_exit_code(code: i32) -> Self {
        if code == 0 {
            Self::Completed
        } else {
            Self::Error
        }
    }
}

// ---------------------------------------------------------------------------
// ContainerState
// ---------------------------------------------------------------------------

/// Typed container lifecycle state. Replaces the stringly-typed
/// `HashMap<String, Value>` used in [`ContainerStatus::state`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ContainerState {
    Waiting {
        reason: WaitingReason,
    },
    Running {
        started_at: String,
    },
    Terminated {
        exit_code: i32,
        reason: TerminatedReason,
        started_at: String,
        finished_at: String,
    },
}

impl ContainerState {
    fn name(&self) -> &'static str {
        match self {
            Self::Waiting { .. } => STATE_NAME_WAITING,
            Self::Running { .. } => STATE_NAME_RUNNING,
            Self::Terminated { .. } => STATE_NAME_TERMINATED,
        }
    }
}

// ---------------------------------------------------------------------------
// Probes
// ---------------------------------------------------------------------------

/// Which probe kind the phase refers to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ProbeKind {
    Startup,
    Liveness,
    Readiness,
}

impl ProbeKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Startup => PROBE_STARTUP,
            Self::Liveness => PROBE_LIVENESS,
            Self::Readiness => PROBE_READINESS,
        }
    }
}

impl std::fmt::Display for ProbeKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Phase of a single probe: pending (no result yet), passing or failing,
/// with a count of consecutive observations in that direction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProbePhase {
    Pending,
    Passing { consecutive: u32 },
    Failing { consecutive: u32 },
}

impl ProbePhase {
    fn name(&self) -> &'static str {
        match self {
            Self::Pending => PROBE_PHASE_NAME_PENDING,
            Self::Passing { .. } => PROBE_PHASE_NAME_PASSING,
            Self::Failing { .. } => PROBE_PHASE_NAME_FAILING,
        }
    }
}

// ---------------------------------------------------------------------------
// ContainerLifecycle
// ---------------------------------------------------------------------------

/// Per-container state machine. Owns the container state, readiness flag,
/// and any probe phases registered for the container.
#[derive(Debug, Clone)]
pub struct ContainerLifecycle {
    name: String,
    role: ContainerRole,
    state: ContainerState,
    ready: bool,
    started: bool,
    restart_count: u32,
    probes: Vec<(ProbeKind, ProbePhase)>,
}

impl ContainerLifecycle {
    /// Create a new container lifecycle in the `Waiting(PodInitializing)`
    /// state with `ready=false`, `started=false`.
    pub fn new(name: impl Into<String>, role: ContainerRole) -> Self {
        let name = name.into();
        Self {
            name,
            role,
            state: ContainerState::Waiting {
                reason: WaitingReason::PodInitializing,
            },
            ready: false,
            started: false,
            restart_count: 0,
            probes: Vec::new(),
        }
    }

    /// Register a probe kind on this container (idempotent).
    pub fn add_probe(&mut self, kind: ProbeKind) {
        if !self.probes.iter().any(|(k, _)| *k == kind) {
            self.probes.push((kind, ProbePhase::Pending));
        }
    }

    // -- transitions -------------------------------------------------------

    /// Transition `Waiting → Running`. Sets `started=true`, `ready=false`.
    ///
    /// Returns [`TransitionError`] if the container is not currently in
    /// the `Waiting` state.
    pub fn on_started(&mut self, started_at: &str) -> Result<(), TransitionError> {
        match &self.state {
            ContainerState::Waiting { .. } => {
                debug!(
                    container = self.name.as_str(),
                    role = %self.role,
                    "Container state: Waiting -> Running",
                );
                self.state = ContainerState::Running {
                    started_at: started_at.to_string(),
                };
                self.started = true;
                self.ready = false;
                Ok(())
            }
            other => Err(TransitionError {
                machine: MACHINE_CONTAINER,
                from: other.name().to_string(),
                to: STATE_NAME_RUNNING.to_string(),
                reason: format!(
                    "container '{}' can only start from {}",
                    self.name, STATE_NAME_WAITING,
                ),
            }),
        }
    }

    /// Transition `Running → Terminated`. Preserves `started_at` from the
    /// previous `Running` state. Sets `ready=false`, `started=false`.
    ///
    /// Returns [`TransitionError`] if the container is not currently in
    /// the `Running` state.
    pub fn on_exited(&mut self, exit_code: i32, finished_at: &str) -> Result<(), TransitionError> {
        match &self.state {
            ContainerState::Running { started_at } => {
                let reason = TerminatedReason::from_exit_code(exit_code);
                debug!(
                    container = self.name.as_str(),
                    role = %self.role,
                    exit_code,
                    reason = reason.as_str(),
                    "Container state: Running -> Terminated",
                );
                self.state = ContainerState::Terminated {
                    exit_code,
                    reason,
                    started_at: started_at.clone(),
                    finished_at: finished_at.to_string(),
                };
                self.started = false;
                self.ready = false;
                Ok(())
            }
            other => Err(TransitionError {
                machine: MACHINE_CONTAINER,
                from: other.name().to_string(),
                to: STATE_NAME_TERMINATED.to_string(),
                reason: format!(
                    "container '{}' can only terminate from {}",
                    self.name, STATE_NAME_RUNNING,
                ),
            }),
        }
    }

    /// Transition `Terminated → Waiting(CrashLoopBackOff)` ahead of an
    /// in-place restart, counting it in `restart_count`.
    ///
    /// Returns [`TransitionError`] if the container is not currently in
    /// the `Terminated` state.
    pub fn on_back_off(&mut self) -> Result<(), TransitionError> {
        match &self.state {
            ContainerState::Terminated { .. } => {
                debug!(
                    container = self.name.as_str(),
                    role = %self.role,
                    "Container state: Terminated -> Waiting(CrashLoopBackOff)",
                );
                self.state = ContainerState::Waiting {
                    reason: WaitingReason::CrashLoopBackOff,
                };
                self.restart_count += 1;
                Ok(())
            }
            other => Err(TransitionError {
                machine: MACHINE_CONTAINER,
                from: other.name().to_string(),
                to: STATE_NAME_WAITING.to_string(),
                reason: format!(
                    "container '{}' can only back off from {}",
                    self.name, STATE_NAME_TERMINATED,
                ),
            }),
        }
    }

    /// Record a probe result. Updates the [`ProbePhase`] for the given
    /// kind (registering it if not already present).
    ///
    /// Returns `Ok(threshold_reached)` — `true` iff the new consecutive
    /// count is equal to (or greater than, for successive re-observations)
    /// the provided threshold in the current direction.
    pub fn on_probe_result(&mut self, kind: ProbeKind, success: bool, threshold: u32) -> bool {
        // Ensure probe is registered
        self.add_probe(kind);

        let slot = self
            .probes
            .iter_mut()
            .find(|(k, _)| *k == kind)
            .expect("probe just registered");
        let old_name = slot.1.name();
        let new_phase = match (&slot.1, success) {
            (ProbePhase::Pending, true) => ProbePhase::Passing { consecutive: 1 },
            (ProbePhase::Pending, false) => ProbePhase::Failing { consecutive: 1 },
            (ProbePhase::Passing { consecutive }, true) => ProbePhase::Passing {
                consecutive: consecutive.saturating_add(1),
            },
            (ProbePhase::Passing { .. }, false) => ProbePhase::Failing { consecutive: 1 },
            (ProbePhase::Failing { consecutive }, false) => ProbePhase::Failing {
                consecutive: consecutive.saturating_add(1),
            },
            (ProbePhase::Failing { .. }, true) => ProbePhase::Passing { consecutive: 1 },
        };
        let threshold_reached = match &new_phase {
            ProbePhase::Passing { consecutive } if success => *consecutive >= threshold,
            ProbePhase::Failing { consecutive } if !success => *consecutive >= threshold,
            _ => false,
        };
        debug!(
            container = self.name.as_str(),
            probe = %kind,
            success,
            threshold,
            from = old_name,
            to = new_phase.name(),
            threshold_reached,
            "Probe result recorded",
        );
        slot.1 = new_phase;
        threshold_reached
    }

    /// Set readiness. Returns `true` if the value changed.
    pub fn set_ready(&mut self, ready: bool) -> bool {
        if self.ready == ready {
            return false;
        }
        debug!(
            container = self.name.as_str(),
            from = self.ready,
            to = ready,
            "Container readiness changed",
        );
        self.ready = ready;
        true
    }

    // -- accessors ---------------------------------------------------------

    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn role(&self) -> ContainerRole {
        self.role
    }
    pub fn state(&self) -> &ContainerState {
        &self.state
    }
    pub fn is_ready(&self) -> bool {
        self.ready
    }
    pub fn is_started(&self) -> bool {
        self.started
    }
    pub fn restart_count(&self) -> u32 {
        self.restart_count
    }

    pub fn is_running(&self) -> bool {
        matches!(self.state, ContainerState::Running { .. })
    }

    pub fn is_terminated(&self) -> bool {
        matches!(self.state, ContainerState::Terminated { .. })
    }

    pub fn terminated_exit_code(&self) -> Option<i32> {
        match &self.state {
            ContainerState::Terminated { exit_code, .. } => Some(*exit_code),
            _ => None,
        }
    }

    pub fn probe_phase(&self, kind: ProbeKind) -> Option<&ProbePhase> {
        self.probes.iter().find(|(k, _)| *k == kind).map(|(_, p)| p)
    }

    /// Serialize to the legacy [`ContainerStatus`] format used by the API.
    pub fn to_container_status(&self) -> ContainerStatus {
        let mut cs = ContainerStatus {
            name: self.name.clone(),
            ready: self.ready,
            started: self.started,
            restart_count: self.restart_count,
            state: HashMap::new(),
        };
        match &self.state {
            ContainerState::Waiting { reason } => {
                cs.state.insert(
                    STATE_KEY_WAITING.to_string(),
                    serde_json::to_value(ContainerStateWaiting {
                        reason: reason.as_str().to_string(),
                    })
                    .unwrap(),
                );
            }
            ContainerState::Running { started_at } => {
                cs.state.insert(
                    STATE_KEY_RUNNING.to_string(),
                    serde_json::to_value(ContainerStateRunning {
                        started_at: started_at.clone(),
                    })
                    .unwrap(),
                );
            }
            ContainerState::Terminated {
                exit_code,
                reason,
                started_at,
                finished_at,
            } => {
                cs.state.insert(
                    STATE_KEY_TERMINATED.to_string(),
                    serde_json::to_value(ContainerStateTerminated {
                        exit_code: *exit_code,
                        reason: reason.as_str().to_string(),
                        started_at: started_at.clone(),
                        finished_at: finished_at.clone(),
                    })
                    .unwrap(),
                );
            }
        }
        cs
    }
}

// ---------------------------------------------------------------------------
// PodLifecycle
// ---------------------------------------------------------------------------

/// Key identifying a pod condition. Maps to the string constants used by
/// the existing [`PodStatus`] serialization.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PodConditionKey {
    Scheduled,
    Initialized,
    ContainersReady,
    Ready,
    DisruptionTarget,
}

impl PodConditionKey {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Scheduled => CONDITION_POD_SCHEDULED,
            Self::Initialized => CONDITION_INITIALIZED,
            Self::ContainersReady => CONDITION_CONTAINERS_READY,
            Self::Ready => CONDITION_READY,
            Self::DisruptionTarget => CONDITION_DISRUPTION_TARGET,
        }
    }
}

/// Boolean pod condition with reason/message and a transition timestamp.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConditionState {
    pub status: bool,
    pub reason: String,
    pub message: String,
    pub last_transition_time: String,
}

/// Pod-level state machine. Owns the pod phase, conditions, and all
/// container lifecycles (init + main). A single lock on this struct
/// guarantees a consistent snapshot for serialization.
#[derive(Debug, Clone)]
pub struct PodLifecycle {
    phase: PodPhase,
    message: String,
    start_time: String,
    last_transition_time: String,
    /// Ordered list of init containers (includes sidecars). Order matches
    /// `PodSpec.init_containers` so API serialization is spec-ordered.
    init_containers: Vec<ContainerLifecycle>,
    /// Ordered list of main containers. Order matches `PodSpec.containers`.
    containers: Vec<ContainerLifecycle>,
    conditions: HashMap<PodConditionKey, ConditionState>,
}

impl PodLifecycle {
    /// Create a new pod lifecycle in `Pending` phase. Containers are
    /// pre-populated in `Waiting(PodInitializing)`.
    ///
    /// `now` is the RFC3339 timestamp to record as `start_time` and
    /// the initial `last_transition_time`.
    pub fn new(
        init_containers: Vec<(String, ContainerRole)>,
        containers: Vec<(String, ContainerRole)>,
        now: &str,
    ) -> Self {
        Self {
            phase: PodPhase::Pending,
            message: String::new(),
            start_time: now.to_string(),
            last_transition_time: now.to_string(),
            init_containers: init_containers
                .into_iter()
                .map(|(n, r)| ContainerLifecycle::new(n, r))
                .collect(),
            containers: containers
                .into_iter()
                .map(|(n, r)| ContainerLifecycle::new(n, r))
                .collect(),
            conditions: HashMap::new(),
        }
    }

    // -- transitions -------------------------------------------------------

    fn require_non_terminal(&self, target: &str) -> Result<(), TransitionError> {
        if self.phase.is_terminal() {
            return Err(TransitionError {
                machine: MACHINE_POD,
                from: self.phase.to_string(),
                to: target.to_string(),
                reason: "pod already in terminal phase".to_string(),
            });
        }
        Ok(())
    }

    /// Set the `PodScheduled` condition to True.
    pub fn schedule(&mut self, now: &str) -> Result<(), TransitionError> {
        self.require_non_terminal(POD_TARGET_SCHEDULED)?;
        debug!("Pod condition: Scheduled=True");
        self.set_condition(
            PodConditionKey::Scheduled,
            true,
            REASON_POD_SCHEDULED,
            "",
            now,
        );
        Ok(())
    }

    /// Set the `Initialized` condition to True.
    pub fn mark_initialized(&mut self, now: &str) -> Result<(), TransitionError> {
        self.require_non_terminal(POD_TARGET_INITIALIZED)?;
        debug!("Pod condition: Initialized=True");
        self.set_condition(
            PodConditionKey::Initialized,
            true,
            REASON_INITIALIZED,
            "",
            now,
        );
        Ok(())
    }

    /// Set the `Initialized` condition to False.
    pub fn mark_not_initialized(&mut self, reason: &str, now: &str) -> Result<(), TransitionError> {
        self.require_non_terminal(POD_TARGET_NOT_INITIALIZED)?;
        debug!(reason, "Pod condition: Initialized=False");
        self.set_condition(
            PodConditionKey::Initialized,
            false,
            REASON_INIT_CONTAINERS_RUNNING,
            reason,
            now,
        );
        Ok(())
    }

    /// Transition `Pending → Running`.
    ///
    /// Returns [`TransitionError`] if the pod is not in `Pending`.
    pub fn start_running(&mut self, now: &str) -> Result<(), TransitionError> {
        match self.phase {
            PodPhase::Pending => {
                debug!("Pod phase: Pending -> Running");
                self.phase = PodPhase::Running;
                self.last_transition_time = now.to_string();
                Ok(())
            }
            other => Err(TransitionError {
                machine: MACHINE_POD,
                from: other.to_string(),
                to: POD_TARGET_RUNNING.to_string(),
                reason: "can only start running from Pending".to_string(),
            }),
        }
    }

    /// Transition `Running → Succeeded`.
    ///
    /// Returns [`TransitionError`] if the pod is not in `Running`.
    pub fn succeed(&mut self, message: &str, now: &str) -> Result<(), TransitionError> {
        match self.phase {
            PodPhase::Running => {
                debug!(message, "Pod phase: Running -> Succeeded");
                self.phase = PodPhase::Succeeded;
                self.message = message.to_string();
                self.last_transition_time = now.to_string();
                Ok(())
            }
            other => Err(TransitionError {
                machine: MACHINE_POD,
                from: other.to_string(),
                to: POD_TARGET_SUCCEEDED.to_string(),
                reason: "can only succeed from Running".to_string(),
            }),
        }
    }

    /// Transition to `Failed` from `Pending` or `Running`.
    ///
    /// Returns [`TransitionError`] if the pod is already terminal.
    pub fn fail(&mut self, message: &str, now: &str) -> Result<(), TransitionError> {
        match self.phase {
            PodPhase::Pending | PodPhase::Running => {
                debug!(
                    from = %self.phase,
                    message,
                    "Pod phase: -> Failed",
                );
                self.phase = PodPhase::Failed;
                self.message = message.to_string();
                self.last_transition_time = now.to_string();
                Ok(())
            }
            other => Err(TransitionError {
                machine: MACHINE_POD,
                from: other.to_string(),
                to: POD_TARGET_FAILED.to_string(),
                reason: "pod already in terminal phase".to_string(),
            }),
        }
    }

    /// Recompute `ContainersReady` and `Ready` conditions based on the
    /// current state of main containers. A container counts as "ready" if
    /// `ready == true` OR it is terminated.
    pub fn update_readiness(&mut self, now: &str) {
        let all_ready = self.containers.iter().all(|c| c.ready || c.is_terminated());
        if all_ready {
            self.set_condition(
                PodConditionKey::ContainersReady,
                true,
                REASON_CONTAINERS_READY,
                "",
                now,
            );
            self.set_condition(PodConditionKey::Ready, true, REASON_POD_READY, "", now);
        } else {
            self.set_condition(
                PodConditionKey::ContainersReady,
                false,
                REASON_CONTAINERS_NOT_READY,
                REASON_CONTAINERS_STARTING,
                now,
            );
            self.set_condition(
                PodConditionKey::Ready,
                false,
                REASON_CONTAINERS_NOT_READY,
                REASON_CONTAINERS_STARTING,
                now,
            );
        }
    }

    /// Mark the pod as a disruption target (deletion requested).
    pub fn mark_disruption_target(&mut self, now: &str) {
        debug!("Pod condition: DisruptionTarget=True, Ready=False");
        self.set_condition(
            PodConditionKey::DisruptionTarget,
            true,
            REASON_DELETION_TRIGGERED,
            "",
            now,
        );
        self.set_condition(
            PodConditionKey::Ready,
            false,
            REASON_DELETION_TRIGGERED,
            "",
            now,
        );
    }

    fn set_condition(
        &mut self,
        key: PodConditionKey,
        status: bool,
        reason: &str,
        message: &str,
        now: &str,
    ) {
        self.conditions.insert(
            key,
            ConditionState {
                status,
                reason: reason.to_string(),
                message: message.to_string(),
                last_transition_time: now.to_string(),
            },
        );
    }

    // -- container accessors -----------------------------------------------

    pub fn container(&self, name: &str) -> Option<&ContainerLifecycle> {
        self.containers.iter().find(|c| c.name == name)
    }

    pub fn container_mut(&mut self, name: &str) -> Option<&mut ContainerLifecycle> {
        self.containers.iter_mut().find(|c| c.name == name)
    }

    pub fn init_container(&self, name: &str) -> Option<&ContainerLifecycle> {
        self.init_containers.iter().find(|c| c.name == name)
    }

    pub fn init_container_mut(&mut self, name: &str) -> Option<&mut ContainerLifecycle> {
        self.init_containers.iter_mut().find(|c| c.name == name)
    }

    /// Route to `init_container_mut` or `container_mut` based on the
    /// container role. Init and Sidecar roles look up in the init list.
    pub fn container_by_role_mut(
        &mut self,
        name: &str,
        role: ContainerRole,
    ) -> Option<&mut ContainerLifecycle> {
        match role {
            ContainerRole::Init | ContainerRole::Sidecar => self.init_container_mut(name),
            ContainerRole::Main => self.container_mut(name),
        }
    }

    pub fn containers(&self) -> &[ContainerLifecycle] {
        &self.containers
    }
    pub fn init_containers(&self) -> &[ContainerLifecycle] {
        &self.init_containers
    }

    // -- accessors ---------------------------------------------------------

    pub fn phase(&self) -> PodPhase {
        self.phase
    }
    pub fn message(&self) -> &str {
        &self.message
    }
    pub fn start_time(&self) -> &str {
        &self.start_time
    }
    pub fn last_transition_time(&self) -> &str {
        &self.last_transition_time
    }
    pub fn is_terminal(&self) -> bool {
        self.phase.is_terminal()
    }

    pub fn condition(&self, key: PodConditionKey) -> Option<&ConditionState> {
        self.conditions.get(&key)
    }

    /// Serialize to the legacy [`PodStatus`] used by the API.
    pub fn to_pod_status(&self) -> PodStatus {
        let mut status = PodStatus {
            phase: self.phase,
            message: self.message.clone(),
            start_time: self.start_time.clone(),
            last_transition_time: self.last_transition_time.clone(),
            conditions: Vec::new(),
            container_statuses: self
                .containers
                .iter()
                .map(|c| c.to_container_status())
                .collect(),
            init_container_statuses: self
                .init_containers
                .iter()
                .map(|c| c.to_container_status())
                .collect(),
            last_monitor_time: 0.0,
        };
        // Emit conditions in a stable order for deterministic output.
        for key in [
            PodConditionKey::Scheduled,
            PodConditionKey::Initialized,
            PodConditionKey::ContainersReady,
            PodConditionKey::Ready,
            PodConditionKey::DisruptionTarget,
        ] {
            if let Some(cond) = self.conditions.get(&key) {
                status.conditions.push(PodCondition {
                    condition_type: key.as_str().to_string(),
                    status: if cond.status {
                        CONDITION_TRUE
                    } else {
                        CONDITION_FALSE
                    }
                    .to_string(),
                    last_transition_time: cond.last_transition_time.clone(),
                    reason: cond.reason.clone(),
                    message: cond.message.clone(),
                });
            }
        }
        status
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    const TS: &str = "2026-01-01T00:00:00Z";
    const TS2: &str = "2026-01-01T00:00:01Z";

    fn container(name: &str) -> ContainerLifecycle {
        ContainerLifecycle::new(name, ContainerRole::Main)
    }

    fn pod_empty() -> PodLifecycle {
        PodLifecycle::new(vec![], vec![], TS)
    }

    fn pod_simple() -> PodLifecycle {
        PodLifecycle::new(vec![], vec![("app".to_string(), ContainerRole::Main)], TS)
    }

    // -- ContainerLifecycle transitions ------------------------------------

    #[test]
    fn container_waiting_to_running_ok() {
        let mut c = container("a");
        assert!(matches!(c.state(), ContainerState::Waiting { .. }));
        c.on_started(TS).unwrap();
        assert!(c.is_running());
        assert!(c.is_started());
        assert!(!c.is_ready());
    }

    #[test]
    fn container_running_to_terminated_ok() {
        let mut c = container("a");
        c.on_started(TS).unwrap();
        c.on_exited(0, TS2).unwrap();
        assert!(c.is_terminated());
        assert_eq!(c.terminated_exit_code(), Some(0));
        assert!(!c.is_started());
        assert!(!c.is_ready());
    }

    #[test]
    fn container_terminated_backs_off_to_waiting_and_counts_the_restart() {
        let mut c = container("a");
        c.on_started(TS).unwrap();
        c.on_exited(1, TS2).unwrap();
        c.on_back_off().unwrap();
        assert_eq!(
            c.state(),
            &ContainerState::Waiting {
                reason: WaitingReason::CrashLoopBackOff
            }
        );
        assert_eq!(c.restart_count(), 1);
        c.on_started(TS).unwrap();
        assert!(c.is_running());
    }

    #[test]
    fn container_running_back_off_rejected() {
        let mut c = container("a");
        c.on_started(TS).unwrap();
        let err = c.on_back_off().unwrap_err();
        assert_eq!(err.machine, MACHINE_CONTAINER);
        assert_eq!(err.from, STATE_NAME_RUNNING);
        assert_eq!(err.to, STATE_NAME_WAITING);
        assert_eq!(c.restart_count(), 0);
    }

    #[test]
    fn container_terminated_to_running_rejected() {
        let mut c = container("a");
        c.on_started(TS).unwrap();
        c.on_exited(0, TS2).unwrap();
        let err = c.on_started(TS).unwrap_err();
        assert_eq!(err.machine, MACHINE_CONTAINER);
        assert_eq!(err.from, STATE_NAME_TERMINATED);
        assert_eq!(err.to, STATE_NAME_RUNNING);
    }

    #[test]
    fn container_waiting_to_terminated_rejected() {
        let mut c = container("a");
        let err = c.on_exited(0, TS).unwrap_err();
        assert_eq!(err.from, STATE_NAME_WAITING);
        assert_eq!(err.to, STATE_NAME_TERMINATED);
    }

    #[test]
    fn container_double_start_rejected() {
        let mut c = container("a");
        c.on_started(TS).unwrap();
        let err = c.on_started(TS).unwrap_err();
        assert_eq!(err.from, STATE_NAME_RUNNING);
    }

    #[test]
    fn container_terminated_preserves_started_at() {
        let mut c = container("a");
        c.on_started("T1").unwrap();
        c.on_exited(3, "T2").unwrap();
        match c.state() {
            ContainerState::Terminated {
                started_at,
                finished_at,
                exit_code,
                reason,
            } => {
                assert_eq!(started_at, "T1");
                assert_eq!(finished_at, "T2");
                assert_eq!(*exit_code, 3);
                assert_eq!(reason, &TerminatedReason::Error);
            }
            _ => panic!("expected Terminated"),
        }
    }

    #[test]
    fn container_set_ready_returns_changed() {
        let mut c = container("a");
        assert!(!c.set_ready(false));
        assert!(c.set_ready(true));
        assert!(!c.set_ready(true));
        assert!(c.set_ready(false));
    }

    // -- Probes ------------------------------------------------------------

    #[test]
    fn probe_threshold_counts_consecutive_successes() {
        let mut c = container("a");
        let t = 3u32;
        assert!(!c.on_probe_result(ProbeKind::Readiness, true, t));
        assert!(!c.on_probe_result(ProbeKind::Readiness, true, t));
        assert!(c.on_probe_result(ProbeKind::Readiness, true, t));
        assert!(matches!(
            c.probe_phase(ProbeKind::Readiness),
            Some(ProbePhase::Passing { consecutive: 3 })
        ));
    }

    #[test]
    fn probe_failure_resets_passing_counter() {
        let mut c = container("a");
        c.on_probe_result(ProbeKind::Liveness, true, 5);
        c.on_probe_result(ProbeKind::Liveness, true, 5);
        c.on_probe_result(ProbeKind::Liveness, false, 1);
        assert!(matches!(
            c.probe_phase(ProbeKind::Liveness),
            Some(ProbePhase::Failing { consecutive: 1 })
        ));
    }

    #[test]
    fn probe_success_resets_failing_counter() {
        let mut c = container("a");
        c.on_probe_result(ProbeKind::Startup, false, 10);
        c.on_probe_result(ProbeKind::Startup, false, 10);
        c.on_probe_result(ProbeKind::Startup, true, 1);
        assert!(matches!(
            c.probe_phase(ProbeKind::Startup),
            Some(ProbePhase::Passing { consecutive: 1 })
        ));
    }

    #[test]
    fn probe_threshold_reached_signals_once_at_boundary() {
        let mut c = container("a");
        // Threshold of 1: first failure trips immediately.
        assert!(c.on_probe_result(ProbeKind::Liveness, false, 1));
    }

    // -- PodLifecycle transitions ------------------------------------------

    #[test]
    fn pod_pending_to_running_ok() {
        let mut p = pod_simple();
        assert_eq!(p.phase(), PodPhase::Pending);
        p.start_running(TS2).unwrap();
        assert_eq!(p.phase(), PodPhase::Running);
    }

    #[test]
    fn pod_running_to_succeeded_ok() {
        let mut p = pod_simple();
        p.start_running(TS2).unwrap();
        p.succeed("done", TS2).unwrap();
        assert_eq!(p.phase(), PodPhase::Succeeded);
        assert_eq!(p.message(), "done");
    }

    #[test]
    fn pod_running_to_failed_ok() {
        let mut p = pod_simple();
        p.start_running(TS2).unwrap();
        p.fail("oops", TS2).unwrap();
        assert_eq!(p.phase(), PodPhase::Failed);
        assert_eq!(p.message(), "oops");
    }

    #[test]
    fn pod_pending_to_failed_ok() {
        let mut p = pod_simple();
        p.fail("init failure", TS2).unwrap();
        assert_eq!(p.phase(), PodPhase::Failed);
    }

    #[test]
    fn pod_succeeded_to_anything_rejected() {
        let mut p = pod_simple();
        p.start_running(TS2).unwrap();
        p.succeed("", TS2).unwrap();
        assert!(p.start_running(TS2).is_err());
        assert!(p.succeed("", TS2).is_err());
        assert!(p.fail("", TS2).is_err());
    }

    #[test]
    fn pod_failed_to_anything_rejected() {
        let mut p = pod_simple();
        p.fail("x", TS2).unwrap();
        assert!(p.start_running(TS2).is_err());
        assert!(p.succeed("", TS2).is_err());
        assert!(p.fail("", TS2).is_err());
    }

    #[test]
    fn pod_running_to_pending_rejected() {
        let mut p = pod_simple();
        p.start_running(TS2).unwrap();
        // start_running only accepts Pending as source — proves Running→Pending impossible
        assert!(p.start_running(TS2).is_err());
    }

    #[test]
    fn pod_succeed_from_pending_rejected() {
        let mut p = pod_simple();
        let err = p.succeed("", TS2).unwrap_err();
        assert_eq!(err.from, PodPhase::Pending.to_string());
        assert_eq!(err.to, POD_TARGET_SUCCEEDED);
    }

    #[test]
    fn pod_terminal_rejects_schedule_and_init_marks() {
        let mut p = pod_simple();
        p.fail("x", TS2).unwrap();
        assert!(p.schedule(TS2).is_err());
        assert!(p.mark_initialized(TS2).is_err());
        assert!(p.mark_not_initialized("r", TS2).is_err());
    }

    // -- Readiness & conditions --------------------------------------------

    #[test]
    fn update_readiness_all_terminated_marks_ready() {
        let mut p = PodLifecycle::new(
            vec![],
            vec![
                ("a".to_string(), ContainerRole::Main),
                ("b".to_string(), ContainerRole::Main),
            ],
            TS,
        );
        for name in ["a", "b"] {
            let c = p.container_mut(name).unwrap();
            c.on_started(TS).unwrap();
            c.on_exited(0, TS2).unwrap();
        }
        p.update_readiness(TS2);
        let cr = p.condition(PodConditionKey::ContainersReady).unwrap();
        assert!(cr.status);
        let r = p.condition(PodConditionKey::Ready).unwrap();
        assert!(r.status);
    }

    #[test]
    fn update_readiness_mixed_not_ready() {
        let mut p = PodLifecycle::new(
            vec![],
            vec![
                ("a".to_string(), ContainerRole::Main),
                ("b".to_string(), ContainerRole::Main),
            ],
            TS,
        );
        // a running but not ready, b still waiting
        p.container_mut("a").unwrap().on_started(TS).unwrap();
        p.update_readiness(TS2);
        assert!(
            !p.condition(PodConditionKey::ContainersReady)
                .unwrap()
                .status
        );
        assert!(!p.condition(PodConditionKey::Ready).unwrap().status);
    }

    #[test]
    fn update_readiness_all_ready_running_marks_ready() {
        let mut p = PodLifecycle::new(vec![], vec![("a".to_string(), ContainerRole::Main)], TS);
        p.container_mut("a").unwrap().on_started(TS).unwrap();
        p.container_mut("a").unwrap().set_ready(true);
        p.update_readiness(TS2);
        assert!(p.condition(PodConditionKey::Ready).unwrap().status);
    }

    // -- Serialization bridges ---------------------------------------------

    #[test]
    fn to_container_status_waiting_shape() {
        let c = container("a");
        let cs = c.to_container_status();
        assert_eq!(cs.name, "a");
        assert!(!cs.ready);
        assert!(!cs.started);
        assert!(cs.state.contains_key(STATE_KEY_WAITING));
        assert!(!cs.state.contains_key(STATE_KEY_RUNNING));
        let w = cs.state.get(STATE_KEY_WAITING).unwrap();
        assert_eq!(w.get("reason").unwrap(), REASON_POD_INITIALIZING);
    }

    #[test]
    fn to_container_status_running_shape() {
        let mut c = container("a");
        c.on_started("T1").unwrap();
        let cs = c.to_container_status();
        assert!(cs.started);
        assert!(cs.state.contains_key(STATE_KEY_RUNNING));
        let r = cs.state.get(STATE_KEY_RUNNING).unwrap();
        assert_eq!(r.get("startedAt").unwrap(), "T1");
    }

    #[test]
    fn to_container_status_terminated_shape() {
        let mut c = container("a");
        c.on_started("T1").unwrap();
        c.on_exited(137, "T2").unwrap();
        let cs = c.to_container_status();
        assert!(cs.state.contains_key(STATE_KEY_TERMINATED));
        let t = cs.state.get(STATE_KEY_TERMINATED).unwrap();
        assert_eq!(t.get("exitCode").unwrap(), 137);
        assert_eq!(t.get("reason").unwrap(), REASON_ERROR);
        assert_eq!(t.get("startedAt").unwrap(), "T1");
        assert_eq!(t.get("finishedAt").unwrap(), "T2");
    }

    #[test]
    fn to_pod_status_carries_phase_and_containers() {
        let mut p = PodLifecycle::new(
            vec![("i".to_string(), ContainerRole::Init)],
            vec![("a".to_string(), ContainerRole::Main)],
            TS,
        );
        p.schedule(TS).unwrap();
        p.start_running(TS2).unwrap();
        let ps = p.to_pod_status();
        assert_eq!(ps.phase, PodPhase::Running);
        assert_eq!(ps.container_statuses.len(), 1);
        assert_eq!(ps.init_container_statuses.len(), 1);
        assert_eq!(ps.container_statuses[0].name, "a");
        assert_eq!(ps.init_container_statuses[0].name, "i");
        // Scheduled condition present
        assert!(
            ps.conditions
                .iter()
                .any(|c| c.condition_type == CONDITION_POD_SCHEDULED && c.status == CONDITION_TRUE)
        );
    }

    #[test]
    fn to_pod_status_preserves_container_order() {
        let mut p = PodLifecycle::new(
            vec![],
            vec![
                ("z".to_string(), ContainerRole::Main),
                ("a".to_string(), ContainerRole::Main),
                ("m".to_string(), ContainerRole::Main),
            ],
            TS,
        );
        p.start_running(TS2).unwrap();
        let ps = p.to_pod_status();
        let names: Vec<&str> = ps
            .container_statuses
            .iter()
            .map(|c| c.name.as_str())
            .collect();
        assert_eq!(names, ["z", "a", "m"]);
    }

    #[test]
    fn to_pod_status_includes_disruption_target_when_set() {
        let mut p = pod_simple();
        p.start_running(TS2).unwrap();
        p.mark_disruption_target(TS2);
        let ps = p.to_pod_status();
        assert!(
            ps.conditions
                .iter()
                .any(|c| c.condition_type == CONDITION_DISRUPTION_TARGET
                    && c.status == CONDITION_TRUE)
        );
        assert!(
            ps.conditions
                .iter()
                .any(|c| c.condition_type == CONDITION_READY && c.status == CONDITION_FALSE)
        );
    }

    #[test]
    fn empty_pod_has_no_conditions_until_marked() {
        let p = pod_empty();
        let ps = p.to_pod_status();
        assert!(ps.conditions.is_empty());
    }
}
