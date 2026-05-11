// Copyright 2026 Tabsdata Inc.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use super::constants::*;

// ---------------------------------------------------------------------------
// Container state variants
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct ContainerStateWaiting {
    pub reason: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct ContainerStateRunning {
    pub started_at: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct ContainerStateTerminated {
    pub exit_code: i32,
    pub reason: String,
    pub started_at: String,
    pub finished_at: String,
}

// ---------------------------------------------------------------------------
// Container status
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct ContainerStatus {
    pub name: String,
    pub ready: bool,
    pub started: bool,
    pub restart_count: u32,
    pub state: HashMap<String, serde_json::Value>,
}

impl ContainerStatus {
    pub fn new_waiting(name: &str) -> Self {
        let mut state = HashMap::new();
        state.insert(
            STATE_KEY_WAITING.to_string(),
            serde_json::to_value(ContainerStateWaiting {
                reason: REASON_POD_INITIALIZING.to_string(),
            }).unwrap(),
        );
        Self {
            name: name.to_string(),
            ready: false,
            started: false,
            restart_count: 0,
            state,
        }
    }

    pub fn set_waiting(&mut self, reason: &str) {
        self.state.clear();
        self.state.insert(
            STATE_KEY_WAITING.to_string(),
            serde_json::to_value(ContainerStateWaiting {
                reason: reason.to_string(),
            }).unwrap(),
        );
        self.ready = false;
        self.started = false;
    }

    pub fn set_running(&mut self, started_at: Option<&str>) {
        let ts = started_at.unwrap_or("").to_string();
        let ts = if ts.is_empty() { chrono::Utc::now().to_rfc3339() } else { ts };
        self.state.clear();
        self.state.insert(
            STATE_KEY_RUNNING.to_string(),
            serde_json::to_value(ContainerStateRunning { started_at: ts }).unwrap(),
        );
        self.ready = false;
        self.started = true;
    }

    pub fn set_terminated(&mut self, exit_code: i32, reason: &str) {
        let started_at = self.state.get(STATE_KEY_RUNNING)
            .and_then(|v| v.get("startedAt"))
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        self.state.clear();
        self.state.insert(
            STATE_KEY_TERMINATED.to_string(),
            serde_json::to_value(ContainerStateTerminated {
                exit_code,
                reason: reason.to_string(),
                started_at,
                finished_at: chrono::Utc::now().to_rfc3339(),
            }).unwrap(),
        );
        self.ready = false;
        self.started = false;
    }

    pub fn is_terminated(&self) -> bool {
        self.state.contains_key(STATE_KEY_TERMINATED)
    }

    pub fn is_running(&self) -> bool {
        self.state.contains_key(STATE_KEY_RUNNING)
    }

    pub fn terminated_exit_code(&self) -> Option<i32> {
        self.state.get(STATE_KEY_TERMINATED)
            .and_then(|v| v.get("exitCode"))
            .and_then(|v| v.as_i64())
            .map(|v| v as i32)
    }
}

// ---------------------------------------------------------------------------
// Pod condition
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct PodCondition {
    #[serde(rename = "type")]
    pub condition_type: String,
    pub status: String,
    pub last_transition_time: String,
    pub reason: String,
    pub message: String,
}

// ---------------------------------------------------------------------------
// Pod status
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct PodStatus {
    pub phase: PodPhase,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub message: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub start_time: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub last_transition_time: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<PodCondition>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub container_statuses: Vec<ContainerStatus>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub init_container_statuses: Vec<ContainerStatus>,
    /// Epoch seconds of the last monitor cycle. Used to throttle
    /// expensive process checks in tight monitoring loops.
    #[serde(default, skip_serializing_if = "is_zero_f64")]
    pub last_monitor_time: f64,
}

fn is_zero_f64(v: &f64) -> bool {
    *v == 0.0
}

impl PodStatus {
    pub fn set_condition(&mut self, cond_type: &str, status: bool, reason: &str, message: &str) {
        let status_str = if status { CONDITION_TRUE } else { CONDITION_FALSE };
        let ts = chrono::Utc::now().to_rfc3339();
        if let Some(cond) = self.conditions.iter_mut().find(|c| c.condition_type == cond_type) {
            cond.status = status_str.to_string();
            cond.reason = reason.to_string();
            cond.message = message.to_string();
            cond.last_transition_time = ts;
        } else {
            self.conditions.push(PodCondition {
                condition_type: cond_type.to_string(),
                status: status_str.to_string(),
                last_transition_time: ts,
                reason: reason.to_string(),
                message: message.to_string(),
            });
        }
    }

    pub fn is_condition_true(&self, cond_type: &str) -> bool {
        self.conditions.iter()
            .any(|c| c.condition_type == cond_type && c.status == CONDITION_TRUE)
    }

    pub fn mark_scheduled(&mut self) {
        self.set_condition(CONDITION_POD_SCHEDULED, true, REASON_POD_SCHEDULED, "");
    }

    pub fn mark_initialized(&mut self) {
        self.set_condition(CONDITION_INITIALIZED, true, REASON_INITIALIZED, "");
    }

    pub fn mark_not_initialized(&mut self, reason: &str) {
        self.set_condition(CONDITION_INITIALIZED, false, REASON_INIT_CONTAINERS_RUNNING, reason);
    }

    pub fn mark_containers_ready(&mut self) {
        self.set_condition(CONDITION_CONTAINERS_READY, true, REASON_CONTAINERS_READY, "");
        self.set_condition(CONDITION_READY, true, REASON_POD_READY, "");
    }

    pub fn mark_containers_not_ready(&mut self, reason: &str) {
        self.set_condition(CONDITION_CONTAINERS_READY, false, REASON_CONTAINERS_NOT_READY, reason);
        self.set_condition(CONDITION_READY, false, REASON_CONTAINERS_NOT_READY, reason);
    }

    pub fn mark_disruption_target(&mut self) {
        self.set_condition(CONDITION_DISRUPTION_TARGET, true, REASON_DELETION_TRIGGERED, "");
        self.set_condition(CONDITION_READY, false, REASON_DELETION_TRIGGERED, "");
    }

    pub fn find_container_status(&self, name: &str) -> Option<&ContainerStatus> {
        self.container_statuses.iter().find(|cs| cs.name == name)
    }

    pub fn find_container_status_mut(&mut self, name: &str) -> Option<&mut ContainerStatus> {
        self.container_statuses.iter_mut().find(|cs| cs.name == name)
    }

    pub fn find_init_container_status(&self, name: &str) -> Option<&ContainerStatus> {
        self.init_container_statuses.iter().find(|cs| cs.name == name)
    }
}

// ---------------------------------------------------------------------------
// Probe state
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct ProbeState {
    pub container: String,
    pub probe_type: String,
    pub timestamp: String,
    pub phase: String,
    pub result: String,
    pub error: String,
    pub consecutive_successes: u32,
    pub consecutive_failures: u32,
    pub last_probe_time: f64,
    pub started_at: f64,
}
