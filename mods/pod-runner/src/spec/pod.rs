// Copyright 2026 Tabsdata Inc.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use super::constants::RestartPolicy;

// ---------------------------------------------------------------------------
// Environment
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct KeyRef {
    pub name: String,
    pub key: String,
    /// K8s `optional` flag. Pod-runner does not support `true` — see
    /// [`crate::pod::admission`] for the admission-time rejection.
    pub optional: Option<bool>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct EnvVarSource {
    pub config_map_key_ref: Option<KeyRef>,
    pub secret_key_ref: Option<KeyRef>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EnvVar {
    #[serde(default)]
    pub name: String,
    pub value: Option<String>,
    pub value_from: Option<EnvVarSource>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct ObjectRef {
    pub name: String,
    /// K8s `optional` flag. Pod-runner does not support `true` — see
    /// [`crate::pod::admission`] for the admission-time rejection.
    pub optional: Option<bool>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct EnvFromSource {
    pub config_map_ref: Option<ObjectRef>,
    pub secret_ref: Option<ObjectRef>,
}

// ---------------------------------------------------------------------------
// Probes
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
#[derive(Default)]
pub struct ExecAction {
    pub command: Vec<String>,
}


#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct HttpGetAction {
    pub path: String,
    pub port: Option<u16>,
    pub host: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct TcpSocketAction {
    pub port: Option<u16>,
    pub host: String,
}

fn default_10() -> u32 { 10 }
fn default_1() -> u32 { 1 }
fn default_3() -> u32 { 3 }

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Probe {
    #[serde(rename = "exec")]
    pub exec_action: Option<ExecAction>,
    pub http_get: Option<HttpGetAction>,
    pub tcp_socket: Option<TcpSocketAction>,
    pub grpc: Option<serde_json::Value>,
    #[serde(default)]
    pub initial_delay_seconds: u32,
    #[serde(default = "default_10")]
    pub period_seconds: u32,
    #[serde(default = "default_1")]
    pub timeout_seconds: u32,
    #[serde(default = "default_1")]
    pub success_threshold: u32,
    #[serde(default = "default_3")]
    pub failure_threshold: u32,
}

impl Default for Probe {
    fn default() -> Self {
        Self {
            exec_action: None,
            http_get: None,
            tcp_socket: None,
            grpc: None,
            initial_delay_seconds: 0,
            period_seconds: default_10(),
            timeout_seconds: default_1(),
            success_threshold: default_1(),
            failure_threshold: default_3(),
        }
    }
}

// ---------------------------------------------------------------------------
// Volumes
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct VolumeMount {
    pub name: String,
    pub mount_path: String,
    #[serde(default)]
    pub read_only: bool,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct HostPathVolume {
    pub path: String,
    #[serde(rename = "type")]
    pub volume_type: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct EmptyDirVolume {}

/// Maps a single ConfigMap key to a relative path under the mount point.
///
/// The `path` is relative to the volume's `mountPath` and may contain
/// subdirectories (e.g. `apiserver/apiserver.yaml`). `mode` (file
/// permission bits) is accepted but ignored.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct KeyToPath {
    pub key: String,
    pub path: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct ConfigMapVolume {
    pub name: String,
    /// Optional key selection. When empty, every key in the configmap is
    /// projected at a file named after the key. When set, only the listed
    /// keys are projected, each at its `path` (relative to the mount).
    pub items: Vec<KeyToPath>,
}

/// One source within a `projected` volume. Only `configMap` sources are
/// supported; any other source type (secret, downwardAPI,
/// serviceAccountToken) lands in `extra` and is skipped with a warning
/// during volume materialization.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct VolumeProjection {
    pub config_map: Option<ConfigMapVolume>,
    #[serde(flatten)]
    pub extra: HashMap<String, serde_json::Value>,
}

/// A `projected` volume. All sources are merged flat into the single mount
/// directory (respecting each item's `path`). On a path collision the last
/// source in `sources` wins — pod-runner logs a warning, matching k8s'
/// silent last-wins behavior but making it visible.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct ProjectedVolume {
    pub sources: Vec<VolumeProjection>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct Volume {
    pub name: String,
    pub host_path: Option<HostPathVolume>,
    pub empty_dir: Option<EmptyDirVolume>,
    pub config_map: Option<ConfigMapVolume>,
    pub projected: Option<ProjectedVolume>,
    #[serde(flatten)]
    pub extra: HashMap<String, serde_json::Value>,
}

// ---------------------------------------------------------------------------
// Container
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct Container {
    pub name: String,
    pub image: String,
    pub command: Vec<String>,
    pub args: Vec<String>,
    pub working_dir: String,
    pub env: Vec<EnvVar>,
    pub env_from: Vec<EnvFromSource>,
    pub volume_mounts: Vec<VolumeMount>,
    pub restart_policy: Option<String>,
    pub startup_probe: Option<Probe>,
    pub liveness_probe: Option<Probe>,
    pub readiness_probe: Option<Probe>,
    #[serde(flatten)]
    pub extra: HashMap<String, serde_json::Value>,
}

impl Container {
    pub fn exec_args(&self) -> Vec<String> {
        let mut args = self.command.clone();
        args.extend(self.args.iter().cloned());
        args
    }

    pub fn is_sidecar(&self) -> bool {
        self.restart_policy.as_deref() == Some("Always")
    }

    pub fn probes(&self) -> Vec<(&str, &Probe)> {
        let mut result = Vec::new();
        if let Some(ref p) = self.startup_probe {
            result.push(("startup", p));
        }
        if let Some(ref p) = self.liveness_probe {
            result.push(("liveness", p));
        }
        if let Some(ref p) = self.readiness_probe {
            result.push(("readiness", p));
        }
        result
    }
}

// ---------------------------------------------------------------------------
// Pod spec body
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct PodSpecBody {
    pub node_name: String,
    pub init_containers: Vec<Container>,
    pub containers: Vec<Container>,
    pub volumes: Vec<Volume>,
    pub restart_policy: RestartPolicy,
    /// K8s `activeDeadlineSeconds`. In pod-runner this bounds the total time
    /// the preparer waits for referenced configmaps and secrets to become
    /// available before the pod is transitioned to Failed. `None` means the
    /// wait is indefinite.
    pub active_deadline_seconds: Option<u64>,
    /// K8s `terminationGracePeriodSeconds`. The grace period pod-runner gives
    /// a container between SIGTERM and SIGKILL when it terminates the pod
    /// (shutdown, probe-driven kill, or a delete with no explicit
    /// `gracePeriod`). `None` falls back to the runner's configured default
    /// grace period. `Some(0)` means immediate SIGKILL.
    pub termination_grace_period_seconds: Option<u64>,
    #[serde(flatten)]
    pub extra: HashMap<String, serde_json::Value>,
}

impl PodSpecBody {
    pub fn all_containers(&self) -> Vec<&Container> {
        self.init_containers.iter().chain(self.containers.iter()).collect()
    }

    pub fn all_containers_mut(&mut self) -> Vec<&mut Container> {
        self.init_containers.iter_mut().chain(self.containers.iter_mut()).collect()
    }

    pub fn volume_map(&self) -> HashMap<&str, &Volume> {
        self.volumes.iter().map(|v| (v.name.as_str(), v)).collect()
    }
}

// ---------------------------------------------------------------------------
// Pod metadata
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct PodMetadata {
    pub uid: String,
    pub name: String,
    pub namespace: String,
    pub annotations: HashMap<String, String>,
    #[serde(flatten)]
    pub extra: HashMap<String, serde_json::Value>,
}

// ---------------------------------------------------------------------------
// Top-level pod
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct PodSpec {
    pub metadata: PodMetadata,
    pub spec: PodSpecBody,
    #[serde(flatten)]
    pub extra: HashMap<String, serde_json::Value>,
}

impl PodSpec {
    pub fn all_container_names(&self) -> Vec<String> {
        self.spec.all_containers().iter().map(|c| c.name.clone()).collect()
    }

    /// The pod's `terminationGracePeriodSeconds` as a [`Duration`], if set.
    /// Used as the grace between SIGTERM and SIGKILL when pod-runner
    /// terminates the pod.
    pub fn termination_grace_period(&self) -> Option<std::time::Duration> {
        self.spec
            .termination_grace_period_seconds
            .map(std::time::Duration::from_secs)
    }
}

// ---------------------------------------------------------------------------
// Pod object (spec + status) — returned by get/list APIs
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct PodObject {
    #[serde(flatten)]
    pub spec: PodSpec,
    pub status: super::status::PodStatus,
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn termination_grace_period_parses_and_converts() {
        let yaml = r#"
spec:
  terminationGracePeriodSeconds: 45
  containers:
    - name: app
      image: /dev/null
"#;
        let pod: PodSpec = serde_yaml_ng::from_str(yaml).unwrap();
        assert_eq!(pod.spec.termination_grace_period_seconds, Some(45));
        assert_eq!(
            pod.termination_grace_period(),
            Some(std::time::Duration::from_secs(45))
        );
    }

    #[test]
    fn termination_grace_period_absent_is_none() {
        let yaml = r#"
spec:
  containers:
    - name: app
      image: /dev/null
"#;
        let pod: PodSpec = serde_yaml_ng::from_str(yaml).unwrap();
        assert_eq!(pod.spec.termination_grace_period_seconds, None);
        assert_eq!(pod.termination_grace_period(), None);
    }

    #[test]
    fn termination_grace_period_zero_is_immediate() {
        let yaml = r#"
spec:
  terminationGracePeriodSeconds: 0
  containers:
    - name: app
      image: /dev/null
"#;
        let pod: PodSpec = serde_yaml_ng::from_str(yaml).unwrap();
        assert_eq!(
            pod.termination_grace_period(),
            Some(std::time::Duration::ZERO)
        );
    }

    #[test]
    fn config_map_volume_deserializes_from_yaml() {
        let yaml = r#"
spec:
  volumes:
    - name: config
      configMap:
        name: app-config
  containers:
    - name: app
      image: /dev/null
      volumeMounts:
        - name: config
          mountPath: /etc/app
          readOnly: true
"#;

        let pod: PodSpec = serde_yaml_ng::from_str(yaml).unwrap();
        let vol = &pod.spec.volumes[0];
        assert_eq!(vol.name, "config");
        let cm = vol.config_map.as_ref().expect("config_map should be set");
        assert_eq!(cm.name, "app-config");
        assert!(vol.empty_dir.is_none());
        assert!(vol.host_path.is_none());

        let vm = &pod.spec.containers[0].volume_mounts[0];
        assert_eq!(vm.name, "config");
        assert_eq!(vm.mount_path, "/etc/app");
        assert!(vm.read_only);
    }

    #[test]
    fn projected_configmap_volume_deserializes_from_yaml() {
        let yaml = r#"
spec:
  volumes:
    - name: configmap-configs
      projected:
        sources:
          - configMap:
              name: tabsdata-configs-apiserver.yaml
              items:
                - key: apiserver.yaml
                  path: apiserver/apiserver.yaml
          - configMap:
              name: tabsdata-configs-apiserver-log
              items:
                - key: apiserver.log.yaml
                  path: apiserver/apiserver.log.yaml
  containers:
    - name: app
      image: /dev/null
"#;

        let pod: PodSpec = serde_yaml_ng::from_str(yaml).unwrap();
        let vol = &pod.spec.volumes[0];
        assert_eq!(vol.name, "configmap-configs");
        assert!(vol.config_map.is_none());
        let proj = vol.projected.as_ref().expect("projected should be set");
        assert_eq!(proj.sources.len(), 2);

        let s0 = proj.sources[0].config_map.as_ref().unwrap();
        assert_eq!(s0.name, "tabsdata-configs-apiserver.yaml");
        assert_eq!(s0.items.len(), 1);
        assert_eq!(s0.items[0].key, "apiserver.yaml");
        assert_eq!(s0.items[0].path, "apiserver/apiserver.yaml");

        let s1 = proj.sources[1].config_map.as_ref().unwrap();
        assert_eq!(s1.name, "tabsdata-configs-apiserver-log");
        assert_eq!(s1.items[0].path, "apiserver/apiserver.log.yaml");
    }
}
