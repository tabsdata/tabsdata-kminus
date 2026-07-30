// Copyright 2026 Tabsdata Inc.

//! YAML configuration deserialization.
//!
//! The pod-runner uses two config files:
//!
//! - **`prconfig.yaml`** — server endpoint and TLS certificates.
//!   Regenerated on every start by the orchestrator (km).
//!
//! - **`prsysconfig.yaml`** — system settings (node identity, pod limits,
//!   executor, cleanup, capacity, resolvers). Persistent and user-editable.
//!   Auto-created with defaults on first start if missing.

use std::collections::HashMap;
use std::path::Path;

use serde::Deserialize;

use crate::server::config::*;

// ---------------------------------------------------------------------------
// prconfig.yaml — endpoint and TLS (regenerated on every start)
// ---------------------------------------------------------------------------

/// Deserialized `prconfig.yaml`.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct YamlPRConfig {
    /// Server endpoint URL. Must be https.
    pub url: String,

    /// Server TLS settings (cert, key, client CA for mTLS).
    pub server: YamlServerConfig,
}

impl Default for YamlPRConfig {
    fn default() -> Self {
        Self {
            url: DEFAULT_URL.to_string(),
            server: YamlServerConfig::default(),
        }
    }
}

impl YamlPRConfig {
    pub fn from_file(path: &Path) -> Result<Self, String> {
        let content = std::fs::read_to_string(path)
            .map_err(|e| format!("Failed to read {}: {}", path.display(), e))?;
        Self::from_str(&content)
    }

    pub fn from_str(yaml: &str) -> Result<Self, String> {
        serde_yaml_ng::from_str(yaml).map_err(|e| format!("Failed to parse prconfig YAML: {e}"))
    }

    /// Validate TLS fields and URL scheme.
    pub fn validate(&self) -> Result<(), String> {
        let tls = &self.server.tls;
        if !tls.certificate.is_empty() && !tls.certificate_data.is_empty() {
            return Err("server.tls: cannot set both certificate and certificate-data".into());
        }
        if !tls.certificate_key.is_empty() && !tls.certificate_key_data.is_empty() {
            return Err(
                "server.tls: cannot set both certificate-key and certificate-key-data".into(),
            );
        }
        if !tls.certificate_authority.is_empty() && !tls.certificate_authority_data.is_empty() {
            return Err(
                "server.tls: cannot set both certificate-authority and certificate-authority-data"
                    .into(),
            );
        }

        if !self.url.is_empty() && !self.url.starts_with("https://") {
            return Err("URL must use https scheme".into());
        }

        if self.url.starts_with("https://") {
            let has_cert = !tls.certificate.is_empty() || !tls.certificate_data.is_empty();
            let has_key = !tls.certificate_key.is_empty() || !tls.certificate_key_data.is_empty();
            let has_ca =
                !tls.certificate_authority.is_empty() || !tls.certificate_authority_data.is_empty();
            if !has_cert || !has_key {
                return Err("HTTPS requires server certificate and key".into());
            }
            if !has_ca {
                return Err("mTLS requires a client CA certificate".into());
            }
        }

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// prsysconfig.yaml — system settings (persistent, user-editable)
// ---------------------------------------------------------------------------

/// Deserialized `prsysconfig.yaml`.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct YamlSysConfig {
    /// Virtual node name reported to K8s.
    pub nodename: String,

    /// Default namespace.
    pub namespace: String,

    /// Node capacity.
    pub capacity: YamlCapacityConfig,

    // -- pod limits --
    #[serde(alias = "maxPods")]
    pub max_pods: usize,

    #[serde(alias = "defaultGracePeriodSecs")]
    pub default_grace_period_secs: u64,

    #[serde(alias = "maxRestartCount")]
    pub max_restart_count: u32,

    #[serde(alias = "restartBackoffBaseSecs")]
    pub restart_backoff_base_secs: u64,

    #[serde(alias = "cleanUpForceKillWaitSecs")]
    pub clean_up_force_kill_wait_secs: u64,

    // -- executor --
    #[serde(alias = "propagateEnv")]
    pub propagate_env: bool,

    #[serde(alias = "dieWithParent")]
    pub die_with_parent: bool,

    // -- security --
    #[serde(alias = "writeSecretsToContainerShellScript")]
    pub write_secrets_to_container_shell_script: bool,

    // -- images --
    /// Root directory used to resolve relative `image` paths in pod specs.
    /// Absolute `image` paths are used as-is. Empty (default) means the
    /// container `image` field is used verbatim.
    #[serde(alias = "imagesDir")]
    pub images_dir: String,

    // -- cleanup --
    #[serde(alias = "keepDeletedPodDir")]
    pub keep_deleted_pod_dir: bool,

    #[serde(alias = "keepFailedPodDir")]
    pub keep_failed_pod_dir: bool,

    #[serde(alias = "deletedPodDirTtlHours")]
    pub deleted_pod_dir_ttl_hours: u64,

    #[serde(alias = "podArchiveTtlDays")]
    pub pod_archive_ttl_days: u64,

    /// Cadence between disk-usage scans for both per-pod and pod-runner
    /// trackers (seconds).
    #[serde(alias = "dirSizeScanIntervalSecs")]
    pub dir_size_scan_interval_secs: u64,

    // -- metrics --
    #[serde(alias = "metricsUrl")]
    pub metrics_url: String,

    // -- resolvers --
    #[serde(alias = "secretResolver")]
    pub secret_resolver: YamlResolverConfig,

    #[serde(alias = "configMapResolver")]
    pub config_map_resolver: YamlResolverConfig,

    #[serde(alias = "nodeEnvsResolver")]
    pub node_envs_resolver: YamlResolverConfig,
}

impl Default for YamlSysConfig {
    fn default() -> Self {
        Self {
            nodename: DEFAULT_NODENAME.to_string(),
            namespace: DEFAULT_NAMESPACE.to_string(),
            capacity: YamlCapacityConfig::default(),
            max_pods: 250,
            default_grace_period_secs: 30,
            max_restart_count: 5,
            restart_backoff_base_secs: 10,
            clean_up_force_kill_wait_secs: 10,
            propagate_env: true,
            die_with_parent: true,
            write_secrets_to_container_shell_script: false,
            images_dir: String::new(),
            keep_deleted_pod_dir: false,
            keep_failed_pod_dir: true,
            deleted_pod_dir_ttl_hours: 24,
            pod_archive_ttl_days: 7,
            dir_size_scan_interval_secs: 60,
            metrics_url: crate::metrics::DEFAULT_METRICS_URL.to_string(),
            secret_resolver: YamlResolverConfig {
                resolver_type: "kube".into(),
                ..Default::default()
            },
            config_map_resolver: YamlResolverConfig {
                resolver_type: "kube".into(),
                ..Default::default()
            },
            node_envs_resolver: YamlResolverConfig {
                resolver_type: "none".into(),
                ..Default::default()
            },
        }
    }
}

impl YamlSysConfig {
    pub fn from_file(path: &Path) -> Result<Self, String> {
        let content = std::fs::read_to_string(path)
            .map_err(|e| format!("Failed to read {}: {}", path.display(), e))?;
        Self::from_str(&content)
    }

    pub fn from_str(yaml: &str) -> Result<Self, String> {
        serde_yaml_ng::from_str(yaml).map_err(|e| format!("Failed to parse prsysconfig YAML: {e}"))
    }
}

// ---------------------------------------------------------------------------
// Server config (nested in prconfig.yaml)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default)]
pub struct YamlServerConfig {
    pub tls: YamlTlsConfig,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default)]
pub struct YamlTlsConfig {
    pub certificate: String,
    #[serde(alias = "certificate-data")]
    pub certificate_data: String,
    #[serde(alias = "certificate-key")]
    pub certificate_key: String,
    #[serde(alias = "certificate-key-data")]
    pub certificate_key_data: String,
    #[serde(alias = "certificate-authority")]
    pub certificate_authority: String,
    #[serde(alias = "certificate-authority-data")]
    pub certificate_authority_data: String,
}

// ---------------------------------------------------------------------------
// Capacity config (nested in prsysconfig.yaml)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct YamlCapacityConfig {
    pub cpu: u32,
    #[serde(alias = "memoryGB")]
    pub memory_gb: u32,
    pub pods: u32,
}

impl Default for YamlCapacityConfig {
    fn default() -> Self {
        Self {
            cpu: 100,
            memory_gb: 100,
            pods: 100,
        }
    }
}

// ---------------------------------------------------------------------------
// Resolver config
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default)]
pub struct YamlResolverConfig {
    #[serde(alias = "type", rename = "type")]
    pub resolver_type: String,
    pub configs: HashMap<String, String>,
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // -- prconfig parsing --

    #[test]
    fn prconfig_defaults() {
        let c = YamlPRConfig::default();
        assert_eq!(c.url, DEFAULT_URL);
    }

    #[test]
    fn prconfig_parse_inline_certs() {
        let yaml = r#"
url: "https://127.0.0.1:10300"
server:
  tls:
    certificate-data: "base64cert"
    certificate-key-data: "base64key"
    certificate-authority-data: "base64ca"
"#;
        let c = YamlPRConfig::from_str(yaml).unwrap();
        assert_eq!(c.server.tls.certificate_data, "base64cert");
        assert_eq!(c.server.tls.certificate_key_data, "base64key");
        assert_eq!(c.server.tls.certificate_authority_data, "base64ca");
    }

    #[test]
    fn prconfig_validate_rejects_http() {
        let mut c = YamlPRConfig::default();
        c.url = "http://localhost:3000".into();
        assert!(c.validate().is_err());
    }

    #[test]
    fn prconfig_validate_ok_with_all_certs() {
        let mut c = YamlPRConfig::default();
        c.url = "https://localhost:3000".into();
        c.server.tls.certificate_data = "cert".into();
        c.server.tls.certificate_key_data = "key".into();
        c.server.tls.certificate_authority_data = "ca".into();
        assert!(c.validate().is_ok());
    }

    #[test]
    fn prconfig_validate_rejects_cert_path_and_data() {
        let mut c = YamlPRConfig::default();
        c.server.tls.certificate = "/path".into();
        c.server.tls.certificate_data = "data".into();
        assert!(c.validate().is_err());
    }

    #[test]
    fn prconfig_validate_rejects_ca_path_and_data() {
        let mut c = YamlPRConfig::default();
        c.server.tls.certificate_authority = "/path".into();
        c.server.tls.certificate_authority_data = "data".into();
        assert!(c.validate().is_err());
    }

    // -- prsysconfig parsing --

    #[test]
    fn syscfg_defaults() {
        let c = YamlSysConfig::default();
        assert_eq!(c.nodename, DEFAULT_NODENAME);
        assert_eq!(c.namespace, DEFAULT_NAMESPACE);
        assert_eq!(c.max_pods, 250);
        assert!(c.propagate_env);
        assert!(c.die_with_parent);
        assert!(c.keep_failed_pod_dir);
    }

    #[test]
    fn syscfg_parse_full() {
        let yaml = r#"
nodename: "my-node"
namespace: "prod"
maxPods: 50
propagateEnv: false
keepDeletedPodDir: true
deletedPodDirTtlHours: 48
capacity:
  cpu: 200
  memoryGB: 64
  pods: 500
secretResolver:
  type: "kube"
  configs: {}
"#;
        let c = YamlSysConfig::from_str(yaml).unwrap();
        assert_eq!(c.nodename, "my-node");
        assert_eq!(c.namespace, "prod");
        assert_eq!(c.max_pods, 50);
        assert!(!c.propagate_env);
        assert!(c.keep_deleted_pod_dir);
        assert_eq!(c.deleted_pod_dir_ttl_hours, 48);
        assert_eq!(c.capacity.cpu, 200);
        assert_eq!(c.secret_resolver.resolver_type, "kube");
    }

    #[test]
    fn syscfg_parse_metrics_url() {
        let yaml = "metricsUrl: \"http://10.0.0.1:9090\"\n";
        let c = YamlSysConfig::from_str(yaml).unwrap();
        assert_eq!(c.metrics_url, "http://10.0.0.1:9090");
    }

    #[test]
    fn syscfg_default_images_dir_is_empty() {
        let c = YamlSysConfig::default();
        assert_eq!(c.images_dir, "");
    }

    #[test]
    fn syscfg_parse_images_dir() {
        let yaml = "imagesDir: \"/var/lib/kminus/images\"\n";
        let c = YamlSysConfig::from_str(yaml).unwrap();
        assert_eq!(c.images_dir, "/var/lib/kminus/images");
    }
}
