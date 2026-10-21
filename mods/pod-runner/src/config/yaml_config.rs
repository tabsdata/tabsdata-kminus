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
use crate::spec::constants::{
    DEFAULT_POD_DATA_REAP_TTL_ON_POD_FAILURE, DEFAULT_POD_DATA_REAP_TTL_ON_POD_SUCCESS,
    DEFAULT_POD_DIR_DELETE_TTL_POD_FAILURE_ON_POD_DELETE,
    DEFAULT_POD_DIR_DELETE_TTL_POD_SUCCESS_ON_POD_DELETE, DEFAULT_POD_LOG_ID_LABEL,
};

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

    // Inherent rather than std::str::FromStr on purpose: the parse is
    // YAML-specific and fails with a message, and making it the trait
    // would change this type's public API and every caller's imports.
    #[allow(clippy::should_implement_trait)]
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

/// Settings that no longer exist, paired with what to use instead.
///
/// `prsysconfig.yaml` is persistent and never regenerated, so an instance
/// created by an older build keeps writing these. Unknown keys deserialize
/// silently, which would mean a retention setting an operator deliberately
/// chose stops applying with no signal — hence the hard failure at startup.
const REMOVED_SYSCONFIG_KEYS: &[(&str, &str)] = &[
    (
        "keepDeletedPodDir",
        "use podDirDeleteTtlPodSuccessOnPodDelete / podDirDeleteTtlPodFailureOnPodDelete",
    ),
    (
        "keepFailedPodDir",
        "use podDirDeleteTtlPodFailureOnPodDelete (\"never\" keeps a failed pod dir indefinitely)",
    ),
    (
        "keepSucceededPodDataDirs",
        "use podDataReapTtlOnPodSuccess (\"never\" keeps the data dirs indefinitely)",
    ),
    (
        "deletedPodDirTtlHours",
        "use podDirDeleteTtlPodSuccessOnPodDelete / podDirDeleteTtlPodFailureOnPodDelete",
    ),
    (
        "podArchiveTtlDays",
        "pod archives no longer exist; logs and specs live in the pod dir until \
         podDirDeleteTtlPodSuccessOnPodDelete / podDirDeleteTtlPodFailureOnPodDelete",
    ),
];

/// Compare config keys ignoring case and `_`, so `keepFailedPodDir` and
/// `keep_failed_pod_dir` are recognized as the same setting.
fn same_key(a: &str, b: &str) -> bool {
    let normalize = |s: &str| s.replace('_', "").to_lowercase();
    normalize(a) == normalize(b)
}

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
    /// TTL before a succeeded pod's data dir (`pod-run/<pod_id>/`: `work/`,
    /// `volumes/`) is reaped, counted from pod completion.
    /// `<number>[s|m|h]` or `never`.
    #[serde(alias = "podDataReapTtlOnPodSuccess")]
    pub pod_data_reap_ttl_on_pod_success: String,

    /// TTL before a failed pod's data dir is reaped, counted from pod
    /// completion. `<number>[s|m|h]` or `never`.
    #[serde(alias = "podDataReapTtlOnPodFailure")]
    pub pod_data_reap_ttl_on_pod_failure: String,

    /// TTL before a succeeded pod incarnation's state dir
    /// (`pod-state/<bucket>/<pod_name>/<stamp>/`: `log/`, `store/`) is
    /// deleted, counted from the pod's removal from the pod runner. `<number>[s|m|h]` or `never`.
    #[serde(alias = "podDirDeleteTtlPodSuccessOnPodDelete")]
    pub pod_dir_delete_ttl_pod_success_on_pod_delete: String,

    /// TTL before a failed pod's state dir is deleted, counted from the pod's
    /// removal from the pod runner. `<number>[s|m|h]` or `never`.
    #[serde(alias = "podDirDeleteTtlPodFailureOnPodDelete")]
    pub pod_dir_delete_ttl_pod_failure_on_pod_delete: String,

    /// Pod label whose value, when it is a valid Kubernetes name, files the
    /// pod's logs under that log pod id instead of the pod name. Empty
    /// disables it: every pod is filed under its name.
    #[serde(alias = "podLogIdLabel")]
    pub pod_log_id_label: String,

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

    /// Keys in the file that this build doesn't recognize. Collected so
    /// startup can warn about settings that silently stopped applying —
    /// see [`REMOVED_SYSCONFIG_KEYS`].
    #[serde(flatten)]
    pub unrecognized: HashMap<String, serde_yaml_ng::Value>,
}

impl Default for YamlSysConfig {
    fn default() -> Self {
        Self {
            nodename: DEFAULT_NODENAME.to_string(),
            namespace: DEFAULT_NAMESPACE.to_string(),
            capacity: YamlCapacityConfig::default(),
            max_pods: 250,
            default_grace_period_secs: 120,
            max_restart_count: 5,
            restart_backoff_base_secs: 10,
            clean_up_force_kill_wait_secs: 10,
            propagate_env: true,
            die_with_parent: true,
            write_secrets_to_container_shell_script: false,
            images_dir: String::new(),
            pod_data_reap_ttl_on_pod_success: DEFAULT_POD_DATA_REAP_TTL_ON_POD_SUCCESS.to_string(),
            pod_data_reap_ttl_on_pod_failure: DEFAULT_POD_DATA_REAP_TTL_ON_POD_FAILURE.to_string(),
            pod_dir_delete_ttl_pod_success_on_pod_delete:
                DEFAULT_POD_DIR_DELETE_TTL_POD_SUCCESS_ON_POD_DELETE.to_string(),
            pod_dir_delete_ttl_pod_failure_on_pod_delete:
                DEFAULT_POD_DIR_DELETE_TTL_POD_FAILURE_ON_POD_DELETE.to_string(),
            pod_log_id_label: DEFAULT_POD_LOG_ID_LABEL.to_string(),
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
            unrecognized: HashMap::new(),
        }
    }
}

impl YamlSysConfig {
    pub fn from_file(path: &Path) -> Result<Self, String> {
        let content = std::fs::read_to_string(path)
            .map_err(|e| format!("Failed to read {}: {}", path.display(), e))?;
        Self::from_str(&content)
    }

    // Inherent rather than std::str::FromStr on purpose: the parse is
    // YAML-specific and fails with a message, and making it the trait
    // would change this type's public API and every caller's imports.
    #[allow(clippy::should_implement_trait)]
    pub fn from_str(yaml: &str) -> Result<Self, String> {
        serde_yaml_ng::from_str(yaml).map_err(|e| format!("Failed to parse prsysconfig YAML: {e}"))
    }

    /// The removed settings this file still carries, each with what to use
    /// instead. Empty for an up-to-date file.
    pub fn removed_keys(&self) -> Vec<(&str, &str)> {
        self.unrecognized
            .keys()
            .filter_map(|present| {
                REMOVED_SYSCONFIG_KEYS
                    .iter()
                    .find(|(removed, _)| same_key(present, removed))
                    .copied()
            })
            .collect()
    }

    /// Reject any setting this build doesn't accept, naming a replacement
    /// for the ones that were removed.
    ///
    /// A setting that silently stops applying is worse than a failed start:
    /// retention chosen deliberately would quietly become something else,
    /// and a typo in a key name would look like it took effect. Config
    /// resolution runs before logging exists, so a warning here would go
    /// nowhere anyway — this has to be an error to be seen at all.
    pub fn validate_known_keys(&self) -> Result<(), String> {
        if self.unrecognized.is_empty() {
            return Ok(());
        }

        // HashMap iteration order is arbitrary; sort so the message reads
        // the same way on every run.
        let mut offenders: Vec<(&String, &str)> = self
            .unrecognized
            .keys()
            .map(|key| {
                let advice = REMOVED_SYSCONFIG_KEYS
                    .iter()
                    .find(|(removed, _)| same_key(key, removed))
                    .map(|(_, advice)| *advice)
                    .unwrap_or("not a known setting");
                (key, advice)
            })
            .collect();
        offenders.sort();

        let detail = offenders
            .iter()
            .map(|(key, advice)| format!("\n  {key}: {advice}"))
            .collect::<String>();
        Err(format!(
            "unsupported settings — remove or replace them:{detail}",
        ))
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
        let c = YamlPRConfig {
            url: "http://localhost:3000".into(),
            ..Default::default()
        };
        assert!(c.validate().is_err());
    }

    #[test]
    fn prconfig_validate_ok_with_all_certs() {
        let mut c = YamlPRConfig {
            url: "https://localhost:3000".into(),
            ..Default::default()
        };
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
        assert_eq!(
            c.pod_data_reap_ttl_on_pod_success,
            DEFAULT_POD_DATA_REAP_TTL_ON_POD_SUCCESS
        );
        assert_eq!(
            c.pod_dir_delete_ttl_pod_failure_on_pod_delete,
            DEFAULT_POD_DIR_DELETE_TTL_POD_FAILURE_ON_POD_DELETE
        );
    }

    #[test]
    fn syscfg_parse_full() {
        let yaml = r#"
nodename: "my-node"
namespace: "prod"
maxPods: 50
propagateEnv: false
podDataReapTtlOnPodFailure: "30m"
podDirDeleteTtlPodFailureOnPodDelete: "48h"
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
        assert_eq!(c.pod_data_reap_ttl_on_pod_failure, "30m");
        assert_eq!(c.pod_dir_delete_ttl_pod_failure_on_pod_delete, "48h");
        assert_eq!(c.capacity.cpu, 200);
        assert_eq!(c.secret_resolver.resolver_type, "kube");
    }

    #[test]
    fn syscfg_parse_never_ttl() {
        let yaml = "podDirDeleteTtlPodFailureOnPodDelete: \"never\"\n";
        let c = YamlSysConfig::from_str(yaml).unwrap();
        assert_eq!(c.pod_dir_delete_ttl_pod_failure_on_pod_delete, "never");
    }

    #[test]
    fn syscfg_pod_log_id_label_defaults_and_parses() {
        assert_eq!(
            YamlSysConfig::default().pod_log_id_label,
            DEFAULT_POD_LOG_ID_LABEL
        );
        let c = YamlSysConfig::from_str("podLogIdLabel: \"tabsdata.com/log-id\"\n").unwrap();
        assert_eq!(c.pod_log_id_label, "tabsdata.com/log-id");
        let c = YamlSysConfig::from_str("podLogIdLabel: \"\"\n").unwrap();
        assert_eq!(c.pod_log_id_label, "", "empty disables the label");
    }

    // -- removed settings --

    /// The cleanup block a pre-reaper build wrote into every
    /// `prsysconfig.yaml`. The file is persistent, so upgraded instances
    /// still have it.
    const LEGACY_CLEANUP_BLOCK: &str = r#"
nodename: "old-node"
keepDeletedPodDir: false
keepFailedPodDir: true
keepSucceededPodDataDirs: false
deletedPodDirTtlHours: 24
podArchiveTtlDays: 7
"#;

    #[test]
    fn syscfg_parses_legacy_cleanup_keys_then_rejects_them() {
        // Deserialization succeeds — the keys land in `unrecognized` — and
        // validation is what refuses them, so the error can name each one.
        let c = YamlSysConfig::from_str(LEGACY_CLEANUP_BLOCK).unwrap();
        assert_eq!(c.nodename, "old-node");
        assert_eq!(c.unrecognized.len(), 5);

        let err = c.validate_known_keys().unwrap_err();
        for key in [
            "keepDeletedPodDir",
            "keepFailedPodDir",
            "keepSucceededPodDataDirs",
            "deletedPodDirTtlHours",
            "podArchiveTtlDays",
        ] {
            assert!(err.contains(key), "{key} not named in error: {err}");
        }
    }

    #[test]
    fn syscfg_rejection_names_the_replacement() {
        let err = YamlSysConfig::from_str("keepFailedPodDir: true\n")
            .unwrap()
            .validate_known_keys()
            .unwrap_err();
        assert!(err.contains("keepFailedPodDir"), "{err}");
        assert!(
            err.contains("podDirDeleteTtlPodFailureOnPodDelete"),
            "{err}"
        );
        assert!(err.contains("never"), "{err}");
    }

    #[test]
    fn syscfg_rejection_matches_snake_case_spelling() {
        // Both spellings were accepted for every other setting, so both
        // have to be recognized on the way out.
        let err = YamlSysConfig::from_str("keep_failed_pod_dir: true\n")
            .unwrap()
            .validate_known_keys()
            .unwrap_err();
        assert!(
            err.contains("podDirDeleteTtlPodFailureOnPodDelete"),
            "{err}"
        );
    }

    #[test]
    fn syscfg_rejects_an_unknown_key() {
        // A typo in a key name must not look like it took effect.
        let err = YamlSysConfig::from_str("podDataReapTtlOnPodSucess: \"5m\"\n")
            .unwrap()
            .validate_known_keys()
            .unwrap_err();
        assert!(err.contains("podDataReapTtlOnPodSucess"), "{err}");
        assert!(err.contains("not a known setting"), "{err}");
    }

    #[test]
    fn syscfg_rejection_lists_keys_in_a_stable_order() {
        let err = YamlSysConfig::from_str(LEGACY_CLEANUP_BLOCK)
            .unwrap()
            .validate_known_keys()
            .unwrap_err();
        let listed: Vec<&str> = err
            .lines()
            .skip(1)
            .filter_map(|l| l.trim().split(':').next())
            .collect();
        let mut sorted = listed.clone();
        sorted.sort();
        assert_eq!(listed, sorted, "{err}");
    }

    #[test]
    fn syscfg_clean_config_validates() {
        let c = YamlSysConfig::from_str(
            "nodename: \"n\"\npodDataReapTtlOnPodFailure: \"5m\"\ncapacity:\n  cpu: 4\n",
        )
        .unwrap();
        assert!(c.validate_known_keys().is_ok());
        assert!(
            c.unrecognized.is_empty(),
            "recognized settings must not land in `unrecognized`: {:?}",
            c.unrecognized.keys().collect::<Vec<_>>(),
        );
    }

    #[test]
    fn syscfg_template_validates() {
        // The generated template must not trip its own validation.
        let c = YamlSysConfig::from_str(crate::config::template::PRSYSCONFIG_TEMPLATE).unwrap();
        assert!(
            c.validate_known_keys().is_ok(),
            "template rejected: {:?}",
            c.validate_known_keys(),
        );
    }

    #[test]
    fn syscfg_defaults_validate() {
        assert!(YamlSysConfig::default().validate_known_keys().is_ok());
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
