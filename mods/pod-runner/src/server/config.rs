// Copyright 2026 Tabsdata Inc.

//! Pod runner configuration.
//!
//! [`PRConfig`] is the single top-level configuration for the pod runner
//! server. It provides `From` conversions to the specialized configs
//! used by the logic layer, executor provider, and file store.

use std::path::PathBuf;
use std::time::Duration;

use crate::logic::{CapacityConfig, CleanupConfig, DefaultExecutorProvider, LogicConfig};
use crate::pod::resolver::ResolverConfig;
use crate::store::file::FileStoreConfig;

// Re-export TLS types so existing `use crate::server::config::*` still works.
pub use crate::tls::{CertSource, ServerTlsConfig};

// ---------------------------------------------------------------------------
// Constants — on-disk layout
// ---------------------------------------------------------------------------
//
// Layout under workdir:
//
//   <workdir>/run/               ← current run (rotated on startup)
//     pods/<pod_id>/             ← per-pod, co-located
//       work/<container>/        ← container working directories
//       log/<container>/         ← container stdout/stderr + probe logs
//       store/                   ← original-spec.json, running-spec.json, status/
//       FINISHED                 ← terminal sentinel
//       archive.tgz             ← per-pod archive (created on cleanup)
//     pids/                      ← process PID tracking files
//   <workdir>/run.0/             ← previous run (auto-rotated)
//   <workdir>/run.1/             ← two runs ago

/// Top-level run directory (under workdir). Rotated on startup.
pub const RUN_DIR: &str = "run";
/// Subdirectory of run/ for per-pod data.
pub const PODS_SUBDIR: &str = "pods";
/// Subdirectory of run/ for process PID tracking files.
pub const PIDS_SUBDIR: &str = "pids";
/// Number of previous run directories to keep (run.0 .. run.{N-1}).
pub const PREV_RUN_DIRS_TO_KEEP: usize = 2;

/// Per-pod subdirectory for container working directories.
pub const POD_WORK_SUBDIR: &str = "work";
/// Per-pod subdirectory for container logs.
pub const POD_LOG_SUBDIR: &str = "log";
/// Per-pod subdirectory for store files (specs + statuses).
pub const POD_STORE_SUBDIR: &str = "store";

/// Default server address.
pub const DEFAULT_ADDRESS: &str = "127.0.0.1";
/// Default server port.
pub const DEFAULT_PORT: u16 = 10300;
/// Default server URL.
pub const DEFAULT_URL: &str = "https://127.0.0.1:10300";

/// Environment variable for kubeconfig path.
pub const KUBECONFIG_ENV: &str = "KUBECONFIG";

/// Default virtual node name.
pub const DEFAULT_NODENAME: &str = "tdknode";
/// Default namespace.
pub const DEFAULT_NAMESPACE: &str = "default";

// ---------------------------------------------------------------------------
// URL helpers
// ---------------------------------------------------------------------------

/// Extract the port from an `http://host:port` URL. Returns 0 if empty or unparseable.
fn parse_port_from_url(url: &str) -> u16 {
    if url.is_empty() {
        return 0;
    }
    // Strip scheme
    let after_scheme = url
        .strip_prefix("http://")
        .or_else(|| url.strip_prefix("https://"))
        .unwrap_or(url);
    // Find port after last ':'
    if let Some(colon_pos) = after_scheme.rfind(':') {
        let port_str = &after_scheme[colon_pos + 1..];
        // Strip any path suffix
        let port_str = port_str.split('/').next().unwrap_or(port_str);
        port_str.parse().unwrap_or(0)
    } else {
        0
    }
}

// ---------------------------------------------------------------------------
// PRConfig
// ---------------------------------------------------------------------------

/// Top-level pod-runner configuration.
///
/// All fields have sensible defaults. Path defaults are relative to the
/// current directory (`./run/...`).
pub struct PRConfig {
    // -- server --
    /// Address to bind the HTTP server to.
    pub bind_address: String,

    /// Node address reported as InternalIP to the K8s API server.
    pub node_address: String,

    /// Path to kubeconfig file for K8s API access (secrets, configmaps).
    /// Used by kube-type resolvers. Empty string means not configured.
    pub kubeconfig: String,

    // -- node identity --
    /// Virtual node name reported to K8s.
    pub nodename: String,

    /// Default namespace.
    pub namespace: String,

    // -- TLS --
    /// Server TLS configuration (always mTLS).
    pub tls: ServerTlsConfig,

    // -- directories --
    /// Run directory: `<workdir>/run/`. Rotated on startup.
    pub run_dir: PathBuf,

    /// Pods directory: `<workdir>/run/pods/`. Per-pod data co-located here.
    pub pods_dir: PathBuf,

    /// Pod-runner's own log directory (for `pod-runner.log`).
    /// NOT container logs — those live under `pods/<pod_id>/log/`.
    pub log_dir: PathBuf,

    /// Directory for process PID tracking files: `<workdir>/run/pids/`.
    pub pid_dir: PathBuf,

    // -- pod limits --
    /// Default grace period (seconds) for pod deletion.
    pub default_grace_period_secs: u64,

    /// Default maximum container restarts before giving up.
    /// Applied to all containers unless overridden in the pod spec.
    pub container_max_restarts: u32,

    /// Default base duration (seconds) for exponential container restart backoff.
    /// Applied to all containers unless overridden in the pod spec.
    pub container_restart_backoff_base_secs: u64,

    // -- node capacity --
    /// CPU capacity reported to K8s.
    pub capacity_cpu: u32,

    /// Memory capacity (GB) reported to K8s.
    pub capacity_memory_gb: u32,

    /// Pod capacity reported to K8s.
    pub capacity_pods: u32,

    // -- executor --
    /// Whether to propagate the parent process environment to containers.
    pub propagate_env: bool,

    /// Whether child processes die when the parent exits.
    pub die_with_parent: bool,

    // -- store --
    /// Whether to persist the running (prepared) spec to disk.
    /// Set to `false` if it may contain resolved secrets.
    pub write_running_spec: bool,

    // -- images --
    /// Root directory for resolving relative container `image` paths.
    /// Absolute `image` paths are used as-is. Empty means the container
    /// `image` field is used verbatim.
    pub images_dir: PathBuf,

    // -- resolvers --
    /// Secret resolver configuration.
    pub secret_resolver: ResolverConfig,

    /// ConfigMap resolver configuration.
    pub config_map_resolver: ResolverConfig,

    /// Node-level environment resolver configuration.
    pub node_env_resolver: ResolverConfig,

    // -- cleanup --
    /// Keep work dir when a pod is deleted (default: false).
    pub keep_deleted_pod_dir: bool,

    /// Keep work dir if the pod failed (default: true).
    pub keep_failed_pod_dir: bool,

    /// TTL in hours for kept work dirs before purge (default: 24).
    pub deleted_pod_dir_ttl_hours: u64,

    /// TTL in days for archive tgz files before purge (default: 7).
    pub pod_archive_ttl_days: u64,

    /// Cadence between disk-usage scans for the per-pod and pod-runner
    /// disk-usage trackers (seconds, default: 60).
    pub dir_size_scan_interval_secs: u64,

    // -- metrics --
    /// Metrics endpoint URL (HTTP only, e.g. `http://127.0.0.1:3001`).
    /// Empty string disables the metrics server.
    pub metrics_url: String,
}

impl Default for PRConfig {
    fn default() -> Self {
        let run = PathBuf::from(RUN_DIR);
        Self {
            bind_address: format!("{DEFAULT_ADDRESS}:{DEFAULT_PORT}"),
            node_address: DEFAULT_ADDRESS.to_string(),
            kubeconfig: String::new(),

            nodename: DEFAULT_NODENAME.to_string(),
            namespace: DEFAULT_NAMESPACE.to_string(),

            tls: ServerTlsConfig::default(),

            run_dir: run.clone(),
            pods_dir: run.join(PODS_SUBDIR),
            log_dir: PathBuf::from("log"),
            pid_dir: run.join(PIDS_SUBDIR),

            default_grace_period_secs: 30,
            container_max_restarts: 0,
            container_restart_backoff_base_secs: 2,

            capacity_cpu: 100,
            capacity_memory_gb: 100,
            capacity_pods: 100,

            propagate_env: true,
            die_with_parent: true,

            write_running_spec: false,

            images_dir: PathBuf::new(),

            secret_resolver: ResolverConfig::default(),
            config_map_resolver: ResolverConfig::default(),
            node_env_resolver: ResolverConfig::default(),

            keep_deleted_pod_dir: false,
            keep_failed_pod_dir: true,
            deleted_pod_dir_ttl_hours: 24,
            pod_archive_ttl_days: 7,
            dir_size_scan_interval_secs: 60,

            metrics_url: crate::metrics::DEFAULT_METRICS_URL.to_string(),
        }
    }
}

impl PRConfig {
    /// Whether TLS is enabled (server certificate and key are configured).
    pub fn tls_enabled(&self) -> bool {
        self.tls.has_cert() && self.tls.has_key()
    }

    /// Extract the port from `metrics_url`. Returns 0 if not configured or unparseable.
    pub fn metrics_port(&self) -> u16 {
        parse_port_from_url(&self.metrics_url)
    }

    /// Whether TLS is fully configured (cert + key + client CA).
    /// Always true when auto-generated.
    pub fn tls_configured(&self) -> bool {
        self.tls.is_configured()
    }
}

// ---------------------------------------------------------------------------
// Conversions
// ---------------------------------------------------------------------------

impl From<&PRConfig> for LogicConfig {
    fn from(c: &PRConfig) -> Self {
        Self {
            max_pods: c.capacity_pods as usize,
            default_grace_period: Duration::from_secs(c.default_grace_period_secs),
            capacity: CapacityConfig {
                cpu: c.capacity_cpu,
                memory_gb: c.capacity_memory_gb,
                pods: c.capacity_pods,
            },
            address: c.node_address.clone(),
            images_dir: c.images_dir.clone(),
        }
    }
}

impl From<&PRConfig> for DefaultExecutorProvider {
    fn from(c: &PRConfig) -> Self {
        Self {
            pods_dir: c.pods_dir.clone(),
            pid_dir: c.pid_dir.clone(),
            propagate_env: c.propagate_env,
            die_with_parent: c.die_with_parent,
            max_restarts: c.container_max_restarts,
            restart_backoff_base: Duration::from_secs(c.container_restart_backoff_base_secs),
            shutdown_grace_period: Duration::from_secs(c.default_grace_period_secs),
        }
    }
}

impl From<&PRConfig> for CleanupConfig {
    fn from(c: &PRConfig) -> Self {
        Self {
            keep_deleted_pod_dir: c.keep_deleted_pod_dir,
            keep_failed_pod_dir: c.keep_failed_pod_dir,
            deleted_pod_dir_ttl_hours: c.deleted_pod_dir_ttl_hours,
            pod_archive_ttl_days: c.pod_archive_ttl_days,
            pods_dir: c.pods_dir.clone(),
        }
    }
}

impl From<&PRConfig> for FileStoreConfig {
    fn from(c: &PRConfig) -> Self {
        Self {
            root_dir: c.pods_dir.clone(),
            write_running_spec: c.write_running_spec,
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn prconfig_default_dirs_use_constants() {
        let c = PRConfig::default();
        let run = PathBuf::from(RUN_DIR);
        assert_eq!(c.run_dir, run);
        assert_eq!(c.pods_dir, run.join(PODS_SUBDIR));
        assert_eq!(c.pid_dir, run.join(PIDS_SUBDIR));
        assert_eq!(c.log_dir, PathBuf::from("log"));
    }

    #[test]
    fn prconfig_default_identity_uses_constants() {
        let c = PRConfig::default();
        assert_eq!(c.bind_address, format!("{DEFAULT_ADDRESS}:{DEFAULT_PORT}"));
        assert_eq!(c.node_address, DEFAULT_ADDRESS);
        assert_eq!(c.nodename, DEFAULT_NODENAME);
        assert_eq!(c.namespace, DEFAULT_NAMESPACE);
        assert_eq!(c.metrics_url, crate::metrics::DEFAULT_METRICS_URL);
        assert_eq!(c.metrics_port(), crate::metrics::DEFAULT_METRICS_PORT);
    }

    // -- TLS (PRConfig integration) --

    #[test]
    fn tls_not_configured_by_default() {
        let c = PRConfig::default();
        assert!(!c.tls_enabled());
        assert!(!c.tls_configured());
    }

    #[test]
    fn tls_configured_with_all_fields() {
        let mut c = PRConfig::default();
        c.tls.certificate.path = Some("server.crt".into());
        c.tls.certificate_key.path = Some("server.key".into());
        c.tls.client_ca.path = Some("client-ca.crt".into());
        assert!(c.tls_enabled());
        assert!(c.tls_configured());
    }

    #[test]
    fn tls_not_enabled_with_cert_only() {
        let mut c = PRConfig::default();
        c.tls.certificate.path = Some("server.crt".into());
        assert!(!c.tls_enabled());
    }

    #[test]
    fn tls_validate_rejects_path_and_data() {
        let mut c = PRConfig::default();
        c.tls.certificate.path = Some("server.crt".into());
        c.tls.certificate.data = Some("base64".into());
        assert!(c.tls.validate().is_err());
    }

    // -- cleanup --

    #[test]
    fn prconfig_default_cleanup_fields() {
        let c = PRConfig::default();
        assert!(!c.keep_deleted_pod_dir);
        assert!(c.keep_failed_pod_dir);
        assert_eq!(c.deleted_pod_dir_ttl_hours, 24);
        assert_eq!(c.pod_archive_ttl_days, 7);
    }

    #[test]
    fn cleanup_config_from_prconfig() {
        let mut pr = PRConfig::default();
        pr.keep_deleted_pod_dir = true;
        pr.keep_failed_pod_dir = false;
        pr.deleted_pod_dir_ttl_hours = 48;
        pr.pod_archive_ttl_days = 14;
        pr.pods_dir = PathBuf::from("/custom/pods");

        let cc = CleanupConfig::from(&pr);
        assert!(cc.keep_deleted_pod_dir);
        assert!(!cc.keep_failed_pod_dir);
        assert_eq!(cc.deleted_pod_dir_ttl_hours, 48);
        assert_eq!(cc.pod_archive_ttl_days, 14);
        assert_eq!(cc.pods_dir, PathBuf::from("/custom/pods"));
    }
}
