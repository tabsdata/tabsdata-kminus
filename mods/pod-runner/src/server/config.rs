// Copyright 2026 Tabsdata Inc.

//! Pod runner configuration.
//!
//! [`PRConfig`] is the single top-level configuration for the pod runner
//! server. It provides `From` conversions to the specialized configs
//! used by the logic layer, executor provider, and file store.

use std::path::PathBuf;
use std::time::Duration;

use crate::logic::ttl::TtlConfig;
use crate::logic::{CapacityConfig, CleanupConfig, DefaultExecutorProvider, LogicConfig};
use crate::pod::resolver::ResolverConfig;
use crate::store::file::FileStoreConfig;
use crate::store::layout::PodLayout;

// Re-export TLS types so existing `use crate::server::config::*` still works.
pub use crate::tls::{CertSource, ServerTlsConfig};

// ---------------------------------------------------------------------------
// Constants — on-disk layout
// ---------------------------------------------------------------------------
//
// Layout under workdir:
//
//   <workdir>/data/
//     pod-run/<pod_id>/                ← pod data, reaped on the reap deadline
//       work/<container>/              ← container working directories
//       volumes/<volume>/              ← pod-level volumes
//     pod-state/<bucket>/<pod_name>/   ← shared by a pod name's incarnations
//       <stamp>/                       ← one incarnation's record, deleted on the delete deadline
//         log/<container>/             ← container stdout/stderr + probe logs
//         store/                       ← original-spec.json, running-spec.json, status/
//     pod-pid/<pid>                    ← process PID tracking files
//
// Every dir is fixed across restarts: pod dirs carry their own deadlines
// (see `crate::store::layout`), and a PID file left by a crashed run is
// exactly what the next run's startup scan needs to find.

/// Subdirectory of data/ for per-pod data dirs (`work/`, `volumes/`).
pub const POD_RUN_DIR: &str = "pod-run";
/// Subdirectory of data/ for per-pod record dirs (`log/`, `store/`).
pub const POD_STATE_DIR: &str = "pod-state";
/// Subdirectory of data/ for process PID tracking files.
pub const POD_PID_DIR: &str = "pod-pid";

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
    /// Directory holding `prconfig.yaml`, `prsysconfig.yaml` and the
    /// generated `vk-prconfig.yaml`.
    pub config_dir: PathBuf,

    /// The instance workdir. Kept whole rather than derived back out of
    /// `data_dir`, so the messages directory km reads can be addressed
    /// directly.
    pub work_dir: PathBuf,

    /// Data directory: `<workdir>/data/`. Holds the pod layout roots and
    /// the PID files.
    pub data_dir: PathBuf,

    /// Pod layout: `<workdir>/data/pod-run/` for pod data,
    /// `<workdir>/data/pod-state/` for pod records.
    pub pod_layout: PodLayout,

    /// Pod-runner's own log directory (for `pod-runner.log`).
    /// NOT container logs — those live in each pod's state dir.
    pub log_dir: PathBuf,

    /// Directory for process PID tracking files: `<workdir>/data/pod-pid/`.
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
    /// Cleanup TTLs governing when a finished pod's data dirs are reaped
    /// and when its pod dir is deleted, parsed from config.
    pub ttls: TtlConfig,

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
        Self {
            bind_address: format!("{DEFAULT_ADDRESS}:{DEFAULT_PORT}"),
            node_address: DEFAULT_ADDRESS.to_string(),
            kubeconfig: String::new(),

            nodename: DEFAULT_NODENAME.to_string(),
            namespace: DEFAULT_NAMESPACE.to_string(),

            tls: ServerTlsConfig::default(),

            config_dir: PathBuf::from(crate::config::cli::DEFAULT_CONFIGDIR),
            work_dir: PathBuf::from("."),
            data_dir: PathBuf::new(),
            pod_layout: PodLayout::default(),
            log_dir: PathBuf::from("log"),
            pid_dir: PathBuf::from(POD_PID_DIR),

            default_grace_period_secs: 120,
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

            ttls: TtlConfig::default(),
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
            layout: c.pod_layout.clone(),
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
            ttls: c.ttls.clone(),
        }
    }
}

impl From<&PRConfig> for FileStoreConfig {
    fn from(c: &PRConfig) -> Self {
        Self {
            layout: c.pod_layout.clone(),
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
        assert_eq!(c.pod_layout, PodLayout::new(&c.data_dir));
        assert_eq!(c.pid_dir, c.data_dir.join(POD_PID_DIR));
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
    fn prconfig_default_cleanup_ttls() {
        let c = PRConfig::default();
        assert_eq!(c.ttls, TtlConfig::default());
        assert_eq!(c.ttls.reap_on_success.raw(), "0s");
        assert_eq!(c.ttls.delete_on_failure.raw(), "24h");
    }

    #[test]
    fn cleanup_config_from_prconfig() {
        use crate::logic::ttl::TtlSpec;
        let pr = PRConfig {
            ttls: TtlConfig {
                reap_on_failure: TtlSpec::parse("30m").unwrap(),
                ..TtlConfig::default()
            },
            ..PRConfig::default()
        };

        let cc = CleanupConfig::from(&pr);
        assert_eq!(cc.ttls.reap_on_failure.raw(), "30m");
    }

    #[test]
    fn executor_provider_and_store_share_the_pod_layout() {
        let pr = PRConfig {
            pod_layout: PodLayout::new(std::path::Path::new("/custom/data")),
            ..PRConfig::default()
        };
        assert_eq!(DefaultExecutorProvider::from(&pr).layout, pr.pod_layout);
        assert_eq!(FileStoreConfig::from(&pr).layout, pr.pod_layout);
    }
}
