// Copyright 2026 Tabsdata Inc.

//! Configuration resolution — merges CLI args, environment variables,
//! and YAML config into the final [`PRConfig`].

use std::path::Path;

use base64::Engine;
use tracing::info;

use super::cli::{CliArgs, DEFAULT_CONFIGDIR, DEFAULT_LOGDIR, DEFAULT_WORKDIR};
use super::pki;
use super::template::{PRCONFIG_FILENAME, VK_PRCONFIG_FILENAME};
use super::vk_config;
use super::yaml_config::YamlPRConfig;
use crate::server::config::*;

// ---------------------------------------------------------------------------
// URL parsing
// ---------------------------------------------------------------------------

/// Parsed URL components.
struct ParsedUrl {
    #[allow(dead_code)] // used in tests
    scheme: String,
    host: String,
    port: u16,
}

/// Parse a URL into scheme, host, and port.
fn parse_url(url: &str) -> Result<ParsedUrl, String> {
    let (scheme, rest) = url
        .split_once("://")
        .ok_or_else(|| format!("Invalid URL (missing scheme): {url}"))?;

    let (host, port) = if let Some((h, p)) = rest.split_once(':') {
        let port: u16 = p
            .parse()
            .map_err(|_| format!("Invalid port in URL: {url}"))?;
        (h.to_string(), port)
    } else {
        let default_port = if scheme == "https" { 443 } else { 80 };
        (rest.to_string(), default_port)
    };

    Ok(ParsedUrl {
        scheme: scheme.to_string(),
        host,
        port,
    })
}

// ---------------------------------------------------------------------------
// CertSource mapping
// ---------------------------------------------------------------------------

/// Map a pair of YAML cert fields (path, base64 data) into a [`CertSource`].
fn map_cert_source(path: &str, data: &str) -> CertSource {
    let path = if path.is_empty() { None } else { Some(path.to_string()) };
    let data = if data.is_empty() { None } else { Some(data.to_string()) };
    CertSource { path, data }
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Resolve the full [`PRConfig`] from CLI arguments.
///
/// 1. Resolves `configdir` (CLI or `./config`)
/// 2. Loads or auto-generates `prconfig.yaml`
/// 3. Validates the YAML config
/// 4. Parses URL → bind address, TLS mode
/// 5. Resolves kubeconfig, workdir, logdir (CLI → env → config → defaults)
/// 6. Rotates and creates `run/` directory structure
/// 7. Regenerates `vk-prconfig.yaml`
/// 8. Returns the final `PRConfig`
pub fn resolve_config(cli: &CliArgs) -> Result<PRConfig, String> {
    // 1. Resolve configdir
    let configdir = cli
        .configdir
        .as_deref()
        .unwrap_or(DEFAULT_CONFIGDIR);
    let configdir = std::fs::canonicalize(configdir)
        .unwrap_or_else(|_| std::env::current_dir().unwrap_or_default().join(configdir));

    // 2. Load or generate prconfig.yaml + prsysconfig.yaml
    let prconfig_path = configdir.join(PRCONFIG_FILENAME);
    if !prconfig_path.is_file() {
        let port = cli.port.unwrap_or(DEFAULT_PORT);
        let metrics_port = cli.metrics_port.unwrap_or(crate::metrics::DEFAULT_METRICS_PORT);
        info!(configdir = %configdir.display(), port, metrics_port, "Generating config files");
        pki::generate_and_write_configs(&configdir, DEFAULT_ADDRESS, port, metrics_port)?;
    }
    let yaml = YamlPRConfig::from_file(&prconfig_path)?;
    yaml.validate()?;

    // 3. Load prsysconfig.yaml (system settings, persistent)
    let syscfg_path = configdir.join(crate::config::template::PRSYSCONFIG_FILENAME);
    let sys = if syscfg_path.is_file() {
        super::yaml_config::YamlSysConfig::from_file(&syscfg_path)?
    } else {
        super::yaml_config::YamlSysConfig::default()
    };

    // 4. Parse URL
    let url = &yaml.url;
    let parsed = parse_url(url)?;
    let bind_address = format!("{}:{}", parsed.host, parsed.port);

    // 5. Resolve kubeconfig: CLI → KUBECONFIG env (not in config files)
    let kubeconfig = cli
        .kubeconfig
        .clone()
        .or_else(|| std::env::var(KUBECONFIG_ENV).ok())
        .unwrap_or_default();

    // 6. Resolve workdir: CLI → default
    let workdir = cli
        .workdir
        .as_deref()
        .unwrap_or(DEFAULT_WORKDIR);
    let workdir = std::fs::canonicalize(workdir)
        .unwrap_or_else(|_| std::env::current_dir().unwrap_or_default().join(workdir));

    // 7. Resolve logdir: CLI → default
    let log_dir = cli
        .logdir
        .as_deref()
        .unwrap_or(DEFAULT_LOGDIR);
    let log_dir = std::fs::canonicalize(log_dir)
        .unwrap_or_else(|_| std::env::current_dir().unwrap_or_default().join(log_dir));

    // 8. Rotate and create data/run/ directory structure
    let data_dir = workdir.join("data");
    std::fs::create_dir_all(&data_dir)
        .map_err(|e| format!("Failed to create {}: {e}", data_dir.display()))?;
    let run_dir = data_dir.join(RUN_DIR);
    rotate_run_dir(&run_dir);
    let pods_dir = run_dir.join(PODS_SUBDIR);
    let pid_dir = run_dir.join(PIDS_SUBDIR);

    for dir in [&run_dir, &pods_dir, &pid_dir, &log_dir] {
        std::fs::create_dir_all(dir)
            .map_err(|e| format!("Failed to create {}: {e}", dir.display()))?;
    }

    // 9. Map TLS config (always mTLS)
    let tls = ServerTlsConfig {
        certificate: map_cert_source(
            &yaml.server.tls.certificate,
            &yaml.server.tls.certificate_data,
        ),
        certificate_key: map_cert_source(
            &yaml.server.tls.certificate_key,
            &yaml.server.tls.certificate_key_data,
        ),
        client_ca: map_cert_source(
            &yaml.server.tls.certificate_authority,
            &yaml.server.tls.certificate_authority_data,
        ),
    };

    // 10. Regenerate vk-prconfig.yaml (every startup)
    regenerate_vk_prconfig(&configdir, url, &sys, &tls)?;

    // 11. Build PRConfig
    Ok(PRConfig {
        bind_address,
        node_address: parsed.host,
        kubeconfig,
        nodename: sys.nodename.clone(),
        namespace: sys.namespace.clone(),

        tls,

        run_dir,
        pods_dir,
        log_dir,
        pid_dir,

        default_grace_period_secs: sys.default_grace_period_secs,
        container_max_restarts: sys.max_restart_count,
        container_restart_backoff_base_secs: sys.restart_backoff_base_secs,

        capacity_cpu: sys.capacity.cpu,
        capacity_memory_gb: sys.capacity.memory_gb,
        capacity_pods: sys.capacity.pods,

        propagate_env: sys.propagate_env,
        die_with_parent: sys.die_with_parent,

        write_running_spec: sys.write_secrets_to_container_shell_script,

        secret_resolver: map_resolver(&sys.secret_resolver),
        config_map_resolver: map_resolver(&sys.config_map_resolver),
        node_env_resolver: map_resolver(&sys.node_envs_resolver),

        keep_deleted_pod_dir: sys.keep_deleted_pod_dir,
        keep_failed_pod_dir: sys.keep_failed_pod_dir,
        deleted_pod_dir_ttl_hours: sys.deleted_pod_dir_ttl_hours,
        pod_archive_ttl_days: sys.pod_archive_ttl_days,

        metrics_url: sys.metrics_url.clone(),
    })
}

/// Rotate the run directory on startup: `run/` → `run.0/`, etc.
///
/// Keeps [`PREV_RUN_DIRS_TO_KEEP`] previous run directories.
/// Called before creating the fresh `run/` tree.
fn rotate_run_dir(run_dir: &std::path::Path) {
    use std::fs;
    if !run_dir.exists() {
        return;
    }
    let parent = match run_dir.parent() {
        Some(p) => p,
        None => return,
    };
    let max = PREV_RUN_DIRS_TO_KEEP;
    // Delete oldest
    let oldest = parent.join(format!("run.{}", max));
    if oldest.exists() {
        let _ = fs::remove_dir_all(&oldest);
    }
    // Shift run.{N-1} → run.{N}
    for i in (0..max).rev() {
        let from = parent.join(format!("run.{}", i));
        let to = parent.join(format!("run.{}", i + 1));
        if from.exists() {
            let _ = fs::rename(&from, &to);
        }
    }
    // Rename run/ → run.0/
    let _ = fs::rename(run_dir, parent.join("run.0"));
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn map_resolver(yaml: &super::yaml_config::YamlResolverConfig) -> crate::pod::resolver::ResolverConfig {
    crate::pod::resolver::ResolverConfig {
        resolver_type: yaml.resolver_type.clone(),
        configs: yaml.configs.clone(),
    }
}

/// Regenerate vk-prconfig.yaml from the current server config.
fn regenerate_vk_prconfig(
    configdir: &Path,
    url: &str,
    sys: &super::yaml_config::YamlSysConfig,
    tls: &ServerTlsConfig,
) -> Result<(), String> {
    let ca_b64 = cert_source_to_b64(&tls.client_ca)
        .unwrap_or_default();

    // Preserve existing client certs from the vk-prconfig if present
    let vk_path = configdir.join(VK_PRCONFIG_FILENAME);
    let (client_cert_b64, client_key_b64) = if vk_path.is_file() {
        let content = std::fs::read_to_string(&vk_path).unwrap_or_default();
        let vk: serde_yaml_ng::Value =
            serde_yaml_ng::from_str(&content).unwrap_or(serde_yaml_ng::Value::Null);
        let client_cert = vk["client"]["certificate-data"]
            .as_str()
            .unwrap_or("")
            .to_string();
        let client_key = vk["client"]["certificate-key-data"]
            .as_str()
            .unwrap_or("")
            .to_string();
        (client_cert, client_key)
    } else {
        (String::new(), String::new())
    };

    vk_config::write_vk_prconfig(
        configdir,
        url,
        &sys.nodename,
        &sys.namespace,
        &ca_b64,
        &client_cert_b64,
        &client_key_b64,
        &sys.metrics_url,
    )
}

/// Extract base64 data from a CertSource.
fn cert_source_to_b64(source: &CertSource) -> Option<String> {
    if let Some(ref data) = source.data {
        Some(data.clone())
    } else if let Some(ref path) = source.path {
        let bytes = std::fs::read(path).ok()?;
        Some(base64::engine::general_purpose::STANDARD.encode(&bytes))
    } else {
        None
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_url_https() {
        let p = parse_url("https://10.0.0.1:8080").unwrap();
        assert_eq!(p.scheme, "https");
        assert_eq!(p.host, "10.0.0.1");
        assert_eq!(p.port, 8080);
    }

    #[test]
    fn parse_url_http_default_port() {
        let p = parse_url("http://0.0.0.0").unwrap();
        assert_eq!(p.scheme, "http");
        assert_eq!(p.host, "0.0.0.0");
        assert_eq!(p.port, 80);
    }

    #[test]
    fn parse_url_https_default_port() {
        let p = parse_url("https://localhost").unwrap();
        assert_eq!(p.scheme, "https");
        assert_eq!(p.host, "localhost");
        assert_eq!(p.port, 443);
    }

    #[test]
    fn parse_url_invalid() {
        assert!(parse_url("not-a-url").is_err());
    }

    #[test]
    fn map_cert_source_path_only() {
        let cs = map_cert_source("/path/to/cert.pem", "");
        assert_eq!(cs.path.as_deref(), Some("/path/to/cert.pem"));
        assert!(cs.data.is_none());
    }

    #[test]
    fn map_cert_source_data_only() {
        let cs = map_cert_source("", "base64data");
        assert!(cs.path.is_none());
        assert_eq!(cs.data.as_deref(), Some("base64data"));
    }

    #[test]
    fn map_cert_source_empty() {
        let cs = map_cert_source("", "");
        assert!(!cs.is_configured());
    }

    #[test]
    fn resolve_config_auto_generates_pki() {
        let tmp = std::env::temp_dir().join(format!(
            "pr-resolve-test-{}",
            std::process::id(),
        ));
        let configdir = tmp.join("config");
        let workdir = tmp.join("work");
        let logdir = tmp.join("log");

        let cli = CliArgs {
            kubeconfig: None,
            configdir: Some(configdir.to_string_lossy().into()),
            workdir: Some(workdir.to_string_lossy().into()),
            logdir: Some(logdir.to_string_lossy().into()),
            port: None,
            metrics_port: None,
            logmaxsizemb: 100,
            logmaxfiles: 3,
        };

        let config = resolve_config(&cli).unwrap();

        assert!(config.tls_enabled(), "TLS should be enabled with auto-gen certs");
        assert!(config.tls_configured(), "mTLS should be fully configured");
        assert_eq!(config.bind_address, format!("{DEFAULT_ADDRESS}:{DEFAULT_PORT}"));
        assert_eq!(config.nodename, DEFAULT_NODENAME);
        assert_eq!(config.namespace, DEFAULT_NAMESPACE);

        assert!(configdir.join(PRCONFIG_FILENAME).exists());
        assert!(configdir.join(VK_PRCONFIG_FILENAME).exists());

        assert!(config.run_dir.exists());
        assert!(config.pods_dir.exists());
        assert!(config.log_dir.exists());
        assert!(config.pid_dir.exists());

        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[test]
    fn resolve_config_loads_existing_config() {
        use std::sync::atomic::{AtomicU64, Ordering};
        static N: AtomicU64 = AtomicU64::new(0);
        let tmp = std::env::temp_dir().join(format!(
            "pr-resolve-existing-{}-{}",
            std::process::id(),
            N.fetch_add(1, Ordering::Relaxed),
        ));
        let configdir = tmp.join("config");
        let workdir = tmp.join("work");
        let logdir = tmp.join("log");
        std::fs::create_dir_all(&configdir).unwrap();

        // Write split config files
        let prconfig = r#"
url: "https://0.0.0.0:8080"
server:
  tls:
    certificate-data: "Y2VydA=="
    certificate-key-data: "a2V5"
    certificate-authority-data: "Y2E="
"#;
        std::fs::write(configdir.join(PRCONFIG_FILENAME), prconfig).unwrap();

        let syscfg = r#"
nodename: "custom-node"
namespace: "staging"
maxPods: 42
propagateEnv: false
keepDeletedPodDir: true
"#;
        std::fs::write(configdir.join(crate::config::template::PRSYSCONFIG_FILENAME), syscfg).unwrap();

        let cli = CliArgs {
            kubeconfig: Some("/my/kubeconfig".into()),
            configdir: Some(configdir.to_string_lossy().into()),
            workdir: Some(workdir.to_string_lossy().into()),
            logdir: Some(logdir.to_string_lossy().into()),
            port: None,
            metrics_port: None,
            logmaxsizemb: 100,
            logmaxfiles: 3,
        };

        let config = resolve_config(&cli).unwrap();

        assert_eq!(config.bind_address, "0.0.0.0:8080");
        assert_eq!(config.node_address, "0.0.0.0");
        assert!(config.tls_enabled());
        assert_eq!(config.nodename, "custom-node");
        assert_eq!(config.namespace, "staging");
        assert_eq!(config.kubeconfig, "/my/kubeconfig");
        // maxPods in YAML is ignored — capacity.pods is authoritative.
        // This YAML doesn't set capacity.pods, so it defaults to 100.
        assert_eq!(config.capacity_pods, 100);
        assert!(!config.propagate_env);
        assert!(config.keep_deleted_pod_dir);

        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[test]
    fn resolve_config_kubeconfig_from_env() {
        use std::sync::atomic::{AtomicU64, Ordering};
        static N: AtomicU64 = AtomicU64::new(0);
        let tmp = std::env::temp_dir().join(format!(
            "pr-resolve-env-{}-{}",
            std::process::id(),
            N.fetch_add(1, Ordering::Relaxed),
        ));
        let configdir = tmp.join("config");
        let workdir = tmp.join("work");
        let logdir = tmp.join("log");
        std::fs::create_dir_all(&configdir).unwrap();

        std::fs::write(
            configdir.join(PRCONFIG_FILENAME),
            "url: \"https://127.0.0.1:3000\"\nserver:\n  tls:\n    certificate-data: \"Y2VydA==\"\n    certificate-key-data: \"a2V5\"\n    certificate-authority-data: \"Y2E=\"\n",
        )
        .unwrap();

        // Set env var (scoped — may affect parallel tests, but kubeconfig
        // resolution is best-effort so this is acceptable)
        // SAFETY: test-only, single-threaded test
        unsafe { std::env::set_var(KUBECONFIG_ENV, "/from/env") };

        let cli = CliArgs {
            kubeconfig: None, // not from CLI
            configdir: Some(configdir.to_string_lossy().into()),
            workdir: Some(workdir.to_string_lossy().into()),
            logdir: Some(logdir.to_string_lossy().into()),
            port: None,
            metrics_port: None,
            logmaxsizemb: 100,
            logmaxfiles: 3,
        };

        let config = resolve_config(&cli).unwrap();
        assert_eq!(config.kubeconfig, "/from/env");

        // SAFETY: test-only cleanup
        unsafe { std::env::remove_var(KUBECONFIG_ENV) };
        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[test]
    fn resolve_config_maps_all_yaml_fields() {
        use std::sync::atomic::{AtomicU64, Ordering};
        static N: AtomicU64 = AtomicU64::new(0);
        let tmp = std::env::temp_dir().join(format!(
            "pr-resolve-fields-{}-{}",
            std::process::id(),
            N.fetch_add(1, Ordering::Relaxed),
        ));
        let configdir = tmp.join("config");
        let workdir = tmp.join("work");
        let logdir = tmp.join("log");
        std::fs::create_dir_all(&configdir).unwrap();

        let prconfig = r#"
url: "https://10.0.0.1:9090"
server:
  tls:
    certificate-data: "Y2VydA=="
    certificate-key-data: "a2V5"
    certificate-authority-data: "Y2E="
"#;
        std::fs::write(configdir.join(PRCONFIG_FILENAME), prconfig).unwrap();

        let syscfg = r#"
nodename: "node-x"
namespace: "prod"
maxPods: 200
defaultGracePeriodSecs: 60
maxRestartCount: 10
restartBackoffBaseSecs: 20
propagateEnv: false
dieWithParent: false
writeSecretsToContainerShellScript: true
keepDeletedPodDir: true
keepFailedPodDir: false
deletedPodDirTtlHours: 72
podArchiveTtlDays: 30
capacity:
  cpu: 64
  memoryGB: 256
  pods: 500
"#;
        std::fs::write(configdir.join(crate::config::template::PRSYSCONFIG_FILENAME), syscfg).unwrap();

        let cli = CliArgs {
            kubeconfig: None,
            configdir: Some(configdir.to_string_lossy().into()),
            workdir: Some(workdir.to_string_lossy().into()),
            logdir: Some(logdir.to_string_lossy().into()),
            port: None,
            metrics_port: None,
            logmaxsizemb: 100,
            logmaxfiles: 3,
        };

        let c = resolve_config(&cli).unwrap();

        assert_eq!(c.bind_address, "10.0.0.1:9090");
        assert_eq!(c.nodename, "node-x");
        assert_eq!(c.namespace, "prod");
        assert_eq!(c.capacity_pods, 500);
        assert_eq!(c.default_grace_period_secs, 60);
        assert_eq!(c.container_max_restarts, 10);
        assert_eq!(c.container_restart_backoff_base_secs, 20);
        assert!(!c.propagate_env);
        assert!(!c.die_with_parent);
        assert!(c.write_running_spec);
        assert!(c.keep_deleted_pod_dir);
        assert!(!c.keep_failed_pod_dir);
        assert_eq!(c.deleted_pod_dir_ttl_hours, 72);
        assert_eq!(c.pod_archive_ttl_days, 30);
        assert_eq!(c.capacity_cpu, 64);
        assert_eq!(c.capacity_memory_gb, 256);
        assert_eq!(c.capacity_pods, 500);

        let _ = std::fs::remove_dir_all(&tmp);
    }
}
