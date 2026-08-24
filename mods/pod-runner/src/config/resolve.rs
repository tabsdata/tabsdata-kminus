// Copyright 2026 Tabsdata Inc.

//! Configuration resolution — merges CLI args, environment variables,
//! and YAML config into the final [`PRConfig`].

use std::path::{Path, PathBuf};

use base64::Engine;
use tracing::{debug, info};

fn canonicalize(label: &str, path: &Path) -> PathBuf {
    let resolved = dunce::canonicalize(path)
        .unwrap_or_else(|_| std::env::current_dir().unwrap_or_default().join(path));
    debug!(site = label, input = %path.display(), output = %resolved.display(), "Canonicalized path.");
    resolved
}

use super::cli::{CliArgs, DEFAULT_CONFIGDIR, DEFAULT_IMAGESDIR, DEFAULT_LOGDIR, DEFAULT_WORKDIR};
use super::pki;
use super::template::{PRCONFIG_FILENAME, VK_PRCONFIG_FILENAME};
use super::vk_config;
use super::yaml_config::YamlPRConfig;
use crate::logic::ttl::{TtlConfig, TtlSpec};
use crate::server::config::*;

// ---------------------------------------------------------------------------
// Cleanup TTL parsing
// ---------------------------------------------------------------------------

/// Parse a configured cleanup TTL, naming the key in the error so the
/// operator knows which line of `prsysconfig.yaml` to fix.
fn parse_ttl(key: &str, value: &str) -> Result<TtlSpec, String> {
    TtlSpec::parse(value).map_err(|e| format!("{}: {}", key, e))
}

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
    let path = if path.is_empty() {
        None
    } else {
        Some(path.to_string())
    };
    let data = if data.is_empty() {
        None
    } else {
        Some(data.to_string())
    };
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
    let configdir = cli.configdir.as_deref().unwrap_or(DEFAULT_CONFIGDIR);
    let configdir = canonicalize("configdir", Path::new(configdir));

    // 2. Load or generate prconfig.yaml + prsysconfig.yaml
    let prconfig_path = configdir.join(PRCONFIG_FILENAME);
    if !prconfig_path.is_file() {
        let port = cli.port.unwrap_or(DEFAULT_PORT);
        let metrics_port = cli
            .metrics_port
            .unwrap_or(crate::metrics::DEFAULT_METRICS_PORT);
        info!(configdir = %configdir.display(), port, metrics_port, "Generating config files");
        pki::generate_and_write_configs(&configdir, DEFAULT_ADDRESS, port, metrics_port)?;
    }
    let yaml = YamlPRConfig::from_file(&prconfig_path)?;
    yaml.validate()?;

    // 3. Load prsysconfig.yaml (system settings, persistent)
    // The file is never regenerated, so one written by an older build can
    // still carry settings this one dropped. Fail rather than silently
    // ignoring a retention choice the operator made deliberately.
    let syscfg_path = configdir.join(crate::config::template::PRSYSCONFIG_FILENAME);
    let sys = if syscfg_path.is_file() {
        let sys = super::yaml_config::YamlSysConfig::from_file(&syscfg_path)?;
        sys.validate_known_keys()
            .map_err(|e| format!("{}: {}", syscfg_path.display(), e))?;
        sys
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
    let workdir = cli.workdir.as_deref().unwrap_or(DEFAULT_WORKDIR);
    let workdir = canonicalize("workdir", Path::new(workdir));

    // 7. Resolve logdir: CLI → default
    let log_dir = cli.logdir.as_deref().unwrap_or(DEFAULT_LOGDIR);
    let log_dir = canonicalize("logdir", Path::new(log_dir));

    // 8. Rotate and create data/run/ directory structure
    let data_dir = workdir.join("data");
    std::fs::create_dir_all(&data_dir)
        .map_err(|e| format!("Failed to create {}: {e}", data_dir.display()))?;
    let run_dir = provision_run_dir(&data_dir);
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

    // 11. Resolve images_dir: CLI > prsysconfig.yaml > DEFAULT_IMAGESDIR.
    //     Absolute path is used as-is; relative is joined against the cwd.
    let images_dir_raw: String = cli
        .imagesdir
        .clone()
        .filter(|s| !s.is_empty())
        .or_else(|| {
            if sys.images_dir.is_empty() {
                None
            } else {
                Some(sys.images_dir.clone())
            }
        })
        .unwrap_or_else(|| DEFAULT_IMAGESDIR.to_string());
    let images_dir = {
        let p = std::path::PathBuf::from(&images_dir_raw);
        if p.is_absolute() {
            p
        } else {
            std::env::current_dir().map(|cwd| cwd.join(&p)).unwrap_or(p)
        }
    };

    // 12. Parse cleanup TTLs — a bad value fails the load rather than
    // silently falling back, since retention is not a setting to guess at.
    let ttls = TtlConfig {
        reap_on_success: parse_ttl(
            "podDataReapTtlOnPodSuccess",
            &sys.pod_data_reap_ttl_on_pod_success,
        )?,
        reap_on_failure: parse_ttl(
            "podDataReapTtlOnPodFailure",
            &sys.pod_data_reap_ttl_on_pod_failure,
        )?,
        delete_on_success: parse_ttl(
            "podDirDeleteTtlPodSuccessOnPodDelete",
            &sys.pod_dir_delete_ttl_pod_success_on_pod_delete,
        )?,
        delete_on_failure: parse_ttl(
            "podDirDeleteTtlPodFailureOnPodDelete",
            &sys.pod_dir_delete_ttl_pod_failure_on_pod_delete,
        )?,
    };

    // 13. Build PRConfig
    Ok(PRConfig {
        bind_address,
        node_address: parsed.host,
        kubeconfig,
        nodename: sys.nodename.clone(),
        namespace: sys.namespace.clone(),

        tls,

        config_dir: configdir.clone(),
        work_dir: workdir.clone(),
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

        images_dir,

        secret_resolver: map_resolver(&sys.secret_resolver),
        config_map_resolver: map_resolver(&sys.config_map_resolver),
        node_env_resolver: map_resolver(&sys.node_envs_resolver),

        ttls,
        dir_size_scan_interval_secs: sys.dir_size_scan_interval_secs,

        metrics_url: sys.metrics_url.clone(),
    })
}

/// Mint this run's directory (`run_<YYYYMMDDThhmmssZ>`), purge old run
/// directories down to [`PREV_RUN_DIRS_TO_KEEP`], and reap the kept ones'
/// pod data.
///
/// A fresh directory per run, never a rename of the previous one. The old
/// scheme renamed `run/` to `run.0/` at startup, and on Windows that
/// rename fails while any handle inside is still open — a crashed run's
/// containers hold plenty — so the failure was swallowed and the old tree
/// silently reused, colliding replayed pods with the previous attempt's
/// leftovers ("response file already exists"). Creating a new name cannot
/// fail into reuse; the worst a held handle can now do is delay a purge to
/// a later start.
///
/// The stamp is fixed-width UTC, so lexicographic order is chronological
/// — which is also what ranks legacy `run`/`run.N` names below every
/// stamped one, making them the first purged.
fn provision_run_dir(data_dir: &std::path::Path) -> std::path::PathBuf {
    use std::fs;

    let stamp = chrono::Utc::now().format("%Y%m%dT%H%M%SZ");
    let mut run_dir = data_dir.join(format!("{RUN_DIR_PREFIX}{stamp}"));
    // A restart within the stamp's second disambiguates rather than reuses.
    let mut n = 1;
    while run_dir.exists() {
        run_dir = data_dir.join(format!("{RUN_DIR_PREFIX}{stamp}.{n}"));
        n += 1;
    }

    // Previous runs, newest first: stamped names sort chronologically, and
    // legacy `run`/`run.N` names sort below them all.
    let mut previous: Vec<std::path::PathBuf> = fs::read_dir(data_dir)
        .map(|entries| {
            entries
                .flatten()
                .filter(|e| e.path().is_dir())
                .filter(|e| {
                    let name = e.file_name();
                    let name = name.to_string_lossy();
                    name.starts_with(RUN_DIR_PREFIX)
                        || name == RUN_DIR
                        || name
                            .strip_prefix(&format!("{RUN_DIR}."))
                            .is_some_and(|i| i.parse::<u32>().is_ok())
                })
                .map(|e| e.path())
                .collect()
        })
        .unwrap_or_default();
    previous.sort();
    previous.reverse();

    for old in previous.iter().skip(PREV_RUN_DIRS_TO_KEEP) {
        // Best effort: on Windows a held handle can refuse the delete, and
        // that is fine — the directory is inert and a later start retries.
        let _ = fs::remove_dir_all(old);
    }
    for kept in previous.iter().take(PREV_RUN_DIRS_TO_KEEP) {
        // Every pod in a previous run belonged to a previous incarnation of
        // the runner, so its data dirs are dead weight from this moment on.
        crate::logic::reaper::reap_previous_run_pod_dirs(
            &kept.join(PODS_SUBDIR),
            chrono::Utc::now(),
        );
    }

    run_dir
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn map_resolver(
    yaml: &super::yaml_config::YamlResolverConfig,
) -> crate::pod::resolver::ResolverConfig {
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
    let ca_b64 = cert_source_to_b64(&tls.client_ca).unwrap_or_default();

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
        let tmp = std::env::temp_dir().join(format!("pr-resolve-test-{}", std::process::id(),));
        let configdir = tmp.join("config");
        let workdir = tmp.join("work");
        let logdir = tmp.join("log");

        let cli = CliArgs {
            kubeconfig: None,
            configdir: Some(configdir.to_string_lossy().into()),
            workdir: Some(workdir.to_string_lossy().into()),
            logdir: Some(logdir.to_string_lossy().into()),
            imagesdir: None,
            port: None,
            metrics_port: None,
            logmaxsizemb: 100,
            logmaxfiles: 3,
            loglevel: crate::config::loglevel::LogLevel::Debug,
            heartbeat_file: None,
            heartbeat_secs: crate::config::cli::DEFAULT_HEARTBEAT_SECS,
        };

        let config = resolve_config(&cli).unwrap();

        assert!(
            config.tls_enabled(),
            "TLS should be enabled with auto-gen certs"
        );
        assert!(config.tls_configured(), "mTLS should be fully configured");
        assert_eq!(
            config.bind_address,
            format!("{DEFAULT_ADDRESS}:{DEFAULT_PORT}")
        );
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
podDataReapTtlOnPodSuccess: "5m"
"#;
        std::fs::write(
            configdir.join(crate::config::template::PRSYSCONFIG_FILENAME),
            syscfg,
        )
        .unwrap();

        let cli = CliArgs {
            kubeconfig: Some("/my/kubeconfig".into()),
            configdir: Some(configdir.to_string_lossy().into()),
            workdir: Some(workdir.to_string_lossy().into()),
            logdir: Some(logdir.to_string_lossy().into()),
            imagesdir: None,
            port: None,
            metrics_port: None,
            logmaxsizemb: 100,
            logmaxfiles: 3,
            loglevel: crate::config::loglevel::LogLevel::Debug,
            heartbeat_file: None,
            heartbeat_secs: crate::config::cli::DEFAULT_HEARTBEAT_SECS,
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
        assert_eq!(config.ttls.reap_on_success.raw(), "5m");

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
            imagesdir: None,
            port: None,
            metrics_port: None,
            logmaxsizemb: 100,
            logmaxfiles: 3,
            loglevel: crate::config::loglevel::LogLevel::Debug,
            heartbeat_file: None,
            heartbeat_secs: crate::config::cli::DEFAULT_HEARTBEAT_SECS,
        };

        let config = resolve_config(&cli).unwrap();
        assert_eq!(config.kubeconfig, "/from/env");

        // SAFETY: test-only cleanup
        unsafe { std::env::remove_var(KUBECONFIG_ENV) };
        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[test]
    fn resolve_config_images_dir_absolute() {
        use std::sync::atomic::{AtomicU64, Ordering};
        static N: AtomicU64 = AtomicU64::new(0);
        let tmp = std::env::temp_dir().join(format!(
            "pr-resolve-images-abs-{}-{}",
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
        std::fs::write(
            configdir.join(crate::config::template::PRSYSCONFIG_FILENAME),
            "imagesDir: \"/var/lib/kminus/images\"\n",
        )
        .unwrap();

        let cli = CliArgs {
            kubeconfig: None,
            configdir: Some(configdir.to_string_lossy().into()),
            workdir: Some(workdir.to_string_lossy().into()),
            logdir: Some(logdir.to_string_lossy().into()),
            imagesdir: None,
            port: None,
            metrics_port: None,
            logmaxsizemb: 100,
            logmaxfiles: 3,
            loglevel: crate::config::loglevel::LogLevel::Debug,
            heartbeat_file: None,
            heartbeat_secs: crate::config::cli::DEFAULT_HEARTBEAT_SECS,
        };

        let c = resolve_config(&cli).unwrap();
        assert_eq!(
            c.images_dir,
            std::path::PathBuf::from("/var/lib/kminus/images")
        );

        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[test]
    fn resolve_config_images_dir_default_when_unset() {
        use std::sync::atomic::{AtomicU64, Ordering};
        static N: AtomicU64 = AtomicU64::new(0);
        let tmp = std::env::temp_dir().join(format!(
            "pr-resolve-images-default-{}-{}",
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
        // No prsysconfig.yaml and no CLI flag — default applies.

        let cli = CliArgs {
            kubeconfig: None,
            configdir: Some(configdir.to_string_lossy().into()),
            workdir: Some(workdir.to_string_lossy().into()),
            logdir: Some(logdir.to_string_lossy().into()),
            imagesdir: None,
            port: None,
            metrics_port: None,
            logmaxsizemb: 100,
            logmaxfiles: 3,
            loglevel: crate::config::loglevel::LogLevel::Debug,
            heartbeat_file: None,
            heartbeat_secs: crate::config::cli::DEFAULT_HEARTBEAT_SECS,
        };

        let c = resolve_config(&cli).unwrap();
        let cwd = std::env::current_dir().unwrap();
        assert_eq!(c.images_dir, cwd.join(DEFAULT_IMAGESDIR));

        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[test]
    fn resolve_config_images_dir_cli_overrides_yaml() {
        use std::sync::atomic::{AtomicU64, Ordering};
        static N: AtomicU64 = AtomicU64::new(0);
        let tmp = std::env::temp_dir().join(format!(
            "pr-resolve-images-cli-{}-{}",
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
        std::fs::write(
            configdir.join(crate::config::template::PRSYSCONFIG_FILENAME),
            "imagesDir: \"/from/yaml\"\n",
        )
        .unwrap();

        let cli = CliArgs {
            kubeconfig: None,
            configdir: Some(configdir.to_string_lossy().into()),
            workdir: Some(workdir.to_string_lossy().into()),
            logdir: Some(logdir.to_string_lossy().into()),
            imagesdir: Some("/from/cli".into()),
            port: None,
            metrics_port: None,
            logmaxsizemb: 100,
            logmaxfiles: 3,
            loglevel: crate::config::loglevel::LogLevel::Debug,
            heartbeat_file: None,
            heartbeat_secs: crate::config::cli::DEFAULT_HEARTBEAT_SECS,
        };

        let c = resolve_config(&cli).unwrap();
        assert_eq!(c.images_dir, std::path::PathBuf::from("/from/cli"));

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
podDataReapTtlOnPodSuccess: "30s"
podDataReapTtlOnPodFailure: "90m"
podDirDeleteTtlPodSuccessOnPodDelete: "2h"
podDirDeleteTtlPodFailureOnPodDelete: "never"
capacity:
  cpu: 64
  memoryGB: 256
  pods: 500
"#;
        std::fs::write(
            configdir.join(crate::config::template::PRSYSCONFIG_FILENAME),
            syscfg,
        )
        .unwrap();

        let cli = CliArgs {
            kubeconfig: None,
            configdir: Some(configdir.to_string_lossy().into()),
            workdir: Some(workdir.to_string_lossy().into()),
            logdir: Some(logdir.to_string_lossy().into()),
            imagesdir: None,
            port: None,
            metrics_port: None,
            logmaxsizemb: 100,
            logmaxfiles: 3,
            loglevel: crate::config::loglevel::LogLevel::Debug,
            heartbeat_file: None,
            heartbeat_secs: crate::config::cli::DEFAULT_HEARTBEAT_SECS,
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
        assert_eq!(c.ttls.reap_on_success.ttl().secs(), Some(30));
        assert_eq!(c.ttls.reap_on_failure.ttl().secs(), Some(5400));
        assert_eq!(c.ttls.delete_on_success.ttl().secs(), Some(7200));
        assert_eq!(
            c.ttls.delete_on_failure.ttl(),
            crate::logic::ttl::Ttl::Never
        );
        assert_eq!(c.capacity_cpu, 64);
        assert_eq!(c.capacity_memory_gb, 256);
        assert_eq!(c.capacity_pods, 500);

        let _ = std::fs::remove_dir_all(&tmp);
    }

    /// Resolve a config from a `prsysconfig.yaml` body, with everything
    /// else defaulted.
    fn resolve_with_syscfg(label: &str, syscfg: &str) -> Result<PRConfig, String> {
        use std::sync::atomic::{AtomicU64, Ordering};
        static N: AtomicU64 = AtomicU64::new(0);
        let tmp = std::env::temp_dir().join(format!(
            "pr-resolve-{}-{}-{}",
            label,
            std::process::id(),
            N.fetch_add(1, Ordering::Relaxed),
        ));
        let configdir = tmp.join("config");
        std::fs::create_dir_all(&configdir).unwrap();
        std::fs::write(
            configdir.join(PRCONFIG_FILENAME),
            r#"
url: "https://127.0.0.1:9090"
server:
  tls:
    certificate-data: "Y2VydA=="
    certificate-key-data: "a2V5"
    certificate-authority-data: "Y2E="
"#,
        )
        .unwrap();
        std::fs::write(
            configdir.join(crate::config::template::PRSYSCONFIG_FILENAME),
            syscfg,
        )
        .unwrap();

        let cli = CliArgs {
            kubeconfig: None,
            configdir: Some(configdir.to_string_lossy().into()),
            workdir: Some(tmp.join("work").to_string_lossy().into()),
            logdir: Some(tmp.join("log").to_string_lossy().into()),
            imagesdir: None,
            port: None,
            metrics_port: None,
            logmaxsizemb: 100,
            logmaxfiles: 3,
            loglevel: crate::config::loglevel::LogLevel::Debug,
            heartbeat_file: None,
            heartbeat_secs: crate::config::cli::DEFAULT_HEARTBEAT_SECS,
        };

        let result = resolve_config(&cli);
        let _ = std::fs::remove_dir_all(&tmp);
        result
    }

    #[test]
    fn resolve_config_defaults_cleanup_ttls_when_absent() {
        let c = resolve_with_syscfg("ttl-default", "nodename: \"n\"\n").unwrap();
        assert_eq!(c.ttls, crate::logic::ttl::TtlConfig::default());
    }

    fn provision_tmp(label: &str) -> std::path::PathBuf {
        use std::sync::atomic::{AtomicU64, Ordering};
        static N: AtomicU64 = AtomicU64::new(0);
        let tmp = std::env::temp_dir().join(format!(
            "pr-provision-{label}-{}-{}",
            std::process::id(),
            N.fetch_add(1, Ordering::Relaxed),
        ));
        std::fs::create_dir_all(&tmp).unwrap();
        tmp
    }

    #[test]
    fn provision_mints_a_fresh_stamped_dir_and_never_reuses() {
        let tmp = provision_tmp("fresh");

        let first = provision_run_dir(&tmp);
        assert!(
            first
                .file_name()
                .unwrap()
                .to_string_lossy()
                .starts_with(RUN_DIR_PREFIX),
        );
        assert!(
            !first.exists(),
            "minted, not created — the caller creates it"
        );

        // A restart within the same second must get its own directory, not
        // the previous run's — reuse is the Windows failure this replaces.
        std::fs::create_dir_all(&first).unwrap();
        let second = provision_run_dir(&tmp);
        assert_ne!(first, second);

        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[test]
    fn provision_purges_down_to_the_kept_count_oldest_first() {
        let tmp = provision_tmp("purge");
        for stamp in [
            "run_20260820T000000Z",
            "run_20260821T000000Z",
            "run_20260822T000000Z",
            "run_20260823T000000Z",
        ] {
            std::fs::create_dir_all(tmp.join(stamp).join(PODS_SUBDIR)).unwrap();
        }

        provision_run_dir(&tmp);

        assert!(!tmp.join("run_20260820T000000Z").exists(), "oldest purged");
        assert!(!tmp.join("run_20260821T000000Z").exists());
        assert!(tmp.join("run_20260822T000000Z").exists(), "newest kept");
        assert!(tmp.join("run_20260823T000000Z").exists());

        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[test]
    fn provision_treats_legacy_run_dirs_as_oldest() {
        let tmp = provision_tmp("legacy");
        // The pre-stamp layout: a live `run/` (possibly reused, on Windows)
        // and its rename-rotated predecessors.
        for legacy in ["run", "run.0", "run.1"] {
            std::fs::create_dir_all(tmp.join(legacy)).unwrap();
        }
        std::fs::create_dir_all(tmp.join("run_20260823T000000Z")).unwrap();
        // Not a run dir; must be left alone.
        std::fs::create_dir_all(tmp.join("runway")).unwrap();

        provision_run_dir(&tmp);

        assert!(!tmp.join("run").exists(), "legacy dirs purge first");
        assert!(!tmp.join("run.0").exists());
        assert!(tmp.join("run.1").exists(), "kept: still within the count");
        assert!(tmp.join("run_20260823T000000Z").exists());
        assert!(tmp.join("runway").exists(), "not a run dir");

        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[test]
    fn provision_reaps_the_kept_previous_runs_pod_dirs() {
        use crate::server::config::POD_LOG_SUBDIR;
        let tmp = provision_tmp("reap");

        // A pod dir as a previous incarnation left it.
        let pod_dir = tmp
            .join("run_20260823T000000Z")
            .join(PODS_SUBDIR)
            .join("default_app_uid-1");
        std::fs::create_dir_all(pod_dir.join(POD_WORK_SUBDIR).join("main")).unwrap();
        std::fs::create_dir_all(pod_dir.join(POD_LOG_SUBDIR).join("main")).unwrap();

        provision_run_dir(&tmp);

        assert!(pod_dir.exists(), "the pod dir stays where it was");
        assert!(
            !pod_dir.join(POD_WORK_SUBDIR).exists(),
            "work/ of a previous run is dead weight and is reaped at startup",
        );
        assert!(
            pod_dir.join(POD_LOG_SUBDIR).exists(),
            "logs survive so a crashed run stays diagnosable",
        );

        let _ = std::fs::remove_dir_all(&tmp);
    }

    /// `PRConfig` holds TLS key material and deliberately has no `Debug`,
    /// so error-path tests can't use `unwrap_err`.
    fn expect_syscfg_error(label: &str, syscfg: &str) -> String {
        match resolve_with_syscfg(label, syscfg) {
            Err(e) => e,
            Ok(_) => panic!("expected config resolution to fail"),
        }
    }

    #[test]
    fn resolve_config_rejects_a_legacy_cleanup_config() {
        // prsysconfig.yaml is persistent, so an instance created by a
        // pre-reaper build still has these. Starting up and silently
        // ignoring them would change retention behind the operator's back.
        let err = expect_syscfg_error(
            "legacy",
            "keepFailedPodDir: true\ndeletedPodDirTtlHours: 24\npodArchiveTtlDays: 7\n",
        );
        assert!(err.contains("keepFailedPodDir"), "{err}");
        assert!(err.contains("deletedPodDirTtlHours"), "{err}");
        assert!(err.contains("podArchiveTtlDays"), "{err}");
        // The error names the file so the operator knows what to edit.
        assert!(err.contains("prsysconfig.yaml"), "{err}");
    }

    #[test]
    fn resolve_config_rejects_a_misspelled_setting() {
        let err = expect_syscfg_error("typo", "podDataReapTtlOnPodSucess: \"5m\"\n");
        assert!(err.contains("podDataReapTtlOnPodSucess"), "{err}");
    }

    #[test]
    fn resolve_config_rejects_invalid_cleanup_ttl() {
        // `1d` is not accepted — 24h is the only way to say a day.
        let err = expect_syscfg_error("ttl-bad", "podDirDeleteTtlPodFailureOnPodDelete: \"1d\"\n");
        assert!(
            err.contains("podDirDeleteTtlPodFailureOnPodDelete"),
            "{}",
            err
        );
        assert!(err.contains("1d"), "{}", err);
    }

    #[test]
    fn resolve_config_rejects_unitless_cleanup_ttl() {
        let err = expect_syscfg_error("ttl-unitless", "podDataReapTtlOnPodSuccess: \"0\"\n");
        assert!(err.contains("podDataReapTtlOnPodSuccess"), "{}", err);
    }
}
