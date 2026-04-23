// Copyright 2026 Tabsdata Inc.

//! km — kminus stack orchestrator.
//!
//! Starts the control-plane, pod-runner, and virtual-kubelet in order,
//! waits for each to be ready, then stays in the foreground handling
//! signals. On shutdown (SIGTERM / SIGINT / Ctrl+C) it stops children
//! in reverse order: virtual-kubelet → pod-runner → control-plane.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Child;

use clap::Parser;
use file_rotate::{
    compression::Compression, suffix::AppendCount, ContentLimit, FileRotate,
};
use tracing::{error, info};

mod config;
mod control_plane;
mod pod_runner;
mod utils;
mod virtual_kubelet;

use config::KmSysConfig;
use utils::{
    check_port, cleanup_stale_pids, kill_child, pid_dir, remove_pid,
    resolve_bin_dir, write_pid, BIN_KM, CONFIG_DIR, LOG_DIR,
};

/// Filename of km's own log file under `<logdir>/`.
const KM_LOG_FILE: &str = "km.log";

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

/// km — kminus stack orchestrator.
///
/// Starts control-plane, pod-runner, and virtual-kubelet as child
/// processes. All binaries are resolved from the same directory as
/// the km binary itself.
///
/// Log settings and ports are stored in `kmsysconfig.yaml` in the
/// config directory — created on first startup with defaults based
/// on `--portstart`, user-editable thereafter. Any port set to 0 in
/// the file is replaced with a free random port at startup.
#[derive(Parser, Debug)]
#[command(name = "km", version, about)]
struct Cli {
    /// Working directory. Component data dirs are created under this.
    #[arg(long, default_value = ".")]
    workdir: String,

    /// Config directory. Shared across all components.
    #[arg(long)]
    configdir: Option<String>,

    /// Log directory.
    #[arg(long)]
    logdir: Option<String>,

    /// Starting port for the initial kmsysconfig.yaml (only used
    /// when the file does not yet exist). Consecutive assignment:
    ///   portstart+0 = control-plane API server (kube-apiserver)
    ///   portstart+1 = kube-controller-manager
    ///   portstart+2 = pod-runner
    ///   portstart+3 = pod-runner metrics
    ///   portstart+4 = virtual-kubelet
    /// If 0, random free ports are used. Default: 10300.
    #[arg(long, default_value_t = 10300)]
    portstart: u16,
}

// ---------------------------------------------------------------------------
// Resolved runtime paths
// ---------------------------------------------------------------------------

/// The set of paths needed by every subprocess and the lifecycle code.
struct Paths {
    workdir: PathBuf,
    configdir: PathBuf,
    logdir: PathBuf,
    bin_dir: PathBuf,
    pids: PathBuf,
}

impl Paths {
    /// Resolve every directory from CLI input. Creates and canonicalizes
    /// `workdir`, `configdir`, `logdir`. Locates the binary directory.
    fn resolve(cli: &Cli) -> Self {
        let workdir = fs::canonicalize(&cli.workdir).unwrap_or_else(|_| {
            fs::create_dir_all(&cli.workdir).ok();
            PathBuf::from(&cli.workdir)
                .canonicalize()
                .expect("Cannot resolve workdir")
        });

        let configdir = cli
            .configdir
            .as_deref()
            .map(PathBuf::from)
            .unwrap_or_else(|| workdir.join(CONFIG_DIR));
        fs::create_dir_all(&configdir).ok();
        let configdir = configdir.canonicalize().unwrap_or(configdir);

        let logdir = cli
            .logdir
            .as_deref()
            .map(PathBuf::from)
            .unwrap_or_else(|| workdir.join(LOG_DIR));
        fs::create_dir_all(&logdir).ok();
        let logdir = logdir.canonicalize().unwrap_or(logdir);

        let bin_dir = resolve_bin_dir();
        let pids = pid_dir(&workdir);

        Self {
            workdir,
            configdir,
            logdir,
            bin_dir,
            pids,
        }
    }
}

// ---------------------------------------------------------------------------
// Logging setup
// ---------------------------------------------------------------------------

/// Initialize tracing to write to `<logdir>/km.log` with size-based
/// rotation. Idempotent — safe to call once per process.
fn setup_logging(logdir: &Path, max_size_mb: u32, max_files: u32) -> PathBuf {
    let km_log_path = logdir.join(KM_LOG_FILE);
    let max_files = max_files.max(1) as usize;
    let writer = FileRotate::new(
        &km_log_path,
        AppendCount::new(max_files - 1),
        ContentLimit::Bytes((max_size_mb as usize) * 1024 * 1024),
        Compression::None,
        None,
    );
    let writer = std::sync::Mutex::new(writer);
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .with_writer(writer)
        .with_ansi(false)
        .init();
    info!(
        log = %km_log_path.display(),
        max_size_mb,
        max_files,
        "Logging initialized (size-based rotation)",
    );
    km_log_path
}

// ---------------------------------------------------------------------------
// Status banner
// ---------------------------------------------------------------------------

/// Print the ready banner to stdout: PIDs, ports, capacity, existing
/// pods, and important paths. Best-effort: missing data is rendered
/// as "?" or skipped.
fn print_status(
    paths: &Paths,
    cp: &Child,
    pr: &Child,
    vk: &Child,
    port_api: &str,
    port_kcm: &str,
    port_pr: &str,
    port_pr_metrics: &str,
    port_vk: &str,
    kubeconfig: &Path,
) {
    let cp_children = control_plane::read_children(&paths.workdir);
    let (cap_cpu, cap_mem, cap_pods) = pod_runner::read_capacity(&paths.configdir);

    println!("K- ready");
    println!();
    println!("  Components:");
    println!(
        "    {}               pid={:<7}port={}  kcm-port={}",
        control_plane::BIN,
        cp.id(),
        port_api,
        port_kcm
    );
    for (name, pid) in &cp_children {
        println!("      {:<24}  pid={}", name, pid);
    }
    println!(
        "    {}                  pid={:<7}port={}  metrics-port={}",
        pod_runner::BIN,
        pr.id(),
        port_pr,
        port_pr_metrics
    );
    println!(
        "    {}             pid={:<7}port={}",
        virtual_kubelet::BIN,
        vk.id(),
        port_vk
    );
    println!("    {BIN_KM}                          pid={}", std::process::id());
    println!();
    println!(
        "  Capacity: cpu={} memoryGB={} pods={}",
        cap_cpu, cap_mem, cap_pods
    );
    println!();

    // Existing pods on this node (best-effort kubectl query).
    let pods = control_plane::list_pods_on_node(&paths.bin_dir, kubeconfig, "tdknode");
    if pods.is_empty() {
        println!("  Pods: (none)");
    } else {
        println!("  Pods ({}):", pods.len());
        for line in &pods {
            println!("    {}", line);
        }
    }
    println!();

    println!("  Directories:");
    println!("    workdir    {}", paths.workdir.display());
    println!("    configdir  {}", paths.configdir.display());
    println!("    logdir     {}", paths.logdir.display());
    println!();
    println!("  export KUBECONFIG={}", kubeconfig.display());
    println!();
    println!("Press Ctrl+C to stop");

    info!(
        kubeconfig = %kubeconfig.display(),
        "export KUBECONFIG={}",
        kubeconfig.display()
    );
}

// ---------------------------------------------------------------------------
// Shutdown
// ---------------------------------------------------------------------------

/// Stop all subprocesses in reverse start order and remove their PID
/// files. Cleans up the km PID dir last.
fn shutdown(paths: &Paths, vk: &mut Child, pr: &mut Child, cp: &mut Child) {
    info!("Shutting down...");

    kill_child(vk, virtual_kubelet::BIN);
    remove_pid(&paths.pids, virtual_kubelet::BIN);

    kill_child(pr, pod_runner::BIN);
    remove_pid(&paths.pids, pod_runner::BIN);

    kill_child(cp, control_plane::BIN);
    remove_pid(&paths.pids, control_plane::BIN);

    let _ = fs::remove_dir_all(control_plane::pids_dir_in(&paths.workdir));
    let _ = fs::remove_dir_all(virtual_kubelet::pids_dir_in(&paths.workdir));

    remove_pid(&paths.pids, BIN_KM);
    let _ = fs::remove_dir(&paths.pids);

    info!("Stopped");
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Verify all non-randomly-allocated ports are free before launching
/// any subprocess. Logs and exits on conflict.
fn verify_ports_free(
    port_api: &str,
    port_kcm: &str,
    port_pr: &str,
    port_vk: &str,
) {
    let cp_api_label = format!("{} API server", control_plane::BIN);
    let port_checks: [(&str, &str); 4] = [
        (port_api, &cp_api_label),
        (port_kcm, control_plane::KCM),
        (port_pr, pod_runner::BIN),
        (port_vk, virtual_kubelet::BIN),
    ];
    let mut port_errors = Vec::new();
    for (port, name) in &port_checks {
        if let Err(e) = check_port(port, name) {
            port_errors.push(e);
        }
    }
    if !port_errors.is_empty() {
        for e in &port_errors {
            error!("{}", e);
        }
        error!("Cannot start: {} port(s) in use", port_errors.len());
        std::process::exit(1);
    }
}

/// Delete config files that are regenerated on every startup so that
/// `wait_for_file` can detect fresh creation. `prsysconfig.yaml` is
/// preserved (user-editable).
fn clear_regenerated_configs(configdir: &Path) {
    for name in [
        control_plane::KUBECONFIG_FILE,
        control_plane::VK_KUBECONFIG_FILE,
        control_plane::KCM_KUBECONFIG_FILE,
        pod_runner::VK_PR_CONFIG_FILE,
        pod_runner::PR_CONFIG_FILE,
    ] {
        let p = configdir.join(name);
        if p.exists() {
            info!(file = %p.display(), "Removing stale config file");
            let _ = fs::remove_file(&p);
        }
    }
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

fn main() {
    let cli = Cli::parse();
    let paths = Paths::resolve(&cli);

    // Load (or create) the system config from kmsysconfig.yaml.
    let mut sys_cfg = KmSysConfig::load_or_create(&paths.configdir, cli.portstart)
        .unwrap_or_else(|e| {
            eprintln!("ERROR: {}", e);
            std::process::exit(1);
        });
    let random_ports_allocated = sys_cfg.resolve_random_ports();

    setup_logging(&paths.logdir, sys_cfg.log.max_size_mb, sys_cfg.log.max_files);

    let port_api = sys_cfg.ports.api.to_string();
    let port_kcm = sys_cfg.ports.kcm.to_string();
    let port_pr = sys_cfg.ports.pr.to_string();
    let port_pr_metrics = sys_cfg.ports.pr_metrics.to_string();
    let port_vk = sys_cfg.ports.vk.to_string();

    if !random_ports_allocated {
        verify_ports_free(&port_api, &port_kcm, &port_pr, &port_vk);
    }

    println!();
    if random_ports_allocated {
        println!("Tabsdata K- (K minus) starting (random ports)");
    } else {
        println!("Tabsdata K- (K minus) starting");
    }
    println!();

    info!(
        workdir = %paths.workdir.display(),
        configdir = %paths.configdir.display(),
        logdir = %paths.logdir.display(),
        bindir = %paths.bin_dir.display(),
        "Starting K-"
    );

    cleanup_stale_pids(&paths.pids);
    fs::create_dir_all(&paths.pids).ok();
    write_pid(&paths.pids, BIN_KM, std::process::id());

    let vk_kubeconfig = paths.configdir.join(control_plane::VK_KUBECONFIG_FILE);
    let vk_prconfig = paths.configdir.join(pod_runner::VK_PR_CONFIG_FILE);
    let kubeconfig = paths.configdir.join(control_plane::KUBECONFIG_FILE);

    clear_regenerated_configs(&paths.configdir);

    // Start subprocesses in dependency order.
    let mut cp = control_plane::start(
        &paths.bin_dir, &paths.workdir, &paths.configdir, &paths.logdir,
        &paths.pids, &sys_cfg, &port_api, &port_kcm, &vk_kubeconfig,
    );
    let mut pr = pod_runner::start(
        &paths.bin_dir, &paths.workdir, &paths.configdir, &paths.logdir,
        &paths.pids, &sys_cfg, &port_pr, &port_pr_metrics,
        &kubeconfig, &vk_prconfig, &mut cp,
    );
    control_plane::cleanup_stale_node_resources(&paths.bin_dir, &kubeconfig);
    let mut vk = virtual_kubelet::start(
        &paths.bin_dir, &paths.workdir, &paths.configdir, &paths.logdir,
        &paths.pids, &sys_cfg, &port_vk, &vk_kubeconfig, &vk_prconfig,
    );

    info!("K- ready");

    print_status(
        &paths, &cp, &pr, &vk,
        &port_api, &port_kcm, &port_pr, &port_pr_metrics, &port_vk,
        &kubeconfig,
    );

    // Wait for shutdown signal.
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        tokio::signal::ctrl_c().await.ok();
    });

    shutdown(&paths, &mut vk, &mut pr, &mut cp);
}

// ---------------------------------------------------------------------------
// Tests for thin helpers in this module. Subprocess launchers and
// status gathering live in their own modules with their own tests.
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;

    fn tempdir() -> PathBuf {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let dir = env::temp_dir().join(format!("km-main-{}-{}", std::process::id(), n));
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    // -- clear_regenerated_configs -----------------------------------------

    #[test]
    fn clear_regenerated_configs_removes_only_listed_files() {
        let dir = tempdir();
        for name in [
            control_plane::KUBECONFIG_FILE,
            control_plane::VK_KUBECONFIG_FILE,
            control_plane::KCM_KUBECONFIG_FILE,
            pod_runner::VK_PR_CONFIG_FILE,
            pod_runner::PR_CONFIG_FILE,
        ] {
            fs::write(dir.join(name), b"x").unwrap();
        }
        // Files that must be preserved.
        fs::write(dir.join(pod_runner::PR_SYSCONFIG_FILE), b"keepme").unwrap();
        fs::write(dir.join(config::KM_SYSCONFIG_FILE), b"keepme").unwrap();

        clear_regenerated_configs(&dir);

        assert!(!dir.join(control_plane::KUBECONFIG_FILE).exists());
        assert!(!dir.join(control_plane::VK_KUBECONFIG_FILE).exists());
        assert!(!dir.join(control_plane::KCM_KUBECONFIG_FILE).exists());
        assert!(!dir.join(pod_runner::VK_PR_CONFIG_FILE).exists());
        assert!(!dir.join(pod_runner::PR_CONFIG_FILE).exists());
        assert!(dir.join(pod_runner::PR_SYSCONFIG_FILE).exists());
        assert!(dir.join(config::KM_SYSCONFIG_FILE).exists());
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn clear_regenerated_configs_no_op_when_files_absent() {
        let dir = tempdir();
        clear_regenerated_configs(&dir);
        fs::remove_dir_all(&dir).ok();
    }

    // -- Paths::resolve -----------------------------------------------------

    #[test]
    fn paths_resolve_creates_workdir_and_subdirs() {
        let dir = tempdir();
        let workdir = dir.join("wd");
        let cli = Cli {
            workdir: workdir.to_string_lossy().to_string(),
            configdir: None,
            logdir: None,
            portstart: 10300,
        };
        let paths = Paths::resolve(&cli);
        assert!(paths.workdir.exists());
        assert!(paths.configdir.exists());
        assert!(paths.logdir.exists());
        assert!(paths.pids.ends_with(
            PathBuf::from(utils::DATA_DIR)
                .join(BIN_KM)
                .join(utils::PIDS_DIR)
        ));
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn paths_resolve_honors_explicit_configdir_and_logdir() {
        let dir = tempdir();
        let workdir = dir.join("wd");
        let cdir = dir.join("custom-config");
        let ldir = dir.join("custom-log");
        let cli = Cli {
            workdir: workdir.to_string_lossy().to_string(),
            configdir: Some(cdir.to_string_lossy().to_string()),
            logdir: Some(ldir.to_string_lossy().to_string()),
            portstart: 10300,
        };
        let paths = Paths::resolve(&cli);
        assert_eq!(paths.configdir, cdir.canonicalize().unwrap());
        assert_eq!(paths.logdir, ldir.canonicalize().unwrap());
        fs::remove_dir_all(&dir).ok();
    }
}
