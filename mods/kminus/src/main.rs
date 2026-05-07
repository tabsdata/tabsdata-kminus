// Copyright 2026 Tabsdata Inc.

//! km — kminus stack orchestrator.
//!
//! `km create` initializes a new instance folder with a default
//! `kmsysconfig.yaml`. `km start` boots a previously created instance:
//! starts the control-plane, pod-runner, and virtual-kubelet in order,
//! waits for each to be ready, then stays in the foreground handling
//! signals. On shutdown (SIGTERM / SIGINT / Ctrl+C) it stops children
//! in reverse order: virtual-kubelet → pod-runner → control-plane.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Child;

use clap::{Args, Parser, Subcommand};
use file_rotate::{ContentLimit, FileRotate, compression::Compression, suffix::AppendCount};
use tracing::{error, info};

mod config;
mod control_plane;
mod pod_runner;
mod utils;
mod virtual_kubelet;

use config::{KM_SYSCONFIG_FILE, KmSysConfig};
use utils::{
    BIN_KM, CONFIG_DIR, LOG_DIR, check_port, cleanup_stale_pids, kill_child, pid_dir, remove_pid,
    resolve_bin_dir, write_pid,
};

/// Filename of km's own log file under `<logdir>/`.
const KM_LOG_FILE: &str = "km.log";

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

/// km — kminus stack orchestrator.
#[derive(Parser, Debug)]
#[command(name = "km", version, about)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Create a new kminus instance in `--workdir`. The folder must not exist.
    Create(CreateArgs),
    /// Start a previously created kminus instance from `--workdir`.
    Start(StartArgs),
}

/// Create-time arguments. Writes `kmsysconfig.yaml` and exits.
///
/// `kmsysconfig.yaml` records log rotation settings and port assignments.
/// Any port set to 0 in the file is replaced with a free random port at
/// startup. Consecutive port assignment from `--portstart`:
///   portstart+0 = control-plane API server (kube-apiserver)
///   portstart+1 = kube-controller-manager
///   portstart+2 = pod-runner
///   portstart+3 = pod-runner metrics
///   portstart+4 = virtual-kubelet
#[derive(Args, Debug)]
struct CreateArgs {
    /// Working directory for the new instance. Must not exist.
    #[arg(long)]
    workdir: String,

    /// Config directory. Defaults to `<workdir>/config`.
    #[arg(long)]
    configdir: Option<String>,

    /// Starting port for the initial `kmsysconfig.yaml`. If 0, random
    /// free ports are used at every start. Default: 10300.
    #[arg(long, default_value_t = 10300)]
    portstart: u16,
}

/// Start-time arguments. Boots the cluster from an existing instance.
#[derive(Args, Debug)]
struct StartArgs {
    /// Working directory of an existing instance.
    #[arg(long)]
    workdir: String,

    /// Config directory. Defaults to `<workdir>/config`.
    #[arg(long)]
    configdir: Option<String>,

    /// Log directory. Defaults to `<workdir>/log`.
    #[arg(long)]
    logdir: Option<String>,
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
    /// Resolve paths for a brand-new instance. The workdir must not
    /// exist yet — this method creates it. Errors otherwise.
    fn for_create(workdir: &str, configdir: Option<&str>) -> Result<Self, String> {
        let workdir = PathBuf::from(workdir);
        if workdir.exists() {
            return Err(format!("Folder '{}' already exists.", workdir.display()));
        }
        fs::create_dir_all(&workdir)
            .map_err(|e| format!("Cannot create folder '{}': {e}", workdir.display()))?;
        let workdir = fs::canonicalize(&workdir).unwrap_or(workdir);

        let configdir = configdir
            .map(PathBuf::from)
            .unwrap_or_else(|| workdir.join(CONFIG_DIR));
        fs::create_dir_all(&configdir)
            .map_err(|e| format!("Cannot create config folder '{}': {e}", configdir.display()))?;
        let configdir = configdir.canonicalize().unwrap_or(configdir);

        // logdir is a runtime concern; record the default but don't create it here.
        let logdir = workdir.join(LOG_DIR);

        let bin_dir = resolve_bin_dir();
        let pids = pid_dir(&workdir);

        Ok(Self {
            workdir,
            configdir,
            logdir,
            bin_dir,
            pids,
        })
    }

    /// Resolve paths for starting an existing instance. The workdir
    /// must already exist and be canonicalizable.
    fn for_start(
        workdir: &str,
        configdir: Option<&str>,
        logdir: Option<&str>,
    ) -> Result<Self, String> {
        let workdir = PathBuf::from(workdir);
        if !workdir.exists() {
            return Err(format!("Folder '{}' does not exist.", workdir.display()));
        }
        let workdir = fs::canonicalize(&workdir)
            .map_err(|e| format!("Cannot resolve folder '{}': {e}", workdir.display()))?;

        let configdir = configdir
            .map(PathBuf::from)
            .unwrap_or_else(|| workdir.join(CONFIG_DIR));
        fs::create_dir_all(&configdir).ok();
        let configdir = configdir.canonicalize().unwrap_or(configdir);

        let logdir = logdir
            .map(PathBuf::from)
            .unwrap_or_else(|| workdir.join(LOG_DIR));
        fs::create_dir_all(&logdir).ok();
        let logdir = logdir.canonicalize().unwrap_or(logdir);

        let bin_dir = resolve_bin_dir();
        let pids = pid_dir(&workdir);

        Ok(Self {
            workdir,
            configdir,
            logdir,
            bin_dir,
            pids,
        })
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
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
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
    println!(
        "    {BIN_KM}                          pid={}",
        std::process::id()
    );
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
fn verify_ports_free(port_api: &str, port_kcm: &str, port_pr: &str, port_vk: &str) {
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
// Subcommands
// ---------------------------------------------------------------------------

/// Initialize a new kminus instance in `args.workdir`.
fn command_create(args: CreateArgs) {
    let paths = Paths::for_create(&args.workdir, args.configdir.as_deref()).unwrap_or_else(|e| {
        eprintln!("ERROR: {e}");
        std::process::exit(1);
    });

    if let Err(e) = KmSysConfig::create(&paths.configdir, args.portstart) {
        eprintln!("ERROR: {e}");
        std::process::exit(1);
    }

    println!();
    println!("Tabsdata K- (K minus) instance created");
    println!();
    println!("  workdir    {}", paths.workdir.display());
    println!("  configdir  {}", paths.configdir.display());
    println!();
    println!(
        "Edit '{}' to customize ports or log settings, then run:",
        paths.configdir.join(KM_SYSCONFIG_FILE).display()
    );
    println!("  km start --workdir {}", paths.workdir.display());
}

/// Boot the cluster from an existing kminus instance in `args.workdir`.
fn command_start(args: StartArgs) {
    let paths = Paths::for_start(
        &args.workdir,
        args.configdir.as_deref(),
        args.logdir.as_deref(),
    )
    .unwrap_or_else(|e| {
        eprintln!("ERROR: {e}");
        std::process::exit(1);
    });

    let mut sys_cfg = KmSysConfig::load(&paths.configdir).unwrap_or_else(|e| {
        eprintln!(
            "ERROR: '{}' is not a kminus instance: {e}",
            paths.workdir.display()
        );
        eprintln!(
            "Run 'km create --workdir {}' first.",
            paths.workdir.display()
        );
        std::process::exit(1);
    });
    let random_ports_allocated = sys_cfg.resolve_random_ports();

    setup_logging(
        &paths.logdir,
        sys_cfg.log.max_size_mb,
        sys_cfg.log.max_files,
    );

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
        &paths.bin_dir,
        &paths.workdir,
        &paths.configdir,
        &paths.logdir,
        &paths.pids,
        &sys_cfg,
        &port_api,
        &port_kcm,
        &vk_kubeconfig,
    );
    let mut pr = pod_runner::start(
        &paths.bin_dir,
        &paths.workdir,
        &paths.configdir,
        &paths.logdir,
        &paths.pids,
        &sys_cfg,
        &port_pr,
        &port_pr_metrics,
        &kubeconfig,
        &vk_prconfig,
        &mut cp,
    );
    control_plane::cleanup_stale_node_resources(&paths.bin_dir, &kubeconfig);
    let mut vk = virtual_kubelet::start(
        &paths.bin_dir,
        &paths.workdir,
        &paths.configdir,
        &paths.logdir,
        &paths.pids,
        &sys_cfg,
        &port_vk,
        &vk_kubeconfig,
        &vk_prconfig,
    );

    info!("K- ready");

    print_status(
        &paths,
        &cp,
        &pr,
        &vk,
        &port_api,
        &port_kcm,
        &port_pr,
        &port_pr_metrics,
        &port_vk,
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
// Main
// ---------------------------------------------------------------------------

fn main() {
    let cli = Cli::parse();
    match cli.command {
        Command::Create(args) => command_create(args),
        Command::Start(args) => command_start(args),
    }
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
        fs::write(dir.join(KM_SYSCONFIG_FILE), b"keepme").unwrap();

        clear_regenerated_configs(&dir);

        assert!(!dir.join(control_plane::KUBECONFIG_FILE).exists());
        assert!(!dir.join(control_plane::VK_KUBECONFIG_FILE).exists());
        assert!(!dir.join(control_plane::KCM_KUBECONFIG_FILE).exists());
        assert!(!dir.join(pod_runner::VK_PR_CONFIG_FILE).exists());
        assert!(!dir.join(pod_runner::PR_CONFIG_FILE).exists());
        assert!(dir.join(pod_runner::PR_SYSCONFIG_FILE).exists());
        assert!(dir.join(KM_SYSCONFIG_FILE).exists());
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn clear_regenerated_configs_no_op_when_files_absent() {
        let dir = tempdir();
        clear_regenerated_configs(&dir);
        fs::remove_dir_all(&dir).ok();
    }

    // -- Paths::for_create --------------------------------------------------

    #[test]
    fn for_create_errors_when_folder_already_exists() {
        let dir = tempdir();
        let workdir = dir.join("wd");
        fs::create_dir_all(&workdir).unwrap();
        let result = Paths::for_create(&workdir.to_string_lossy(), None);
        assert!(result.is_err());
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn for_create_creates_workdir_and_configdir() {
        let dir = tempdir();
        let workdir = dir.join("wd");
        let paths = Paths::for_create(&workdir.to_string_lossy(), None).unwrap();
        assert!(paths.workdir.exists());
        assert!(paths.configdir.exists());
        assert!(
            paths.pids.ends_with(
                PathBuf::from(utils::DATA_DIR)
                    .join(BIN_KM)
                    .join(utils::PIDS_DIR)
            )
        );
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn for_create_honors_explicit_configdir() {
        let dir = tempdir();
        let workdir = dir.join("wd");
        let cdir = dir.join("custom-config");
        let paths =
            Paths::for_create(&workdir.to_string_lossy(), Some(&cdir.to_string_lossy())).unwrap();
        assert_eq!(paths.configdir, cdir.canonicalize().unwrap());
        fs::remove_dir_all(&dir).ok();
    }

    // -- Paths::for_start ---------------------------------------------------

    #[test]
    fn for_start_errors_when_workdir_missing() {
        let dir = tempdir();
        let workdir = dir.join("does-not-exist");
        let result = Paths::for_start(&workdir.to_string_lossy(), None, None);
        assert!(result.is_err());
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn for_start_resolves_existing_workdir() {
        let dir = tempdir();
        let workdir = dir.join("wd");
        fs::create_dir_all(&workdir).unwrap();
        let paths = Paths::for_start(&workdir.to_string_lossy(), None, None).unwrap();
        assert_eq!(paths.workdir, workdir.canonicalize().unwrap());
        assert!(paths.configdir.exists());
        assert!(paths.logdir.exists());
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn for_start_honors_explicit_configdir_and_logdir() {
        let dir = tempdir();
        let workdir = dir.join("wd");
        fs::create_dir_all(&workdir).unwrap();
        let cdir = dir.join("custom-config");
        let ldir = dir.join("custom-log");
        let paths = Paths::for_start(
            &workdir.to_string_lossy(),
            Some(&cdir.to_string_lossy()),
            Some(&ldir.to_string_lossy()),
        )
        .unwrap();
        assert_eq!(paths.configdir, cdir.canonicalize().unwrap());
        assert_eq!(paths.logdir, ldir.canonicalize().unwrap());
        fs::remove_dir_all(&dir).ok();
    }
}
