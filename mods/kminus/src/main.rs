// Copyright 2026 Tabsdata Inc.

//! km — kminus stack orchestrator.
//!
//! `km create` initializes a new instance folder with a default
//! `kmsysconfig.yaml`. `km start` boots a previously created instance:
//! starts the control-plane, pod-runner, and virtual-kubelet in order,
//! waits for each to be ready, then stays in the foreground handling
//! signals. On shutdown (SIGTERM / SIGINT / Ctrl+C) it stops children
//! in reverse order: virtual-kubelet → pod-runner → control-plane.
//! `km stop` terminates a running instance from another terminal by
//! signalling the foreground `km start` process. `km status` reports
//! which components are alive. `km restart` stops then starts.

use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use clap::{Args, Parser, Subcommand};
use file_rotate::{ContentLimit, FileRotate, compression::Compression, suffix::AppendCount};
use tracing::{debug, error, info, warn};
use tracing_subscriber::filter::LevelFilter;

mod config;
mod exit;
mod loglevel;
mod process;
mod system;

use loglevel::LogLevel;

use config::{KM_SYSCONFIG_FILE, KmHeartbeat, KmPorts, KmSysConfig};
use exit::ExitReason;
use kcommon::scan::{kill_strays_of_dead_runs, kill_until_clean};
use kcommon::token::{KM_KILL_TOKEN_VAR, Run};
use kcommon::tracked::Exit;
use process::{control_plane, pod_runner, virtual_kubelet};
use system::components::Components;
use system::file::{CONFIG_DIR, LOG_DIR};
use system::heartbeat::{self, Beat, HeartbeatMonitor, heartbeat_file};
use system::messages;
#[cfg(target_os = "macos")]
use system::pid::kill_descendants;
use system::pid::{
    BIN_KM, Recorded, cleanup_stale_pids, pids_dir, recorded, remove_dir_all_best_effort,
    remove_file_best_effort, remove_pid, stop_recorded, write_pid,
};
use system::port::{PortCheck, check_ports, port_failures, probe_ports};
use system::startup;

/// Filename of km's own log file under `<logdir>/`.
const KM_LOG_FILE: &str = "km.log";

/// Default `--portstart` for `km create`.
const DEFAULT_PORTSTART: u16 = 10300;

/// Development-only env var honored by the control-plane subprocess: when set
/// to "true", the admin token written to `kubeconfig.yaml` is derived
/// deterministically from the current username instead of being random. km
/// inherits its environment to the control-plane, so setting it before
/// `km start` is enough; km reads it here only to warn — on both `km create`
/// and every `km start`/`km restart` — that the fixed dev token is in use.
const FIXED_AUTH_TOKEN_ENV: &str = "TD_DEV_FIXED_AUTH_TOKEN";

fn canonicalize(label: &str, path: &Path) -> std::io::Result<PathBuf> {
    let result = dunce::canonicalize(path);
    if let Ok(resolved) = &result {
        debug!(site = label, input = %path.display(), output = %resolved.display(), "Canonicalized path.");
    }
    result
}

/// Whether the [`FIXED_AUTH_TOKEN_ENV`] dev override is enabled.
fn fixed_auth_token_enabled() -> bool {
    env_flag_enabled(std::env::var(FIXED_AUTH_TOKEN_ENV).ok().as_deref())
}

/// Whether an env-var value denotes an enabled boolean flag: present and
/// equal to "true" (case-insensitive). Pure so it is testable without
/// mutating process-global environment state.
fn env_flag_enabled(value: Option<&str>) -> bool {
    value.is_some_and(|v| v.eq_ignore_ascii_case("true"))
}

/// Print a prominent warning (stdout + log) that the development fixed-token
/// override is active, so the fixed, non-secret `admin` credential in
/// `kubeconfig.yaml` is never mistaken for a secure default.
fn warn_fixed_auth_token() {
    println!(
        "  WARNING: {FIXED_AUTH_TOKEN_ENV}=true — the 'admin' token in kubeconfig.yaml is a fixed,"
    );
    println!("           non-secret value derived from your username. For development only.");
    warn!(
        "{FIXED_AUTH_TOKEN_ENV}=true: 'admin' token in kubeconfig.yaml is a fixed dev value derived from the username"
    );
}

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
    /// Stop a running kminus instance in `--workdir`.
    Stop(StopArgs),
    /// Report the status of a kminus instance in `--workdir`.
    Status(StatusArgs),
    /// Restart a kminus instance in `--workdir`: stop then start.
    Restart(StartArgs),
}

/// Create-time arguments. Verifies the ports and target folders, then
/// writes `kmsysconfig.yaml` and exits.
///
/// `kmsysconfig.yaml` records log rotation settings and port assignments.
/// Any port set to 0 in the file is replaced with a free random port at
/// startup. Consecutive port assignment from `--portstart`:
///   portstart+0 = control-plane API server (kube-apiserver)
///   portstart+1 = kube-controller-manager
///   portstart+2 = pod-runner
///   portstart+3 = pod-runner metrics
///   portstart+4 = virtual-kubelet
///   portstart+5 = etcd (client)
///   portstart+6 = etcd (peer, bound but unused by a single node)
#[derive(Args, Debug)]
struct CreateArgs {
    /// Working directory for the new instance. Must not exist.
    #[arg(long)]
    workdir: String,

    /// Config directory. Defaults to `<workdir>/config`.
    #[arg(long)]
    configdir: Option<String>,

    /// Starting port for the initial `kmsysconfig.yaml`. The whole range
    /// must be free, or create fails without writing anything. If 0,
    /// random free ports are used at every start. Default: 10300.
    #[arg(long, default_value_t = DEFAULT_PORTSTART)]
    portstart: u16,

    /// Run the checks only: the port range must be free and neither the
    /// workdir nor the configdir may exist. Creates nothing and reports
    /// what would be created.
    #[arg(long)]
    dryrun: bool,
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

    /// Root directory for resolving relative container `image` paths
    /// in pod specs. Forwarded to the pod-runner as `--imagesdir`.
    /// When unset, the pod-runner falls back to `imagesDir` in
    /// `prsysconfig.yaml`, then to `./images`.
    #[arg(long)]
    imagesdir: Option<String>,

    /// Level for km's own log file and, forwarded verbatim, for the
    /// pod-runner's. Independent of `RUST_LOG`, which is ignored.
    #[arg(long, value_enum, default_value_t = LogLevel::Debug)]
    loglevel: LogLevel,

    /// Delete every workload a previous run left in the cluster —
    /// cronjobs, deployments, replicasets, jobs, and their pods — after
    /// the control-plane is up and before the virtual-kubelet starts, so
    /// nothing stale is dispatched only to be killed. Secrets, configmaps
    /// and every other non-workload resource are left untouched.
    #[arg(long)]
    purge: bool,
}

/// Stop-time arguments. Terminates a running instance.
#[derive(Args, Debug)]
struct StopArgs {
    /// Working directory of the running instance.
    #[arg(long)]
    workdir: String,
}

/// Status-time arguments. Reports which components are alive.
#[derive(Args, Debug)]
struct StatusArgs {
    /// Working directory of the instance.
    #[arg(long)]
    workdir: String,
}

// ---------------------------------------------------------------------------
// Resolved runtime paths
// ---------------------------------------------------------------------------

/// The set of paths needed by every subprocess and the lifecycle code.
#[derive(Clone)]
struct Paths {
    workdir: PathBuf,
    configdir: PathBuf,
    logdir: PathBuf,
    pids: PathBuf,
}

/// Folders a `km create` would create, resolved but not yet touched.
///
/// Resolving is kept separate from creating so that `km create` can run
/// every check — ports included — before anything lands on disk, and so
/// that `--dryrun` can run the same checks and create nothing.
struct CreateTargets {
    workdir: PathBuf,
    configdir: PathBuf,
}

impl CreateTargets {
    /// Resolve `--workdir` and `--configdir` without touching the
    /// filesystem. Errors if either folder already exists.
    fn resolve(workdir: &str, configdir: Option<&str>) -> Result<Self, String> {
        let workdir = PathBuf::from(workdir);
        if workdir.exists() {
            return Err(format!("Folder '{}' already exists.", workdir.display()));
        }
        let configdir = configdir
            .map(PathBuf::from)
            .unwrap_or_else(|| workdir.join(CONFIG_DIR));
        if configdir.exists() {
            return Err(format!(
                "Config folder '{}' already exists.",
                configdir.display()
            ));
        }
        Ok(Self { workdir, configdir })
    }
}

impl Paths {
    /// Create the folders of a brand-new instance and resolve the rest
    /// of its paths. Both targets are expected to be absent — that is
    /// checked by [`CreateTargets::resolve`].
    fn for_create(targets: &CreateTargets) -> Result<Self, String> {
        fs::create_dir_all(&targets.workdir)
            .map_err(|e| format!("Cannot create folder '{}': {e}", targets.workdir.display()))?;
        let workdir = canonicalize("create.workdir", &targets.workdir)
            .unwrap_or_else(|_| targets.workdir.clone());

        fs::create_dir_all(&targets.configdir).map_err(|e| {
            format!(
                "Cannot create config folder '{}': {e}",
                targets.configdir.display()
            )
        })?;
        let configdir = canonicalize("create.configdir", &targets.configdir)
            .unwrap_or_else(|_| targets.configdir.clone());

        // logdir is a runtime concern; record the default but don't create it here.
        let logdir = workdir.join(LOG_DIR);

        let pids = pids_dir(&workdir, BIN_KM);

        Ok(Self {
            workdir,
            configdir,
            logdir,
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
        let workdir = canonicalize("start.workdir", &workdir)
            .map_err(|e| format!("Cannot resolve folder '{}': {e}", workdir.display()))?;

        let configdir = configdir
            .map(PathBuf::from)
            .unwrap_or_else(|| workdir.join(CONFIG_DIR));
        fs::create_dir_all(&configdir).ok();
        let configdir = canonicalize("start.configdir", &configdir).unwrap_or(configdir);

        let logdir = logdir
            .map(PathBuf::from)
            .unwrap_or_else(|| workdir.join(LOG_DIR));
        fs::create_dir_all(&logdir).ok();
        let logdir = canonicalize("start.logdir", &logdir).unwrap_or(logdir);

        let pids = pids_dir(&workdir, BIN_KM);

        Ok(Self {
            workdir,
            configdir,
            logdir,
            pids,
        })
    }

    /// Resolve paths for stop/status of an existing instance. Only the
    /// workdir and pids directory are meaningful here; configdir/logdir
    /// are set to their defaults but not required to exist.
    fn for_existing(workdir: &str) -> Result<Self, String> {
        let workdir = PathBuf::from(workdir);
        if !workdir.exists() {
            return Err(format!("Folder '{}' does not exist.", workdir.display()));
        }
        let workdir = canonicalize("existing.workdir", &workdir)
            .map_err(|e| format!("Cannot resolve folder '{}': {e}", workdir.display()))?;
        let configdir = workdir.join(CONFIG_DIR);
        let logdir = workdir.join(LOG_DIR);
        let pids = pids_dir(&workdir, BIN_KM);
        Ok(Self {
            workdir,
            configdir,
            logdir,
            pids,
        })
    }
}

// ---------------------------------------------------------------------------
// Logging setup
// ---------------------------------------------------------------------------

/// Initialize tracing to write to `<logdir>/km.log` with size-based
/// rotation. Idempotent — safe to call once per process.
fn setup_logging(logdir: &Path, max_size_mb: u32, max_files: u32, loglevel: LogLevel) -> PathBuf {
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
            tracing_subscriber::EnvFilter::default()
                .add_directive(LevelFilter::from(loglevel).into()),
        )
        .with_writer(writer)
        .with_ansi(false)
        .init();
    info!(
        log = %km_log_path.display(),
        max_size_mb,
        max_files,
        %loglevel,
        "Logging initialized (size-based rotation)",
    );
    km_log_path
}

// ---------------------------------------------------------------------------
// Startup progress
// ---------------------------------------------------------------------------

/// A startup phase in progress: its label and start time, returned by
/// [`step`] and closed by [`step_done`].
struct Step {
    label: &'static str,
    start: Instant,
}

/// Announce the start of a phase to stdout and begin timing it.
fn step(label: &'static str) -> Step {
    println!("  {label} ...");
    info!("{label}");
    Step {
        label,
        start: Instant::now(),
    }
}

/// Report a phase complete, with how long it took.
fn step_done(step: Step) {
    let secs = step.start.elapsed().as_secs_f64();
    println!("  {} done ({:.1}s)", step.label, secs);
}

// ---------------------------------------------------------------------------
// Status banner
// ---------------------------------------------------------------------------

/// Print the ready banner to stdout: PIDs, ports, capacity, existing
/// pods, and important paths. Best-effort: missing data is rendered
/// as "?" or skipped.
// Launcher-style: every parameter is an independent value the caller
// already has, so bundling them into a struct moves the list rather
// than shortening it.
#[allow(clippy::too_many_arguments)]
fn print_status(paths: &Paths, components: &Components, ports: &KmPorts, kubeconfig: &Path) {
    /// A registered component's PID, or a placeholder if it somehow is not
    /// there — a banner is not worth failing a started stack over.
    fn pid_of(components: &Components, name: &str) -> String {
        components
            .pid(name)
            .map_or_else(|| "?".to_string(), |pid| pid.to_string())
    }

    let cp_children = control_plane::read_children(&paths.workdir);
    let (cap_cpu, cap_mem, cap_pods) = pod_runner::read_capacity(&paths.configdir);

    println!("K- ready");
    println!();
    println!("  Components:");
    println!(
        "    {}               pid={:<7}port={}  kcm-port={}",
        control_plane::BIN,
        pid_of(components, control_plane::BIN),
        ports.api,
        ports.kcm
    );
    for (name, pid) in &cp_children {
        // Trimmed so a child with no known ports leaves no trailing padding.
        let line = format!(
            "      {:<24}  pid={:<7}{}",
            name,
            pid,
            child_ports(name, ports)
        );
        println!("{}", line.trim_end());
    }
    println!(
        "    {}                  pid={:<7}port={}  metrics-port={}",
        pod_runner::BIN,
        pid_of(components, pod_runner::BIN),
        ports.pr,
        ports.pr_metrics
    );
    println!(
        "    {}             pid={:<7}port={}",
        virtual_kubelet::BIN,
        pid_of(components, virtual_kubelet::BIN),
        ports.vk
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

    // Existing pods on this node (best-effort kmc query).
    let pods = control_plane::list_pods_on_node(kubeconfig, control_plane::NODE_NAME);
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

    info!(
        kubeconfig = %kubeconfig.display(),
        "export KUBECONFIG={}",
        kubeconfig.display()
    );
}

// ---------------------------------------------------------------------------
// Shutdown
// ---------------------------------------------------------------------------

/// Install signal-driven shutdown and block until it is armed.
///
/// A dedicated thread installs the SIGINT/SIGTERM (Ctrl+C/Ctrl+Break on
/// Windows) handlers, signals readiness, then waits for a signal. On
/// signal it tears the whole stack down from the recorded PID files and
/// exits the process. This is installed *before* any subprocess is
/// spawned so that a Ctrl+C during the multi-second startup still drives
/// an orderly teardown instead of killing km outright (the default
/// disposition) and orphaning whatever has already been launched — which
/// matters precisely because the children run in their own process groups
/// and so never receive the terminal's signal themselves.
///
/// Returns once the handlers are installed, closing the window where an
/// early signal would hit the default disposition.
fn install_signal_shutdown(paths: Paths, ports: KmPorts) {
    let (armed_tx, armed_rx) = std::sync::mpsc::channel::<()>();
    let workdir = paths.workdir.clone();
    std::thread::Builder::new()
        .name("km-signal".into())
        .spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            let signal = rt.block_on(arm_and_wait_for_shutdown(armed_tx, workdir));
            if !exit::claim() {
                // A component failure already owns the exit; let it finish
                // rather than reporting a second reason over the top.
                return;
            }
            exit::report(&ExitReason::Signal(signal));
            shutdown_from_pids(&paths, Some(&ports));
            std::process::exit(130);
        })
        .expect("spawn km-signal thread");
    // Proceed only once the handlers are installed.
    let _ = armed_rx.recv();
}

/// Stop the whole stack using the recorded PID files, in reverse start
/// order, then remove the PID files. Runs on the signal thread, so it
/// works at any point — including mid-startup, before the per-child
/// `Child` handles exist. Terminating the control-plane also reaps its
/// children (etcd, kube-apiserver, kube-controller-manager) via its
/// process-tree kill. A final descendant sweep catches anything spawned
/// but not yet recorded in a PID file.
/// Grace before km escalates to SIGKILL. The pod-runner needs much more than
/// the others: on shutdown it drains every pod (SIGTERM each container, then
/// wait up to that pod's termination grace, then SIGKILL) and only then exits.
/// A pod like the database also runs its own stop on that SIGTERM (stopping the
/// embedded postgres it owns). Killing pod-runner after only a few seconds
/// SIGKILLs it mid-drain, so those pod stops never complete and their servers
/// are orphaned. The other components own no pods and stop promptly.
///
/// Must stay above the pod-runner's `defaultGracePeriodSecs` (120), or a
/// single default-grace pod outwaits pod-runner's own grace and the drain
/// is cut short by exactly the SIGKILL described above.
const POD_RUNNER_SHUTDOWN_GRACE: Duration = Duration::from_secs(150);
const DEFAULT_SHUTDOWN_GRACE: Duration = Duration::from_secs(5);

/// Filename of the seed file under `<workdir>/data/km/`, next to the PID
/// files: the one piece of kill-token state that crosses restarts, so the
/// next run can reclaim whatever this one leaves behind.
const KM_RUN_TOKEN_FILE: &str = "run-token";

/// How many fresh-scan sweep rounds a teardown gives stragglers. Round two
/// exists for processes forked between a scan and its signals; round three
/// is margin. An adversarial forker exhausts this and is reported.
const SWEEP_ROUNDS: usize = 3;

/// Path of the run-token file for the instance at `workdir`.
fn run_token_file(workdir: &Path) -> PathBuf {
    workdir
        .join(system::pid::DATA_DIR)
        .join(BIN_KM)
        .join(KM_RUN_TOKEN_FILE)
}

fn shutdown_grace_for(name: &str) -> Duration {
    if name == pod_runner::BIN {
        POD_RUNNER_SHUTDOWN_GRACE
    } else {
        DEFAULT_SHUTDOWN_GRACE
    }
}

/// How long km waits after the stack is down for its ports to become
/// bindable again. Generous: the point is to outlast the kernel finishing
/// with a socket, not to give a leaked one time to go away.
const PORT_RELEASE_WAIT: Duration = Duration::from_secs(30);

/// How often the release wait re-tests the ports.
const PORT_RELEASE_TICK: Duration = Duration::from_millis(250);

/// Re-test `labeled` until every port is free or `deadline` passes, and
/// return the last observation either way.
///
/// Probed rather than checked: the same port assigned to two components is a
/// configuration error, and treating the repeat as a duplicate here would
/// only stop it ever reading as released.
fn await_port_release(labeled: &[(u16, String)], deadline: Instant) -> Vec<PortCheck> {
    let mut checks = probe_ports(labeled);
    while !checks.iter().all(PortCheck::is_free) && Instant::now() < deadline {
        std::thread::sleep(PORT_RELEASE_TICK);
        checks = probe_ports(labeled);
    }
    checks
}

/// Wait for every port the stack bound to become bindable again, then report
/// what came back.
///
/// A component's port can outlive the component: the kernel takes a moment to
/// finish with a socket, and anything the component spawned that inherited
/// one keeps it open for as long as it lives. Either way the next `km start`
/// fails its own port check for reasons that have nothing to do with the new
/// run, so it is worth saying here, while the instance that owned the port is
/// still the obvious suspect.
///
/// Reports and returns either way. A port that never comes back is not a
/// reason to hold the exit open — km has already stopped everything it knows
/// about, and the name in the table is the useful part.
fn wait_for_ports_released(ports: &KmPorts) {
    let labeled = ports.labeled();
    let checks = await_port_release(&labeled, Instant::now() + PORT_RELEASE_WAIT);

    println!();
    print_ports(ports, Some(&checks), PORTS_AFTER_STOP);

    let held: Vec<String> = checks
        .iter()
        .filter(|c| !c.is_free())
        .map(|c| format!("{} port {}", c.label, c.port))
        .collect();
    if held.is_empty() {
        info!(ports = checks.len(), "All ports released");
        return;
    }
    let note = format!(
        "{} port(s) still held after {}s: {}",
        held.len(),
        PORT_RELEASE_WAIT.as_secs(),
        held.join(", ")
    );
    println!();
    eprintln!("WARNING: {note}");
    warn!("{}", note);
}

fn shutdown_from_pids(paths: &Paths, ports: Option<&KmPorts>) {
    info!("Shutting down...");

    // Prefer the components this run owns: their monitors answer the
    // question a PID file cannot — has this one already exited? — which
    // is what keeps km from spending a grace period asking a corpse to
    // stop. `km stop` runs in another process and owns none, so it falls
    // through to the PID files, signalling only entries that still verify.
    let handled = Components::current()
        .map(|components| components.teardown(&paths.pids))
        .unwrap_or_default();

    for name in [virtual_kubelet::BIN, pod_runner::BIN, control_plane::BIN] {
        if handled.contains(&name) {
            continue;
        }
        // Acts only on a file whose subject still verifies; whatever is
        // really left of a run that does not is the sweep's to find.
        stop_recorded(&paths.pids, name, shutdown_grace_for(name));
        remove_pid(&paths.pids, name);
    }

    // Everything the named teardown could not reach — children reparented
    // away, spawns that never made a PID file — still carries this
    // instance's token. The run-token file serves both km's own shutdown
    // and a `km stop` cleaning up after a dead km.
    match Run::load(&run_token_file(&paths.workdir)) {
        Ok(Some(run)) => {
            kill_until_clean(KM_KILL_TOKEN_VAR, run.as_prefix(), SWEEP_ROUNDS);
        }
        Ok(None) => {}
        Err(e) => {
            warn!(error = %e, "Cannot read run token; strays wait for the next start's sweep")
        }
    }
    remove_file_best_effort(&run_token_file(&paths.workdir), "run-token file");

    // A SIP-hidden `/bin/sh` wrapper is invisible to the sweep but still
    // parented here. See `kill_descendants`.
    #[cfg(target_os = "macos")]
    kill_descendants(std::process::id());

    remove_dir_all_best_effort(&control_plane::pids_dir_in(&paths.workdir), "PID directory");
    remove_dir_all_best_effort(
        &virtual_kubelet::pids_dir_in(&paths.workdir),
        "PID directory",
    );

    remove_pid(&paths.pids, BIN_KM);
    // Opportunistic by design: fails whenever anything is still in the
    // directory, which is not an error worth reporting.
    let _ = fs::remove_dir(&paths.pids);
    // A stopped instance must not read as ready; the km pid is gone too, so
    // this only guards against the marker outliving the directory sweep.
    heartbeat::clear_ready(&paths.workdir);

    // Read after the components have gone, so anything they wrote on their
    // way out is already there. The startup set is reported too: a stack that
    // failed before it was ready never reached the report above, and those
    // messages are usually the ones explaining why. Both calls are no-ops if
    // their phase has already been printed.
    messages::report(&paths.workdir, messages::Phase::Start);
    messages::report(&paths.workdir, messages::Phase::Stop);

    // Last, because it is the only step that can take time, and because the
    // answer is only meaningful once everything above has gone.
    if let Some(ports) = ports {
        wait_for_ports_released(ports);
    }

    info!("Stopped");
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Verify every port the instance will bind is free before launching any
/// subprocess. Logs and exits on conflict.
///
/// Run unconditionally, including for ports just drawn by
/// [`KmSysConfig::resolve_random_ports`]: re-checking one of those is a
/// cheap no-op bind, and skipping the whole check whenever any port was
/// 0 would silently exempt the fixed ports of a hand-edited config that
/// mixes the two.
fn verify_ports_free(ports: &KmPorts) {
    let checks = check_ports(&ports.labeled());

    println!();
    print_ports(ports, Some(&checks), PORTS_BEFORE_START);

    if checks.iter().all(PortCheck::is_free) {
        info!(ports = checks.len(), "All ports free");
        return;
    }
    let failures = port_failures(&checks);
    // Printed as well as logged. This is the most common reason a start does
    // not happen, and it used to reach only km.log — so `km start` exited
    // non-zero having said nothing at all to the terminal it was run from.
    println!();
    for failure in &failures {
        eprintln!("ERROR: {failure}");
        error!("{}", failure);
    }
    eprintln!("Cannot start: {} port(s) unusable", failures.len());
    error!("Cannot start: {} port(s) unusable", failures.len());
    std::process::exit(1);
}

/// Slice the monitor sleeps in, so a shutdown request is noticed promptly
/// rather than one whole heartbeat interval later.
const MONITOR_TICK: Duration = Duration::from_millis(250);

/// How often the process watcher looks for a component that has gone away.
/// Checking a pid is far cheaper than a heartbeat round, so this runs tight:
/// a crash is worth knowing about immediately, and unlike a stale beat it
/// needs no interval to interpret.
const PROCESS_WATCH_TICK: Duration = Duration::from_millis(250);

/// Sleep up to `total`, returning false as soon as a shutdown request is
/// seen — the monitor must not act on a component that km is stopping.
fn sleep_unless_shutdown(total: Duration) -> bool {
    let deadline = Instant::now() + total;
    while Instant::now() < deadline {
        if exit::in_progress() {
            return false;
        }
        std::thread::sleep(MONITOR_TICK);
    }
    !exit::in_progress()
}

/// One component's fate for one round.
#[derive(Debug, PartialEq, Eq)]
enum Verdict {
    Ok,
    Failed(String),
}

/// A supervised component's heartbeat counter.
///
/// Process liveness is not here: that is judged from the [`Components`]
/// registry, whose answers come from the monitor that owns each child —
/// never from a number the process table could have recycled. This side
/// owns the beats, which is the case a live-but-unresponsive component
/// falls into.
struct Watched {
    name: &'static str,
    monitor: HeartbeatMonitor,
}

/// Judge one component from its heartbeat.
///
/// A single miss is not yet a failure: that is what the consecutive-miss
/// budget is for.
fn assess(monitor: &mut HeartbeatMonitor) -> Verdict {
    match monitor.poll() {
        Beat::Healthy | Beat::Missed { .. } => Verdict::Ok,
        Beat::Failed { misses, reason } => {
            Verdict::Failed(format!("missed {misses} consecutive heartbeats — {reason}"))
        }
    }
}

/// One heartbeat round over every component. Returns the first failure,
/// having polled the components before it — so their miss counters stay
/// current and their warnings are logged even when a later one fails.
fn monitor_round(watched: &mut [Watched]) -> Option<(&'static str, String)> {
    let mut failure = None;
    for w in watched.iter_mut() {
        if let Verdict::Failed(reason) = assess(&mut w.monitor) {
            failure = failure.or(Some((w.name, reason)));
        }
    }
    failure
}

/// A component's exit, said plainly enough to act on.
///
/// The hex matters on Windows, where an abnormal exit's code is the
/// NTSTATUS that ended the process — `0xC0000409` names an abort where a
/// decimal rendering names nothing.
fn exit_detail(exit: &Exit) -> String {
    match exit {
        Exit::Code(code) if *code < 0 || *code > 0xFF => {
            format!("process exited (code {code}, {:#010x})", *code as u32)
        }
        Exit::Code(code) => format!("process exited (code {code})"),
        Exit::Signal(sig) => format!("process exited (signal {sig})"),
        Exit::Unknown => "process state became unreadable".to_string(),
    }
}

/// What a component last reported in its heartbeat, when that was trouble.
///
/// A component usually publishes a `degraded` beat naming the cause before it
/// exits, so surfacing it turns "the control-plane is gone" into "the
/// control-plane is gone because kube-apiserver stopped listening" — the
/// difference between reading a log afterwards and not having to.
fn last_reported(paths: &Paths, name: &str) -> Option<String> {
    let (_, beat) = heartbeat::read(&heartbeat_file(&paths.workdir, name))?;
    if beat.status == heartbeat::STATUS_READY || beat.detail.is_empty() {
        return None;
    }
    Some(beat.detail)
}

/// Watch every component km has spawned for its process exiting, from the
/// first spawn until shutdown.
///
/// This runs alongside — and ahead of — the heartbeat monitor, which only
/// starts once the whole stack is ready. Without it, a component dying while
/// km is still gating on a later one goes unnoticed until that gate times
/// out: the full startup budget spent waiting, and then the failure reported
/// against the wrong component.
///
/// Judged from each component's own monitor rather than from PID files:
/// the answer cannot be fooled by a recycled number, and it carries the
/// exit outcome — for a component that died without logging, often the
/// only account of what happened.
fn watch_components(paths: Paths, ports: KmPorts, components: Components) {
    std::thread::Builder::new()
        .name("km-procwatch".into())
        .spawn(move || {
            loop {
                std::thread::sleep(PROCESS_WATCH_TICK);
                if exit::in_progress() {
                    return;
                }
                if let Some((name, exit)) = components.first_failed() {
                    let how = exit_detail(&exit);
                    let reason = match last_reported(&paths, name) {
                        Some(detail) => format!("{how} (last reported: {detail})"),
                        None => how,
                    };
                    fail_stack(&paths, &ports, name, &reason);
                }
            }
        })
        .expect("spawn km-procwatch thread");
}

/// Supervise the stack's heartbeats until a component fails or a signal
/// arrives.
///
/// This catches the component that is alive but no longer serving. The
/// other half — a process that has outright exited — belongs to
/// [`watch_components`], which asks each component's own monitor and runs
/// at a far tighter tick. One owner per question.
///
/// Never returns: a component failure exits 1 here, a signal exits from the
/// km-signal thread.
fn monitor_stack(paths: &Paths, ports: &KmPorts, hb: &KmHeartbeat) -> ! {
    let interval = hb.interval();
    let max_missed = hb.max_missed();
    let stale_after = heartbeat::stale_after(interval);

    let mut watched: Vec<Watched> = [control_plane::BIN, pod_runner::BIN, virtual_kubelet::BIN]
        .into_iter()
        .map(|name| Watched {
            name,
            monitor: HeartbeatMonitor::new(
                name,
                heartbeat_file(&paths.workdir, name),
                stale_after,
                max_missed,
            ),
        })
        .collect();

    info!(
        interval_secs = interval.as_secs(),
        max_missed,
        stale_after_secs = stale_after.as_secs(),
        "Monitoring component heartbeats"
    );

    loop {
        if !sleep_unless_shutdown(interval) {
            stand_down();
        }
        if let Some((name, reason)) = monitor_round(&mut watched) {
            fail_stack(paths, ports, name, &reason);
        }
        // Every component answered this round, so readiness still holds. Not
        // restamping is how the marker goes stale, which is the only signal
        // `km status` reads.
        heartbeat::touch_ready(&paths.workdir);
    }
}

/// Park forever: km is stopping and the signal thread owns teardown and exit.
fn stand_down() -> ! {
    loop {
        std::thread::park();
    }
}

/// Log the failing component, tear the stack down and exit non-zero.
///
/// The process watcher, the heartbeat monitor and the signal thread can all
/// reach teardown, so the flag is claimed first: whoever gets it drives the
/// shutdown and the exit code, and the others stand down rather than tearing
/// down a second time over the top.
fn fail_stack(paths: &Paths, ports: &KmPorts, name: &'static str, reason: &str) -> ! {
    if !exit::claim() {
        stand_down();
    }
    exit::report(&ExitReason::Liveness {
        component: name,
        detail: reason.to_string(),
    });
    shutdown_from_pids(paths, Some(ports));
    std::process::exit(1);
}

/// Delete config files that are regenerated on every startup so that a
/// stale copy is never mistaken for this run's. `prsysconfig.yaml` is
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
            remove_file_best_effort(&p, "stale config file");
        }
    }
}

// ---------------------------------------------------------------------------
// Subcommands
// ---------------------------------------------------------------------------

/// Print the `label  port` block of a create summary, or a one-liner
/// when `portstart` is 0 and ports are drawn randomly at every start.
fn print_port_plan(portstart: u16, ports: &KmPorts, checks: Option<&[PortCheck]>) {
    if portstart == 0 {
        println!("  ports      random free ports at every start");
        return;
    }
    print_ports(ports, checks, PORTS_BEFORE_START);
}

/// How the status column is worded. Checking before a start asks whether a
/// port is free; confirming after a stop asks whether it came back.
#[derive(Clone, Copy)]
struct PortWording {
    free: &'static str,
    busy: &'static str,
}

/// Wording for the check that runs before anything is spawned.
const PORTS_BEFORE_START: PortWording = PortWording {
    free: "free",
    busy: "IN USE",
};

/// Wording for the confirmation that runs once the stack is down.
const PORTS_AFTER_STOP: PortWording = PortWording {
    free: "released",
    busy: "STILL IN USE",
};

/// Print one row per port the instance will occupy, with the result of the
/// availability check beside it when one has been run.
///
/// The whole list is printed whether or not the check passed. Passing, it is
/// how an operator confirms which ports this instance takes and that the
/// `kmsysconfig.yaml` being read is the one they edited — neither of which is
/// visible anywhere else. Failing, the whole list beats the first conflict
/// followed by another run to discover the next.
fn print_ports(ports: &KmPorts, checks: Option<&[PortCheck]>, wording: PortWording) {
    let labeled = ports.labeled();
    let width = labeled.iter().map(|(_, l)| l.len()).max().unwrap_or(0);
    for (i, (port, label)) in labeled.iter().enumerate() {
        let head = if i == 0 { "  ports    " } else { "           " };
        let status = match checks.and_then(|c| c.get(i)) {
            None => String::new(),
            Some(check) => match &check.reason {
                None => format!("  {}", wording.free),
                Some(reason) => format!("  {} ({reason})", wording.busy),
            },
        };
        println!("{head}  {label:width$}  {port}{status}");
    }
}

/// Initialize a new kminus instance in `args.workdir`.
///
/// Every check runs before anything is written: the `--portstart` range
/// must fit and be free, and neither the workdir nor the configdir may
/// exist. A failed `km create` therefore leaves nothing behind. With
/// `--dryrun` the checks run and nothing is created either way.
fn command_create(args: CreateArgs) {
    let mut errors: Vec<String> = Vec::new();

    // Ports first, as the check most likely to collide with another
    // instance — no folder is created if the range is unusable.
    let mut checks: Option<Vec<PortCheck>> = None;
    let ports = match KmPorts::from_portstart(args.portstart) {
        Ok(ports) => {
            // portstart 0 means "random free ports at every start":
            // there is no fixed range to check now.
            if args.portstart != 0 {
                let run = check_ports(&ports.labeled());
                errors.extend(port_failures(&run));
                checks = Some(run);
            }
            Some(ports)
        }
        Err(e) => {
            errors.push(e);
            None
        }
    };

    let targets = match CreateTargets::resolve(&args.workdir, args.configdir.as_deref()) {
        Ok(targets) => Some(targets),
        Err(e) => {
            errors.push(e);
            None
        }
    };

    if !errors.is_empty() {
        // The check comes before the complaints: which ports were tried, and
        // which of them are the problem.
        if let Some(ports) = &ports {
            println!();
            print_port_plan(args.portstart, ports, checks.as_deref());
        }
        println!();
        for e in &errors {
            eprintln!("ERROR: {e}");
        }
        eprintln!("Cannot create: {} check(s) failed", errors.len());
        std::process::exit(1);
    }
    let (Some(ports), Some(targets)) = (ports, targets) else {
        unreachable!("no errors collected, so both checks produced a value");
    };

    if args.dryrun {
        println!();
        println!("Tabsdata K- (K minus) instance can be created");
        println!();
        println!("  workdir    {}", targets.workdir.display());
        println!("  configdir  {}", targets.configdir.display());
        print_port_plan(args.portstart, &ports, checks.as_deref());
        println!();
        println!("Nothing was created. Re-run without --dryrun to create it.");
        return;
    }

    let paths = Paths::for_create(&targets).unwrap_or_else(|e| {
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
    if fixed_auth_token_enabled() {
        warn_fixed_auth_token();
        println!();
    }
    println!("  workdir    {}", paths.workdir.display());
    println!("  configdir  {}", paths.configdir.display());
    print_port_plan(args.portstart, &ports, checks.as_deref());
    println!();
    println!(
        "Edit '{}' to customize ports or log settings, then run:",
        paths.configdir.join(KM_SYSCONFIG_FILE).display()
    );
    println!("  km start --workdir {}", paths.workdir.display());
}

/// Boot the cluster from an existing kminus instance in `args.workdir`,
/// then supervise it in [`monitor_stack`], which owns the `Child` handles
/// and reaps them.
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

    // Refuse to start over a live instance. `cleanup_stale_pids` (below)
    // would otherwise silently kill the running km and take over, which
    // reads as a no-op to the user. Report status and exit instead.
    if matches!(recorded(&paths.pids, BIN_KM), Recorded::Live(_)) {
        println!();
        println!("Tabsdata K- (K minus) is already running");
        println!("  workdir          {}", paths.workdir.display());
        println!();
        print_instance_status(&paths);
        println!();
        println!("To stop it:    km stop --workdir {}", args.workdir);
        println!("To restart it: km restart --workdir {}", args.workdir);
        std::process::exit(1);
    }

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
        args.loglevel,
    );

    let port_api = sys_cfg.ports.api.to_string();
    let port_kcm = sys_cfg.ports.kcm.to_string();
    let port_pr = sys_cfg.ports.pr.to_string();
    let port_pr_metrics = sys_cfg.ports.pr_metrics.to_string();
    let port_vk = sys_cfg.ports.vk.to_string();

    println!();
    if random_ports_allocated {
        println!("Tabsdata K- (K minus) starting (random ports)");
    } else {
        println!("Tabsdata K- (K minus) starting");
    }

    // Reclaim the previous life before the port check: its processes can
    // still hold this instance's ports, and killing them is what frees
    // those ports — checked first, the start would refuse over ports the
    // reclaim is about to release. The run-token file names everything the
    // previous run ever started — including processes that escaped their
    // groups and were reparented, which no PID file or parent link can
    // reach — and the dead-owner sweep behind it covers runs whose file
    // was lost. Safe next to a *concurrent* instance because the
    // single-instance guard above is identity-verified, and because a
    // dead owner is decidable from a token alone.
    let token_path = run_token_file(&paths.workdir);
    match Run::load(&token_path) {
        Ok(Some(previous)) => {
            kill_until_clean(KM_KILL_TOKEN_VAR, previous.as_prefix(), SWEEP_ROUNDS);
        }
        Ok(None) => {}
        Err(e) => {
            warn!(error = %e, "Cannot read previous run token; relying on the dead-owner sweep")
        }
    }
    kill_strays_of_dead_runs(KM_KILL_TOKEN_VAR);

    // After the banner, and after the previous life is gone so the answer
    // reflects what this run will actually find: the port table is the
    // first thing the start reports, and on a clash it is the only thing.
    verify_ports_free(&sys_cfg.ports);
    println!();

    if fixed_auth_token_enabled() {
        warn_fixed_auth_token();
        println!();
    }

    info!(
        workdir = %paths.workdir.display(),
        configdir = %paths.configdir.display(),
        logdir = %paths.logdir.display(),
        "Starting K-"
    );

    cleanup_stale_pids(&paths.pids);
    fs::create_dir_all(&paths.pids).ok();
    write_pid(&paths.pids, BIN_KM, std::process::id());

    // Mint and persist this run's token root before anything is spawned,
    // so a km that dies mid-startup still left the trail the next run
    // needs.
    let run = Run::current();
    if let Err(e) = run.save(&token_path) {
        // Startable, but the next run cannot reclaim by prefix — only by
        // dead owner. Worth a loud line, not a refusal to boot.
        warn!(error = %e, path = %token_path.display(), "Cannot persist run token");
    }
    info!(token = run.as_str(), "Kill token for this run");

    // Installed once this run owns the token and the PID files, and still
    // before the first spawn: a panic from here on abandons children that
    // hold this instance's ports, and the teardown is only ever pointed at
    // state this run minted.
    let teardown_paths = paths.clone();
    kcommon::panic::install_with_teardown(BIN_KM, move || {
        shutdown_from_pids(&teardown_paths, None);
    });

    let vk_kubeconfig = paths.configdir.join(control_plane::VK_KUBECONFIG_FILE);
    let vk_prconfig = paths.configdir.join(pod_runner::VK_PR_CONFIG_FILE);
    let kubeconfig = paths.configdir.join(control_plane::KUBECONFIG_FILE);

    clear_regenerated_configs(&paths.configdir);
    // Emptied before any component runs, so a previous run's messages cannot
    // be read back as this one's.
    messages::clear(&paths.workdir);
    // Same reason: a marker left by the last run would make `km status`
    // report this one ready before it is.
    heartbeat::clear_ready(&paths.workdir);

    // Install signal-driven shutdown BEFORE spawning anything, so a
    // Ctrl+C at any point during the (multi-second) startup tears the
    // stack down in order rather than orphaning it.
    install_signal_shutdown(paths.clone(), sys_cfg.ports.clone());
    // Started before anything is spawned, so a component dying mid-startup is
    // caught while km is still gating on a later one. Published so both
    // teardown paths — a signal and a component failure — reach the
    // tracked components rather than PID files.
    let components = Components::new();
    components.install();
    watch_components(paths.clone(), sys_cfg.ports.clone(), components.clone());

    // Start subprocesses in dependency order, reporting each phase to stdout
    // so the (multi-second) startup shows live progress instead of a silent
    // gap before the ready banner.
    let started = Instant::now();

    let mut phase = step("Starting control-plane");
    control_plane::start(
        &paths.workdir,
        &paths.configdir,
        &paths.logdir,
        &paths.pids,
        &sys_cfg,
        &port_api,
        &port_kcm,
        &run,
        &components,
    );
    step_done(phase);

    phase = step("Starting pod-runner");
    pod_runner::start(
        &paths.workdir,
        &paths.configdir,
        &paths.logdir,
        &paths.pids,
        &sys_cfg,
        &port_pr,
        &port_pr_metrics,
        &kubeconfig,
        args.imagesdir.as_deref(),
        args.loglevel,
        &run,
        &components,
    );
    step_done(phase);

    phase = step("Cleaning up stale node resources");
    control_plane::cleanup_stale_node_resources(&kubeconfig);
    step_done(phase);

    if args.purge {
        phase = step("Purging previous-run workloads");
        control_plane::purge_workloads(&kubeconfig);
        step_done(phase);
    }

    phase = step("Starting virtual-kubelet");
    virtual_kubelet::start(
        &paths.workdir,
        &paths.configdir,
        &paths.logdir,
        &paths.pids,
        &sys_cfg,
        &port_vk,
        &vk_kubeconfig,
        &vk_prconfig,
        &run,
        &components,
    );
    step_done(phase);

    println!(
        "  all components ready in {:.1}s",
        started.elapsed().as_secs_f64()
    );
    info!("K- ready");
    // Stamped here rather than waiting for the first supervision round: the
    // stack is ready now, and a `km status` in the next interval should say
    // so rather than "not reported yet".
    heartbeat::touch_ready(&paths.workdir);

    print_status(&paths, &components, &sys_cfg.ports, &kubeconfig);

    println!(
        "  liveness/readiness check every {}ms/{}s, readiness tolerance {} misses",
        PROCESS_WATCH_TICK.as_millis(),
        sys_cfg.heartbeat.interval().as_secs(),
        sys_cfg.heartbeat.max_missed()
    );

    // Whatever the components asked to have said on the way up. Printed after
    // the status block so the banner stays in one piece, and before the
    // supervision loop takes over the terminal.
    messages::report(&paths.workdir, messages::Phase::Start);

    println!();
    println!("Press Ctrl+C to stop");

    startup::announce(startup::Outcome::Ready);

    // Supervise from here on. Teardown on signal is still owned by the
    // km-signal thread installed above; the monitor stands down when it
    // fires. The components stay in the registry, where each one's own
    // monitor reaps it.
    monitor_stack(&paths, &sys_cfg.ports, &sys_cfg.heartbeat)
}

/// Grace period km's own signal-driven teardown may need before `stop`
/// escalates to killing the stack directly. Sized to cover the
/// pod-runner drain (see [`POD_RUNNER_SHUTDOWN_GRACE`]) plus overhead for
/// the other components and process bookkeeping.
const STOP_WAIT: Duration = Duration::from_secs(120);

/// Stop a running kminus instance in `args.workdir`.
///
/// The running `km start` process owns orderly teardown via its
/// signal-driven shutdown thread, so the clean path is simply to signal
/// it and wait for it to exit: it stops virtual-kubelet → pod-runner →
/// control-plane in order (draining pods) and removes the PID files. If
/// that process is not alive (crashed, or never fully started) we fall
/// back to tearing the stack down directly from the recorded PID files,
/// which also reaps any orphaned children.
fn command_stop(args: StopArgs) {
    let paths = Paths::for_existing(&args.workdir).unwrap_or_else(|e| {
        eprintln!("ERROR: {e}");
        std::process::exit(1);
    });

    // Only pinned ports can be confirmed released. With `portstart: 0` the
    // file keeps its zeros and the ports actually drawn live in the memory of
    // the km being stopped, so this process has nothing to wait on — that km
    // reports them in its own terminal instead.
    let ports = KmSysConfig::load(&paths.configdir)
        .ok()
        .map(|cfg| cfg.ports)
        .filter(|ports| ports.labeled().iter().all(|(port, _)| *port != 0));

    match recorded(&paths.pids, BIN_KM) {
        Recorded::Live(pid) => {
            println!("Stopping Tabsdata K- (K minus) [km pid={pid}]...");
            // Signal the foreground `km start` and let its own shutdown
            // thread drive the orderly teardown, then wait for it to exit.
            stop_recorded(&paths.pids, BIN_KM, STOP_WAIT);
            // Asked of the PID file again rather than of the number: km
            // removes its file on a clean exit, so a still-verifying entry
            // is km itself refusing to go, where a bare liveness check
            // would also fire for whatever inherited its number.
            if matches!(recorded(&paths.pids, BIN_KM), Recorded::Live(_)) {
                eprintln!(
                    "ERROR: km (pid={pid}) did not stop within {}s; forcing teardown.",
                    STOP_WAIT.as_secs()
                );
            }
            // km's own shutdown thread removes the PID files on a clean
            // exit, but sweep any it left behind (e.g. it was SIGKILLed
            // mid-teardown or crashed) so a later `status` reads clean.
            shutdown_from_pids(&paths, ports.as_ref());
            println!("Stopped.");
        }
        _ => {
            // No live km orchestrator. Either it is not running, or it
            // died leaving children behind. Clean up directly.
            let stale: Vec<&str> = [virtual_kubelet::BIN, pod_runner::BIN, control_plane::BIN]
                .into_iter()
                .filter(|name| matches!(recorded(&paths.pids, name), Recorded::Live(_)))
                .collect();

            if stale.is_empty() {
                println!("Tabsdata K- (K minus) is not running.");
                // Best-effort cleanup of any leftover PID files/dirs. No
                // port wait: nothing was running, so nothing can be holding
                // one on this instance's behalf.
                shutdown_from_pids(&paths, None);
                return;
            }
            println!(
                "km orchestrator not running; tearing down orphaned components: {}",
                stale.join(", ")
            );
            shutdown_from_pids(&paths, ports.as_ref());
            println!("Stopped.");
        }
    }
}

/// The ports a control-plane child binds, for the status block. km allocates
/// all of them, so they can be reported even though km never probes these
/// processes itself.
fn child_ports(name: &str, ports: &KmPorts) -> String {
    if name == control_plane::ETCD {
        format!("port={}  peer-port={}", ports.etcd, ports.etcd_peer)
    } else if name == control_plane::KUBE_APISERVER {
        format!("port={}", ports.api)
    } else if name == control_plane::KCM {
        format!("port={}", ports.kcm)
    } else {
        String::new()
    }
}

/// One line of status output for a component.
fn print_component_status(paths: &Paths, name: &str) {
    // Heartbeat age is what distinguishes "serving" from merely "alive";
    // km itself does not beat, so it has none to show.
    let beat = match heartbeat::age(&heartbeat_file(&paths.workdir, name)) {
        Some(age) => format!("  beat={}s ago", age.as_secs()),
        None if name == BIN_KM => String::new(),
        None => "  beat=none".to_string(),
    };
    match recorded(&paths.pids, name) {
        Recorded::Live(pid) => println!("  {:<16} running   pid={}{}", name, pid, beat),
        Recorded::Stale(pid) => println!("  {:<16} DEAD      pid={} (stale PID file)", name, pid),
        Recorded::Absent => println!("  {:<16} stopped", name),
    }
}

/// Render the per-component status block (km, control-plane + children,
/// pod-runner, virtual-kubelet) for the instance at `paths`. Returns
/// whether the km orchestrator itself is alive. Shared by `km status`
/// and the "already running" guard in `km start`.
fn print_instance_status(paths: &Paths) -> bool {
    let km_running = matches!(recorded(&paths.pids, BIN_KM), Recorded::Live(_));

    print_component_status(paths, BIN_KM);
    print_component_status(paths, control_plane::BIN);
    for (name, pid) in control_plane::read_children(&paths.workdir) {
        // A bare liveness check, unlike everything else here: these files
        // are the control-plane's own and record a number without a start
        // stamp, so there is nothing to verify against. It only decides a
        // status line, never a signal.
        let alive = pid
            .parse::<u32>()
            .map(kcommon::signal::is_pid_alive)
            .unwrap_or(false);
        let state = if alive { "running" } else { "DEAD   " };
        println!("    {:<14} {}   pid={}", name, state, pid);
    }
    print_etcd_storage(paths);
    print_component_status(paths, pod_runner::BIN);
    print_component_status(paths, virtual_kubelet::BIN);

    km_running
}

/// Print what the control-plane last reported about etcd's backend file.
///
/// Silent when no report exists: an instance that has never started has
/// nothing to say, and the etcd line above already shows whether it is
/// running. The figures are as of the report's timestamp rather than now,
/// since km cannot query etcd itself.
fn print_etcd_storage(paths: &Paths) {
    let Some(storage) = control_plane::read_etcd_storage(&paths.workdir) else {
        return;
    };
    let free = storage.db_size.saturating_sub(storage.db_size_in_use);
    println!(
        "    {:<14} {} live of {} on disk ({} reusable), {} quota",
        "etcd storage",
        control_plane::human_bytes(storage.db_size_in_use),
        control_plane::human_bytes(storage.db_size),
        control_plane::human_bytes(free),
        control_plane::human_bytes(storage.quota_bytes),
    );
    let defrag = if storage.defrag_mins > 0 {
        format!("defrag every {}m", storage.defrag_mins)
    } else {
        "defrag disabled".to_string()
    };
    println!(
        "    {:<14} fsync {}, {}",
        "",
        if storage.fsync { "on" } else { "OFF" },
        defrag
    );
}

/// What `km status` found: the three cases a caller scripting on it needs to
/// tell apart.
///
/// The two signals are deliberately independent. *Live* is km's own process
/// being alive — an instance exists. *Ready* is the age of the marker km
/// stamps on each supervision round where the whole stack is healthy. An
/// instance whose km is up but whose stack is still coming up, or has gone
/// degraded, is neither serving nor absent; collapsing that into either one
/// makes a script wait on something that will never be ready, or give up on
/// something that is about to be.
#[derive(Debug, PartialEq, Eq)]
enum InstanceState {
    /// km is live and the ready marker is fresh.
    Ready,
    /// km is live, but the marker is stale or was never written. Carries why,
    /// so the reason is on screen rather than only in a log.
    Live { reason: String },
    /// No km orchestrator for this instance.
    NotRunning,
}

impl InstanceState {
    /// 0 ready, 1 live, 2 not running.
    fn exit_code(&self) -> i32 {
        match self {
            Self::Ready => 0,
            Self::Live { .. } => 1,
            Self::NotRunning => 2,
        }
    }
}

/// Classify the instance at `paths`.
///
/// Readiness is not recomputed here. km already decides it once per round,
/// across every component, and records that decision by stamping the marker;
/// status reads the age and applies the same `intervalSecs` km supervises on.
/// Anything else would be a second opinion, free to disagree with the one
/// actually driving the stack.
fn instance_state(paths: &Paths, hb: &KmHeartbeat) -> InstanceState {
    if !matches!(recorded(&paths.pids, BIN_KM), Recorded::Live(_)) {
        return InstanceState::NotRunning;
    }
    let stale_after = heartbeat::stale_after(hb.interval());
    match heartbeat::ready_age(&paths.workdir) {
        Some(age) if age <= stale_after => InstanceState::Ready,
        Some(age) => InstanceState::Live {
            reason: format!(
                "last reported ready {}s ago, stale after {}s",
                age.as_secs(),
                stale_after.as_secs()
            ),
        },
        None => InstanceState::Live {
            reason: "has not reported ready yet".to_string(),
        },
    }
}

/// Report the status of the kminus instance in `args.workdir`.
///
/// Exit code reflects overall state: 0 ready, 1 live but not ready, 2 not
/// running. A caller waiting for an instance can therefore poll for 0 and
/// tell "still coming up" (1) from "never started" (2), which one code for
/// both could not.
fn command_status(args: StatusArgs) {
    let paths = Paths::for_existing(&args.workdir).unwrap_or_else(|e| {
        eprintln!("ERROR: {e}");
        std::process::exit(1);
    });
    // Only the heartbeat interval is needed, to know when a beat is stale.
    // A config that will not load is not worth failing status over: the
    // defaults answer the same question, slightly less precisely.
    let hb = KmSysConfig::load(&paths.configdir)
        .map(|cfg| cfg.heartbeat)
        .unwrap_or_default();

    println!();
    println!("Tabsdata K- (K minus) status");
    println!("  workdir          {}", paths.workdir.display());
    println!();
    print_instance_status(&paths);
    println!();

    let state = instance_state(&paths, &hb);
    match &state {
        InstanceState::Ready => println!("Overall: ready"),
        InstanceState::Live { reason } => println!("Overall: live, not ready ({reason})"),
        InstanceState::NotRunning => println!("Overall: not running"),
    }
    std::process::exit(state.exit_code());
}

/// Restart the kminus instance: stop it (if running), then start it.
fn command_restart(args: StartArgs) {
    command_stop(StopArgs {
        workdir: args.workdir.clone(),
    });
    println!();
    command_start(args);
}

/// Install the SIGINT/SIGTERM (Ctrl+C/Ctrl+Break) handlers, signal that
/// they are armed via `armed`, then resolve when a signal arrives. On
/// Windows, listen for both Ctrl+C and Ctrl+Break — `tdkserver stop`
/// requests graceful shutdown via `GenerateConsoleCtrlEvent(
/// CTRL_BREAK_EVENT, ...)`, which only fires `ctrl_break`.
#[cfg(unix)]
async fn arm_and_wait_for_shutdown(
    armed: std::sync::mpsc::Sender<()>,
    _workdir: PathBuf,
) -> String {
    use tokio::signal::unix::{SignalKind, signal};
    let mut sigterm = signal(SignalKind::terminate()).expect("install SIGTERM handler");
    let mut sigint = signal(SignalKind::interrupt()).expect("install SIGINT handler");
    let _ = armed.send(());
    tokio::select! {
        _ = sigterm.recv() => "SIGTERM".to_string(),
        _ = sigint.recv() => "SIGINT (Ctrl+C)".to_string(),
    }
}

#[cfg(windows)]
async fn arm_and_wait_for_shutdown(armed: std::sync::mpsc::Sender<()>, workdir: PathBuf) -> String {
    use tokio::signal::windows::{ctrl_break, ctrl_c};
    let mut ctrlc = ctrl_c().expect("install Ctrl+C handler");
    let mut ctrlbreak = ctrl_break().expect("install Ctrl+Break handler");
    let stop_event = kcommon::stopevent::StopEvent::create(&workdir.to_string_lossy());
    let stopped = tokio::task::spawn_blocking(move || {
        if let Some(event) = stop_event {
            event.wait();
            true
        } else {
            false
        }
    });
    let _ = armed.send(());
    tokio::select! {
        _ = ctrlc.recv() => "Ctrl+C".to_string(),
        _ = ctrlbreak.recv() => "Ctrl+Break".to_string(),
        Ok(true) = stopped => "stop request".to_string(),
    }
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

fn main() {
    // Before Cli::parse: a re-exec'd graceful-shutdown helper must never
    // reach the normal CLI (see kcommon::signal::ctrl_break_helper_hook).
    kcommon::signal::ctrl_break_helper_hook();
    let cli = Cli::parse();
    match cli.command {
        Command::Create(args) => command_create(args),
        Command::Start(args) => command_start(args),
        Command::Stop(args) => command_stop(args),
        Command::Status(args) => command_status(args),
        Command::Restart(args) => command_restart(args),
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
    use std::net::TcpListener;

    // -- await_port_release ------------------------------------------------

    #[test]
    fn await_port_release_returns_at_once_when_every_port_is_free() {
        let labeled = vec![
            (system::port::find_free_port(), "a".to_string()),
            (system::port::find_free_port(), "b".to_string()),
        ];
        let started = Instant::now();
        // A deadline far enough out that returning early is the only way to
        // finish quickly.
        let checks = await_port_release(&labeled, started + Duration::from_secs(30));

        assert_eq!(checks.len(), 2);
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "waited {:?} for ports that were already free",
            started.elapsed()
        );
    }

    // A port still held must not hold the exit open past the deadline, and
    // must come back named so the report can say which one it was.
    #[test]
    fn await_port_release_gives_up_at_the_deadline_and_names_the_holder() {
        let held = TcpListener::bind("127.0.0.1:0").unwrap();
        let held_port = held.local_addr().unwrap().port();
        let labeled = vec![
            (system::port::find_free_port(), "free-one".to_string()),
            (held_port, "still-held".to_string()),
        ];

        let started = Instant::now();
        let checks = await_port_release(&labeled, started + Duration::from_millis(600));
        let waited = started.elapsed();

        assert!(
            waited >= Duration::from_millis(500),
            "returned after {waited:?}, before the deadline"
        );
        assert!(
            waited < Duration::from_secs(10),
            "waited {waited:?}, well past the deadline"
        );
        let holder = checks.iter().find(|c| !c.is_free()).expect("the held port");
        assert_eq!(holder.label, "still-held");
        assert_eq!(holder.port, held_port);
    }

    // Releasing mid-wait must be noticed rather than waited out.
    #[test]
    fn await_port_release_notices_a_port_coming_back() {
        let held = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = held.local_addr().unwrap().port();
        let labeled = vec![(port, "late".to_string())];

        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(300));
            drop(held);
        });

        let started = Instant::now();
        let checks = await_port_release(&labeled, started + Duration::from_secs(20));

        assert!(checks[0].is_free(), "the released port still reads as held");
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "waited {:?} for a port released after 300ms",
            started.elapsed()
        );
    }

    // -- port wording ------------------------------------------------------

    #[test]
    fn port_wording_differs_between_a_start_and_a_stop() {
        assert_eq!(PORTS_BEFORE_START.free, "free");
        assert_eq!(PORTS_AFTER_STOP.free, "released");
        assert_ne!(PORTS_BEFORE_START.busy, PORTS_AFTER_STOP.busy);
    }

    fn tempdir() -> PathBuf {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let dir = env::temp_dir().join(format!("km-main-{}-{}", std::process::id(), n));
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    // -- instance_state ----------------------------------------------------

    fn status_paths() -> Paths {
        let workdir = tempdir();
        let paths = Paths {
            pids: pids_dir(&workdir, BIN_KM),
            configdir: workdir.join("config"),
            logdir: workdir.join("log"),
            workdir,
        };
        fs::create_dir_all(&paths.pids).unwrap();
        paths
    }

    /// Record km as running under this test's own pid, which is alive by
    /// definition.
    fn mark_km_live(paths: &Paths) {
        write_pid(&paths.pids, BIN_KM, std::process::id());
    }

    /// Backdate the ready marker so it reads as stale.
    fn age_ready_marker(paths: &Paths, by: Duration) {
        let path = heartbeat::ready_file(&paths.workdir);
        let when = std::time::SystemTime::now() - by;
        let file = fs::File::options().write(true).open(&path).unwrap();
        file.set_times(fs::FileTimes::new().set_modified(when))
            .unwrap();
    }

    // Not running is km's own process being absent — nothing to do with the
    // marker, which may well still be lying around.
    #[test]
    fn instance_state_is_not_running_without_a_live_km() {
        let paths = status_paths();
        heartbeat::touch_ready(&paths.workdir);

        let state = instance_state(&paths, &KmHeartbeat::default());
        assert_eq!(state, InstanceState::NotRunning);
        assert_eq!(state.exit_code(), 2);
        fs::remove_dir_all(&paths.workdir).ok();
    }

    #[test]
    fn instance_state_is_ready_when_the_marker_is_fresh() {
        let paths = status_paths();
        mark_km_live(&paths);
        heartbeat::touch_ready(&paths.workdir);

        let state = instance_state(&paths, &KmHeartbeat::default());
        assert_eq!(state, InstanceState::Ready);
        assert_eq!(state.exit_code(), 0);
        fs::remove_dir_all(&paths.workdir).ok();
    }

    // km up but nothing stamped yet: still coming up, which must read as
    // neither ready nor absent.
    #[test]
    fn instance_state_is_live_before_the_first_stamp() {
        let paths = status_paths();
        mark_km_live(&paths);

        let state = instance_state(&paths, &KmHeartbeat::default());
        assert_eq!(state.exit_code(), 1);
        let InstanceState::Live { reason } = &state else {
            panic!("expected live, got {state:?}");
        };
        assert!(reason.contains("not reported ready"), "{reason}");
        fs::remove_dir_all(&paths.workdir).ok();
    }

    // km stopped stamping: the stack went degraded under a km that is still
    // alive. Staleness is the whole signal.
    #[test]
    fn instance_state_is_live_when_the_marker_goes_stale() {
        let paths = status_paths();
        mark_km_live(&paths);
        heartbeat::touch_ready(&paths.workdir);
        age_ready_marker(&paths, Duration::from_secs(3600));

        let state = instance_state(&paths, &KmHeartbeat::default());
        assert_eq!(state.exit_code(), 1);
        let InstanceState::Live { reason } = &state else {
            panic!("expected live, got {state:?}");
        };
        assert!(reason.contains("3600s ago"), "{reason}");
        fs::remove_dir_all(&paths.workdir).ok();
    }

    // Status ages the marker with the same interval km supervises on, so a
    // slower heartbeat widens the window rather than needing its own rule.
    #[test]
    fn instance_state_uses_the_configured_interval_for_staleness() {
        let paths = status_paths();
        mark_km_live(&paths);
        heartbeat::touch_ready(&paths.workdir);
        age_ready_marker(&paths, Duration::from_secs(30));

        let brisk = KmHeartbeat {
            interval_secs: 5,
            ..KmHeartbeat::default()
        };
        assert_eq!(instance_state(&paths, &brisk).exit_code(), 1);

        let relaxed = KmHeartbeat {
            interval_secs: 60,
            ..KmHeartbeat::default()
        };
        assert_eq!(instance_state(&paths, &relaxed), InstanceState::Ready);
        fs::remove_dir_all(&paths.workdir).ok();
    }

    #[test]
    fn instance_state_exit_codes_are_distinct() {
        assert_eq!(InstanceState::Ready.exit_code(), 0);
        assert_eq!(InstanceState::Live { reason: "x".into() }.exit_code(), 1);
        assert_eq!(InstanceState::NotRunning.exit_code(), 2);
    }

    #[test]
    fn ready_marker_round_trips_and_clears() {
        let paths = status_paths();
        assert!(heartbeat::ready_age(&paths.workdir).is_none());

        heartbeat::touch_ready(&paths.workdir);
        let age = heartbeat::ready_age(&paths.workdir).expect("marker should exist");
        assert!(age < Duration::from_secs(5), "fresh marker aged {age:?}");

        heartbeat::clear_ready(&paths.workdir);
        assert!(heartbeat::ready_age(&paths.workdir).is_none());
        // Clearing something already absent is not an error.
        heartbeat::clear_ready(&paths.workdir);
        fs::remove_dir_all(&paths.workdir).ok();
    }

    // -- env_flag_enabled --------------------------------------------------

    #[test]
    fn env_flag_enabled_only_for_true() {
        assert!(env_flag_enabled(Some("true")));
        assert!(env_flag_enabled(Some("TRUE")));
        assert!(env_flag_enabled(Some("True")));
        assert!(!env_flag_enabled(Some("false")));
        assert!(!env_flag_enabled(Some("1")));
        assert!(!env_flag_enabled(Some("yes")));
        assert!(!env_flag_enabled(Some("")));
        assert!(!env_flag_enabled(None));
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

    // -- heartbeat round ----------------------------------------------------

    fn ready_beat(dir: &Path) -> PathBuf {
        let path = dir.join(system::heartbeat::HEARTBEAT_FILE);
        fs::write(&path, r#"{"ts":1,"status":"ready","detail":""}"#).unwrap();
        path
    }

    fn watched(name: &'static str, beat: PathBuf, max_missed: u32) -> Watched {
        Watched {
            name,
            monitor: HeartbeatMonitor::new(name, beat, Duration::from_secs(15), max_missed),
        }
    }

    #[test]
    fn assess_accepts_a_fresh_beat() {
        let dir = tempdir();
        let mut monitor =
            HeartbeatMonitor::new("test", ready_beat(&dir), Duration::from_secs(15), 4);
        assert_eq!(assess(&mut monitor), Verdict::Ok);
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn assess_tolerates_misses_until_the_budget_is_spent() {
        let dir = tempdir();
        // No beat file at all: every round is a miss.
        let mut monitor = HeartbeatMonitor::new(
            "test",
            dir.join(system::heartbeat::HEARTBEAT_FILE),
            Duration::from_secs(15),
            3,
        );
        assert_eq!(assess(&mut monitor), Verdict::Ok);
        assert_eq!(assess(&mut monitor), Verdict::Ok);
        let Verdict::Failed(reason) = assess(&mut monitor) else {
            panic!("the third miss must fail the component");
        };
        assert!(reason.starts_with("missed 3 consecutive heartbeats"));
        assert!(reason.contains("no readable heartbeat"));
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn monitor_round_passes_when_every_component_beats() {
        let dir = tempdir();
        let beat = ready_beat(&dir);
        let mut all = vec![
            watched("cp", beat.clone(), 4),
            watched("pr", beat.clone(), 4),
            watched("vk", beat, 4),
        ];
        assert_eq!(monitor_round(&mut all), None);
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn monitor_round_reports_the_first_failure() {
        let dir = tempdir();
        let beat = ready_beat(&dir);
        let missing = dir.join("absent.json");
        let mut all = vec![
            watched("cp", beat, 4),
            // Budget of one: a single missing beat fails it outright.
            watched("pr", missing.clone(), 1),
            watched("vk", missing, 1),
        ];
        let (name, reason) = monitor_round(&mut all).unwrap();
        assert_eq!(name, "pr");
        assert!(reason.starts_with("missed 1 consecutive heartbeats"));
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn monitor_round_keeps_counting_components_after_a_failure() {
        let dir = tempdir();
        let missing = dir.join(system::heartbeat::HEARTBEAT_FILE);
        let mut all = vec![watched("cp", missing.clone(), 1), watched("pr", missing, 2)];
        // cp fails on round one, but pr must still be polled: its miss
        // counter has to be current if the stack somehow keeps running.
        assert_eq!(monitor_round(&mut all).unwrap().0, "cp");
        assert_eq!(monitor_round(&mut all).unwrap().0, "cp");
        // pr has now missed twice, so it would fail on its own merits.
        assert!(matches!(assess(&mut all[1].monitor), Verdict::Failed(_)));
        fs::remove_dir_all(&dir).ok();
    }

    // -- exit detail ---------------------------------------------------------

    #[test]
    fn exit_detail_says_ordinary_codes_plainly() {
        assert_eq!(exit_detail(&Exit::Code(0)), "process exited (code 0)");
        assert_eq!(exit_detail(&Exit::Code(15)), "process exited (code 15)");
        assert_eq!(exit_detail(&Exit::Signal(9)), "process exited (signal 9)");
        assert_eq!(
            exit_detail(&Exit::Unknown),
            "process state became unreadable"
        );
    }

    #[test]
    fn exit_detail_names_an_ntstatus_in_hex() {
        // 0xC0000409 arrives as a negative i32; the hex rendering is what
        // lets a Windows abort be recognised on sight.
        let status = 0xC0000409u32 as i32;
        assert_eq!(
            exit_detail(&Exit::Code(status)),
            "process exited (code -1073740791, 0xc0000409)",
        );
    }

    #[test]
    fn child_ports_reports_what_each_control_plane_child_binds() {
        let ports = KmPorts::from_portstart(20000).unwrap();
        assert_eq!(
            child_ports(control_plane::ETCD, &ports),
            "port=20005  peer-port=20006"
        );
        assert_eq!(
            child_ports(control_plane::KUBE_APISERVER, &ports),
            "port=20000"
        );
        assert_eq!(child_ports(control_plane::KCM, &ports), "port=20001");
        // An unknown child must not have a port invented for it.
        assert_eq!(child_ports("kube-scheduler", &ports), "");
    }

    #[test]
    fn last_reported_surfaces_a_components_own_complaint() {
        let dir = tempdir();
        let workdir = dir.join("wd");
        let paths = Paths {
            workdir: workdir.clone(),
            configdir: workdir.join(CONFIG_DIR),
            logdir: workdir.join(LOG_DIR),
            pids: pids_dir(&workdir, BIN_KM),
        };
        let beat = heartbeat_file(&workdir, control_plane::BIN);
        fs::create_dir_all(beat.parent().unwrap()).unwrap();

        // Nothing written yet: nothing to add to the failure.
        assert_eq!(last_reported(&paths, control_plane::BIN), None);

        // A ready beat is not a complaint either.
        fs::write(&beat, r#"{"ts":1,"status":"ready","detail":""}"#).unwrap();
        assert_eq!(last_reported(&paths, control_plane::BIN), None);

        // A degraded beat is what the operator needs to see.
        fs::write(
            &beat,
            r#"{"ts":1,"status":"degraded","detail":"kube-apiserver is no longer listening"}"#,
        )
        .unwrap();
        assert_eq!(
            last_reported(&paths, control_plane::BIN).as_deref(),
            Some("kube-apiserver is no longer listening")
        );
        fs::remove_dir_all(&dir).ok();
    }

    // -- heartbeat startup gate --------------------------------------------

    #[test]
    fn wait_until_ready_returns_at_once_for_a_ready_component() {
        let dir = tempdir();
        let beat = ready_beat(&dir);
        let started = Instant::now();
        // A generous timeout the call must not wait out. The bound is loose
        // enough to survive a loaded machine, tight enough to prove it
        // returned on the first poll rather than on the deadline.
        assert!(heartbeat::wait_until_ready(
            "test",
            &beat,
            Duration::from_secs(30)
        ));
        assert!(started.elapsed() < Duration::from_secs(5));
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn wait_until_ready_times_out_when_no_beat_appears() {
        let dir = tempdir();
        let started = Instant::now();
        let timeout = Duration::from_millis(600);
        assert!(!heartbeat::wait_until_ready(
            "test",
            &dir.join(system::heartbeat::HEARTBEAT_FILE),
            timeout
        ));
        assert!(started.elapsed() >= timeout);
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn wait_until_ready_ignores_a_degraded_beat() {
        let dir = tempdir();
        let path = dir.join(system::heartbeat::HEARTBEAT_FILE);
        fs::write(&path, r#"{"ts":1,"status":"degraded","detail":"starting"}"#).unwrap();
        assert!(!heartbeat::wait_until_ready(
            "test",
            &path,
            Duration::from_millis(400)
        ));
        fs::remove_dir_all(&dir).ok();
    }

    // -- CreateTargets::resolve ---------------------------------------------

    #[test]
    fn resolve_errors_when_workdir_already_exists() {
        let dir = tempdir();
        let workdir = dir.join("wd");
        fs::create_dir_all(&workdir).unwrap();
        let result = CreateTargets::resolve(&workdir.to_string_lossy(), None);
        assert!(result.is_err());
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn resolve_errors_when_explicit_configdir_already_exists() {
        let dir = tempdir();
        let workdir = dir.join("wd");
        let cdir = dir.join("custom-config");
        fs::create_dir_all(&cdir).unwrap();
        let result =
            CreateTargets::resolve(&workdir.to_string_lossy(), Some(&cdir.to_string_lossy()));
        assert!(result.is_err());
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn resolve_defaults_configdir_under_workdir_and_creates_nothing() {
        let dir = tempdir();
        let workdir = dir.join("wd");
        let targets = CreateTargets::resolve(&workdir.to_string_lossy(), None).unwrap();
        assert_eq!(targets.workdir, workdir);
        assert_eq!(targets.configdir, workdir.join(CONFIG_DIR));
        // Resolving must not touch the filesystem.
        assert!(!targets.workdir.exists());
        assert!(!targets.configdir.exists());
        fs::remove_dir_all(&dir).ok();
    }

    // -- Paths::for_create --------------------------------------------------

    #[test]
    fn for_create_creates_workdir_and_configdir() {
        let dir = tempdir();
        let workdir = dir.join("wd");
        let targets = CreateTargets::resolve(&workdir.to_string_lossy(), None).unwrap();
        let paths = Paths::for_create(&targets).unwrap();
        assert!(paths.workdir.exists());
        assert!(paths.configdir.exists());
        assert!(
            paths.pids.ends_with(
                PathBuf::from(system::pid::DATA_DIR)
                    .join(BIN_KM)
                    .join(system::pid::PIDS_DIR)
            )
        );
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn for_create_honors_explicit_configdir() {
        let dir = tempdir();
        let workdir = dir.join("wd");
        let cdir = dir.join("custom-config");
        let targets =
            CreateTargets::resolve(&workdir.to_string_lossy(), Some(&cdir.to_string_lossy()))
                .unwrap();
        let paths = Paths::for_create(&targets).unwrap();
        assert_eq!(
            paths.configdir.canonicalize().unwrap(),
            cdir.canonicalize().unwrap()
        );
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
        assert_eq!(
            paths.workdir.canonicalize().unwrap(),
            workdir.canonicalize().unwrap()
        );
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
        assert_eq!(
            paths.configdir.canonicalize().unwrap(),
            cdir.canonicalize().unwrap()
        );
        assert_eq!(
            paths.logdir.canonicalize().unwrap(),
            ldir.canonicalize().unwrap()
        );
        fs::remove_dir_all(&dir).ok();
    }

    // -- Paths::for_existing ------------------------------------------------

    #[test]
    fn for_existing_errors_when_workdir_missing() {
        let dir = tempdir();
        let workdir = dir.join("does-not-exist");
        let result = Paths::for_existing(&workdir.to_string_lossy());
        assert!(result.is_err());
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn for_existing_resolves_workdir_and_pids() {
        let dir = tempdir();
        let workdir = dir.join("wd");
        fs::create_dir_all(&workdir).unwrap();
        let paths = Paths::for_existing(&workdir.to_string_lossy()).unwrap();
        assert_eq!(
            paths.workdir.canonicalize().unwrap(),
            workdir.canonicalize().unwrap()
        );
        assert!(
            paths.pids.ends_with(
                PathBuf::from(system::pid::DATA_DIR)
                    .join(BIN_KM)
                    .join(system::pid::PIDS_DIR)
            )
        );
        // configdir/logdir default under workdir but need not exist.
        assert_eq!(paths.configdir, paths.workdir.join(CONFIG_DIR));
        assert_eq!(paths.logdir, paths.workdir.join(LOG_DIR));
        fs::remove_dir_all(&dir).ok();
    }
}
