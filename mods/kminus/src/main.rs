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
use std::process::Child;
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
use process::{control_plane, pod_runner, virtual_kubelet};
use system::file::{CONFIG_DIR, LOG_DIR};
use system::heartbeat::{self, Beat, HeartbeatMonitor, heartbeat_file};
use system::messages;
use system::pid::{
    BIN_KM, cleanup_stale_pids, is_alive, kill_descendants, pids_dir, read_pid, remove_pid,
    terminate_process, write_pid,
};
use system::port::{PortCheck, check_ports, port_failures, probe_ports};

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
        #[cfg(unix)]
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
fn print_status(
    paths: &Paths,
    cp: &Child,
    pr: &Child,
    vk: &Child,
    ports: &KmPorts,
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
        pr.id(),
        ports.pr,
        ports.pr_metrics
    );
    println!(
        "    {}             pid={:<7}port={}",
        virtual_kubelet::BIN,
        vk.id(),
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
    std::thread::Builder::new()
        .name("km-signal".into())
        .spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            let signal = rt.block_on(arm_and_wait_for_shutdown(armed_tx));
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
const POD_RUNNER_SHUTDOWN_GRACE: Duration = Duration::from_secs(90);
const DEFAULT_SHUTDOWN_GRACE: Duration = Duration::from_secs(5);

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

    for name in [virtual_kubelet::BIN, pod_runner::BIN, control_plane::BIN] {
        if let Some(pid) = read_pid(&paths.pids, name) {
            let grace = shutdown_grace_for(name);
            info!(
                name,
                pid,
                grace_secs = grace.as_secs(),
                "Requesting graceful shutdown"
            );
            terminate_process(pid, grace);
        }
        remove_pid(&paths.pids, name);
    }

    // Backstop: SIGKILL anything still parented under km that the named
    // teardown missed (e.g. spawned just before the signal arrived).
    kill_descendants(std::process::id());

    let _ = fs::remove_dir_all(control_plane::pids_dir_in(&paths.workdir));
    let _ = fs::remove_dir_all(virtual_kubelet::pids_dir_in(&paths.workdir));

    remove_pid(&paths.pids, BIN_KM);
    let _ = fs::remove_dir(&paths.pids);

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

/// What a supervision round observed about a child process, kept separate
/// from [`Child`] so the decision below can be exercised without spawning
/// one.
#[derive(Debug, PartialEq, Eq)]
enum ProcessState {
    Running,
    /// The child is gone; the string describes how.
    Exited(String),
    /// Its state could not be read at all, which is itself a failure.
    Unknown(String),
}

/// One component's fate for one round.
#[derive(Debug, PartialEq, Eq)]
enum Verdict {
    Ok,
    Failed(String),
}

/// A supervised child: its process handle and its heartbeat counter.
struct Watched<C> {
    name: &'static str,
    child: C,
    monitor: HeartbeatMonitor,
}

/// Read a child's process state without blocking.
trait ChildState {
    fn state(&mut self) -> ProcessState;
}

impl ChildState for Child {
    fn state(&mut self) -> ProcessState {
        match self.try_wait() {
            Ok(Some(status)) => ProcessState::Exited(format!("process exited ({status})")),
            Ok(None) => ProcessState::Running,
            Err(e) => ProcessState::Unknown(format!("cannot check process state: {e}")),
        }
    }
}

/// Judge one component from its process state and its heartbeat.
///
/// An exit is failure immediately — there is no point waiting for beats to
/// go stale once the process is gone. A live process is judged only by its
/// heartbeat, and a single miss is not yet a failure: that is what the
/// consecutive-miss budget is for.
fn assess(process: ProcessState, monitor: &mut HeartbeatMonitor) -> Verdict {
    match process {
        ProcessState::Exited(how) | ProcessState::Unknown(how) => Verdict::Failed(how),
        ProcessState::Running => match monitor.poll() {
            Beat::Healthy | Beat::Missed { .. } => Verdict::Ok,
            Beat::Failed { misses, reason } => {
                Verdict::Failed(format!("missed {misses} consecutive heartbeats — {reason}"))
            }
        },
    }
}

/// One supervision round over every component. Returns the first failure,
/// having polled the components before it — so their miss counters stay
/// current and their warnings are logged even when a later one fails.
fn monitor_round<C: ChildState>(watched: &mut [Watched<C>]) -> Option<(&'static str, String)> {
    let mut failure = None;
    for w in watched.iter_mut() {
        if let Verdict::Failed(reason) = assess(w.child.state(), &mut w.monitor) {
            failure = failure.or(Some((w.name, reason)));
        }
    }
    failure
}

/// The first component whose recorded process is no longer alive.
///
/// Reads the PID files km writes immediately after each spawn — so the list
/// grows as the stack comes up, and a component not started yet is simply
/// absent rather than reported dead.
fn first_dead_component(pids: &Path) -> Option<(&'static str, u32)> {
    [control_plane::BIN, pod_runner::BIN, virtual_kubelet::BIN]
        .into_iter()
        .find_map(|name| match read_pid(pids, name) {
            Some(pid) if !is_alive(pid) => Some((name, pid)),
            _ => None,
        })
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

/// Watch every component km has spawned for its process disappearing, from
/// the first spawn until shutdown.
///
/// This runs alongside — and ahead of — the heartbeat monitor, which only
/// starts once the whole stack is ready. Without it, a component dying while
/// km is still gating on a later one goes unnoticed until that gate times
/// out: the full startup budget spent waiting, and then the failure reported
/// against the wrong component.
fn watch_spawned_processes(paths: Paths, ports: KmPorts) {
    std::thread::Builder::new()
        .name("km-procwatch".into())
        .spawn(move || {
            loop {
                std::thread::sleep(PROCESS_WATCH_TICK);
                if exit::in_progress() {
                    return;
                }
                if let Some((name, pid)) = first_dead_component(&paths.pids) {
                    let reason = match last_reported(&paths, name) {
                        Some(detail) => format!("process {pid} is gone (last reported: {detail})"),
                        None => format!("process {pid} is gone"),
                    };
                    fail_stack(&paths, &ports, name, &reason);
                }
            }
        })
        .expect("spawn km-procwatch thread");
}

/// Supervise the stack until a component fails or a signal arrives.
///
/// Each round checks both channels: the child process itself, which makes an
/// outright exit immediate rather than waiting for heartbeats to go stale,
/// and the component's heartbeat file, which catches a process that is alive
/// but no longer serving. Never returns: a component failure exits 1 here, a
/// signal exits from the km-signal thread.
fn monitor_stack(
    paths: &Paths,
    ports: &KmPorts,
    hb: &KmHeartbeat,
    children: Vec<(&'static str, Child)>,
) -> ! {
    let interval = hb.interval();
    let max_missed = hb.max_missed();
    let stale_after = heartbeat::stale_after(interval);

    let mut watched: Vec<Watched<Child>> = children
        .into_iter()
        .map(|(name, child)| Watched {
            name,
            child,
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

    // Process liveness is the watcher thread's job, at its own tighter tick;
    // this loop owns the heartbeat rounds. `monitor_round` still checks the
    // child handles, which is what turns a race the monitor wins into an
    // "exited (status)" message rather than a bare "process is gone".
    loop {
        if !sleep_unless_shutdown(interval) {
            stand_down();
        }
        if let Some((name, reason)) = monitor_round(&mut watched) {
            fail_stack(paths, ports, name, &reason);
        }
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
            let _ = fs::remove_file(&p);
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
    if read_pid(&paths.pids, BIN_KM).is_some_and(is_alive) {
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

    // After the banner, before anything is spawned: the port table is the
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

    let vk_kubeconfig = paths.configdir.join(control_plane::VK_KUBECONFIG_FILE);
    let vk_prconfig = paths.configdir.join(pod_runner::VK_PR_CONFIG_FILE);
    let kubeconfig = paths.configdir.join(control_plane::KUBECONFIG_FILE);

    clear_regenerated_configs(&paths.configdir);
    // Emptied before any component runs, so a previous run's messages cannot
    // be read back as this one's.
    messages::clear(&paths.workdir);

    // Install signal-driven shutdown BEFORE spawning anything, so a
    // Ctrl+C at any point during the (multi-second) startup tears the
    // stack down in order rather than orphaning it.
    install_signal_shutdown(paths.clone(), sys_cfg.ports.clone());
    // Started before anything is spawned, so a component dying mid-startup is
    // caught while km is still gating on a later one.
    watch_spawned_processes(paths.clone(), sys_cfg.ports.clone());

    // Start subprocesses in dependency order, reporting each phase to stdout
    // so the (multi-second) startup shows live progress instead of a silent
    // gap before the ready banner.
    let started = Instant::now();

    let mut phase = step("Starting control-plane");
    let mut cp = control_plane::start(
        &paths.workdir,
        &paths.configdir,
        &paths.logdir,
        &paths.pids,
        &sys_cfg,
        &port_api,
        &port_kcm,
    );
    step_done(phase);

    phase = step("Starting pod-runner");
    let mut pr = pod_runner::start(
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
        &mut cp,
    );
    step_done(phase);

    phase = step("Cleaning up stale node resources");
    control_plane::cleanup_stale_node_resources(&kubeconfig);
    step_done(phase);

    phase = step("Starting virtual-kubelet");
    let vk = virtual_kubelet::start(
        &paths.workdir,
        &paths.configdir,
        &paths.logdir,
        &paths.pids,
        &sys_cfg,
        &port_vk,
        &vk_kubeconfig,
        &vk_prconfig,
        &mut cp,
        &mut pr,
    );
    step_done(phase);

    println!(
        "  all components ready in {:.1}s",
        started.elapsed().as_secs_f64()
    );
    info!("K- ready");

    print_status(&paths, &cp, &pr, &vk, &sys_cfg.ports, &kubeconfig);

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

    // Supervise from here on. Teardown on signal is still owned by the
    // km-signal thread installed above; the monitor stands down when it
    // fires. The Child handles move into the monitor, which reaps them.
    monitor_stack(
        &paths,
        &sys_cfg.ports,
        &sys_cfg.heartbeat,
        vec![
            (control_plane::BIN, cp),
            (pod_runner::BIN, pr),
            (virtual_kubelet::BIN, vk),
        ],
    )
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

    let km_pid = read_pid(&paths.pids, BIN_KM);
    match km_pid {
        Some(pid) if is_alive(pid) => {
            println!("Stopping Tabsdata K- (K minus) [km pid={pid}]...");
            // Signal the foreground `km start` and let its own shutdown
            // thread drive the orderly teardown, then wait for it to exit.
            terminate_process(pid, STOP_WAIT);
            if is_alive(pid) {
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
                .filter(|name| read_pid(&paths.pids, name).is_some_and(is_alive))
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
    match read_pid(&paths.pids, name) {
        Some(pid) if is_alive(pid) => println!("  {:<16} running   pid={}{}", name, pid, beat),
        Some(pid) => println!("  {:<16} DEAD      pid={} (stale PID file)", name, pid),
        None => println!("  {:<16} stopped", name),
    }
}

/// Render the per-component status block (km, control-plane + children,
/// pod-runner, virtual-kubelet) for the instance at `paths`. Returns
/// whether the km orchestrator itself is alive. Shared by `km status`
/// and the "already running" guard in `km start`.
fn print_instance_status(paths: &Paths) -> bool {
    let km_running = read_pid(&paths.pids, BIN_KM).is_some_and(is_alive);

    print_component_status(paths, BIN_KM);
    print_component_status(paths, control_plane::BIN);
    for (name, pid) in control_plane::read_children(&paths.workdir) {
        let alive = pid.parse::<u32>().map(is_alive).unwrap_or(false);
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

/// Report the status of the kminus instance in `args.workdir`.
///
/// Exit code reflects overall state: 0 if the km orchestrator is running,
/// 3 if it is stopped — mirroring LSB `status` conventions so callers can
/// script on it.
fn command_status(args: StatusArgs) {
    let paths = Paths::for_existing(&args.workdir).unwrap_or_else(|e| {
        eprintln!("ERROR: {e}");
        std::process::exit(1);
    });

    println!();
    println!("Tabsdata K- (K minus) status");
    println!("  workdir          {}", paths.workdir.display());
    println!();
    let km_running = print_instance_status(&paths);
    println!();

    if km_running {
        println!("Overall: running");
    } else {
        println!("Overall: stopped");
        std::process::exit(3);
    }
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
async fn arm_and_wait_for_shutdown(armed: std::sync::mpsc::Sender<()>) -> String {
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
async fn arm_and_wait_for_shutdown(armed: std::sync::mpsc::Sender<()>) -> String {
    use tokio::signal::windows::{ctrl_break, ctrl_c};
    let mut ctrlc = ctrl_c().expect("install Ctrl+C handler");
    let mut ctrlbreak = ctrl_break().expect("install Ctrl+Break handler");
    let _ = armed.send(());
    tokio::select! {
        _ = ctrlc.recv() => "Ctrl+C".to_string(),
        _ = ctrlbreak.recv() => "Ctrl+Break".to_string(),
    }
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

fn main() {
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

    // -- monitor round ------------------------------------------------------

    /// Scripted process states, so the round can be driven without
    /// spawning children.
    struct FakeChild(ProcessState);

    impl ChildState for FakeChild {
        fn state(&mut self) -> ProcessState {
            match &self.0 {
                ProcessState::Running => ProcessState::Running,
                ProcessState::Exited(how) => ProcessState::Exited(how.clone()),
                ProcessState::Unknown(how) => ProcessState::Unknown(how.clone()),
            }
        }
    }

    fn ready_beat(dir: &Path) -> PathBuf {
        let path = dir.join(system::heartbeat::HEARTBEAT_FILE);
        fs::write(&path, r#"{"ts":1,"status":"ready","detail":""}"#).unwrap();
        path
    }

    fn watched(
        name: &'static str,
        state: ProcessState,
        beat: PathBuf,
        max_missed: u32,
    ) -> Watched<FakeChild> {
        Watched {
            name,
            child: FakeChild(state),
            monitor: HeartbeatMonitor::new(name, beat, Duration::from_secs(15), max_missed),
        }
    }

    #[test]
    fn assess_fails_immediately_on_an_exited_process() {
        let dir = tempdir();
        let mut monitor =
            HeartbeatMonitor::new("test", ready_beat(&dir), Duration::from_secs(15), 4);
        // A fresh, ready beat must not save a process that is already gone.
        let verdict = assess(
            ProcessState::Exited("process exited (0)".into()),
            &mut monitor,
        );
        assert_eq!(verdict, Verdict::Failed("process exited (0)".into()));
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn assess_fails_when_the_process_state_is_unreadable() {
        let dir = tempdir();
        let mut monitor =
            HeartbeatMonitor::new("test", ready_beat(&dir), Duration::from_secs(15), 4);
        let verdict = assess(
            ProcessState::Unknown("permission denied".into()),
            &mut monitor,
        );
        assert_eq!(verdict, Verdict::Failed("permission denied".into()));
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn assess_accepts_a_live_process_with_a_fresh_beat() {
        let dir = tempdir();
        let mut monitor =
            HeartbeatMonitor::new("test", ready_beat(&dir), Duration::from_secs(15), 4);
        assert_eq!(assess(ProcessState::Running, &mut monitor), Verdict::Ok);
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
        assert_eq!(assess(ProcessState::Running, &mut monitor), Verdict::Ok);
        assert_eq!(assess(ProcessState::Running, &mut monitor), Verdict::Ok);
        let Verdict::Failed(reason) = assess(ProcessState::Running, &mut monitor) else {
            panic!("the third miss must fail the component");
        };
        assert!(reason.starts_with("missed 3 consecutive heartbeats"));
        assert!(reason.contains("no readable heartbeat"));
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn monitor_round_passes_when_every_component_is_healthy() {
        let dir = tempdir();
        let beat = ready_beat(&dir);
        let mut all = vec![
            watched("cp", ProcessState::Running, beat.clone(), 4),
            watched("pr", ProcessState::Running, beat.clone(), 4),
            watched("vk", ProcessState::Running, beat, 4),
        ];
        assert_eq!(monitor_round(&mut all), None);
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn monitor_round_reports_the_first_failure() {
        let dir = tempdir();
        let beat = ready_beat(&dir);
        let mut all = vec![
            watched("cp", ProcessState::Running, beat.clone(), 4),
            watched(
                "pr",
                ProcessState::Exited("process exited (1)".into()),
                beat.clone(),
                4,
            ),
            watched(
                "vk",
                ProcessState::Exited("process exited (2)".into()),
                beat,
                4,
            ),
        ];
        let (name, reason) = monitor_round(&mut all).unwrap();
        assert_eq!(name, "pr");
        assert_eq!(reason, "process exited (1)");
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn monitor_round_keeps_counting_components_after_a_failure() {
        let dir = tempdir();
        let missing = dir.join(system::heartbeat::HEARTBEAT_FILE);
        let mut all = vec![
            watched(
                "cp",
                ProcessState::Exited("process exited (1)".into()),
                missing.clone(),
                4,
            ),
            watched("pr", ProcessState::Running, missing, 2),
        ];
        // cp fails on round one, but pr must still be polled: its miss
        // counter has to be current if the stack somehow keeps running.
        assert_eq!(monitor_round(&mut all).unwrap().0, "cp");
        assert_eq!(monitor_round(&mut all).unwrap().0, "cp");
        // pr has now missed twice, so it would fail on its own merits.
        let pr_verdict = assess(ProcessState::Running, &mut all[1].monitor);
        assert!(matches!(pr_verdict, Verdict::Failed(_)));
        fs::remove_dir_all(&dir).ok();
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

    // -- process watcher ---------------------------------------------------

    #[test]
    fn first_dead_component_ignores_components_not_started_yet() {
        let dir = tempdir();
        // No PID files at all: the stack has not started, nothing is dead.
        assert_eq!(first_dead_component(&dir), None);

        // A component that is up must not be reported either.
        write_pid(&dir, control_plane::BIN, std::process::id());
        assert_eq!(first_dead_component(&dir), None);
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn first_dead_component_reports_a_recorded_process_that_is_gone() {
        let dir = tempdir();
        // A pid this high cannot belong to a live process.
        let dead_pid = u32::MAX - 1;
        write_pid(&dir, control_plane::BIN, std::process::id());
        write_pid(&dir, pod_runner::BIN, dead_pid);

        assert_eq!(
            first_dead_component(&dir),
            Some((pod_runner::BIN, dead_pid))
        );
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn first_dead_component_reports_in_start_order() {
        let dir = tempdir();
        let dead_pid = u32::MAX - 1;
        // Both gone: the control-plane is named, being the one started first
        // and the one the others depend on.
        write_pid(&dir, control_plane::BIN, dead_pid);
        write_pid(&dir, virtual_kubelet::BIN, dead_pid);

        assert_eq!(
            first_dead_component(&dir),
            Some((control_plane::BIN, dead_pid))
        );
        fs::remove_dir_all(&dir).ok();
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
        assert_eq!(paths.workdir, workdir.canonicalize().unwrap());
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
