// Copyright 2026 Tabsdata Inc.

//! Pod executor — manages the full lifecycle of a pod's containers.
//!
//! # Architecture
//!
//! The pod executor uses a shared-state + channel architecture:
//!
//! - **[`PodState`]** (behind `Arc<Mutex<>>`) holds the mutable status and
//!   container process handles. Both the internal orchestrator task and the
//!   external [`PodHandle`] access it through brief, non-overlapping locks.
//!   **The lock is never held across an `.await` point** to prevent deadlocks.
//!
//! - **[`ContainerConfig`]** (behind `Arc`) holds immutable launch
//!   configuration. Shared freely without locking.
//!
//! - **Per-container monitor tasks** watch each main container concurrently
//!   and report exits through an mpsc channel. This avoids sequential
//!   monitoring where container B's exit would go unnoticed until container
//!   A finishes. Each monitor task takes its handle out of shared state
//!   (brief lock), waits on it (no lock held), then puts it back (brief lock).
//!
//! - **Kill channel** (`kill_tx`/`kill_rx`): `PodHandle::kill()` sends the
//!   grace period on this channel and returns immediately. The orchestrator
//!   receives the kill request inside its `select!` loop — where it has
//!   coordination over the process handles — and forwards SIGTERM/SIGKILL
//!   to all containers. This avoids the deadlock that would occur if kill
//!   tried to access handles directly while monitor tasks hold them.
//!
//! - **Shutdown signal** is propagated to all container processes (their
//!   I/O pump tasks) and checked at every phase boundary. On shutdown,
//!   the orchestrator kills all running containers before exiting.
//!
//! - **Drop safety**: dropping [`PodHandle`] aborts the orchestrator task.
//!   Container processes with `kill_on_drop(true)` are cleaned up when
//!   their `ExecutorHandle` is dropped from the shared state.
//!
//! # Example
//!
//! ```rust,no_run
//! use std::sync::Arc;
//! use std::time::Duration;
//! use pod_runner::pod::executor::*;
//! use pod_runner::spec::pod::PodSpec;
//! use pod_runner::spec::constants::PodPhase;
//!
//! # async fn example() -> std::io::Result<()> {
//! let pod_spec: PodSpec = todo!("load from JSON/YAML");
//! let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(());
//!
//! let mut handle = PodExecutor::new(Arc::new(pod_spec))
//!     .work_dir("/tmp/pod-work")
//!     .log_dir("/tmp/pod-logs")
//!     .propagate_env(false)
//!     .die_with_parent(true)
//!     .spawn(shutdown_rx)
//!     .await?;
//!
//! // Reactive — receive every state change
//! loop {
//!     let event = handle.wait().await?;
//!     // event.status is a full PodStatus snapshot
//!     if let PodEventKind::Completed { .. } = &event.kind {
//!         break;
//!     }
//! }
//! # Ok(())
//! # }
//! ```

use std::collections::HashMap;
use std::io;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use tokio::sync::{Mutex, mpsc, watch};
use tokio::task::{JoinHandle, JoinSet};
use tracing::{Instrument, debug, debug_span, info_span, trace, warn};

use crate::pod::container_log::{ContainerLogStream, read_only_mount_warning};
use crate::pod::lifecycle::PodLifecycle;
use crate::pod::probe::{self, ProbeAction};
use crate::process::executor::{
    DevNullOutput, DirectFileOutput, ExecutorHandle, MergedOutput, OutputRedirector,
    ProcessExecutor,
};
use crate::process::sweep;
use crate::spec::constants::*;
use crate::spec::pod::{Container, PodSpec};
use crate::spec::status::PodStatus;

// ---------------------------------------------------------------------------
// Container role
// ---------------------------------------------------------------------------

/// Role of a container within the pod lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContainerRole {
    /// Init container — runs sequentially before main containers.
    Init,
    /// Sidecar — init container with `restart_policy: Always`.
    /// Starts async during init phase and runs alongside main containers.
    Sidecar,
    /// Main container — runs concurrently after init completes.
    Main,
}

impl std::fmt::Display for ContainerRole {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Init => write!(f, "init"),
            Self::Sidecar => write!(f, "sidecar"),
            Self::Main => write!(f, "main"),
        }
    }
}

// ---------------------------------------------------------------------------
// PodTask — user-registered task that runs for a pod's full lifetime
// ---------------------------------------------------------------------------

/// Context handed to a registered [`PodTask`] when the pod starts.
///
/// The task observes both signals; they are independent. `pod_kill` fires
/// when this specific pod is killed (or reaches a terminal state); the
/// orchestrator sets it to `true` on its way out. `runner_shutdown` fires
/// when the whole pod runner is shutting down.
#[derive(Clone)]
pub struct PodTaskContext {
    /// The pod's spec. Tasks should treat this as read-only.
    pub pod: Arc<PodSpec>,
    /// The pod's root working directory (matches what the executor was
    /// configured with via [`PodExecutor::work_dir`]). May be empty when
    /// no work dir was configured.
    pub work_dir: PathBuf,
    /// Fires (`true`) when this pod is being terminated. Tasks should
    /// exit promptly once observed.
    pub pod_kill: watch::Receiver<bool>,
    /// Fires when the pod runner itself is shutting down. Tasks should
    /// exit promptly once observed.
    pub runner_shutdown: watch::Receiver<()>,
}

/// A user-registered task that runs alongside a pod for its full lifetime.
///
/// Registered tasks are spawned when [`PodExecutor::spawn`] returns and
/// are expected to exit when either [`PodTaskContext::pod_kill`] or
/// [`PodTaskContext::runner_shutdown`] fires. The executor allows
/// `shutdown_grace_period` for each task to exit voluntarily after
/// signalling, then aborts any that overrun.
///
/// If [`PodHandle`] is dropped before the orchestrator finishes (e.g.
/// the pod is force-deleted), tasks are aborted immediately via
/// [`JoinSet`]'s drop semantics — no grace period in that path.
///
/// Tasks are typically registered on [`crate::logic::PodLogic`], which
/// adds them to every executor it creates.
#[async_trait]
pub trait PodTask: Send + Sync {
    /// Run the task. The task should exit when either of the watch
    /// signals in `ctx` fires.
    async fn run(&self, ctx: PodTaskContext);
}

// ---------------------------------------------------------------------------
// PodEvent — emitted on every state change
// ---------------------------------------------------------------------------

/// An event emitted by the pod executor on every state change.
///
/// The `status` field contains a full [`PodStatus`] snapshot at the time
/// of the event, ready to attach to the pod spec for API responses.
#[derive(Debug, Clone)]
pub struct PodEvent {
    pub kind: PodEventKind,
    pub status: PodStatus,
}

/// The kind of state change that occurred.
#[derive(Debug, Clone)]
pub enum PodEventKind {
    /// A container has started running.
    ContainerStarted { name: String, role: ContainerRole },
    /// A container's readiness probe passed.
    ContainerReady { name: String },
    /// A container exited with a non-zero exit code.
    ContainerFailed { name: String, exit_code: i32 },
    /// A container exited with exit code 0.
    ContainerSucceeded { name: String },
    /// A failed container waits out its back-off before restarting in place.
    ContainerBackOff {
        name: String,
        restart_count: u32,
        delay: Duration,
    },
    /// A probe executed with a result.
    ProbeResult {
        container: String,
        probe_type: String,
        success: bool,
    },
    /// The pod phase transitioned.
    PhaseChanged { from: PodPhase, to: PodPhase },
    /// The pod reached a terminal state (Succeeded or Failed).
    Completed { phase: PodPhase },
}

// ---------------------------------------------------------------------------
// PodState — shared mutable state between orchestrator and PodHandle
// ---------------------------------------------------------------------------

/// Shared mutable state for a running pod.
///
/// Accessed by both the orchestrator task and the external [`PodHandle`].
/// All access goes through `Arc<Mutex<PodState>>` with locks held briefly
/// — **never across an `.await` point** — to prevent deadlocks.
///
/// [`PodLifecycle`] is the single source of truth for pod and container
/// state. The legacy [`PodStatus`] view is derived on demand via
/// [`PodState::status`] — there is no parallel mirror to keep in sync.
pub(crate) struct PodState {
    pub lifecycle: PodLifecycle,
    pub handles: HashMap<String, ExecutorHandle>,
}

impl PodState {
    /// Generate a [`PodStatus`] snapshot from the lifecycle. Cheap
    /// enough to call on every emit (a few allocations).
    pub fn status(&self) -> PodStatus {
        self.lifecycle.to_pod_status()
    }
}

// ---------------------------------------------------------------------------
// PodHandle — returned after spawning
// ---------------------------------------------------------------------------

/// Handle to a running pod.
///
/// Provides a reactive event stream via [`wait`](PodHandle::wait) and
/// the ability to kill all containers. Every event carries a full
/// [`PodStatus`] snapshot.
pub struct PodHandle {
    pod: Arc<PodSpec>,
    event_rx: mpsc::Receiver<PodEvent>,
    #[allow(dead_code)] // kept alive to prevent orchestrator task cancellation on drop
    orchestrator: JoinHandle<()>,
    state: Arc<Mutex<PodState>>,
    /// Channel to request the orchestrator to kill all containers.
    /// The mpsc carries the grace period to the monitor phase's select!.
    kill_tx: mpsc::Sender<Duration>,
    /// Watch channel that signals "kill requested" to ALL phases (init,
    /// monitor, sidecar termination). Unlike the mpsc kill_tx which is
    /// only consumed in the monitor loop, this watch is checked in every
    /// select! the orchestrator uses, including during init container waits.
    kill_watch_tx: watch::Sender<bool>,
    /// Cached status snapshot, updated on each `wait()`.
    cached_status: PodStatus,
    /// Log directory (if logging was configured on the executor).
    log_dir: Option<PathBuf>,
}

impl PodHandle {
    /// The pod spec (shared reference).
    pub fn pod(&self) -> &Arc<PodSpec> {
        &self.pod
    }

    /// Wait for the next pod event.
    ///
    /// Returns `Ok(event)` on each state change. Returns an error when
    /// the orchestrator has finished and there are no more events.
    pub async fn wait(&mut self) -> io::Result<PodEvent> {
        match self.event_rx.recv().await {
            Some(event) => {
                self.cached_status = event.status.clone();
                Ok(event)
            }
            None => Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "pod orchestrator finished, no more events",
            )),
        }
    }

    /// Wait until the pod reaches a terminal state (Succeeded or Failed).
    ///
    /// Consumes all intermediate events and returns the final phase.
    pub async fn wait_for_completion(&mut self) -> io::Result<PodPhase> {
        loop {
            let event = self.wait().await?;
            if let PodEventKind::Completed { phase } = &event.kind {
                return Ok(*phase);
            }
        }
    }

    /// Current status snapshot (updated after each `wait()` call).
    pub fn status(&self) -> &PodStatus {
        &self.cached_status
    }

    /// Read the live status from shared state (may differ from cached).
    pub async fn live_status(&self) -> PodStatus {
        let st = self.state.lock().await;
        st.status()
    }

    /// Kill all running containers.
    ///
    /// Returns immediately. The kill request is broadcast to all phases
    /// of the orchestrator:
    /// - The **kill watch** wakes up any `select!` in init/monitor/sidecar
    ///   phases so they exit promptly.
    /// - The **kill mpsc** carries the grace period to the monitor phase
    ///   which forwards it to per-container monitor tasks.
    ///
    /// To know when containers have actually exited, continue calling
    /// [`wait()`](PodHandle::wait) and look for `ContainerFailed` /
    /// `ContainerSucceeded` / `Completed` events.
    pub fn kill(&self, grace_period: Duration) {
        let _ = self.kill_watch_tx.send(true);
        let _ = self.kill_tx.try_send(grace_period);
    }

    /// Create a [`PodKillHandle`] — a clonable, lightweight handle that
    /// can kill the pod without holding the full `PodHandle`.
    ///
    /// Use this when the `PodHandle` is owned by a monitor task (calling
    /// `wait()`) and you need a separate path to trigger kills.
    pub fn kill_handle(&self) -> PodKillHandle {
        PodKillHandle {
            kill_tx: self.kill_tx.clone(),
            kill_watch_tx: self.kill_watch_tx.clone(),
            state: Arc::clone(&self.state),
        }
    }

    /// Wait for the orchestrator task to fully finish, including its
    /// post-completion cleanup of registered [`PodTask`]s.
    ///
    /// Call this after observing a [`PodEventKind::Completed`] event
    /// but **before** dropping the handle — otherwise
    /// [`PodHandle::Drop`] aborts the orchestrator, which in turn
    /// aborts in-flight registered tasks before they can run any
    /// post-loop code (e.g. a final scan, a flush, an emit).
    ///
    /// The wait is naturally bounded by the executor's
    /// `shutdown_grace_period`: the orchestrator's task-cleanup phase
    /// uses that as its timeout, so this method returns within roughly
    /// that window.
    pub async fn wait_for_finish(&mut self) {
        use std::future::{Future, poll_fn};
        use std::pin::Pin;
        // JoinHandle is `Unpin`. Poll via poll_fn so we drive the
        // task to completion without consuming the JoinHandle —
        // PodHandle::Drop can still call .abort() on a finished
        // handle (idempotent), and the by-mutable-ref borrow keeps
        // self valid for subsequent operations.
        let _ = poll_fn(|cx| Pin::new(&mut self.orchestrator).poll(cx)).await;
    }

    /// Create a [`PodLogHandle`] for streaming container logs.
    ///
    /// Use this when the `PodHandle` is owned by a monitor task and
    /// you need a separate path to read logs.
    pub fn log_handle(&self) -> PodLogHandle {
        PodLogHandle {
            pod: Arc::clone(&self.pod),
            log_dir: self.log_dir.clone(),
            state: Arc::clone(&self.state),
            kill_watch: self.kill_watch_tx.subscribe(),
        }
    }
}

/// Lightweight, clonable handle for killing a pod.
///
/// Extracted from [`PodHandle::kill_handle()`] so that kill can be
/// triggered independently of the event loop (which holds `&mut PodHandle`
/// for [`wait()`](PodHandle::wait)).
#[derive(Clone)]
pub struct PodKillHandle {
    kill_tx: mpsc::Sender<Duration>,
    kill_watch_tx: watch::Sender<bool>,
    /// Shared state reference — lets the handle mark the pod as a
    /// disruption target on the live lifecycle without going through
    /// the event channel.
    state: Arc<Mutex<PodState>>,
}

impl PodKillHandle {
    /// Kill all running containers. Returns immediately.
    ///
    /// Same mechanism as [`PodHandle::kill()`] — broadcasts to all
    /// orchestrator phases.
    pub fn kill(&self, grace_period: Duration) {
        let _ = self.kill_watch_tx.send(true);
        let _ = self.kill_tx.try_send(grace_period);
    }

    /// Mark the pod as a disruption target on the live lifecycle. The
    /// next event emitted by the orchestrator will carry this condition
    /// to the monitor and on to the store (status is derived from
    /// lifecycle on every emit).
    pub async fn mark_disruption_target(&self) {
        let now = chrono::Utc::now().to_rfc3339();
        let mut st = self.state.lock().await;
        st.lifecycle.mark_disruption_target(&now);
    }
}

/// Handle for streaming container logs from a running pod.
///
/// Extracted from [`PodHandle::log_handle()`]. Knows the pod spec
/// (container names), log directory, and shared state (restart counts,
/// terminal phase) — everything needed to resolve and stream log files.
#[derive(Clone)]
pub struct PodLogHandle {
    pod: Arc<PodSpec>,
    log_dir: Option<PathBuf>,
    state: Arc<Mutex<PodState>>,
    kill_watch: watch::Receiver<bool>,
}

impl PodLogHandle {
    /// Stream container logs.
    ///
    /// Returns an `mpsc::Receiver` that yields log chunks as `Bytes`.
    /// If `container` is `None`, uses the first container in the spec.
    pub fn stream(
        &self,
        container: Option<&str>,
        options: super::container_log::ContainerLogOptions,
    ) -> Result<tokio::sync::mpsc::Receiver<Result<bytes::Bytes, io::Error>>, io::Error> {
        use super::container_log;

        // Resolve container name
        let container_name = match container {
            Some(c) => {
                let exists = self.pod.spec.all_containers().iter().any(|ct| ct.name == c);
                if !exists {
                    return Err(io::Error::new(
                        io::ErrorKind::NotFound,
                        format!("container '{}' not found in pod", c),
                    ));
                }
                c.to_string()
            }
            None => self
                .pod
                .spec
                .containers
                .first()
                .map(|c| c.name.clone())
                .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "pod has no containers"))?,
        };

        let log_dir = self.log_dir.clone().ok_or_else(|| {
            io::Error::new(io::ErrorKind::NotFound, "log directory not configured")
        })?;

        let container_name_owned = container_name;
        let state = Arc::clone(&self.state);
        let mut kill_watch = self.kill_watch.clone();
        let (tx, rx) = tokio::sync::mpsc::channel(32);

        tokio::spawn(async move {
            // Get restart count from shared state (async lock)
            let restart_count = {
                let st = state.lock().await;
                st.lifecycle
                    .container(&container_name_owned)
                    .or_else(|| st.lifecycle.init_container(&container_name_owned))
                    .map(|cl| cl.restart_count())
                    .unwrap_or(0)
            };

            let log_path = container_log::resolve_log_path(
                &log_dir,
                &container_name_owned,
                restart_count,
                options.previous,
            );

            if let Err(e) = container_log::stream_log_file(
                &tx,
                &log_path,
                options.tail_lines,
                options.follow,
                options.timestamps,
                &state,
                &mut kill_watch,
            )
            .await
            {
                let _ = tx.send(Err(e)).await;
            }
        });

        Ok(rx)
    }
}

impl Drop for PodHandle {
    fn drop(&mut self) {
        // Abort the orchestrator task to prevent it from running after
        // the handle is dropped. Container processes with kill_on_drop(true)
        // will be cleaned up when their ExecutorHandle is dropped from
        // the shared state (when the Arc<Mutex<PodState>> refcount hits zero).
        self.orchestrator.abort();
    }
}

// ---------------------------------------------------------------------------
// Log config for containers
// ---------------------------------------------------------------------------

/// How container output should be logged.
#[derive(Clone)]
enum LogConfig {
    /// Discard all output.
    DevNull,
    /// Log to a directory with K8s-style container log naming.
    /// Each container gets `<log_dir>/<container_name>/<restart_count>.log`
    /// with standard numbered rotation (`.1`, `.2`, etc.).
    ContainerLog {
        log_dir: PathBuf,
        max_size_bytes: u64,
        max_files: usize,
    },
}

/// Default log rotation: 100MB per file, 5 files.
const DEFAULT_LOG_MAX_SIZE_BYTES: u64 = 100 * 1024 * 1024;
const DEFAULT_LOG_MAX_FILES: usize = 5;
const RESTART_BACKOFF_DOUBLINGS: u32 = 4;

/// Timeout for waiting on a sidecar to exit after being killed.
/// If exceeded, the sidecar is force-killed (SIGKILL, no grace period).
const SIDECAR_KILL_TIMEOUT: Duration = Duration::from_secs(10);

// ---------------------------------------------------------------------------
// PodExecutor — builder
// ---------------------------------------------------------------------------

/// Builder for configuring and spawning a pod's containers.
///
/// Wraps the [`PodSpec`] in an `Arc` and configures how containers
/// are launched, logged, and managed.
pub struct PodExecutor {
    pod: Arc<PodSpec>,
    work_dir: Option<PathBuf>,
    pid_dir: Option<PathBuf>,
    log_config: LogConfig,
    propagate_env: bool,
    extra_env: HashMap<String, String>,
    die_with_parent: bool,
    max_restarts: u32,
    restart_backoff_base: Duration,
    shutdown_grace_period: Duration,
    tasks: Vec<Arc<dyn PodTask>>,
}

impl PodExecutor {
    /// Create a new pod executor for the given pod spec.
    pub fn new(pod: Arc<PodSpec>) -> Self {
        Self {
            pod,
            work_dir: None,
            pid_dir: None,
            log_config: LogConfig::DevNull,
            propagate_env: true,
            extra_env: HashMap::new(),
            die_with_parent: true,
            max_restarts: 0,
            restart_backoff_base: Duration::from_secs(2),
            shutdown_grace_period: Duration::from_secs(30),
            tasks: Vec::new(),
        }
    }

    /// Set the working directory for all containers.
    ///
    /// If not set, each container uses its own `working_dir` from the spec,
    /// falling back to the parent's cwd.
    pub fn work_dir(mut self, dir: impl Into<PathBuf>) -> Self {
        self.work_dir = Some(dir.into());
        self
    }

    /// Set the log directory. Each container gets a subdirectory with
    /// K8s-style log naming: `<log_dir>/<container_name>/<restart_count>.log`.
    pub fn log_dir(mut self, dir: impl Into<PathBuf>) -> Self {
        self.log_config = LogConfig::ContainerLog {
            log_dir: dir.into(),
            max_size_bytes: DEFAULT_LOG_MAX_SIZE_BYTES,
            max_files: DEFAULT_LOG_MAX_FILES,
        };
        self
    }

    /// Set the log directory with custom rolling parameters.
    pub fn log_dir_rolling(
        mut self,
        dir: impl Into<PathBuf>,
        max_size_bytes: u64,
        max_files: usize,
    ) -> Self {
        self.log_config = LogConfig::ContainerLog {
            log_dir: dir.into(),
            max_size_bytes,
            max_files,
        };
        self
    }

    /// Set the PID tracking directory for spawned processes.
    pub fn pid_dir(mut self, dir: impl Into<PathBuf>) -> Self {
        self.pid_dir = Some(dir.into());
        self
    }

    /// Whether to propagate the launcher's environment to containers.
    ///
    /// Default: `true`.
    pub fn propagate_env(mut self, propagate: bool) -> Self {
        self.propagate_env = propagate;
        self
    }

    /// Add an extra environment variable for all containers.
    pub fn env(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.extra_env.insert(key.into(), value.into());
        self
    }

    /// Add multiple extra environment variables for all containers.
    pub fn envs<I, K, V>(mut self, vars: I) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        for (k, v) in vars {
            self.extra_env.insert(k.into(), v.into());
        }
        self
    }

    /// Whether containers should die when the parent exits.
    ///
    /// Default: `true`.
    pub fn die_with_parent(mut self, die: bool) -> Self {
        self.die_with_parent = die;
        self
    }

    /// Maximum number of restarts for containers with `OnFailure` restart policy.
    ///
    /// Default: `0` (no restarts).
    pub fn max_restarts(mut self, max: u32) -> Self {
        self.max_restarts = max;
        self
    }

    /// Base duration for exponential restart backoff.
    ///
    /// Actual backoff: `base * 2^attempt` (capped at `base * 16`).
    /// Default: `2s`.
    pub fn restart_backoff_base(mut self, base: Duration) -> Self {
        self.restart_backoff_base = base;
        self
    }

    /// Grace period for container kills on shutdown or phase failures.
    /// Default: `30s`.
    pub fn shutdown_grace_period(mut self, d: Duration) -> Self {
        self.shutdown_grace_period = d;
        self
    }

    /// Register a [`PodTask`] to run alongside the pod for its full
    /// lifetime.
    ///
    /// Each registered task is spawned when the pod starts and is expected
    /// to exit when [`PodTaskContext::pod_kill`] or
    /// [`PodTaskContext::runner_shutdown`] fires. The executor allows up
    /// to `shutdown_grace_period` for each task to exit voluntarily,
    /// then aborts any that overrun.
    ///
    /// Call multiple times to register more than one task. Tasks see the
    /// same context (modulo separate clones of the watch receivers).
    pub fn register_task(mut self, task: Arc<dyn PodTask>) -> Self {
        self.tasks.push(task);
        self
    }

    /// Spawn the pod and return a [`PodHandle`].
    ///
    /// Returns immediately after creating the orchestrator task. The
    /// orchestrator runs in the background, launching init containers
    /// sequentially (sidecars async), then main containers concurrently.
    /// Events are emitted on the handle for every state change.
    pub async fn spawn(self, shutdown: watch::Receiver<()>) -> io::Result<PodHandle> {
        let pod = self.pod;
        let tasks = self.tasks;
        let task_work_dir = self.work_dir.clone().unwrap_or_default();
        let task_grace = self.shutdown_grace_period;

        let initial_lifecycle = build_initial_lifecycle(&pod);
        let initial_status = initial_lifecycle.to_pod_status();

        let state = Arc::new(Mutex::new(PodState {
            lifecycle: initial_lifecycle,
            handles: HashMap::new(),
        }));

        let (event_tx, event_rx) = mpsc::channel(256);
        let (kill_tx, kill_rx) = mpsc::channel(4);
        let (kill_watch_tx, kill_watch_rx) = watch::channel(false);

        // The pod's terminationGracePeriodSeconds (if set) is the grace
        // between SIGTERM and SIGKILL for every kill this executor
        // initiates (shutdown, probe-driven kill, init/start-failure
        // cleanup); otherwise the runner's configured default applies.
        let shutdown_grace_period = pod
            .termination_grace_period()
            .unwrap_or(self.shutdown_grace_period);

        let config = Arc::new(ContainerConfig {
            work_dir: self.work_dir,
            pid_dir: self.pid_dir,
            log_config: self.log_config,
            propagate_env: self.propagate_env,
            extra_env: self.extra_env,
            die_with_parent: self.die_with_parent,
            max_restarts: self.max_restarts,
            restart_backoff_base: self.restart_backoff_base,
            shutdown_grace_period,
            kill_token_prefix: Some(sweep::pod_prefix(&pod.metadata.name, &pod.metadata.uid)),
        });

        let log_dir = config.log_dir().cloned();

        // Root span: the executor outlives the preparer task that
        // spawns it, so don't inherit the prepare{...} span context.
        let pod_span = info_span!(
            parent: None,
            "pod",
            pod = format!("{}_{}", pod.metadata.name, pod.metadata.uid).as_str(),
        );

        // Clones for the task-lifecycle wrapper. The original
        // `kill_watch_tx` is moved into PodHandle so `kill()` can still
        // fire the same channel.
        let kill_watch_tx_tasks = kill_watch_tx.clone();
        let task_pod = Arc::clone(&pod);
        let orch_pod = Arc::clone(&pod);
        let orch_state = Arc::clone(&state);

        let orchestrator = tokio::spawn(
            async move {
                // Spawn registered tasks into a JoinSet. JoinSet owns
                // the handles — its Drop aborts every task if this
                // outer future is itself aborted (e.g. PodHandle is
                // dropped before the orchestrator finishes), which is
                // the force-stop path.
                let mut task_set: JoinSet<()> = JoinSet::new();
                for task in &tasks {
                    let task = Arc::clone(task);
                    let ctx = PodTaskContext {
                        pod: Arc::clone(&task_pod),
                        work_dir: task_work_dir.clone(),
                        pod_kill: kill_watch_tx_tasks.subscribe(),
                        runner_shutdown: shutdown.clone(),
                    };
                    // Inherit the orchestrator's pod span so events
                    // emitted inside `task.run` carry pod context
                    // (`pod = <name>`) automatically.
                    task_set.spawn(async move { task.run(ctx).await }.in_current_span());
                }

                run_orchestrator(
                    orch_pod,
                    orch_state,
                    config,
                    event_tx,
                    kill_rx,
                    kill_watch_rx,
                    shutdown,
                )
                .await;

                // Orchestrator finished naturally. Fire kill_watch so
                // tasks observe pod termination, then reap with grace.
                // `kill_watch_tx.send(true)` is idempotent if the
                // orchestrator was already in kill/shutdown mode.
                let _ = kill_watch_tx_tasks.send(true);
                if !task_set.is_empty() {
                    // If the timeout future is dropped (grace exceeded),
                    // task_set is dropped with it, aborting any
                    // tasks that have not exited.
                    let _ = tokio::time::timeout(task_grace, async {
                        while task_set.join_next().await.is_some() {}
                    })
                    .await;
                }
            }
            .instrument(pod_span),
        );

        Ok(PodHandle {
            pod,
            event_rx,
            orchestrator,
            state,
            kill_tx,
            kill_watch_tx,
            cached_status: initial_status,
            log_dir,
        })
    }
}

// ---------------------------------------------------------------------------
// Internal — container config (immutable, shared via Arc)
// ---------------------------------------------------------------------------

/// Immutable configuration for how containers are launched.
/// Shared via `Arc` — no locking needed.
struct ContainerConfig {
    work_dir: Option<PathBuf>,
    pid_dir: Option<PathBuf>,
    log_config: LogConfig,
    propagate_env: bool,
    extra_env: HashMap<String, String>,
    die_with_parent: bool,
    max_restarts: u32,
    restart_backoff_base: Duration,
    /// Grace period for container kills triggered by shutdown or phase failures.
    shutdown_grace_period: Duration,
    /// Prefix of the [`sweep::PR_KILL_TOKEN_VAR`] token injected into every
    /// container of this pod, from [`sweep::pod_prefix`].
    ///
    /// Every process in a container's tree inherits the token, which is
    /// what lets the orphan sweep find the ones that escaped the
    /// container's process group. `None` disables the sweep for this pod.
    kill_token_prefix: Option<String>,
}

impl ContainerConfig {
    /// Return the log directory if configured, for probe log output.
    fn log_dir(&self) -> Option<&PathBuf> {
        match &self.log_config {
            LogConfig::DevNull => None,
            LogConfig::ContainerLog { log_dir, .. } => Some(log_dir),
        }
    }

    /// Build an output redirector for a container.
    ///
    /// Uses [`ContainerLogStream`] for K8s-style log naming:
    /// `<log_dir>/<container_name>/<restart_count>.log`
    ///
    /// Read-only mount warnings (one per `readOnly` volume mount) are
    /// written at the very beginning of the log, before any container
    /// output — pod-runner cannot enforce read-only mounts, so the
    /// container is warned not to write into those volumes.
    fn output_for(&self, container: &Container, restart_count: u32) -> Box<dyn OutputRedirector> {
        match &self.log_config {
            LogConfig::DevNull => Box::new(DevNullOutput),
            LogConfig::ContainerLog {
                log_dir,
                max_size_bytes,
                max_files,
            } => {
                let warnings = read_only_mount_warnings(container);
                if self.die_with_parent {
                    Box::new(MergedOutput::new(
                        ContainerLogStream::new(
                            log_dir,
                            &container.name,
                            restart_count,
                            *max_size_bytes,
                            *max_files,
                        )
                        .with_header_lines(warnings),
                    ))
                } else {
                    // Detached processes can't use piped output — fall back
                    // to direct file. Uses the same naming convention.
                    let path = log_dir
                        .join(&container.name)
                        .join(format!("{}.log", restart_count));
                    Box::new(DirectFileOutput::merged(path).with_header_lines(warnings))
                }
            }
        }
    }

    /// Build a ProcessExecutor for a container.
    fn build_process(&self, container: &Container, restart_count: u32) -> ProcessExecutor {
        let exec_args = container.exec_args();
        let mut builder = ProcessExecutor::new(&exec_args[0]);

        if exec_args.len() > 1 {
            builder = builder.args(&exec_args[1..]);
        }

        // Working directory: container spec > pod executor config > inherit
        let work_dir = if !container.working_dir.is_empty() {
            Some(PathBuf::from(&container.working_dir))
        } else {
            self.work_dir.clone()
        };
        if let Some(dir) = work_dir {
            builder = builder.work_dir(dir);
        }

        if let Some(ref dir) = self.pid_dir {
            builder = builder.pid_dir(dir.as_path());
        }
        builder = builder.propagate_env(self.propagate_env);
        builder = builder.die_with_parent(self.die_with_parent);

        if !self.extra_env.is_empty() {
            builder = builder.envs(self.extra_env.iter().map(|(k, v)| (k.as_str(), v.as_str())));
        }

        // Container-level env vars (value-only; value_from requires external resolution)
        let container_envs: Vec<(String, String)> = container
            .env
            .iter()
            .filter_map(|ev| {
                let val = ev.value.as_ref()?;
                Some((ev.name.clone(), val.clone()))
            })
            .collect();
        if !container_envs.is_empty() {
            builder = builder.envs(container_envs);
        }

        // Injected last: a pod spec that declares the same variable must
        // not be able to shadow the token the orphan sweep matches on.
        if let Some(ref prefix) = self.kill_token_prefix {
            builder = builder.env(
                sweep::PR_KILL_TOKEN_VAR,
                sweep::container_token(prefix, &container.name),
            );
        }

        builder = builder.output_boxed(self.output_for(container, restart_count));
        builder
    }
}

/// One warning line per read-only volume mount of `container`, to be
/// written at the very beginning of its log
/// (see [`read_only_mount_warning`]).
fn read_only_mount_warnings(container: &Container) -> Vec<String> {
    container
        .volume_mounts
        .iter()
        .filter(|vm| vm.read_only)
        .map(|vm| read_only_mount_warning(&vm.name))
        .collect()
}

// ---------------------------------------------------------------------------
// Internal — event emission
// ---------------------------------------------------------------------------

/// Emit a pod event. Best-effort: if the receiver is full or dropped, lost.
fn emit(event_tx: &mpsc::Sender<PodEvent>, kind: PodEventKind, status: &PodStatus) {
    trace!(kind = ?kind, phase = ?status.phase, "Emitting event");
    if let Err(e) = event_tx.try_send(PodEvent {
        kind,
        status: status.clone(),
    }) {
        warn!("Failed to emit event: {}", e);
    }
}

/// Transition the pod phase. Locks state briefly, emits PhaseChanged.
///
/// Proposes the transition to the [`PodLifecycle`] first. If the lifecycle
/// rejects it (e.g. Running→Pending, or any mutation after a terminal
/// phase), the mutation is logged and skipped instead of being applied
/// silently.
async fn transition_phase(
    state: &Mutex<PodState>,
    event_tx: &mpsc::Sender<PodEvent>,
    new_phase: PodPhase,
) {
    let mut st = state.lock().await;
    let old = st.lifecycle.phase();
    if old == new_phase {
        return;
    }
    let now = chrono::Utc::now().to_rfc3339();
    // Route through the lifecycle validator.
    let result = match new_phase {
        PodPhase::Running => st.lifecycle.start_running(&now),
        PodPhase::Succeeded => st.lifecycle.succeed("", &now),
        PodPhase::Failed => st.lifecycle.fail("", &now),
        PodPhase::Pending => {
            warn!("Rejected transition to Pending (only valid as initial state)");
            return;
        }
    };
    if let Err(e) = result {
        warn!("Rejected phase transition: {}", e);
        return;
    }
    let snapshot = st.status();
    drop(st); // release before emitting

    debug!(from = ?old, to = ?new_phase, "Phase changed");
    emit(
        event_tx,
        PodEventKind::PhaseChanged {
            from: old,
            to: new_phase,
        },
        &snapshot,
    );
}

// ---------------------------------------------------------------------------
// Internal — container lifecycle helpers
// ---------------------------------------------------------------------------

/// Spawn a container process. Locks state briefly to insert handle + update status.
///
/// Creates a `container` span that wraps the process spawn, so the
/// process executor's internal `process` span becomes a child:
/// `pod → container → process`. All subsequent process logs (exit, kill)
/// inherit the pod and container context.
async fn spawn_container(
    container: &Container,
    role: ContainerRole,
    restart_count: u32,
    config: &ContainerConfig,
    state: &Mutex<PodState>,
    event_tx: &mpsc::Sender<PodEvent>,
    shutdown: &watch::Receiver<()>,
) -> io::Result<()> {
    let container_span = debug_span!(
        "container",
        container = container.name.as_str(),
        role = %role,
    );
    let _enter = container_span.enter();

    let builder = config.build_process(container, restart_count);
    let handle = builder.spawn(shutdown.clone()).await?;
    let pid = handle.pid();

    debug!(pid, "Container started");

    let now = chrono::Utc::now().to_rfc3339();
    let snapshot = {
        let mut st = state.lock().await;
        st.handles.insert(container.name.clone(), handle);
        // Route through the lifecycle validator. The process is already
        // spawned; on reject we log and preserve state rather than
        // corrupt it further.
        if let Some(cl) = st.lifecycle.container_by_role_mut(&container.name, role)
            && let Err(e) = cl.on_started(&now)
        {
            warn!("Rejected container start transition: {}", e);
        }
        st.status()
    };

    emit(
        event_tx,
        PodEventKind::ContainerStarted {
            name: container.name.clone(),
            role,
        },
        &snapshot,
    );

    Ok(())
}

/// Mark a container as finished. Locks state briefly to update status.
async fn mark_container_finished(
    name: &str,
    exit_code: i32,
    role: ContainerRole,
    state: &Mutex<PodState>,
    event_tx: &mpsc::Sender<PodEvent>,
) {
    if exit_code == 0 {
        debug!(container = name, role = %role, "Container exited (exit_code=0)");
    } else {
        debug!(container = name, role = %role, exit_code, "Container exited");
    }
    let now = chrono::Utc::now().to_rfc3339();
    let (kind, snapshot) = {
        let mut st = state.lock().await;
        // Route through the lifecycle validator; rejects terminating
        // a container that isn't Running (e.g. double-exit).
        if let Some(cl) = st.lifecycle.container_by_role_mut(name, role)
            && let Err(e) = cl.on_exited(exit_code, &now)
        {
            warn!("Rejected container exit transition: {}", e);
        }
        let kind = if exit_code == 0 {
            PodEventKind::ContainerSucceeded {
                name: name.to_string(),
            }
        } else {
            PodEventKind::ContainerFailed {
                name: name.to_string(),
                exit_code,
            }
        };
        (kind, st.status())
    };
    emit(event_tx, kind, &snapshot);
}

/// Wait for a container to exit. Takes the handle out of state (brief lock),
/// waits on it (NO lock held — critical to avoid deadlocks), puts it back.
async fn wait_container(name: &str, state: &Mutex<PodState>) -> io::Result<i32> {
    let mut handle = {
        let mut st = state.lock().await;
        st.handles.remove(name).ok_or_else(|| {
            io::Error::new(io::ErrorKind::NotFound, format!("no handle for '{}'", name))
        })?
    };

    let code = handle.wait().await?;

    {
        let mut st = state.lock().await;
        st.handles.insert(name.to_string(), handle);
    }

    Ok(code)
}

/// Kill a single container by name. Takes handle out (brief lock), kills,
/// puts back (brief lock). The kill itself holds no lock.
async fn kill_container(name: &str, state: &Mutex<PodState>, grace_period: Duration) {
    let handle_opt = {
        let mut st = state.lock().await;
        st.handles.remove(name)
    };
    if let Some(mut handle) = handle_opt {
        handle.kill(grace_period).await;
        let mut st = state.lock().await;
        st.handles.insert(name.to_string(), handle);
    }
}

/// Send SIGTERM to a container without waiting for exit. Brief lock.
async fn sigterm_container(name: &str, state: &Mutex<PodState>) {
    let st = state.lock().await;
    if let Some(handle) = st.handles.get(name) {
        handle.send_sigterm();
    }
}

// ---------------------------------------------------------------------------
// Internal — container exit message
// ---------------------------------------------------------------------------

/// Message sent by per-container monitor tasks when a container exits.
struct ContainerExited {
    name: String,
    exit_code: i32,
}

// ---------------------------------------------------------------------------
// Internal — orchestrator
// ---------------------------------------------------------------------------

/// The main orchestrator coroutine. Runs the full pod lifecycle:
///
/// 1. **Init containers** — sequential, sidecars start async
/// 2. **Main containers** — started concurrently
/// 3. **Monitor** — concurrent per-container tasks report exits via channel;
///    select! also listens for kill requests and shutdown
/// 4. **Sidecar termination** — SIGTERM to all, then wait with timeout
/// 5. **Finalize** — emit Completed event
///
/// On failure at any phase, all running containers are killed.
///
/// Wraps [`run_pod`] so that nothing the pod started outlives it. The
/// final sweep belongs here rather than in each of the phases' exit
/// paths because it has to happen on all of them — including the ones
/// that kill nothing at all: a container that daemonises a server and
/// then exits on its own leaves that server running without any kill
/// path ever being involved.
async fn run_orchestrator(
    pod: Arc<PodSpec>,
    state: Arc<Mutex<PodState>>,
    config: Arc<ContainerConfig>,
    event_tx: mpsc::Sender<PodEvent>,
    kill_rx: mpsc::Receiver<Duration>,
    kill_watch: watch::Receiver<bool>,
    shutdown: watch::Receiver<()>,
) {
    let token_prefix = config.kill_token_prefix.clone();
    // Observed after the pod finishes, to tell a single pod ending from
    // the whole runner going down. `run_pod` consumes its own receiver.
    let runner_shutdown = shutdown.clone();

    run_pod(pod, state, config, event_tx, kill_rx, kill_watch, shutdown).await;

    // On a runner shutdown the sweep is done once for all pods, so skip
    // the per-pod one. An unreadable channel counts as "not shutting
    // down": sweeping redundantly is cheap, not sweeping is a leak.
    if runner_shutdown.has_changed().unwrap_or(false) {
        return;
    }
    if let Some(prefix) = token_prefix {
        sweep::kill_orphans(&prefix).await;
    }
}

/// Drive the pod through its phases and return once it is terminal.
async fn run_pod(
    pod: Arc<PodSpec>,
    state: Arc<Mutex<PodState>>,
    config: Arc<ContainerConfig>,
    event_tx: mpsc::Sender<PodEvent>,
    kill_rx: mpsc::Receiver<Duration>,
    mut kill_watch: watch::Receiver<bool>,
    mut shutdown: watch::Receiver<()>,
) {
    let pod_start = Instant::now();

    debug!(
        init_containers = pod.spec.init_containers.len(),
        containers = pod.spec.containers.len(),
        "Orchestrator starting",
    );

    {
        let mut st = state.lock().await;
        let ts = chrono::Utc::now().to_rfc3339();
        if let Err(e) = st.lifecycle.schedule(&ts) {
            warn!("Rejected schedule transition: {}", e);
        }
        // start_time / last_transition_time are owned by PodLifecycle
        // and set by `new()` / `schedule()`. No mirror write needed.
    }

    // Phase 1: init containers (sequential, sidecars async)
    let phase_start = Instant::now();
    let init_result = run_init_containers(
        &pod,
        &state,
        &config,
        &event_tx,
        &mut kill_watch,
        &mut shutdown,
    )
    .await;
    let init_ms = phase_start.elapsed().as_millis();
    if let Err(e) = init_result {
        warn!("Init containers failed: {}", e);
        kill_all_in_state(
            &state,
            config.shutdown_grace_period,
            config.kill_token_prefix.as_deref(),
        )
        .await;
        fail_pod(&state, &event_tx, &format!("Init failed: {}", e)).await;
        debug!(
            init_ms,
            total_ms = pod_start.elapsed().as_millis(),
            "Pod timing (failed in init)"
        );
        return;
    }
    if shutdown.has_changed().unwrap_or(true) || *kill_watch.borrow() {
        kill_all_in_state(
            &state,
            config.shutdown_grace_period,
            config.kill_token_prefix.as_deref(),
        )
        .await;
        fail_pod(&state, &event_tx, "Shutdown/kill during init").await;
        debug!(
            init_ms,
            total_ms = pod_start.elapsed().as_millis(),
            "Pod timing (killed in init)"
        );
        return;
    }
    {
        let mut st = state.lock().await;
        let ts = chrono::Utc::now().to_rfc3339();
        if let Err(e) = st.lifecycle.mark_initialized(&ts) {
            warn!("Rejected mark_initialized transition: {}", e);
        }
    }

    // Phase 2: start main containers
    if pod.spec.containers.is_empty() {
        terminate_sidecars(&pod, &state, &event_tx).await;
        succeed_pod(&state, &event_tx, "No containers to run").await;
        debug!(
            init_ms,
            total_ms = pod_start.elapsed().as_millis(),
            "Pod timing (no containers)"
        );
        return;
    }
    let phase_start = Instant::now();
    let start_result = start_main_containers(&pod, &state, &config, &event_tx, &shutdown).await;
    let start_ms = phase_start.elapsed().as_millis();
    if let Err(e) = start_result {
        warn!("Failed to start main containers: {}", e);
        kill_all_in_state(
            &state,
            config.shutdown_grace_period,
            config.kill_token_prefix.as_deref(),
        )
        .await;
        fail_pod(&state, &event_tx, &format!("Container start failed: {}", e)).await;
        debug!(
            init_ms,
            total_ms = pod_start.elapsed().as_millis(),
            "Pod timing (start failed)"
        );
        return;
    }
    transition_phase(&state, &event_tx, PodPhase::Running).await;
    debug!(
        init_ms,
        start_ms,
        total_ms = pod_start.elapsed().as_millis(),
        "Pod timing (running)"
    );

    // Phase 3: monitor main containers + probes (concurrent, with kill/shutdown)
    let phase_start = Instant::now();
    let final_phase =
        monitor_main_containers(&pod, &state, &config, &event_tx, kill_rx, &mut shutdown).await;
    let main_ms = phase_start.elapsed().as_millis();

    // Phase 4: terminate sidecars
    let phase_start = Instant::now();
    terminate_sidecars(&pod, &state, &event_tx).await;
    let sidecar_term_ms = phase_start.elapsed().as_millis();

    let total_ms = pod_start.elapsed().as_millis();
    debug!(
        init_ms,
        main_ms,
        sidecar_term_ms,
        total_ms,
        phase = ?final_phase,
        "Pod execution completed",
    );

    // Finalize
    match final_phase {
        PodPhase::Succeeded => succeed_pod(&state, &event_tx, "Pod Succeeded").await,
        _ => fail_pod(&state, &event_tx, "Pod Failed").await,
    }
}

async fn fail_pod(state: &Mutex<PodState>, event_tx: &mpsc::Sender<PodEvent>, message: &str) {
    let (old, snapshot) = {
        let mut st = state.lock().await;
        let old = st.lifecycle.phase();
        if let Err(e) = st.lifecycle.fail(message, &chrono::Utc::now().to_rfc3339()) {
            warn!("Rejected fail transition: {}", e);
        }
        (old, st.status())
    };
    if old != PodPhase::Failed {
        emit(
            event_tx,
            PodEventKind::PhaseChanged {
                from: old,
                to: PodPhase::Failed,
            },
            &snapshot,
        );
    }
    emit(
        event_tx,
        PodEventKind::Completed {
            phase: PodPhase::Failed,
        },
        &snapshot,
    );
}

async fn succeed_pod(state: &Mutex<PodState>, event_tx: &mpsc::Sender<PodEvent>, message: &str) {
    let (old, snapshot) = {
        let mut st = state.lock().await;
        let old = st.lifecycle.phase();
        if let Err(e) = st
            .lifecycle
            .succeed(message, &chrono::Utc::now().to_rfc3339())
        {
            warn!("Rejected succeed transition: {}", e);
        }
        (old, st.status())
    };
    if old != PodPhase::Succeeded {
        emit(
            event_tx,
            PodEventKind::PhaseChanged {
                from: old,
                to: PodPhase::Succeeded,
            },
            &snapshot,
        );
    }
    emit(
        event_tx,
        PodEventKind::Completed {
            phase: PodPhase::Succeeded,
        },
        &snapshot,
    );
}

/// Kill all containers whose handles are currently in shared state.
/// Used during init/start failure cleanup. Handles that are held by
/// monitor tasks (removed from state during wait) won't be found here —
/// those are reached via the kill channel during monitoring.
/// Kill every running container of the pod, and every process that
/// escaped a container's process group.
///
/// The escapees are signalled in two stages around the group kills, so
/// they see the same grace period the containers do: SIGTERM first, so an
/// escaped server shuts itself down while the group kills are still
/// waiting, then SIGKILL for whatever is left once that grace has
/// elapsed. Both stages act on one scan — the token prefix covers every
/// container of the pod, so a pod kill walks the process table once, not
/// once per container and not once per stage.
///
/// `token_prefix` is the pod's [`sweep::pod_prefix`]. `None` skips the
/// sweep, which is what a runner-wide shutdown does: it sweeps once for
/// all pods instead of once per pod.
async fn kill_all_in_state(
    state: &Mutex<PodState>,
    grace_period: Duration,
    token_prefix: Option<&str>,
) {
    let orphans = match token_prefix {
        Some(prefix) => {
            let mut orphans = sweep::Orphans::find(prefix).await;
            orphans.terminate();
            Some(orphans)
        }
        None => None,
    };

    let names: Vec<String> = {
        let st = state.lock().await;
        st.handles.keys().cloned().collect()
    };
    if !names.is_empty() {
        debug!(containers = ?names, "Killing {} container(s)", names.len());
    }
    for name in &names {
        kill_container(name, state, grace_period).await;
    }

    if let Some(orphans) = orphans {
        // The containers are down, but an escapee shuts itself down on its
        // own clock and nothing above waited for it — least of all when the
        // main process was waiting on that escapee, so its exit ended the
        // container's kill early rather than late.
        orphans.await_exit(grace_period).await;
        orphans.force_kill();
    }
}

// ---------------------------------------------------------------------------
// Phase 1: init containers
// ---------------------------------------------------------------------------

async fn run_init_containers(
    pod: &PodSpec,
    state: &Mutex<PodState>,
    config: &ContainerConfig,
    event_tx: &mpsc::Sender<PodEvent>,
    kill_watch: &mut watch::Receiver<bool>,
    shutdown: &mut watch::Receiver<()>,
) -> io::Result<()> {
    {
        let mut st = state.lock().await;
        let ts = chrono::Utc::now().to_rfc3339();
        if let Err(e) = st
            .lifecycle
            .mark_not_initialized("init containers running", &ts)
        {
            warn!("Rejected mark_not_initialized transition: {}", e);
        }
    }

    let total = pod.spec.init_containers.len();
    debug!("Running {} init container(s)", total);

    for (idx, container) in pod.spec.init_containers.iter().enumerate() {
        if shutdown.has_changed().unwrap_or(true) || *kill_watch.borrow() {
            return Err(io::Error::new(io::ErrorKind::Interrupted, "shutdown/kill"));
        }

        if container.is_sidecar() {
            debug!(
                container = container.name.as_str(),
                step = idx + 1,
                total,
                "Starting sidecar"
            );
            spawn_container(
                container,
                ContainerRole::Sidecar,
                0,
                config,
                state,
                event_tx,
                shutdown,
            )
            .await?;
        } else {
            debug!(
                container = container.name.as_str(),
                step = idx + 1,
                total,
                "Running init container"
            );
            run_init_container(
                container,
                restarts_in_place(&pod.spec.restart_policy),
                config,
                state,
                event_tx,
                kill_watch,
                shutdown,
            )
            .await?;
        }
    }

    Ok(())
}

/// Only `Always`, which Kubernetes rejects on Job pods, restarts a failed
/// init container in place: a Job's init containers do real work, so their
/// failure must fail the pod, `OnFailure` included.
fn restarts_in_place(policy: &RestartPolicy) -> bool {
    matches!(policy, RestartPolicy::Always)
}

fn restart_delay(base: Duration, restart_count: u32) -> Duration {
    base * 2u32.pow(restart_count.min(RESTART_BACKOFF_DOUBLINGS))
}

/// Run one init container to completion. With a restart policy that
/// restarts in place, a failed run waits out an exponential back-off in
/// `Waiting(CrashLoopBackOff)` and runs again, up to the configured maximum
/// of restarts; the pod stays `Pending` throughout, as under a kubelet.
async fn run_init_container(
    container: &Container,
    restarts: bool,
    config: &ContainerConfig,
    state: &Mutex<PodState>,
    event_tx: &mpsc::Sender<PodEvent>,
    kill_watch: &mut watch::Receiver<bool>,
    shutdown: &mut watch::Receiver<()>,
) -> io::Result<()> {
    let mut restart_count = 0;
    loop {
        spawn_container(
            container,
            ContainerRole::Init,
            restart_count,
            config,
            state,
            event_tx,
            shutdown,
        )
        .await?;

        // Wait for init container to exit, with shutdown AND kill support.
        // kill_watch is checked here so PodHandle::kill() during init
        // phase is handled promptly instead of waiting for the init
        // container to finish naturally.
        let exit_code = tokio::select! {
            result = wait_container(&container.name, state) => result?,
            _ = shutdown.changed() => {
                return Err(io::Error::new(io::ErrorKind::Interrupted, "shutdown"));
            }
            _ = kill_watch.changed() => {
                return Err(io::Error::new(io::ErrorKind::Interrupted, "kill requested"));
            }
        };

        mark_container_finished(
            &container.name,
            exit_code,
            ContainerRole::Init,
            state,
            event_tx,
        )
        .await;

        if exit_code == 0 {
            return Ok(());
        }
        if !restarts || restart_count >= config.max_restarts {
            return Err(io::Error::other(format!(
                "init container '{}' failed with exit code {}",
                container.name, exit_code
            )));
        }

        let delay = restart_delay(config.restart_backoff_base, restart_count);
        restart_count += 1;
        back_off_container(
            &container.name,
            ContainerRole::Init,
            restart_count,
            delay,
            state,
            event_tx,
        )
        .await;
        tokio::select! {
            _ = tokio::time::sleep(delay) => {}
            _ = shutdown.changed() => {
                return Err(io::Error::new(io::ErrorKind::Interrupted, "shutdown"));
            }
            _ = kill_watch.changed() => {
                return Err(io::Error::new(io::ErrorKind::Interrupted, "kill requested"));
            }
        }
    }
}

async fn back_off_container(
    name: &str,
    role: ContainerRole,
    restart_count: u32,
    delay: Duration,
    state: &Mutex<PodState>,
    event_tx: &mpsc::Sender<PodEvent>,
) {
    warn!(
        container = name,
        role = %role,
        restart_count,
        delay_ms = delay.as_millis() as u64,
        "Container failed; restarting in place after back-off"
    );
    let snapshot = {
        let mut st = state.lock().await;
        if let Some(cl) = st.lifecycle.container_by_role_mut(name, role)
            && let Err(e) = cl.on_back_off()
        {
            warn!("Rejected container back-off transition: {}", e);
        }
        st.status()
    };
    emit(
        event_tx,
        PodEventKind::ContainerBackOff {
            name: name.to_string(),
            restart_count,
            delay,
        },
        &snapshot,
    );
}

// ---------------------------------------------------------------------------
// Phase 2: start main containers
// ---------------------------------------------------------------------------

async fn start_main_containers(
    pod: &PodSpec,
    state: &Mutex<PodState>,
    config: &ContainerConfig,
    event_tx: &mpsc::Sender<PodEvent>,
    shutdown: &watch::Receiver<()>,
) -> io::Result<()> {
    debug!("Starting {} main container(s)", pod.spec.containers.len());

    for container in &pod.spec.containers {
        spawn_container(
            container,
            ContainerRole::Main,
            0,
            config,
            state,
            event_tx,
            shutdown,
        )
        .await?;

        if container.readiness_probe.is_none() {
            debug!(
                container = container.name.as_str(),
                "Container ready (no readiness probe)"
            );
            let snapshot = {
                let mut st = state.lock().await;
                if let Some(cl) = st.lifecycle.container_mut(&container.name) {
                    cl.set_ready(true);
                }
                st.status()
            };
            emit(
                event_tx,
                PodEventKind::ContainerReady {
                    name: container.name.clone(),
                },
                &snapshot,
            );
        }
    }

    {
        let mut st = state.lock().await;
        update_readiness_conditions(&mut st);
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Phase 3: monitor main containers
// ---------------------------------------------------------------------------

/// Monitor all main containers concurrently.
///
/// Spawns a per-container monitor task that:
/// 1. Takes its `ExecutorHandle` out of shared state (brief lock)
/// 2. Awaits the process exit (NO lock held — avoids deadlocks)
/// 3. Puts the handle back (brief lock)
/// 4. Sends the exit code through a channel
///
/// The orchestrator `select!`s over:
/// - Container exit messages (from monitor tasks)
/// - Kill requests (from `PodHandle::kill()` via kill channel)
/// - Shutdown signal
///
/// On kill/shutdown: a `kill_signal` watch channel is triggered, which
/// each monitor task listens to via `select!`. The monitor task then
/// kills its own handle directly — no lock contention because the
/// monitor owns the handle at that point.
///
/// Monitor tasks are managed via a [`JoinSet`] — if the orchestrator
/// exits early (panic, abort via `PodHandle::drop`), the `JoinSet` is
/// dropped and all monitor tasks are automatically aborted. This makes
/// task leaks structurally impossible.
/// Monitor all main containers + probes concurrently.
///
/// Container monitors: per-container tasks in a `JoinSet<ContainerExited>`.
/// Probe tasks: in a separate `JoinSet<()>`, report actions via mpsc.
///
/// When a container exits, its probes are stopped via a per-container
/// `stop` signal. When a probe kills a container (startup/liveness
/// failure), it sends a `ProbeAction::KillContainer` which triggers
/// the kill signal for that container's monitor task.
///
/// Both JoinSets are dropped when this function returns, aborting all
/// monitor and probe tasks.
async fn monitor_main_containers(
    pod: &PodSpec,
    state: &Arc<Mutex<PodState>>,
    config: &ContainerConfig,
    event_tx: &mpsc::Sender<PodEvent>,
    mut kill_rx: mpsc::Receiver<Duration>,
    shutdown: &mut watch::Receiver<()>,
) -> PodPhase {
    // Kill signal broadcast to all monitor tasks.
    let (kill_signal_tx, _) = watch::channel::<Option<Duration>>(None);

    // Per-container stop signals — probes stop when their container exits.
    let mut stop_txs: HashMap<String, watch::Sender<bool>> = HashMap::new();

    // Container monitor JoinSet — aborted on drop.
    let mut monitor_set: JoinSet<ContainerExited> = JoinSet::new();

    // Spawn per-container monitor tasks.
    for container in &pod.spec.containers {
        let name = container.name.clone();
        let state_clone = Arc::clone(state);
        let mut kill_signal = kill_signal_tx.subscribe();
        let monitor_span = info_span!(
            "container",
            container = container.name.as_str(),
            role = "main",
        );

        let (stop_tx, _) = watch::channel(false);
        stop_txs.insert(name.clone(), stop_tx);

        monitor_set.spawn(
            async move {
                let mut handle = {
                    let mut st = state_clone.lock().await;
                    match st.handles.remove(&name) {
                        Some(h) => h,
                        None => {
                            return ContainerExited {
                                name,
                                exit_code: -1,
                            };
                        }
                    }
                };

                let exit_code = tokio::select! {
                    result = handle.wait() => result.unwrap_or(-1),
                    Ok(()) = kill_signal.changed() => {
                        let grace = kill_signal.borrow().unwrap_or(Duration::from_secs(5));
                        debug!("Kill signal received, killing process");
                        handle.kill(grace).await;
                        handle.wait().await.unwrap_or(-1)
                    }
                };

                {
                    let mut st = state_clone.lock().await;
                    st.handles.insert(name.clone(), handle);
                }

                ContainerExited { name, exit_code }
            }
            .instrument(monitor_span),
        );
    }

    // Probe JoinSet — aborted on drop (when this function returns).
    let mut probe_set: JoinSet<()> = JoinSet::new();
    let (probe_action_tx, mut probe_action_rx) = mpsc::channel::<ProbeAction>(64);

    // Spawn probe tasks for containers that have probes defined.
    // Each container's probes run in a single task that loops until
    // the container's stop signal fires (when the container exits).
    probe::spawn_probe_tasks(
        &pod.spec.containers,
        &mut probe_set,
        probe_action_tx,
        event_tx.clone(),
        Arc::clone(state),
        config.log_dir().cloned(),
        shutdown.clone(),
        &stop_txs,
    );

    let mut all_succeeded = true;
    // Track whether channels are still open to prevent busy-looping.
    // When an mpsc receiver returns None (all senders dropped), subsequent
    // recv() calls return None immediately. Without these guards, the
    // select! loop would spin — starving other tasks on the runtime and
    // preventing pods from reaching a terminal state under concurrent load.
    let mut probe_channel_open = true;
    let mut kill_channel_open = true;

    loop {
        tokio::select! {
            // Container exited
            result = monitor_set.join_next() => {
                match result {
                    Some(Ok(exited)) => {
                        // Signal this container's probes to stop
                        if let Some(stop_tx) = stop_txs.get(&exited.name) {
                            let _ = stop_tx.send(true);
                        }

                        mark_container_finished(
                            &exited.name, exited.exit_code, ContainerRole::Main,
                            state, event_tx,
                        ).await;
                        if exited.exit_code != 0 {
                            all_succeeded = false;
                        }
                    }
                    Some(Err(e)) => {
                        warn!("Monitor task failed: {}", e);
                        all_succeeded = false;
                    }
                    None => break, // all containers finished
                }
            }
            // Probe action (kill container, readiness change)
            action = probe_action_rx.recv(), if probe_channel_open => {
                match action {
                    Some(ProbeAction::KillContainer { container, reason }) => {
                        warn!(container = container.as_str(), "Probe killing container: {}", reason);
                        // Kill via the kill signal — the monitor task will handle it.
                        // Honor the pod's terminationGracePeriodSeconds (folded
                        // into shutdown_grace_period at spawn).
                        let _ = kill_signal_tx.send(Some(config.shutdown_grace_period));
                    }
                    Some(ProbeAction::ReadinessChanged { container, ready }) => {
                        debug!(container = container.as_str(), ready, "Readiness changed by probe");
                        let mut st = state.lock().await;
                        update_readiness_conditions(&mut st);
                    }
                    None => {
                        debug!("Probe action channel closed");
                        probe_channel_open = false;
                    }
                }
            }
            // External kill request
            grace = kill_rx.recv(), if kill_channel_open => {
                match grace {
                    Some(grace_period) => {
                        debug!("Kill request received — broadcasting to monitor tasks");
                        let _ = kill_signal_tx.send(Some(grace_period));
                        kill_all_in_state(state, grace_period, config.kill_token_prefix.as_deref()).await;
                    }
                    None => {
                        debug!("Kill channel closed");
                        kill_channel_open = false;
                    }
                }
            }
            // Shutdown
            _ = shutdown.changed() => {
                debug!("Shutdown during container monitoring — killing all");
                // Broadcast the kill to the per-container monitor tasks, which
                // own the live handles: each runs `handle.kill(grace)` (SIGTERM,
                // then SIGKILL after the pod's termination grace) and lets the
                // container run its own shutdown — e.g. the database container
                // stopping the embedded postgres it owns. Then wait for those
                // tasks to finish before returning; returning immediately would
                // drop `monitor_set` and abort them mid-kill, orphaning the
                // container processes.
                let _ = kill_signal_tx.send(Some(config.shutdown_grace_period));
                let drain_timeout = config.shutdown_grace_period + Duration::from_secs(10);
                let _ = tokio::time::timeout(drain_timeout, async {
                    while monitor_set.join_next().await.is_some() {}
                })
                .await;
                // Backstop: on a drain timeout some tasks may have re-inserted
                // their (already-signaled) handles without exiting. Kill anything
                // still recorded; a no-op for processes already gone.
                //
                // No per-pod sweep: the runner is going down, and it
                // sweeps once for every pod it started — at the start of
                // shutdown, so escapees get their SIGTERM immediately
                // rather than waiting for their own pod's turn, and again
                // after the drain.
                kill_all_in_state(state, config.shutdown_grace_period, None).await;
                return PodPhase::Failed;
            }
        }
    }

    // All containers exited — probe_set is dropped here, aborting probes

    {
        let mut st = state.lock().await;
        update_readiness_conditions(&mut st);
    }

    if all_succeeded {
        PodPhase::Succeeded
    } else {
        PodPhase::Failed
    }
}

// ---------------------------------------------------------------------------
// Phase 4: terminate sidecars
// ---------------------------------------------------------------------------

/// Terminate all sidecars. Sends SIGTERM to all first (so they start
/// dying concurrently), then waits for each with a timeout. Force-kills
/// on timeout.
async fn terminate_sidecars(
    pod: &PodSpec,
    state: &Mutex<PodState>,
    event_tx: &mpsc::Sender<PodEvent>,
) {
    let sidecar_names: Vec<String> = pod
        .spec
        .init_containers
        .iter()
        .filter(|c| c.is_sidecar())
        .map(|c| c.name.clone())
        .collect();

    if sidecar_names.is_empty() {
        return;
    }

    debug!(sidecars = ?sidecar_names, "Terminating {} sidecar(s)", sidecar_names.len());

    // Send SIGTERM to all sidecars — they start shutting down concurrently.
    for name in &sidecar_names {
        debug!(container = name.as_str(), "Sending SIGTERM to sidecar");
        sigterm_container(name, state).await;
    }

    // Wait for each to exit. Since SIGTERM was already sent to all,
    // most sidecars will be dying concurrently. The sequential wait
    // just collects results. Force-kill on timeout.
    for name in &sidecar_names {
        let wait_result =
            tokio::time::timeout(SIDECAR_KILL_TIMEOUT, wait_container(name, state)).await;

        let exit_code = match wait_result {
            Ok(Ok(code)) => code,
            Ok(Err(e)) => {
                warn!("Error waiting for sidecar '{}': {}", name, e);
                -1
            }
            Err(_) => {
                warn!("Timeout waiting for sidecar '{}' — force killing", name);
                kill_container(name, state, Duration::ZERO).await;
                -1
            }
        };

        mark_container_finished(name, exit_code, ContainerRole::Sidecar, state, event_tx).await;
    }
}

// ---------------------------------------------------------------------------
// Status helpers
// ---------------------------------------------------------------------------

/// Build the initial [`PodLifecycle`] matching the pod spec. Containers
/// are created in `Waiting(PodInitializing)` state, preserving spec order.
fn build_initial_lifecycle(pod: &PodSpec) -> PodLifecycle {
    let ts = chrono::Utc::now().to_rfc3339();
    let init: Vec<(String, ContainerRole)> = pod
        .spec
        .init_containers
        .iter()
        .map(|c| {
            let role = if c.is_sidecar() {
                ContainerRole::Sidecar
            } else {
                ContainerRole::Init
            };
            (c.name.clone(), role)
        })
        .collect();
    let main: Vec<(String, ContainerRole)> = pod
        .spec
        .containers
        .iter()
        .map(|c| (c.name.clone(), ContainerRole::Main))
        .collect();
    PodLifecycle::new(init, main, &ts)
}

/// Recompute the `ContainersReady` and `Ready` conditions based on the
/// current state of main containers. A container counts as ready if its
/// `ready` flag is set OR it is terminated.
///
/// Delegates entirely to [`PodLifecycle::update_readiness`] — the
/// legacy [`PodStatus`] is derived from the lifecycle on demand.
fn update_readiness_conditions(st: &mut PodState) {
    let now = chrono::Utc::now().to_rfc3339();
    st.lifecycle.update_readiness(&now);
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[cfg(unix)] // All pod executor tests use Unix shell commands (sh, echo, sleep, etc.)
mod tests {
    use std::path::Path;

    use super::*;
    use crate::spec::pod::{EnvVar, PodMetadata, PodSpecBody};
    use crate::testutils::test_dir;

    fn shutdown() -> (watch::Sender<()>, watch::Receiver<()>) {
        watch::channel(())
    }

    fn container(name: &str, cmd: &str) -> Container {
        Container {
            name: name.to_string(),
            command: vec!["sh".to_string(), "-c".to_string(), cmd.to_string()],
            ..Default::default()
        }
    }

    fn sidecar(name: &str, cmd: &str) -> Container {
        Container {
            restart_policy: Some("Always".to_string()),
            ..container(name, cmd)
        }
    }

    /// A pod spec with a unique uid.
    ///
    /// The uid matters even though no test asserts on it: it is half of
    /// the pod's orphan-sweep token prefix, and tests run in parallel in
    /// one process. Sharing a uid would let one test's pod teardown sweep
    /// another test's container processes.
    fn pod_spec(init_containers: Vec<Container>, containers: Vec<Container>) -> Arc<PodSpec> {
        Arc::new(PodSpec {
            metadata: PodMetadata {
                name: "test-pod".to_string(),
                namespace: "default".to_string(),
                uid: crate::testutils::unique_name("uid"),
                ..Default::default()
            },
            spec: PodSpecBody {
                init_containers,
                containers,
                ..Default::default()
            },
            ..Default::default()
        })
    }

    fn pod_spec_with_grace(containers: Vec<Container>, grace_secs: Option<u64>) -> Arc<PodSpec> {
        Arc::new(PodSpec {
            metadata: PodMetadata {
                name: "test-pod".to_string(),
                namespace: "default".to_string(),
                uid: crate::testutils::unique_name("uid"),
                ..Default::default()
            },
            spec: PodSpecBody {
                containers,
                termination_grace_period_seconds: grace_secs,
                ..Default::default()
            },
            ..Default::default()
        })
    }

    async fn collect_events(handle: &mut PodHandle) -> Vec<PodEvent> {
        let mut events = Vec::new();
        while let Ok(event) = handle.wait().await {
            let done = matches!(&event.kind, PodEventKind::Completed { .. });
            events.push(event);
            if done {
                break;
            }
        }
        events
    }

    // -- basic lifecycle -----------------------------------------------------

    #[tokio::test]
    async fn test_single_container_succeeds() {
        let (_tx, rx) = shutdown();
        let mut h = PodExecutor::new(pod_spec(vec![], vec![container("main", "exit 0")]))
            .spawn(rx)
            .await
            .unwrap();
        assert_eq!(h.wait_for_completion().await.unwrap(), PodPhase::Succeeded);
    }

    #[tokio::test]
    async fn test_single_container_fails() {
        let (_tx, rx) = shutdown();
        let mut h = PodExecutor::new(pod_spec(vec![], vec![container("main", "exit 1")]))
            .spawn(rx)
            .await
            .unwrap();
        assert_eq!(h.wait_for_completion().await.unwrap(), PodPhase::Failed);
    }

    #[tokio::test]
    async fn test_no_containers_succeeds() {
        let (_tx, rx) = shutdown();
        let mut h = PodExecutor::new(pod_spec(vec![], vec![]))
            .spawn(rx)
            .await
            .unwrap();
        assert_eq!(h.wait_for_completion().await.unwrap(), PodPhase::Succeeded);
    }

    #[tokio::test]
    async fn test_multiple_containers_all_succeed() {
        let (_tx, rx) = shutdown();
        let mut h = PodExecutor::new(pod_spec(
            vec![],
            vec![
                container("a", "exit 0"),
                container("b", "exit 0"),
                container("c", "exit 0"),
            ],
        ))
        .spawn(rx)
        .await
        .unwrap();
        assert_eq!(h.wait_for_completion().await.unwrap(), PodPhase::Succeeded);
    }

    #[tokio::test]
    async fn test_multiple_containers_one_fails() {
        let (_tx, rx) = shutdown();
        let mut h = PodExecutor::new(pod_spec(
            vec![],
            vec![container("ok", "exit 0"), container("fail", "exit 42")],
        ))
        .spawn(rx)
        .await
        .unwrap();
        assert_eq!(h.wait_for_completion().await.unwrap(), PodPhase::Failed);
    }

    // -- event stream --------------------------------------------------------

    #[tokio::test]
    async fn test_events_contain_started_and_completed() {
        let (_tx, rx) = shutdown();
        let mut h = PodExecutor::new(pod_spec(vec![], vec![container("main", "exit 0")]))
            .spawn(rx)
            .await
            .unwrap();
        let events = collect_events(&mut h).await;

        assert!(events.iter().any(|e| matches!(&e.kind,
            PodEventKind::ContainerStarted { name, .. } if name == "main")));
        assert!(events.iter().any(|e| matches!(&e.kind,
            PodEventKind::ContainerSucceeded { name } if name == "main")));
        assert!(events.iter().any(|e| matches!(
            &e.kind,
            PodEventKind::Completed {
                phase: PodPhase::Succeeded
            }
        )));
    }

    #[tokio::test]
    async fn test_events_contain_failed_container() {
        let (_tx, rx) = shutdown();
        let mut h = PodExecutor::new(pod_spec(vec![], vec![container("bad", "exit 7")]))
            .spawn(rx)
            .await
            .unwrap();
        let events = collect_events(&mut h).await;

        assert!(
            events.iter().any(|e| matches!(&e.kind,
            PodEventKind::ContainerFailed { name, exit_code } if name == "bad" && *exit_code == 7))
        );
    }

    #[tokio::test]
    async fn test_events_contain_phase_transitions() {
        let (_tx, rx) = shutdown();
        let mut h = PodExecutor::new(pod_spec(vec![], vec![container("main", "exit 0")]))
            .spawn(rx)
            .await
            .unwrap();
        let events = collect_events(&mut h).await;

        let transitions: Vec<_> = events
            .iter()
            .filter_map(|e| {
                if let PodEventKind::PhaseChanged { from, to } = &e.kind {
                    Some((*from, *to))
                } else {
                    None
                }
            })
            .collect();
        assert!(transitions.contains(&(PodPhase::Pending, PodPhase::Running)));
        assert!(transitions.contains(&(PodPhase::Running, PodPhase::Succeeded)));
    }

    #[tokio::test]
    async fn test_event_status_snapshot_is_current() {
        let (_tx, rx) = shutdown();
        let mut h = PodExecutor::new(pod_spec(vec![], vec![container("main", "exit 0")]))
            .spawn(rx)
            .await
            .unwrap();
        let events = collect_events(&mut h).await;

        let completed = events
            .iter()
            .find(|e| matches!(&e.kind, PodEventKind::Completed { .. }))
            .unwrap();
        assert_eq!(completed.status.phase, PodPhase::Succeeded);
    }

    #[tokio::test]
    async fn test_container_ready_event_no_readiness_probe() {
        let (_tx, rx) = shutdown();
        let mut h = PodExecutor::new(pod_spec(vec![], vec![container("main", "sleep 0.1")]))
            .spawn(rx)
            .await
            .unwrap();
        let events = collect_events(&mut h).await;

        assert!(events.iter().any(|e| matches!(&e.kind,
            PodEventKind::ContainerReady { name } if name == "main")));
    }

    // -- init containers -----------------------------------------------------

    #[tokio::test]
    async fn test_init_container_runs_before_main() {
        let dir = test_dir!();
        let marker = dir.join("init_done");
        let (_tx, rx) = shutdown();
        let mut h = PodExecutor::new(pod_spec(
            vec![container("init", &format!("touch {}", marker.display()))],
            vec![container("main", &format!("test -f {}", marker.display()))],
        ))
        .spawn(rx)
        .await
        .unwrap();
        assert_eq!(h.wait_for_completion().await.unwrap(), PodPhase::Succeeded);
    }

    #[tokio::test]
    async fn test_init_container_failure_prevents_main() {
        let (_tx, rx) = shutdown();
        let mut h = PodExecutor::new(pod_spec(
            vec![container("init", "exit 1")],
            vec![container("main", "exit 0")],
        ))
        .spawn(rx)
        .await
        .unwrap();
        let events = collect_events(&mut h).await;

        assert!(events.iter().any(|e| matches!(
            &e.kind,
            PodEventKind::Completed {
                phase: PodPhase::Failed
            }
        )));
        assert!(!events.iter().any(|e| matches!(&e.kind,
            PodEventKind::ContainerStarted { name, .. } if name == "main")));
    }

    fn pod_spec_restarting(
        init_containers: Vec<Container>,
        containers: Vec<Container>,
        restart_policy: RestartPolicy,
    ) -> Arc<PodSpec> {
        let mut pod = Arc::unwrap_or_clone(pod_spec(init_containers, containers));
        pod.spec.restart_policy = restart_policy;
        Arc::new(pod)
    }

    fn failing_until(attempts_file: &Path, failures: u32) -> String {
        format!(
            "n=$(cat {file} 2>/dev/null || echo 0); n=$((n + 1)); echo $n > {file}; \
             [ $n -gt {failures} ]",
            file = attempts_file.display(),
        )
    }

    fn back_offs(events: &[PodEvent]) -> usize {
        events
            .iter()
            .filter(|e| matches!(&e.kind, PodEventKind::ContainerBackOff { .. }))
            .count()
    }

    #[tokio::test]
    async fn test_init_container_restarts_in_place_until_it_succeeds() {
        let dir = test_dir!();
        let (_tx, rx) = shutdown();
        let mut h = PodExecutor::new(pod_spec_restarting(
            vec![container("init", &failing_until(&dir.join("attempts"), 2))],
            vec![container("main", "exit 0")],
            RestartPolicy::Always,
        ))
        .max_restarts(5)
        .restart_backoff_base(Duration::from_millis(10))
        .spawn(rx)
        .await
        .unwrap();
        let events = collect_events(&mut h).await;

        assert_eq!(back_offs(&events), 2);
        assert!(!events.iter().any(|e| matches!(
            &e.kind,
            PodEventKind::Completed {
                phase: PodPhase::Failed
            }
        )));
        assert!(events.iter().any(|e| matches!(&e.kind,
            PodEventKind::ContainerStarted { name, .. } if name == "main")));
        let st = h.state.lock().await;
        assert_eq!(
            st.lifecycle.init_container("init").unwrap().restart_count(),
            2
        );
    }

    #[tokio::test]
    async fn test_init_container_stops_restarting_after_the_maximum() {
        let (_tx, rx) = shutdown();
        let mut h = PodExecutor::new(pod_spec_restarting(
            vec![container("init", "exit 1")],
            vec![container("main", "exit 0")],
            RestartPolicy::Always,
        ))
        .max_restarts(2)
        .restart_backoff_base(Duration::from_millis(10))
        .spawn(rx)
        .await
        .unwrap();
        let events = collect_events(&mut h).await;

        assert_eq!(back_offs(&events), 2);
        assert!(events.iter().any(|e| matches!(
            &e.kind,
            PodEventKind::Completed {
                phase: PodPhase::Failed
            }
        )));
        let st = h.state.lock().await;
        assert_eq!(
            st.lifecycle.init_container("init").unwrap().restart_count(),
            2
        );
    }

    #[tokio::test]
    async fn test_init_container_of_a_job_pod_never_restarts() {
        for restart_policy in [RestartPolicy::Never, RestartPolicy::OnFailure] {
            let (_tx, rx) = shutdown();
            let mut h = PodExecutor::new(pod_spec_restarting(
                vec![container("init", "exit 1")],
                vec![container("main", "exit 0")],
                restart_policy,
            ))
            .max_restarts(5)
            .restart_backoff_base(Duration::from_millis(10))
            .spawn(rx)
            .await
            .unwrap();
            let events = collect_events(&mut h).await;

            assert_eq!(back_offs(&events), 0);
            assert!(events.iter().any(|e| matches!(
                &e.kind,
                PodEventKind::Completed {
                    phase: PodPhase::Failed
                }
            )));
        }
    }

    #[test]
    fn test_restart_delay_doubles_up_to_sixteen_times_the_base() {
        let delays: Vec<u128> = (0..6)
            .map(|restart_count| {
                restart_delay(Duration::from_millis(10), restart_count).as_millis()
            })
            .collect();
        assert_eq!(delays, vec![10, 20, 40, 80, 160, 160]);
    }

    #[tokio::test]
    async fn test_multiple_init_containers_sequential() {
        let dir = test_dir!();
        let f1 = dir.join("step1");
        let f2 = dir.join("step2");
        let (_tx, rx) = shutdown();
        let mut h = PodExecutor::new(pod_spec(
            vec![
                container("init1", &format!("touch {}", f1.display())),
                container(
                    "init2",
                    &format!("test -f {} && touch {}", f1.display(), f2.display()),
                ),
            ],
            vec![container("main", &format!("test -f {}", f2.display()))],
        ))
        .spawn(rx)
        .await
        .unwrap();
        assert_eq!(h.wait_for_completion().await.unwrap(), PodPhase::Succeeded);
    }

    // -- sidecars ------------------------------------------------------------

    #[tokio::test]
    async fn test_sidecar_runs_alongside_main() {
        let dir = test_dir!();
        let marker = dir.join("sidecar_alive");
        let (_tx, rx) = shutdown();
        let mut h = PodExecutor::new(pod_spec(
            vec![sidecar(
                "sidecar",
                &format!("touch {} && sleep 60", marker.display()),
            )],
            vec![container(
                "main",
                &format!("sleep 0.3 && test -f {}", marker.display()),
            )],
        ))
        .spawn(rx)
        .await
        .unwrap();
        assert_eq!(h.wait_for_completion().await.unwrap(), PodPhase::Succeeded);
    }

    #[tokio::test]
    async fn test_sidecar_terminated_after_main_exits() {
        let (_tx, rx) = shutdown();
        let mut h = PodExecutor::new(pod_spec(
            vec![sidecar("sidecar", "sleep 60")],
            vec![container("main", "exit 0")],
        ))
        .spawn(rx)
        .await
        .unwrap();
        let events = collect_events(&mut h).await;

        assert!(events.iter().any(|e| matches!(&e.kind,
            PodEventKind::ContainerFailed { name, .. } | PodEventKind::ContainerSucceeded { name }
            if name == "sidecar")));
    }

    // -- kill ----------------------------------------------------------------

    #[tokio::test]
    async fn test_kill_terminates_running_containers() {
        let (_tx, rx) = shutdown();
        let mut h = PodExecutor::new(pod_spec(vec![], vec![container("long", "sleep 60")]))
            .spawn(rx)
            .await
            .unwrap();

        // Wait for started
        loop {
            let ev = h.wait().await.unwrap();
            if matches!(&ev.kind, PodEventKind::ContainerStarted { .. }) {
                break;
            }
        }

        h.kill(Duration::from_secs(1));

        let result = tokio::time::timeout(Duration::from_secs(10), h.wait_for_completion()).await;
        assert!(result.is_ok(), "kill should cause completion");
        assert!(result.unwrap().unwrap().is_terminal());
    }

    #[tokio::test]
    async fn test_kill_concurrent_does_not_deadlock() {
        let (_tx, rx) = shutdown();
        let mut h = PodExecutor::new(pod_spec(
            vec![],
            vec![
                container("a", "sleep 60"),
                container("b", "sleep 60"),
                container("c", "sleep 60"),
            ],
        ))
        .spawn(rx)
        .await
        .unwrap();

        // Wait for all started
        let mut started = 0;
        while started < 3 {
            let ev = h.wait().await.unwrap();
            if matches!(&ev.kind, PodEventKind::ContainerStarted { .. }) {
                started += 1;
            }
        }

        h.kill(Duration::from_millis(500));

        let result = tokio::time::timeout(Duration::from_secs(10), h.wait_for_completion()).await;
        assert!(result.is_ok(), "kill should not deadlock");
    }

    // -- shutdown ------------------------------------------------------------

    #[tokio::test]
    async fn test_shutdown_kills_running_containers() {
        let (tx, rx) = shutdown();
        let mut h = PodExecutor::new(pod_spec(vec![], vec![container("long", "sleep 60")]))
            .spawn(rx)
            .await
            .unwrap();

        loop {
            let ev = h.wait().await.unwrap();
            if matches!(&ev.kind, PodEventKind::ContainerStarted { .. }) {
                break;
            }
        }

        tx.send(()).unwrap();

        let result = tokio::time::timeout(Duration::from_secs(10), h.wait_for_completion()).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap().unwrap(), PodPhase::Failed);
    }

    #[tokio::test]
    async fn test_shutdown_during_init() {
        let (tx, rx) = shutdown();
        let mut h = PodExecutor::new(pod_spec(
            vec![container("slow-init", "sleep 60")],
            vec![container("main", "exit 0")],
        ))
        .spawn(rx)
        .await
        .unwrap();

        tokio::time::sleep(Duration::from_millis(300)).await;
        tx.send(()).unwrap();

        let result = tokio::time::timeout(Duration::from_secs(10), h.wait_for_completion()).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap().unwrap(), PodPhase::Failed);
    }

    // -- status snapshots ----------------------------------------------------

    #[tokio::test]
    async fn test_status_updated_on_wait() {
        let (_tx, rx) = shutdown();
        let mut h = PodExecutor::new(pod_spec(vec![], vec![container("main", "exit 0")]))
            .spawn(rx)
            .await
            .unwrap();
        assert_eq!(h.status().phase, PodPhase::Pending);
        let _ = h.wait_for_completion().await;
        assert!(h.status().phase.is_terminal());
    }

    #[tokio::test]
    async fn test_live_status() {
        let (_tx, rx) = shutdown();
        let mut h = PodExecutor::new(pod_spec(vec![], vec![container("main", "sleep 0.5")]))
            .spawn(rx)
            .await
            .unwrap();

        loop {
            let ev = h.wait().await.unwrap();
            if matches!(
                &ev.kind,
                PodEventKind::PhaseChanged {
                    to: PodPhase::Running,
                    ..
                }
            ) {
                break;
            }
        }
        assert_eq!(h.live_status().await.phase, PodPhase::Running);
        let _ = h.wait_for_completion().await;
    }

    // -- container statuses --------------------------------------------------

    #[tokio::test]
    async fn test_container_statuses_in_final_status() {
        let (_tx, rx) = shutdown();
        let mut h = PodExecutor::new(pod_spec(
            vec![],
            vec![container("ok", "exit 0"), container("fail", "exit 3")],
        ))
        .spawn(rx)
        .await
        .unwrap();
        let _ = h.wait_for_completion().await;

        let ok = h
            .status()
            .container_statuses
            .iter()
            .find(|cs| cs.name == "ok")
            .unwrap();
        let fail = h
            .status()
            .container_statuses
            .iter()
            .find(|cs| cs.name == "fail")
            .unwrap();
        assert!(ok.is_terminated());
        assert_eq!(ok.terminated_exit_code(), Some(0));
        assert!(fail.is_terminated());
        assert_eq!(fail.terminated_exit_code(), Some(3));
    }

    #[tokio::test]
    async fn test_init_container_statuses_in_final_status() {
        let (_tx, rx) = shutdown();
        let mut h = PodExecutor::new(pod_spec(
            vec![container("init", "exit 0")],
            vec![container("main", "exit 0")],
        ))
        .spawn(rx)
        .await
        .unwrap();
        let _ = h.wait_for_completion().await;

        let init = h
            .status()
            .init_container_statuses
            .iter()
            .find(|cs| cs.name == "init")
            .unwrap();
        assert!(init.is_terminated());
        assert_eq!(init.terminated_exit_code(), Some(0));
    }

    // -- env vars ------------------------------------------------------------

    #[tokio::test]
    async fn test_pod_level_env_vars() {
        let dir = test_dir!();
        let out = dir.join("env.txt");
        let (_tx, rx) = shutdown();
        let mut h = PodExecutor::new(pod_spec(
            vec![],
            vec![container(
                "main",
                &format!("echo $MY_POD_VAR > {}", out.display()),
            )],
        ))
        .env("MY_POD_VAR", "pod_value")
        .spawn(rx)
        .await
        .unwrap();
        let _ = h.wait_for_completion().await;
        assert!(std::fs::read_to_string(&out).unwrap().contains("pod_value"));
    }

    #[tokio::test]
    async fn test_container_level_env_vars() {
        let dir = test_dir!();
        let out = dir.join("env.txt");
        let mut c = container("main", &format!("echo $CONTAINER_VAR > {}", out.display()));
        c.env.push(EnvVar {
            name: "CONTAINER_VAR".to_string(),
            value: Some("cv".to_string()),
            value_from: None,
        });
        let (_tx, rx) = shutdown();
        let mut h = PodExecutor::new(pod_spec(vec![], vec![c]))
            .spawn(rx)
            .await
            .unwrap();
        let _ = h.wait_for_completion().await;
        assert!(std::fs::read_to_string(&out).unwrap().contains("cv"));
    }

    // -- orphan sweep --------------------------------------------------------

    /// Poll until `count` processes carry a token under `prefix`.
    async fn wait_for_orphans(prefix: &str, count: usize) -> Vec<u32> {
        for _ in 0..100 {
            let pids = sweep::orphan_pids(prefix).await;
            if pids.len() >= count {
                return pids;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        sweep::orphan_pids(prefix).await
    }

    async fn wait_dead(pid: u32) -> bool {
        for _ in 0..100 {
            if !kcommon::signal::is_pid_alive(pid) {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        false
    }

    /// A pod spec with a chosen name and its own uid.
    fn named_pod_spec(name: &str, containers: Vec<Container>) -> Arc<PodSpec> {
        Arc::new(PodSpec {
            metadata: PodMetadata {
                name: name.to_string(),
                namespace: "default".to_string(),
                uid: crate::testutils::unique_name("uid"),
                ..Default::default()
            },
            spec: PodSpecBody {
                containers,
                ..Default::default()
            },
            ..Default::default()
        })
    }

    #[tokio::test]
    async fn test_kill_token_injected_into_container_env() {
        let dir = test_dir!();
        let out = dir.join("token.txt");
        let pod = pod_spec(
            vec![],
            vec![container(
                "main",
                &format!("echo ${} > {}", sweep::PR_KILL_TOKEN_VAR, out.display()),
            )],
        );
        let expected = sweep::container_token(
            &sweep::pod_prefix(&pod.metadata.name, &pod.metadata.uid),
            "main",
        );
        let (_tx, rx) = shutdown();
        let mut h = PodExecutor::new(pod).spawn(rx).await.unwrap();
        let _ = h.wait_for_completion().await;

        // A process can always read its own environment — the platform
        // restrictions only apply to reading someone else's.
        let written = std::fs::read_to_string(&out).unwrap();
        assert_eq!(written.trim(), expected);
    }

    #[tokio::test]
    async fn test_pod_spec_cannot_shadow_the_kill_token() {
        let dir = test_dir!();
        let out = dir.join("token.txt");
        let mut c = container(
            "main",
            &format!("echo ${} > {}", sweep::PR_KILL_TOKEN_VAR, out.display()),
        );
        c.env.push(EnvVar {
            name: sweep::PR_KILL_TOKEN_VAR.to_string(),
            value: Some("hijacked".to_string()),
            value_from: None,
        });
        let pod = pod_spec(vec![], vec![c]);
        let expected = sweep::container_token(
            &sweep::pod_prefix(&pod.metadata.name, &pod.metadata.uid),
            "main",
        );
        let (_tx, rx) = shutdown();
        let mut h = PodExecutor::new(pod).spawn(rx).await.unwrap();
        let _ = h.wait_for_completion().await;

        let written = std::fs::read_to_string(&out).unwrap();
        assert_eq!(
            written.trim(),
            expected,
            "a pod spec must not be able to shadow the token",
        );
    }

    /// The regression test for a container that daemonises a server the
    /// way `pg_ctl` daemonises the postmaster: fork, `setsid`, exec, and
    /// the result sits outside every process group pod-runner signals.
    /// Killing the pod must still reap it.
    // Serial with the sweep tests in `process::sweep`: a
    // runner-wide sweep there reaches this pod's escapee too.
    #[tokio::test]
    #[serial_test::serial]
    async fn test_pod_kill_reaps_process_that_escaped_container_group() {
        use crate::testutils::{HELPER_DAEMONIZE, HELPER_VAR, helper_command};
        use nix::unistd::{Pid as NixPid, getpgid};

        let mut c = Container {
            name: "main".to_string(),
            command: helper_command(),
            ..Default::default()
        };
        c.env.push(EnvVar {
            name: HELPER_VAR.to_string(),
            value: Some(HELPER_DAEMONIZE.to_string()),
            value_from: None,
        });
        let pod = named_pod_spec("escapee-pod", vec![c]);
        let prefix = sweep::pod_prefix(&pod.metadata.name, &pod.metadata.uid);

        let (_tx, rx) = shutdown();
        let mut h = PodExecutor::new(pod)
            .shutdown_grace_period(Duration::from_secs(2))
            .spawn(rx)
            .await
            .unwrap();

        // The container plus the process it daemonised, both carrying the
        // token the sweep matches on.
        let pids = wait_for_orphans(&prefix, 2).await;
        assert_eq!(pids.len(), 2, "expected container + escapee, got {pids:?}");

        // They are in different process groups, so no single group signal
        // can reach both: whichever of the two the group kill targets, the
        // other survives it.
        let pgids: Vec<i32> = pids
            .iter()
            .map(|pid| {
                getpgid(Some(NixPid::from_raw(*pid as i32)))
                    .expect("pgid")
                    .as_raw()
            })
            .collect();
        assert_ne!(
            pgids[0], pgids[1],
            "the daemonised process must have left the container's group",
        );

        h.kill(Duration::from_secs(1));
        let _ = h.wait_for_completion().await;

        for pid in pids {
            assert!(wait_dead(pid).await, "pid {pid} outlived its pod");
        }
    }

    /// The commonest shape of the bug: the container starts a server that
    /// daemonises itself and then exits on its own. The pod succeeds and
    /// no kill path runs at all, so only the sweep at the end of the
    /// orchestrator can reap what the container left behind.
    // Serial with the sweep tests in `process::sweep`: a
    // runner-wide sweep there reaches this pod's escapee too.
    #[tokio::test]
    #[serial_test::serial]
    async fn test_pod_completion_reaps_process_that_escaped_container_group() {
        use crate::testutils::{
            HELPER_DAEMONIZE_AND_EXIT, HELPER_PID_FILE_VAR, HELPER_VAR, helper_command,
        };

        let dir = test_dir!();
        let pid_file = dir.join("escapee.pid");
        let mut c = Container {
            name: "main".to_string(),
            command: helper_command(),
            ..Default::default()
        };
        c.env.push(EnvVar {
            name: HELPER_VAR.to_string(),
            value: Some(HELPER_DAEMONIZE_AND_EXIT.to_string()),
            value_from: None,
        });
        c.env.push(EnvVar {
            name: HELPER_PID_FILE_VAR.to_string(),
            value: Some(pid_file.display().to_string()),
            value_from: None,
        });

        let (_tx, rx) = shutdown();
        // The handle stays alive for the whole test: dropping it would
        // abort the orchestrator before its final sweep, which is exactly
        // what `PodHandle::wait_for_finish` protects against in the logic
        // layer.
        let mut h = PodExecutor::new(named_pod_spec("daemonizing-pod", vec![c]))
            .spawn(rx)
            .await
            .unwrap();
        assert_eq!(h.wait_for_completion().await.unwrap(), PodPhase::Succeeded);

        // Written by the container before it exited, so the process
        // existed and was sleeping — anything that ended it was a sweep.
        let escapee: u32 = std::fs::read_to_string(&pid_file)
            .expect("container should have recorded its daemon's pid")
            .trim()
            .parse()
            .expect("pid");
        assert!(
            wait_dead(escapee).await,
            "pid {escapee} outlived the pod that started it",
        );
    }

    // -- logging -------------------------------------------------------------

    #[tokio::test]
    async fn test_log_dir_captures_output() {
        let dir = test_dir!("logs");
        let (_tx, rx) = shutdown();
        let mut h = PodExecutor::new(pod_spec(vec![], vec![container("main", "echo hello_pod")]))
            .log_dir(&dir)
            .spawn(rx)
            .await
            .unwrap();
        let _ = h.wait_for_completion().await;
        tokio::time::sleep(Duration::from_millis(100)).await;

        // K8s-style naming: <log_dir>/<container_name>/<restart_count>.log
        let log = dir.join("main").join("0.log");
        assert!(log.exists(), "expected {}", log.display());
        assert!(std::fs::read_to_string(&log).unwrap().contains("hello_pod"));
    }

    #[tokio::test]
    async fn test_read_only_mount_warning_at_log_start() {
        use crate::spec::pod::VolumeMount;

        let dir = test_dir!("logs-ro-warning");
        let mut c = container("main", "echo container_output");
        c.volume_mounts = vec![
            VolumeMount {
                name: "ro-vol".to_string(),
                mount_path: "/mounts/input".to_string(),
                read_only: true,
                ..Default::default()
            },
            VolumeMount {
                name: "rw-vol".to_string(),
                mount_path: "/mounts/scratch".to_string(),
                read_only: false,
                ..Default::default()
            },
        ];
        let (_tx, rx) = shutdown();
        let mut h = PodExecutor::new(pod_spec(vec![], vec![c]))
            .log_dir(&dir)
            .spawn(rx)
            .await
            .unwrap();
        let _ = h.wait_for_completion().await;
        tokio::time::sleep(Duration::from_millis(100)).await;

        let log = std::fs::read_to_string(dir.join("main").join("0.log")).unwrap();
        let first_line = log.lines().next().unwrap();
        // The warning is the very first line, before any container output.
        assert!(
            first_line.contains(&read_only_mount_warning("ro-vol")),
            "first line should be the read-only warning: {first_line}"
        );
        assert!(
            first_line.contains(" stderr F "),
            "warning is a stderr line: {first_line}"
        );
        assert!(
            log.find(&read_only_mount_warning("ro-vol")).unwrap()
                < log.find("container_output").unwrap(),
            "warning must precede container output"
        );
        // No warning for the read-write mount.
        assert!(!log.contains("'rw-vol'"));
    }

    #[tokio::test]
    async fn test_read_only_mount_warning_written_for_each_restart_log() {
        use crate::spec::pod::VolumeMount;

        // Restarts re-create the output redirector via
        // `output_for(container, restart_count)`, so the warning must
        // appear at the top of every restart's log file (0.log, 1.log, …).
        // The orchestrator doesn't drive restarts yet (always
        // restart_count=0), so this exercises the seam restarts will use.
        let dir = test_dir!("logs-ro-warning-restart");
        let mut c = container("main", "true");
        c.volume_mounts = vec![VolumeMount {
            name: "ro-vol".to_string(),
            mount_path: "/mounts/input".to_string(),
            read_only: true,
            ..Default::default()
        }];

        let config = ContainerConfig {
            work_dir: None,
            pid_dir: None,
            log_config: LogConfig::ContainerLog {
                log_dir: dir.clone(),
                max_size_bytes: DEFAULT_LOG_MAX_SIZE_BYTES,
                max_files: DEFAULT_LOG_MAX_FILES,
            },
            propagate_env: false,
            extra_env: HashMap::new(),
            die_with_parent: true,
            max_restarts: 0,
            restart_backoff_base: Duration::from_secs(2),
            shutdown_grace_period: Duration::from_secs(1),
            kill_token_prefix: None,
        };

        for restart_count in [0u32, 1, 2] {
            // Driving the redirector with no process streams still opens
            // the log writer, which writes the header lines.
            let (_tx, rx) = shutdown();
            config
                .output_for(&c, restart_count)
                .start(None, None, rx)
                .await
                .unwrap();

            let log_path = dir.join("main").join(format!("{restart_count}.log"));
            let log = std::fs::read_to_string(&log_path)
                .unwrap_or_else(|e| panic!("missing {}: {e}", log_path.display()));
            let first_line = log.lines().next().unwrap();
            assert!(
                first_line.contains(&read_only_mount_warning("ro-vol")),
                "restart {restart_count}: first line should be the warning: {first_line}"
            );
        }
    }

    // -- log streaming ---------------------------------------------------------

    use crate::pod::container_log::ContainerLogOptions;

    /// Collect a log stream to a string until it closes (bounded by a timeout).
    async fn collect_stream(
        mut rx: mpsc::Receiver<Result<bytes::Bytes, io::Error>>,
        secs: u64,
    ) -> String {
        let mut out = String::new();
        let _ = tokio::time::timeout(Duration::from_secs(secs), async {
            while let Some(item) = rx.recv().await {
                match item {
                    Ok(b) => out.push_str(std::str::from_utf8(&b).unwrap()),
                    Err(_) => break,
                }
            }
        })
        .await;
        out
    }

    #[tokio::test]
    async fn test_log_stream_strips_cri_prefix() {
        let dir = test_dir!("logs-stream-strip");
        let (_tx, rx) = shutdown();
        let mut h = PodExecutor::new(pod_spec(
            vec![],
            vec![container("main", "echo first && echo second")],
        ))
        .log_dir(&dir)
        .spawn(rx)
        .await
        .unwrap();
        let log_handle = h.log_handle();
        let _ = h.wait_for_completion().await;
        tokio::time::sleep(Duration::from_millis(200)).await;

        let stream = log_handle
            .stream(None, ContainerLogOptions::default())
            .unwrap();
        let out = collect_stream(stream, 10).await;
        assert_eq!(out, "first\nsecond\n");

        // With timestamps, each line is "<RFC3339Nano ts> <message>".
        let stream = log_handle
            .stream(
                None,
                ContainerLogOptions {
                    timestamps: true,
                    ..Default::default()
                },
            )
            .unwrap();
        let out = collect_stream(stream, 10).await;
        let first = out.lines().next().unwrap();
        let (ts, msg) = first.split_once(' ').unwrap();
        assert!(
            ts.ends_with('Z') && ts.contains('T'),
            "not a timestamp: {ts}"
        );
        assert_eq!(msg, "first");
    }

    #[tokio::test]
    async fn test_log_stream_follow_survives_rotation() {
        let dir = test_dir!("logs-rotation-follow");
        // Each CRI line is ~50 bytes; rotate every ~200 bytes so the log
        // rotates several times while the stream is following.
        let c = container(
            "main",
            "i=0; while [ $i -lt 30 ]; do echo line-$i; i=$((i+1)); sleep 0.05; done; echo done-marker",
        );
        let (_tx, rx) = shutdown();
        let mut h = PodExecutor::new(pod_spec(vec![], vec![c]))
            .log_dir_rolling(&dir, 200, 5)
            .spawn(rx)
            .await
            .unwrap();

        let log_handle = h.log_handle();
        let stream = log_handle
            .stream(
                None,
                ContainerLogOptions {
                    follow: true,
                    ..Default::default()
                },
            )
            .unwrap();
        let collector = tokio::spawn(collect_stream(stream, 20));

        let _ = h.wait_for_completion().await;
        let out = collector.await.unwrap();

        // Rotation actually happened…
        assert!(
            dir.join("main").join("0.log.1").exists(),
            "expected rotation to have occurred"
        );
        // …and the follow stream kept delivering lines written after it,
        // all the way to the final marker in the freshest file.
        assert!(
            out.contains("done-marker"),
            "follow stream lost data after rotation; got: {out}"
        );
    }

    // -- drop ----------------------------------------------------------------

    #[tokio::test]
    async fn test_drop_aborts_orchestrator() {
        let (_tx, rx) = shutdown();
        let h = PodExecutor::new(pod_spec(vec![], vec![container("long", "sleep 60")]))
            .spawn(rx)
            .await
            .unwrap();
        drop(h);
        tokio::time::sleep(Duration::from_millis(200)).await;
        // No panic or hang = success
    }

    // -- concurrent monitoring -----------------------------------------------

    #[tokio::test]
    async fn test_fast_container_exit_detected_before_slow() {
        let (_tx, rx) = shutdown();
        let mut h = PodExecutor::new(pod_spec(
            vec![],
            vec![
                container("slow", "sleep 2 && exit 0"),
                container("fast", "exit 0"),
            ],
        ))
        .spawn(rx)
        .await
        .unwrap();
        let events = collect_events(&mut h).await;

        let succeeded: Vec<String> = events
            .iter()
            .filter_map(|e| {
                if let PodEventKind::ContainerSucceeded { name } = &e.kind {
                    Some(name.clone())
                } else {
                    None
                }
            })
            .collect();
        assert_eq!(succeeded.len(), 2);
        assert_eq!(
            succeeded[0], "fast",
            "fast should be reported first: {:?}",
            succeeded
        );
    }

    // ========================================================================
    // Container crash scenarios
    // ========================================================================

    #[tokio::test]
    async fn test_container_killed_by_signal() {
        // Container kills itself with SIGKILL — no exit code
        let (_tx, rx) = shutdown();
        let mut h = PodExecutor::new(pod_spec(vec![], vec![container("crash", "kill -KILL $$")]))
            .spawn(rx)
            .await
            .unwrap();
        let events = collect_events(&mut h).await;

        assert!(events.iter().any(|e| matches!(&e.kind,
            PodEventKind::ContainerFailed { name, exit_code } if name == "crash" && *exit_code == -1)));
        assert!(events.iter().any(|e| matches!(
            &e.kind,
            PodEventKind::Completed {
                phase: PodPhase::Failed
            }
        )));
    }

    #[tokio::test]
    async fn test_container_segfault() {
        let (_tx, rx) = shutdown();
        let mut h = PodExecutor::new(pod_spec(vec![], vec![container("segv", "kill -SEGV $$")]))
            .spawn(rx)
            .await
            .unwrap();

        let phase = h.wait_for_completion().await.unwrap();
        assert_eq!(phase, PodPhase::Failed);
    }

    #[tokio::test]
    async fn test_multiple_containers_some_crash() {
        let (_tx, rx) = shutdown();
        let mut h = PodExecutor::new(pod_spec(
            vec![],
            vec![
                container("ok", "exit 0"),
                container("crash", "kill -KILL $$"),
                container("also-ok", "exit 0"),
            ],
        ))
        .spawn(rx)
        .await
        .unwrap();

        let phase = h.wait_for_completion().await.unwrap();
        assert_eq!(phase, PodPhase::Failed, "any crash should fail the pod");

        let status = h.status();
        let ok = status
            .container_statuses
            .iter()
            .find(|cs| cs.name == "ok")
            .unwrap();
        let crash = status
            .container_statuses
            .iter()
            .find(|cs| cs.name == "crash")
            .unwrap();
        assert!(ok.is_terminated());
        assert!(crash.is_terminated());
        assert_eq!(crash.terminated_exit_code(), Some(-1));
    }

    // ========================================================================
    // Run-away process that ignores SIGTERM
    // ========================================================================

    #[tokio::test]
    async fn test_kill_container_that_ignores_sigterm() {
        let (_tx, rx) = shutdown();
        let mut h = PodExecutor::new(pod_spec(
            vec![],
            vec![container("stubborn", "trap '' TERM; sleep 60")],
        ))
        .spawn(rx)
        .await
        .unwrap();

        // Wait for started
        loop {
            let ev = h.wait().await.unwrap();
            if matches!(&ev.kind, PodEventKind::ContainerStarted { .. }) {
                break;
            }
        }

        // Kill with short grace — should escalate to SIGKILL via process executor
        h.kill(Duration::from_millis(500));

        let result = tokio::time::timeout(Duration::from_secs(10), h.wait_for_completion()).await;
        assert!(result.is_ok(), "should eventually complete after SIGKILL");
    }

    // ========================================================================
    // terminationGracePeriodSeconds wired to the kill path
    // ========================================================================

    // A container that ignores SIGTERM and only dies on SIGKILL: the shell
    // traps TERM, and the re-looping `sleep` means the single group-SIGTERM
    // (which kills the current child sleep) doesn't end the process — only
    // the SIGKILL fired after the grace deadline does. So the time from the
    // kill decision to container exit tracks the grace the executor used.
    const SIGTERM_PROOF_CMD: &str = "trap '' TERM; while true; do sleep 0.2; done";

    #[tokio::test]
    async fn test_pod_grace_honored_on_probe_kill() {
        // A liveness probe fails immediately, so the executor kills the
        // container (a kill path that *waits* for graceful termination,
        // unlike runner shutdown). The container ignores SIGTERM, so the
        // pod only goes terminal after the SIGKILL fired at the grace
        // deadline. The pod asks for a 1s grace while the runner default is
        // 30s — completion must land on the pod's 1s budget.
        let mut c = container_with_liveness_exec("main", SIGTERM_PROOF_CMD, "exit 1");
        // Fail fast: one failed check ~1s in triggers the kill.
        c.liveness_probe.as_mut().unwrap().failure_threshold = 1;

        let (_tx, rx) = shutdown();
        let mut h = PodExecutor::new(pod_spec_with_grace(vec![c], Some(1)))
            .shutdown_grace_period(Duration::from_secs(30))
            .spawn(rx)
            .await
            .unwrap();

        let t = Instant::now();
        let phase = tokio::time::timeout(Duration::from_secs(15), h.wait_for_completion())
            .await
            .expect("probe-kill should terminate on the pod's 1s grace, not the 30s default")
            .unwrap();

        assert_eq!(phase, PodPhase::Failed);
        assert!(
            t.elapsed() < Duration::from_secs(12),
            "expected SIGKILL on the pod's 1s grace; took {:?}",
            t.elapsed()
        );
    }

    // ========================================================================
    // Kill during init phase
    // ========================================================================

    #[tokio::test]
    async fn test_kill_during_init() {
        let (_tx, rx) = shutdown();
        let mut h = PodExecutor::new(pod_spec(
            vec![container("slow-init", "sleep 60")],
            vec![container("main", "exit 0")],
        ))
        .spawn(rx)
        .await
        .unwrap();

        // Wait for init to start
        loop {
            let ev = h.wait().await.unwrap();
            if matches!(&ev.kind, PodEventKind::ContainerStarted { name, .. } if name == "slow-init")
            {
                break;
            }
        }

        h.kill(Duration::from_secs(1));

        let result = tokio::time::timeout(Duration::from_secs(10), h.wait_for_completion()).await;
        assert!(result.is_ok(), "kill during init should complete");
        assert_eq!(result.unwrap().unwrap(), PodPhase::Failed);
    }

    // ========================================================================
    // Init container crash
    // ========================================================================

    #[tokio::test]
    async fn test_init_container_crash() {
        let (_tx, rx) = shutdown();
        let mut h = PodExecutor::new(pod_spec(
            vec![container("init-crash", "kill -KILL $$")],
            vec![container("main", "exit 0")],
        ))
        .spawn(rx)
        .await
        .unwrap();
        let events = collect_events(&mut h).await;

        assert!(events.iter().any(|e| matches!(
            &e.kind,
            PodEventKind::Completed {
                phase: PodPhase::Failed
            }
        )));
        // Main should never start
        assert!(!events.iter().any(|e| matches!(&e.kind,
            PodEventKind::ContainerStarted { name, .. } if name == "main")));
    }

    // ========================================================================
    // Sidecar refuses to die
    // ========================================================================

    #[tokio::test]
    async fn test_sidecar_ignores_sigterm_force_killed() {
        let (_tx, rx) = shutdown();
        let mut h = PodExecutor::new(pod_spec(
            vec![sidecar("stubborn-sidecar", "trap '' TERM; sleep 60")],
            vec![container("main", "sleep 0.3 && exit 0")],
        ))
        .spawn(rx)
        .await
        .unwrap();

        // Should complete — sidecar gets SIGTERM then eventually force-killed
        let result = tokio::time::timeout(Duration::from_secs(30), h.wait_for_completion()).await;

        assert!(
            result.is_ok(),
            "pod should complete even with stubborn sidecar"
        );
        // Main succeeded, so pod should succeed (sidecar termination is cleanup)
        assert_eq!(result.unwrap().unwrap(), PodPhase::Succeeded);
    }

    // ========================================================================
    // Sidecar-only pod (no main containers)
    // ========================================================================

    #[tokio::test]
    async fn test_sidecar_only_no_main_containers() {
        let (_tx, rx) = shutdown();
        let mut h = PodExecutor::new(pod_spec(
            vec![sidecar("sidecar", "sleep 60")],
            vec![], // no main containers
        ))
        .spawn(rx)
        .await
        .unwrap();

        // With no main containers, pod should succeed immediately
        // and terminate the sidecar
        let result = tokio::time::timeout(Duration::from_secs(20), h.wait_for_completion()).await;

        assert!(result.is_ok(), "sidecar-only pod should complete");
        assert_eq!(result.unwrap().unwrap(), PodPhase::Succeeded);
    }

    // ========================================================================
    // Spawn failure mid-phase
    // ========================================================================

    #[tokio::test]
    async fn test_main_container_spawn_failure() {
        // First container is valid, second has a bad command
        let bad = Container {
            name: "bad".to_string(),
            command: vec!["/nonexistent/binary".to_string()],
            ..Default::default()
        };
        let (_tx, rx) = shutdown();
        let mut h = PodExecutor::new(pod_spec(vec![], vec![container("ok", "sleep 60"), bad]))
            .spawn(rx)
            .await
            .unwrap();

        let phase = h.wait_for_completion().await.unwrap();
        assert_eq!(phase, PodPhase::Failed, "spawn failure should fail the pod");
    }

    #[tokio::test]
    async fn test_init_container_spawn_failure() {
        let bad = Container {
            name: "bad-init".to_string(),
            command: vec!["/nonexistent/binary".to_string()],
            ..Default::default()
        };
        let (_tx, rx) = shutdown();
        let mut h = PodExecutor::new(pod_spec(vec![bad], vec![container("main", "exit 0")]))
            .spawn(rx)
            .await
            .unwrap();

        let phase = h.wait_for_completion().await.unwrap();
        assert_eq!(phase, PodPhase::Failed);
    }

    // ========================================================================
    // Probes
    // ========================================================================

    use crate::spec::pod::{ExecAction, HttpGetAction, Probe, TcpSocketAction};

    /// Create a container with an exec readiness probe.
    fn container_with_readiness_exec(name: &str, cmd: &str, probe_cmd: &str) -> Container {
        Container {
            readiness_probe: Some(Probe {
                exec_action: Some(ExecAction {
                    command: vec!["sh".to_string(), "-c".to_string(), probe_cmd.to_string()],
                }),
                initial_delay_seconds: 0,
                period_seconds: 1,
                timeout_seconds: 2,
                success_threshold: 1,
                failure_threshold: 3,
                ..Default::default()
            }),
            ..container(name, cmd)
        }
    }

    /// Create a container with an exec liveness probe.
    fn container_with_liveness_exec(name: &str, cmd: &str, probe_cmd: &str) -> Container {
        Container {
            liveness_probe: Some(Probe {
                exec_action: Some(ExecAction {
                    command: vec!["sh".to_string(), "-c".to_string(), probe_cmd.to_string()],
                }),
                initial_delay_seconds: 0,
                period_seconds: 1,
                timeout_seconds: 2,
                success_threshold: 1,
                failure_threshold: 2,
                ..Default::default()
            }),
            ..container(name, cmd)
        }
    }

    /// Create a container with an exec startup probe.
    fn container_with_startup_exec(name: &str, cmd: &str, probe_cmd: &str) -> Container {
        Container {
            startup_probe: Some(Probe {
                exec_action: Some(ExecAction {
                    command: vec!["sh".to_string(), "-c".to_string(), probe_cmd.to_string()],
                }),
                initial_delay_seconds: 0,
                period_seconds: 1,
                timeout_seconds: 2,
                success_threshold: 1,
                failure_threshold: 3,
                ..Default::default()
            }),
            ..container(name, cmd)
        }
    }

    #[tokio::test]
    async fn test_readiness_probe_exec_pass() {
        // Container runs, readiness probe succeeds → ContainerReady event
        let dir = test_dir!();
        let marker = dir.join("ready");
        let cmd = format!("touch {} && sleep 5", marker.display());
        let probe_cmd = format!("test -f {}", marker.display());

        let c = container_with_readiness_exec("app", &cmd, &probe_cmd);
        let (_tx, rx) = shutdown();
        let mut h = PodExecutor::new(pod_spec(vec![], vec![c]))
            .spawn(rx)
            .await
            .unwrap();

        // Wait for ContainerReady or Completed
        let mut got_ready = false;
        let result = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let ev = h.wait().await.unwrap();
                if matches!(&ev.kind, PodEventKind::ContainerReady { name } if name == "app") {
                    got_ready = true;
                }
                if matches!(&ev.kind, PodEventKind::Completed { .. }) {
                    break;
                }
            }
        })
        .await;

        assert!(result.is_ok(), "should complete within timeout");
        assert!(got_ready, "should have received ContainerReady event");
    }

    #[tokio::test]
    async fn test_readiness_probe_exec_fail_then_pass() {
        // Readiness probe fails initially (marker doesn't exist),
        // then container creates it and probe passes.
        let dir = test_dir!();
        let marker = dir.join("ready_later");
        let cmd = format!("sleep 2 && touch {} && sleep 5", marker.display());
        let probe_cmd = format!("test -f {}", marker.display());

        let c = container_with_readiness_exec("app", &cmd, &probe_cmd);
        let (_tx, rx) = shutdown();
        let mut h = PodExecutor::new(pod_spec(vec![], vec![c]))
            .spawn(rx)
            .await
            .unwrap();

        let mut probe_results = Vec::new();
        let mut got_ready = false;
        let result = tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                let ev = h.wait().await.unwrap();
                if let PodEventKind::ProbeResult { success, .. } = &ev.kind {
                    probe_results.push(*success);
                }
                if matches!(&ev.kind, PodEventKind::ContainerReady { .. }) {
                    got_ready = true;
                }
                if matches!(&ev.kind, PodEventKind::Completed { .. }) {
                    break;
                }
            }
        })
        .await;

        assert!(result.is_ok(), "should complete within timeout");
        // Should have some failures then a success
        assert!(
            probe_results.contains(&false),
            "should have probe failures: {:?}",
            probe_results
        );
        assert!(
            probe_results.contains(&true),
            "should have probe success: {:?}",
            probe_results
        );
        assert!(got_ready, "should eventually become ready");
    }

    #[tokio::test]
    async fn test_liveness_probe_failure_kills_container() {
        // Liveness probe always fails → container killed after threshold
        let c = container_with_liveness_exec("app", "sleep 60", "exit 1");
        let (_tx, rx) = shutdown();
        let mut h = PodExecutor::new(pod_spec(vec![], vec![c]))
            .spawn(rx)
            .await
            .unwrap();

        let result = tokio::time::timeout(Duration::from_secs(15), h.wait_for_completion()).await;
        assert!(
            result.is_ok(),
            "liveness failure should kill container and complete pod"
        );
        assert_eq!(result.unwrap().unwrap(), PodPhase::Failed);
    }

    #[tokio::test]
    async fn test_startup_probe_gates_other_probes() {
        // Container has startup probe (passes immediately) and readiness probe.
        // Readiness probe should only run after startup passes.
        let dir = test_dir!();
        let marker = dir.join("started");
        let cmd = format!("touch {} && sleep 5", marker.display());

        let mut c = container("app", &cmd);
        c.startup_probe = Some(Probe {
            exec_action: Some(ExecAction {
                command: vec![
                    "sh".to_string(),
                    "-c".to_string(),
                    format!("test -f {}", marker.display()),
                ],
            }),
            initial_delay_seconds: 0,
            period_seconds: 1,
            timeout_seconds: 2,
            success_threshold: 1,
            failure_threshold: 10,
            ..Default::default()
        });
        c.readiness_probe = Some(Probe {
            exec_action: Some(ExecAction {
                command: vec!["sh".to_string(), "-c".to_string(), "exit 0".to_string()],
            }),
            initial_delay_seconds: 0,
            period_seconds: 1,
            timeout_seconds: 2,
            success_threshold: 1,
            failure_threshold: 3,
            ..Default::default()
        });

        let (_tx, rx) = shutdown();
        let mut h = PodExecutor::new(pod_spec(vec![], vec![c]))
            .spawn(rx)
            .await
            .unwrap();

        let mut probe_events = Vec::new();
        let mut got_ready = false;
        let result = tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                let ev = h.wait().await.unwrap();
                if let PodEventKind::ProbeResult {
                    probe_type,
                    success,
                    ..
                } = &ev.kind
                {
                    probe_events.push((probe_type.clone(), *success));
                }
                if matches!(&ev.kind, PodEventKind::ContainerReady { .. }) {
                    got_ready = true;
                }
                if matches!(&ev.kind, PodEventKind::Completed { .. }) {
                    break;
                }
            }
        })
        .await;

        assert!(result.is_ok(), "should complete within timeout");
        // Startup probe should have run first
        assert!(!probe_events.is_empty(), "should have probe events");
        assert_eq!(
            probe_events[0].0, "startup",
            "first probe should be startup: {:?}",
            probe_events
        );
        assert!(got_ready, "readiness probe should eventually pass");
    }

    #[tokio::test]
    async fn test_startup_probe_failure_kills_container() {
        // Startup probe always fails → container killed after threshold
        let c = container_with_startup_exec("app", "sleep 60", "exit 1");
        let (_tx, rx) = shutdown();
        let mut h = PodExecutor::new(pod_spec(vec![], vec![c]))
            .spawn(rx)
            .await
            .unwrap();

        let result = tokio::time::timeout(Duration::from_secs(15), h.wait_for_completion()).await;
        assert!(result.is_ok(), "startup failure should kill container");
        assert_eq!(result.unwrap().unwrap(), PodPhase::Failed);
    }

    #[tokio::test]
    async fn test_tcp_probe_on_listening_port() {
        // Start a container that listens on a port, TCP probe connects
        let c = Container {
            readiness_probe: Some(Probe {
                tcp_socket: Some(TcpSocketAction {
                    port: Some(18765),
                    host: "127.0.0.1".to_string(),
                }),
                initial_delay_seconds: 0,
                period_seconds: 1,
                timeout_seconds: 2,
                success_threshold: 1,
                failure_threshold: 10,
                ..Default::default()
            }),
            // nc -l listens on the port
            ..container("app", "nc -l 18765 & sleep 5")
        };
        let (_tx, rx) = shutdown();
        let mut h = PodExecutor::new(pod_spec(vec![], vec![c]))
            .spawn(rx)
            .await
            .unwrap();

        let mut got_ready = false;
        let result = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let ev = h.wait().await.unwrap();
                if matches!(&ev.kind, PodEventKind::ContainerReady { .. }) {
                    got_ready = true;
                }
                if matches!(&ev.kind, PodEventKind::Completed { .. }) {
                    break;
                }
            }
        })
        .await;

        assert!(result.is_ok(), "should complete within timeout");
        assert!(got_ready, "TCP probe should detect listening port");
    }

    #[tokio::test]
    async fn test_probe_logs_to_file() {
        let dir = test_dir!("logs");
        let c = container_with_readiness_exec("app", "sleep 3", "echo probe_output && exit 0");

        let (_tx, rx) = shutdown();
        let mut h = PodExecutor::new(pod_spec(vec![], vec![c]))
            .log_dir(&dir)
            .spawn(rx)
            .await
            .unwrap();

        // Wait for at least one probe result
        let result = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let ev = h.wait().await.unwrap();
                if matches!(&ev.kind, PodEventKind::ProbeResult { .. }) {
                    break;
                }
                if matches!(&ev.kind, PodEventKind::Completed { .. }) {
                    break;
                }
            }
        })
        .await;
        assert!(result.is_ok());

        // Give pump a moment to flush
        tokio::time::sleep(Duration::from_millis(200)).await;

        let probe_log = dir.join("app").join("probe-readiness.log");
        assert!(
            probe_log.exists(),
            "probe log should exist at {}",
            probe_log.display()
        );
        let content = std::fs::read_to_string(&probe_log).unwrap();
        assert!(
            content.contains("probe_output"),
            "probe log should contain output: {}",
            content
        );

        h.kill(Duration::from_secs(1));
        let _ = h.wait_for_completion().await;
    }

    #[tokio::test]
    async fn test_no_probes_no_events() {
        // Container without probes should not emit ProbeResult events
        let (_tx, rx) = shutdown();
        let mut h = PodExecutor::new(pod_spec(vec![], vec![container("main", "sleep 0.5")]))
            .spawn(rx)
            .await
            .unwrap();
        let events = collect_events(&mut h).await;

        let probe_events: Vec<_> = events
            .iter()
            .filter(|e| matches!(&e.kind, PodEventKind::ProbeResult { .. }))
            .collect();
        assert!(
            probe_events.is_empty(),
            "no probes defined, should have no probe events"
        );
    }

    #[tokio::test]
    async fn test_tcp_probe_non_listening_port() {
        // TCP probe to port nobody listens on → readiness never passes,
        // but container runs and exits normally
        let c = Container {
            readiness_probe: Some(Probe {
                tcp_socket: Some(TcpSocketAction {
                    port: Some(19999),
                    host: "127.0.0.1".to_string(),
                }),
                initial_delay_seconds: 0,
                period_seconds: 1,
                timeout_seconds: 1,
                success_threshold: 1,
                failure_threshold: 100, // high so it doesn't kill
                ..Default::default()
            }),
            ..container("app", "sleep 2")
        };
        let (_tx, rx) = shutdown();
        let mut h = PodExecutor::new(pod_spec(vec![], vec![c]))
            .spawn(rx)
            .await
            .unwrap();
        let events = collect_events(&mut h).await;

        // Should have probe failures but no ContainerReady
        let failures: Vec<_> = events
            .iter()
            .filter(|e| {
                matches!(&e.kind,
            PodEventKind::ProbeResult { success, .. } if !success)
            })
            .collect();
        assert!(
            !failures.is_empty(),
            "should have probe failures for non-listening port"
        );
        let ready = events
            .iter()
            .any(|e| matches!(&e.kind, PodEventKind::ContainerReady { .. }));
        assert!(!ready, "should not become ready on non-listening port");
    }

    #[tokio::test]
    async fn test_exec_probe_nonzero_exit_is_failure() {
        // Exec probe returns exit code 2 → probe failure
        let c = container_with_readiness_exec("app", "sleep 3", "exit 2");
        let (_tx, rx) = shutdown();
        let mut h = PodExecutor::new(pod_spec(vec![], vec![c]))
            .spawn(rx)
            .await
            .unwrap();

        let mut had_failure = false;
        let result = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let ev = h.wait().await.unwrap();
                if matches!(&ev.kind, PodEventKind::ProbeResult { success, .. } if !success) {
                    had_failure = true;
                }
                if matches!(&ev.kind, PodEventKind::Completed { .. }) {
                    break;
                }
            }
        })
        .await;
        assert!(result.is_ok());
        assert!(had_failure, "exit code 2 should be a probe failure");
    }

    #[tokio::test]
    async fn test_liveness_failure_actually_terminates_process() {
        // Verify the container process is actually dead after liveness kills it
        let c = container_with_liveness_exec("app", "sleep 60", "exit 1");
        let (_tx, rx) = shutdown();
        let mut h = PodExecutor::new(pod_spec(vec![], vec![c]))
            .spawn(rx)
            .await
            .unwrap();

        let events = collect_events(&mut h).await;

        // Container must have a ContainerFailed event (killed by liveness)
        let failed = events.iter().any(|e| {
            matches!(&e.kind,
            PodEventKind::ContainerFailed { name, .. } if name == "app")
        });
        assert!(failed, "container should be killed and reported as failed");
        assert_eq!(
            events
                .iter()
                .find_map(|e| if let PodEventKind::Completed { phase } = &e.kind {
                    Some(*phase)
                } else {
                    None
                }),
            Some(PodPhase::Failed),
        );
    }

    #[tokio::test]
    async fn test_probe_initial_delay() {
        // Readiness probe with initial_delay=2s. Container runs for 1s and exits.
        // Probe should NOT run (initial delay not elapsed).
        let c = Container {
            readiness_probe: Some(Probe {
                exec_action: Some(ExecAction {
                    command: vec!["sh".to_string(), "-c".to_string(), "exit 0".to_string()],
                }),
                initial_delay_seconds: 5, // 5 second delay
                period_seconds: 1,
                timeout_seconds: 1,
                success_threshold: 1,
                failure_threshold: 3,
                ..Default::default()
            }),
            ..container("app", "sleep 1")
        };
        let (_tx, rx) = shutdown();
        let mut h = PodExecutor::new(pod_spec(vec![], vec![c]))
            .spawn(rx)
            .await
            .unwrap();
        let events = collect_events(&mut h).await;

        // No probe should have run (initial delay > container lifetime)
        let probe_events: Vec<_> = events
            .iter()
            .filter(|e| matches!(&e.kind, PodEventKind::ProbeResult { .. }))
            .collect();
        assert!(
            probe_events.is_empty(),
            "probe should not run before initial_delay expires: {:?}",
            probe_events
        );
        // Container should not be marked ready (no probe ran)
        let ready = events
            .iter()
            .any(|e| matches!(&e.kind, PodEventKind::ContainerReady { .. }));
        assert!(!ready, "should not be ready — probe never ran");
    }

    #[tokio::test]
    async fn test_multiple_probes_on_same_container() {
        // Container with both startup and readiness probes
        let dir = test_dir!();
        let marker = dir.join("alive");
        let cmd = format!("touch {} && sleep 10", marker.display());

        let mut c = container("app", &cmd);
        c.startup_probe = Some(Probe {
            exec_action: Some(ExecAction {
                command: vec![
                    "sh".to_string(),
                    "-c".to_string(),
                    format!("test -f {}", marker.display()),
                ],
            }),
            period_seconds: 1,
            success_threshold: 1,
            failure_threshold: 5,
            ..Default::default()
        });
        c.readiness_probe = Some(Probe {
            exec_action: Some(ExecAction {
                command: vec!["sh".to_string(), "-c".to_string(), "exit 0".to_string()],
            }),
            period_seconds: 1,
            success_threshold: 1,
            failure_threshold: 3,
            ..Default::default()
        });
        c.liveness_probe = Some(Probe {
            exec_action: Some(ExecAction {
                command: vec!["sh".to_string(), "-c".to_string(), "exit 0".to_string()],
            }),
            period_seconds: 1,
            success_threshold: 1,
            failure_threshold: 3,
            ..Default::default()
        });

        let (_tx, rx) = shutdown();
        let mut h = PodExecutor::new(pod_spec(vec![], vec![c]))
            .spawn(rx)
            .await
            .unwrap();

        let mut probe_types: Vec<String> = Vec::new();
        let mut got_ready = false;
        let result = tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                let ev = h.wait().await.unwrap();
                if let PodEventKind::ProbeResult { probe_type, .. } = &ev.kind
                    && !probe_types.contains(probe_type)
                {
                    probe_types.push(probe_type.clone());
                }
                if matches!(&ev.kind, PodEventKind::ContainerReady { .. }) {
                    got_ready = true;
                    break; // we've seen enough
                }
                if matches!(&ev.kind, PodEventKind::Completed { .. }) {
                    break;
                }
            }
        })
        .await;

        assert!(result.is_ok(), "should complete within timeout");
        assert!(
            probe_types.contains(&"startup".to_string()),
            "startup probe should run: {:?}",
            probe_types
        );
        assert!(
            probe_types.contains(&"readiness".to_string()),
            "readiness probe should run after startup passes: {:?}",
            probe_types
        );
        assert!(got_ready, "readiness probe should eventually pass");

        h.kill(Duration::from_secs(1));
        let _ = h.wait_for_completion().await;
    }

    #[tokio::test]
    async fn test_probe_stops_when_container_exits() {
        // Container exits quickly, probe should stop (not hang)
        let c = container_with_readiness_exec("app", "exit 0", "exit 0");
        let (_tx, rx) = shutdown();
        let mut h = PodExecutor::new(pod_spec(vec![], vec![c]))
            .spawn(rx)
            .await
            .unwrap();

        // Should complete quickly — probe task stops when container exits
        let result = tokio::time::timeout(Duration::from_secs(5), h.wait_for_completion()).await;
        assert!(
            result.is_ok(),
            "pod should complete — probe should stop when container exits"
        );
    }

    #[tokio::test]
    async fn test_http_probe_with_server() {
        // Find a free port to avoid conflicts with other tests or services
        let port = {
            let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            l.local_addr().unwrap().port()
        };

        let c = Container {
            readiness_probe: Some(Probe {
                http_get: Some(HttpGetAction {
                    path: "/".to_string(),
                    port: Some(port),
                    host: "127.0.0.1".to_string(),
                }),
                initial_delay_seconds: 2, // give server time to start
                period_seconds: 1,
                timeout_seconds: 2,
                success_threshold: 1,
                failure_threshold: 15,
                ..Default::default()
            }),
            ..container(
                "app",
                &format!(
                    "python3 -m http.server {} 2>/dev/null || python -m SimpleHTTPServer {} 2>/dev/null || sleep 10",
                    port, port
                ),
            )
        };
        let (_tx, rx) = shutdown();
        let mut h = PodExecutor::new(pod_spec(vec![], vec![c]))
            .spawn(rx)
            .await
            .unwrap();

        let mut got_ready = false;
        let result = tokio::time::timeout(Duration::from_secs(25), async {
            loop {
                let ev = h.wait().await.unwrap();
                if matches!(&ev.kind, PodEventKind::ContainerReady { .. }) {
                    got_ready = true;
                    break;
                }
                if matches!(&ev.kind, PodEventKind::Completed { .. }) {
                    break;
                }
            }
        })
        .await;

        assert!(result.is_ok(), "should complete within timeout");
        assert!(got_ready, "HTTP probe should detect 200 response");

        h.kill(Duration::from_secs(1));
        let _ = h.wait_for_completion().await;
    }

    // -- probe signal propagation -------------------------------------------

    #[tokio::test]
    async fn test_probe_interrupted_by_shutdown() {
        // Long-running exec probe should be killed when shutdown fires
        let c = container_with_readiness_exec("app", "sleep 60", "sleep 60");
        let (tx, rx) = shutdown();
        let mut h = PodExecutor::new(pod_spec(vec![], vec![c]))
            .spawn(rx)
            .await
            .unwrap();

        // Wait for container to start
        loop {
            let ev = h.wait().await.unwrap();
            if matches!(&ev.kind, PodEventKind::ContainerStarted { .. }) {
                break;
            }
        }

        // Let probe start executing
        tokio::time::sleep(Duration::from_millis(800)).await;

        // Send shutdown — probe should be interrupted, container killed
        tx.send(()).unwrap();

        let result = tokio::time::timeout(Duration::from_secs(10), h.wait_for_completion()).await;
        assert!(
            result.is_ok(),
            "shutdown should interrupt probe and complete pod"
        );
        assert_eq!(result.unwrap().unwrap(), PodPhase::Failed);
    }

    #[tokio::test]
    async fn test_probe_interrupted_by_container_exit() {
        // Container exits while probe is running — probe task should stop
        // Use a readiness probe that sleeps longer than the container
        let c = container_with_readiness_exec("app", "sleep 0.5", "sleep 60");
        let (_tx, rx) = shutdown();
        let mut h = PodExecutor::new(pod_spec(vec![], vec![c]))
            .spawn(rx)
            .await
            .unwrap();

        // Pod should complete quickly (container exits after 0.5s, probe stops)
        let result = tokio::time::timeout(Duration::from_secs(10), h.wait_for_completion()).await;
        assert!(result.is_ok(), "probe should stop when container exits");
    }

    #[tokio::test]
    async fn test_probe_interrupted_by_kill() {
        // Kill during long probe execution
        let c = container_with_readiness_exec("app", "sleep 60", "sleep 60");
        let (_tx, rx) = shutdown();
        let mut h = PodExecutor::new(pod_spec(vec![], vec![c]))
            .spawn(rx)
            .await
            .unwrap();

        // Wait for container to start
        loop {
            let ev = h.wait().await.unwrap();
            if matches!(&ev.kind, PodEventKind::ContainerStarted { .. }) {
                break;
            }
        }

        // Let probe start
        tokio::time::sleep(Duration::from_millis(800)).await;

        h.kill(Duration::from_secs(1));

        let result = tokio::time::timeout(Duration::from_secs(10), h.wait_for_completion()).await;
        assert!(
            result.is_ok(),
            "kill should interrupt probe and complete pod"
        );
    }

    // -- probe edge cases ---------------------------------------------------

    #[tokio::test]
    async fn test_exec_probe_empty_command() {
        // Exec probe with empty command should fail (not crash)
        let mut c = container("app", "sleep 3");
        c.readiness_probe = Some(Probe {
            exec_action: Some(ExecAction { command: vec![] }),
            period_seconds: 1,
            failure_threshold: 100, // high so it doesn't kill
            ..Default::default()
        });
        let (_tx, rx) = shutdown();
        let mut h = PodExecutor::new(pod_spec(vec![], vec![c]))
            .spawn(rx)
            .await
            .unwrap();

        let mut had_failure = false;
        let result = tokio::time::timeout(Duration::from_secs(8), async {
            loop {
                let ev = h.wait().await.unwrap();
                if matches!(&ev.kind, PodEventKind::ProbeResult { success, .. } if !success) {
                    had_failure = true;
                }
                if matches!(&ev.kind, PodEventKind::Completed { .. }) {
                    break;
                }
            }
        })
        .await;
        assert!(result.is_ok());
        assert!(had_failure, "empty command probe should report failure");
    }

    #[tokio::test]
    async fn test_http_probe_connection_refused() {
        // HTTP probe to port with no server → probe fails
        let c = Container {
            readiness_probe: Some(Probe {
                http_get: Some(HttpGetAction {
                    path: "/".to_string(),
                    port: Some(19998),
                    host: "127.0.0.1".to_string(),
                }),
                period_seconds: 1,
                timeout_seconds: 1,
                failure_threshold: 100,
                ..Default::default()
            }),
            ..container("app", "sleep 2")
        };
        let (_tx, rx) = shutdown();
        let mut h = PodExecutor::new(pod_spec(vec![], vec![c]))
            .spawn(rx)
            .await
            .unwrap();
        let events = collect_events(&mut h).await;

        let failures: Vec<_> = events
            .iter()
            .filter(|e| {
                matches!(&e.kind,
            PodEventKind::ProbeResult { success, .. } if !success)
            })
            .collect();
        assert!(
            !failures.is_empty(),
            "HTTP probe to closed port should fail"
        );
        let ready = events
            .iter()
            .any(|e| matches!(&e.kind, PodEventKind::ContainerReady { .. }));
        assert!(!ready, "should not become ready");
    }

    #[tokio::test]
    async fn test_http_probe_non_200_response() {
        // HTTP server returns 404 → probe fails
        let c = Container {
            readiness_probe: Some(Probe {
                http_get: Some(HttpGetAction {
                    path: "/nonexistent_path_404".to_string(),
                    port: Some(18767),
                    host: "127.0.0.1".to_string(),
                }),
                initial_delay_seconds: 1,
                period_seconds: 1,
                timeout_seconds: 2,
                success_threshold: 1,
                failure_threshold: 100,
                ..Default::default()
            }),
            // python http.server returns 404 for missing paths
            ..container(
                "app",
                "python3 -m http.server 18767 2>/dev/null || sleep 10",
            )
        };
        let (_tx, rx) = shutdown();
        let mut h = PodExecutor::new(pod_spec(vec![], vec![c]))
            .spawn(rx)
            .await
            .unwrap();

        let mut had_failure = false;
        let result = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let ev = h.wait().await.unwrap();
                if matches!(&ev.kind, PodEventKind::ProbeResult { success, .. } if !success) {
                    had_failure = true;
                    break;
                }
                if matches!(&ev.kind, PodEventKind::Completed { .. }) {
                    break;
                }
            }
        })
        .await;
        assert!(result.is_ok());
        assert!(had_failure, "404 response should be a probe failure");

        h.kill(Duration::from_secs(1));
        let _ = h.wait_for_completion().await;
    }

    #[tokio::test]
    async fn test_readiness_toggle_ready_then_not_ready() {
        // Readiness probe passes initially (marker exists), then fails
        // (marker removed by container), toggling ready false→true→false.
        let dir = test_dir!();
        let marker = dir.join("ready_toggle");

        // Create marker, then remove it after 2s
        let cmd = format!(
            "touch {} && sleep 2 && rm -f {} && sleep 10",
            marker.display(),
            marker.display()
        );
        let probe_cmd = format!("test -f {}", marker.display());

        let c = Container {
            readiness_probe: Some(Probe {
                exec_action: Some(ExecAction {
                    command: vec!["sh".to_string(), "-c".to_string(), probe_cmd],
                }),
                initial_delay_seconds: 0,
                period_seconds: 1,
                timeout_seconds: 2,
                success_threshold: 1,
                failure_threshold: 2, // 2 failures to toggle not-ready
                ..Default::default()
            }),
            ..container("app", &cmd)
        };
        let (_tx, rx) = shutdown();
        let mut h = PodExecutor::new(pod_spec(vec![], vec![c]))
            .spawn(rx)
            .await
            .unwrap();

        let mut readiness_changes = Vec::new();
        let result = tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                let ev = h.wait().await.unwrap();
                match &ev.kind {
                    PodEventKind::ContainerReady { .. } => {
                        readiness_changes.push(true);
                    }
                    PodEventKind::ProbeResult { .. } => {
                        // Track readiness in status
                        let ready = ev
                            .status
                            .container_statuses
                            .iter()
                            .find(|cs| cs.name == "app")
                            .map(|cs| cs.ready)
                            .unwrap_or(false);
                        // Detect transition to not-ready
                        if !ready && readiness_changes.last() == Some(&true) {
                            readiness_changes.push(false);
                            break; // we've seen the toggle
                        }
                    }
                    PodEventKind::Completed { .. } => break,
                    _ => {}
                }
            }
        })
        .await;

        assert!(result.is_ok(), "should complete within timeout");
        assert!(
            readiness_changes == vec![true, false],
            "should toggle ready→not-ready: {:?}",
            readiness_changes,
        );

        h.kill(Duration::from_secs(1));
        let _ = h.wait_for_completion().await;
    }

    #[tokio::test]
    async fn test_probe_respects_period() {
        // With period=2s and container running for 5s, should get ~2-3 probes
        // (not one per 500ms tick)
        let c = Container {
            readiness_probe: Some(Probe {
                exec_action: Some(ExecAction {
                    command: vec!["sh".to_string(), "-c".to_string(), "exit 0".to_string()],
                }),
                initial_delay_seconds: 0,
                period_seconds: 2, // every 2 seconds
                timeout_seconds: 1,
                success_threshold: 100, // never reaches threshold
                failure_threshold: 100,
                ..Default::default()
            }),
            ..container("app", "sleep 5")
        };
        let (_tx, rx) = shutdown();
        let mut h = PodExecutor::new(pod_spec(vec![], vec![c]))
            .spawn(rx)
            .await
            .unwrap();
        let events = collect_events(&mut h).await;

        let probe_count = events
            .iter()
            .filter(|e| matches!(&e.kind, PodEventKind::ProbeResult { .. }))
            .count();
        // With 5s runtime and 2s period, expect 2-3 probes (not 10)
        assert!(
            (1..=4).contains(&probe_count),
            "with period=2s over 5s, expected 2-3 probes, got {}",
            probe_count,
        );
    }

    // -- concurrent pods (regression: channel-close busy-loop) ----------------

    /// Regression test: many concurrent pods must all complete without
    /// hanging. Before the fix, closed probe/kill mpsc channels caused
    /// `recv()` to return `None` immediately on every `select!` iteration,
    /// creating a busy-loop that starved the tokio runtime and prevented
    /// other pods from making progress.
    #[tokio::test]
    async fn test_concurrent_pods_all_complete() {
        let (_tx, rx) = shutdown();
        let n = 20;
        let mut handles = Vec::with_capacity(n);

        for i in 0..n {
            let spec = Arc::new(PodSpec {
                metadata: PodMetadata {
                    name: format!("pod-{}", i),
                    namespace: "default".to_string(),
                    ..Default::default()
                },
                spec: PodSpecBody {
                    init_containers: vec![],
                    containers: vec![container(&format!("c-{}", i), "sleep 1")],
                    ..Default::default()
                },
                ..Default::default()
            });
            let h = PodExecutor::new(spec).spawn(rx.clone()).await.unwrap();
            handles.push(h);
        }

        // All pods must complete within a reasonable timeout.
        // Before the fix, most would hang indefinitely.
        let phases = tokio::time::timeout(Duration::from_secs(30), async {
            let mut phases = Vec::new();
            for h in &mut handles {
                phases.push(h.wait_for_completion().await.unwrap());
            }
            phases
        })
        .await
        .expect("timed out — pods did not complete (possible busy-loop regression)");

        assert_eq!(phases.len(), n);
        for phase in &phases {
            assert_eq!(*phase, PodPhase::Succeeded);
        }
    }

    /// Same as above but with a mix of containers, init containers, and
    /// sidecars — mirrors a realistic load-test scenario.
    #[tokio::test]
    async fn test_concurrent_mixed_pods_all_complete() {
        let (_tx, rx) = shutdown();

        let specs: Vec<Arc<PodSpec>> = (0..15)
            .map(|i| {
                let (init, containers) = match i % 3 {
                    // No init, no sidecar
                    0 => (vec![], vec![container(&format!("app-{}", i), "sleep 1")]),
                    // Init containers + sidecar
                    1 => (
                        vec![
                            container(&format!("init-{}", i), "sleep 0"),
                            sidecar(&format!("sc-{}", i), "trap exit TERM; sleep 3600 & wait $!"),
                        ],
                        vec![container(&format!("app-{}", i), "sleep 1")],
                    ),
                    // Multiple main containers
                    _ => (
                        vec![],
                        vec![
                            container(&format!("a-{}", i), "sleep 1"),
                            container(&format!("b-{}", i), "exit 1"),
                        ],
                    ),
                };

                Arc::new(PodSpec {
                    metadata: PodMetadata {
                        name: format!("mixed-{}", i),
                        namespace: "default".to_string(),
                        ..Default::default()
                    },
                    spec: PodSpecBody {
                        init_containers: init,
                        containers,
                        ..Default::default()
                    },
                    ..Default::default()
                })
            })
            .collect();

        let mut handles = Vec::with_capacity(specs.len());
        for spec in specs {
            handles.push(PodExecutor::new(spec).spawn(rx.clone()).await.unwrap());
        }

        let phases = tokio::time::timeout(Duration::from_secs(30), async {
            let mut phases = Vec::new();
            for h in &mut handles {
                phases.push(h.wait_for_completion().await.unwrap());
            }
            phases
        })
        .await
        .expect("timed out — mixed pods did not complete (possible busy-loop regression)");

        assert_eq!(phases.len(), 15);
        for (i, phase) in phases.iter().enumerate() {
            let expected = if i % 3 == 2 {
                PodPhase::Failed
            } else {
                PodPhase::Succeeded
            };
            assert_eq!(*phase, expected, "pod mixed-{} expected {:?}", i, expected);
        }
    }

    // -- lifecycle integration --------------------------------------------

    /// The live-run lifecycle should mirror the emitted pod phase:
    /// after a successful run, the lifecycle reaches `Succeeded` via
    /// valid `Pending → Running → Succeeded` transitions.
    #[tokio::test]
    async fn test_lifecycle_tracks_successful_run() {
        let (_tx, rx) = shutdown();
        let mut h = PodExecutor::new(pod_spec(vec![], vec![container("main", "exit 0")]))
            .spawn(rx)
            .await
            .unwrap();
        assert_eq!(h.wait_for_completion().await.unwrap(), PodPhase::Succeeded);

        // After completion, the lifecycle must be in the terminal phase
        // matching status.phase, proving the two stayed in lockstep
        // through every orchestrator transition.
        let st = h.state.lock().await;
        assert_eq!(st.lifecycle.phase(), PodPhase::Succeeded);
        assert_eq!(st.lifecycle.phase(), st.status().phase);
        assert!(st.lifecycle.is_terminal());
    }

    /// Failure path should reach Failed via `Pending → Running → Failed`.
    #[tokio::test]
    async fn test_lifecycle_tracks_failed_run() {
        let (_tx, rx) = shutdown();
        let mut h = PodExecutor::new(pod_spec(vec![], vec![container("main", "exit 7")]))
            .spawn(rx)
            .await
            .unwrap();
        assert_eq!(h.wait_for_completion().await.unwrap(), PodPhase::Failed);

        let st = h.state.lock().await;
        assert_eq!(st.lifecycle.phase(), PodPhase::Failed);
        assert_eq!(st.lifecycle.phase(), st.status().phase);
    }

    /// Init-failure path: the pod goes `Pending → Failed` directly,
    /// bypassing Running. The lifecycle must accept that shortcut.
    #[tokio::test]
    async fn test_lifecycle_tracks_init_failure() {
        let (_tx, rx) = shutdown();
        let mut h = PodExecutor::new(pod_spec(
            vec![container("init", "exit 1")],
            vec![container("main", "exit 0")],
        ))
        .spawn(rx)
        .await
        .unwrap();
        assert_eq!(h.wait_for_completion().await.unwrap(), PodPhase::Failed);

        let st = h.state.lock().await;
        assert_eq!(st.lifecycle.phase(), PodPhase::Failed);
        // Container lifecycle reflects the init failure
        let init = st.lifecycle.init_container("init").unwrap();
        assert!(init.is_terminated());
        assert_eq!(init.terminated_exit_code(), Some(1));
        // Main container never started
        let main = st.lifecycle.container("main").unwrap();
        assert!(!main.is_started());
    }

    /// After a readiness probe passes, the lifecycle's ProbePhase for
    /// Readiness should reflect at least one passing observation and
    /// the container should be marked ready.
    #[tokio::test]
    async fn test_lifecycle_tracks_readiness_probe() {
        use crate::pod::lifecycle::{ProbeKind, ProbePhase};
        let (_tx, rx) = shutdown();
        let c = container_with_readiness_exec("app", "sleep 2", "true");
        let mut h = PodExecutor::new(pod_spec(vec![], vec![c]))
            .spawn(rx)
            .await
            .unwrap();
        let _ = h.wait_for_completion().await.unwrap();

        let st = h.state.lock().await;
        let cl = st.lifecycle.container("app").unwrap();
        // Probe was registered and recorded at least one result
        let phase = cl
            .probe_phase(ProbeKind::Readiness)
            .expect("readiness probe should be registered");
        assert!(
            matches!(phase, ProbePhase::Passing { consecutive } if *consecutive >= 1),
            "expected Passing phase, got {:?}",
            phase,
        );
    }

    /// After all containers complete, the lifecycle's ContainersReady and
    /// Ready conditions should be True (containers are terminated, which
    /// counts as "ready" for the condition). Proves update_readiness is
    /// routed through the lifecycle.
    #[tokio::test]
    async fn test_lifecycle_readiness_conditions_after_exit() {
        use crate::pod::lifecycle::PodConditionKey;
        let (_tx, rx) = shutdown();
        let mut h = PodExecutor::new(pod_spec(
            vec![],
            vec![container("a", "exit 0"), container("b", "exit 0")],
        ))
        .spawn(rx)
        .await
        .unwrap();
        assert_eq!(h.wait_for_completion().await.unwrap(), PodPhase::Succeeded);

        let st = h.state.lock().await;
        let cr = st
            .lifecycle
            .condition(PodConditionKey::ContainersReady)
            .expect("ContainersReady condition should be set");
        assert!(cr.status, "ContainersReady should be True after all exited");
        let r = st
            .lifecycle
            .condition(PodConditionKey::Ready)
            .expect("Ready condition should be set");
        assert!(r.status, "Ready should be True after all exited");
    }

    // -- registered tasks ---------------------------------------------------

    use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};

    /// Observable task: records that it started, observes both signals,
    /// and reports which one ended its run.
    struct ObserverTask {
        started: Arc<AtomicBool>,
        finished_via_pod_kill: Arc<AtomicBool>,
        finished_via_runner_shutdown: Arc<AtomicBool>,
        recorded_work_dir: Arc<Mutex<Option<PathBuf>>>,
        recorded_pod_name: Arc<Mutex<Option<String>>>,
    }

    #[async_trait]
    impl PodTask for ObserverTask {
        async fn run(&self, mut ctx: PodTaskContext) {
            self.started.store(true, Ordering::SeqCst);
            *self.recorded_work_dir.lock().await = Some(ctx.work_dir.clone());
            *self.recorded_pod_name.lock().await = Some(ctx.pod.metadata.name.clone());

            tokio::select! {
                _ = ctx.pod_kill.changed() => {
                    self.finished_via_pod_kill.store(true, Ordering::SeqCst);
                }
                _ = ctx.runner_shutdown.changed() => {
                    self.finished_via_runner_shutdown.store(true, Ordering::SeqCst);
                }
            }
        }
    }

    /// Counts how many task instances are spawned across a pod's lifetime.
    struct CountingTask {
        spawn_count: Arc<AtomicU32>,
    }

    #[async_trait]
    impl PodTask for CountingTask {
        async fn run(&self, mut ctx: PodTaskContext) {
            self.spawn_count.fetch_add(1, Ordering::SeqCst);
            let _ = ctx.pod_kill.changed().await;
        }
    }

    /// Task that ignores the signals and runs forever — used to exercise
    /// the grace-period abort path.
    struct HangingTask {
        aborted: Arc<AtomicBool>,
    }

    #[async_trait]
    impl PodTask for HangingTask {
        async fn run(&self, _ctx: PodTaskContext) {
            // Use a drop guard to detect abort.
            struct Guard(Arc<AtomicBool>);
            impl Drop for Guard {
                fn drop(&mut self) {
                    self.0.store(true, Ordering::SeqCst);
                }
            }
            let _g = Guard(Arc::clone(&self.aborted));
            // Hang until aborted.
            std::future::pending::<()>().await;
        }
    }

    #[tokio::test]
    async fn task_observes_pod_kill_on_natural_completion() {
        let (_tx, rx) = shutdown();
        let started = Arc::new(AtomicBool::new(false));
        let via_kill = Arc::new(AtomicBool::new(false));
        let via_shutdown = Arc::new(AtomicBool::new(false));
        let work_dir = Arc::new(Mutex::new(None));
        let pod_name = Arc::new(Mutex::new(None));

        let task = Arc::new(ObserverTask {
            started: Arc::clone(&started),
            finished_via_pod_kill: Arc::clone(&via_kill),
            finished_via_runner_shutdown: Arc::clone(&via_shutdown),
            recorded_work_dir: Arc::clone(&work_dir),
            recorded_pod_name: Arc::clone(&pod_name),
        });

        let dir = test_dir!();
        let mut h = PodExecutor::new(pod_spec(vec![], vec![container("main", "exit 0")]))
            .work_dir(&dir)
            .register_task(task)
            .spawn(rx)
            .await
            .unwrap();

        assert_eq!(h.wait_for_completion().await.unwrap(), PodPhase::Succeeded);
        // The orchestrator awaits the task with grace_period on its way
        // out. Give the orchestrator a moment to finish that await so
        // the task's flags are reliably set.
        let _ = tokio::time::timeout(Duration::from_secs(2), &mut h.orchestrator).await;

        assert!(started.load(Ordering::SeqCst), "task did not start");
        assert!(
            via_kill.load(Ordering::SeqCst),
            "task should have exited via pod_kill"
        );
        assert!(
            !via_shutdown.load(Ordering::SeqCst),
            "runner_shutdown should not have fired"
        );
        assert_eq!(work_dir.lock().await.as_ref().unwrap(), &dir);
        assert_eq!(pod_name.lock().await.as_deref(), Some("test-pod"));
    }

    #[tokio::test]
    async fn task_observes_runner_shutdown() {
        let (tx, rx) = shutdown();
        let started = Arc::new(AtomicBool::new(false));
        let via_kill = Arc::new(AtomicBool::new(false));
        let via_shutdown = Arc::new(AtomicBool::new(false));

        let task = Arc::new(ObserverTask {
            started: Arc::clone(&started),
            finished_via_pod_kill: Arc::clone(&via_kill),
            finished_via_runner_shutdown: Arc::clone(&via_shutdown),
            recorded_work_dir: Arc::new(Mutex::new(None)),
            recorded_pod_name: Arc::new(Mutex::new(None)),
        });

        // Long-running container so the pod doesn't complete on its own.
        let mut h = PodExecutor::new(pod_spec(vec![], vec![container("main", "sleep 30")]))
            .register_task(task)
            .spawn(rx)
            .await
            .unwrap();

        // Wait briefly so the task is definitely spawned.
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(
            started.load(Ordering::SeqCst),
            "task did not start before shutdown"
        );

        // Fire runner shutdown — orchestrator should kill containers and
        // exit, and tasks should observe runner_shutdown.
        let _ = tx.send(());

        assert_eq!(h.wait_for_completion().await.unwrap(), PodPhase::Failed);
        let _ = tokio::time::timeout(Duration::from_secs(2), &mut h.orchestrator).await;

        // The orchestrator also fires kill_watch on its way out for task
        // cleanup. Whichever signal the task observed first wins the
        // select!, but at least one of the two must have fired.
        assert!(
            via_shutdown.load(Ordering::SeqCst) || via_kill.load(Ordering::SeqCst),
            "task observed neither runner_shutdown nor pod_kill",
        );
    }

    #[tokio::test]
    async fn multiple_tasks_all_spawn() {
        let (_tx, rx) = shutdown();
        let count = Arc::new(AtomicU32::new(0));
        let a = Arc::new(CountingTask {
            spawn_count: Arc::clone(&count),
        });
        let b = Arc::new(CountingTask {
            spawn_count: Arc::clone(&count),
        });
        let c = Arc::new(CountingTask {
            spawn_count: Arc::clone(&count),
        });

        let mut h = PodExecutor::new(pod_spec(vec![], vec![container("main", "exit 0")]))
            .register_task(a)
            .register_task(b)
            .register_task(c)
            .spawn(rx)
            .await
            .unwrap();

        assert_eq!(h.wait_for_completion().await.unwrap(), PodPhase::Succeeded);
        let _ = tokio::time::timeout(Duration::from_secs(2), &mut h.orchestrator).await;

        assert_eq!(count.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn hanging_task_is_aborted_after_grace_period() {
        let (_tx, rx) = shutdown();
        let aborted = Arc::new(AtomicBool::new(false));
        let task = Arc::new(HangingTask {
            aborted: Arc::clone(&aborted),
        });

        let mut h = PodExecutor::new(pod_spec(vec![], vec![container("main", "exit 0")]))
            .shutdown_grace_period(Duration::from_millis(200))
            .register_task(task)
            .spawn(rx)
            .await
            .unwrap();

        assert_eq!(h.wait_for_completion().await.unwrap(), PodPhase::Succeeded);
        // Orchestrator's task-grace timeout (200ms) plus a margin.
        let _ = tokio::time::timeout(Duration::from_secs(3), &mut h.orchestrator).await;

        assert!(
            aborted.load(Ordering::SeqCst),
            "hanging task was not aborted"
        );
    }

    #[tokio::test]
    async fn pod_with_no_registered_tasks_works_unchanged() {
        // Sanity: the task pathway must not affect pods without tasks.
        let (_tx, rx) = shutdown();
        let mut h = PodExecutor::new(pod_spec(vec![], vec![container("main", "exit 0")]))
            .spawn(rx)
            .await
            .unwrap();
        assert_eq!(h.wait_for_completion().await.unwrap(), PodPhase::Succeeded);
    }

    #[tokio::test]
    async fn dropping_pod_handle_aborts_tasks() {
        let (_tx, rx) = shutdown();
        let aborted = Arc::new(AtomicBool::new(false));
        let task = Arc::new(HangingTask {
            aborted: Arc::clone(&aborted),
        });

        let h = PodExecutor::new(pod_spec(vec![], vec![container("main", "sleep 30")]))
            .register_task(task)
            .spawn(rx)
            .await
            .unwrap();

        // Let the task start.
        tokio::time::sleep(Duration::from_millis(200)).await;
        drop(h);
        // Drop aborts the orchestrator JoinHandle. JoinSet inside the
        // orchestrator drops with it, aborting registered tasks.
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(
            aborted.load(Ordering::SeqCst),
            "task should be aborted when handle is dropped"
        );
    }
}
