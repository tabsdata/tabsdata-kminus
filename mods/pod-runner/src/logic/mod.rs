// Copyright 2026 Tabsdata Inc.

//! Pod logic layer — wires the [`PodStore`] with the [`PodExecutor`].
//!
//! [`PodLogic`] provides the operations that an API server (or CLI)
//! calls: create, delete, get, list. It owns the pod lifecycles:
//!
//! - **Create**: admits the pod (rejects unsupported features), checks
//!   uniqueness/capacity, stores the pod in **Pending**, and spawns a
//!   background preparer task. The preparer runs
//!   [`crate::pod::prepare::prepare_pod`] which may wait (with
//!   exponential backoff) for any referenced configmap/secret to appear.
//!   On success the preparer spawns the executor and a monitor task,
//!   replacing the preparer entry in the runtime map; on failure (incl.
//!   `activeDeadlineSeconds` timeout) it transitions the pod to
//!   **Failed** in the store.
//! - **Delete**: cancels the preparer if the pod is still being
//!   prepared, otherwise kills running containers (via [`PodKillHandle`])
//!   and marks the pod as a disruption target. Terminal pods are removed
//!   immediately.
//! - **Get / List**: thin wrappers over the store.

pub mod disk_usage;
pub mod pod_logs;
pub mod reaper;
pub mod ttl;

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex as StdMutex, RwLock};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use tokio::sync::{mpsc, watch};
use tokio::task::{JoinHandle, JoinSet};
use tracing::{Instrument, debug, error, info, info_span, trace, trace_span, warn};

use crate::metrics::Metrics;
pub use crate::pod::container_log::ContainerLogOptions;
use crate::pod::executor::{
    PodEventKind, PodExecutor, PodHandle, PodKillHandle, PodLogHandle, PodTask,
};
use crate::pod::prepare::{PrepareError, prepare_pod};
pub use crate::pod::resolver::ResolverSet;
use crate::process::sweep;
use crate::spec::constants::*;
use crate::spec::node::{NodeAddress, NodeCapacity, NodeCondition};
use crate::spec::pod::PodSpec;
use crate::spec::status::{ContainerStatus, PodStatus};
use crate::store::layout::PodLayout;
use crate::store::{PodId, PodSnapshot, PodStore, SpecView};

pub use reaper::CleanupConfig;

// ---------------------------------------------------------------------------
// Executor provider
// ---------------------------------------------------------------------------

/// Factory for creating configured [`PodExecutor`]s.
///
/// The logic layer uses this to create executors without knowing about
/// working directories, log directories, or other executor settings.
pub trait ExecutorProvider: Send + Sync {
    /// Create a [`PodExecutor`] configured for the given pod.
    fn create(&self, pod: Arc<PodSpec>, id: &PodId) -> PodExecutor;

    /// The on-disk pod layout.
    fn layout(&self) -> &PodLayout;

    /// Working directory for a pod — its data dir.
    fn work_dir(&self, id: &PodId) -> PathBuf {
        self.layout().run_dir(id)
    }

    /// State directory for the pod incarnation `spec` describes — its logs
    /// and store.
    fn state_dir(&self, spec: &PodSpec) -> PathBuf {
        self.layout().state_dir(spec)
    }

    /// Log directory for a pod incarnation's containers, if logging is
    /// configured.
    fn log_dir(&self, spec: &PodSpec) -> Option<PathBuf> {
        Some(self.layout().log_dir(spec))
    }
}

/// Default executor provider with configurable base directories.
///
/// Per-pod layout: `work/<container>/` under the pod's data dir,
/// `log/<container>/` under its state dir (see [`PodLayout`]).
pub struct DefaultExecutorProvider {
    /// Pod layout: data dirs under `pod-run/`, state dirs under
    /// `pod-state/`.
    pub layout: PodLayout,
    /// Directory for process PID tracking files.
    pub pid_dir: PathBuf,
    /// Whether to propagate the parent process environment.
    pub propagate_env: bool,
    /// Whether child processes die when the parent exits.
    pub die_with_parent: bool,
    /// Maximum number of container restarts.
    pub max_restarts: u32,
    /// Base duration for exponential restart backoff.
    pub restart_backoff_base: Duration,
    /// Grace period for container kills on shutdown.
    pub shutdown_grace_period: Duration,
}

impl ExecutorProvider for DefaultExecutorProvider {
    fn layout(&self) -> &PodLayout {
        &self.layout
    }

    fn create(&self, pod: Arc<PodSpec>, id: &PodId) -> PodExecutor {
        let pod_dir = self.layout.run_dir(id);
        let log_dir = self.layout.log_dir(&pod);

        // Ensure directories exist before the executor tries to use them
        if let Err(e) = std::fs::create_dir_all(&pod_dir) {
            warn!(pod = %id, error = %e, "Failed to create pod directory");
        } else if let Err(e) = self.layout.link_run_dir(id) {
            warn!(pod = %id, error = %e, "Failed to link pod directory by name");
        }
        if let Err(e) = std::fs::create_dir_all(&log_dir) {
            warn!(pod = %id, error = %e, "Failed to create log directory");
        }

        PodExecutor::new(pod)
            .work_dir(pod_dir)
            .log_dir(log_dir)
            .pid_dir(self.pid_dir.clone())
            .propagate_env(self.propagate_env)
            .die_with_parent(self.die_with_parent)
            .max_restarts(self.max_restarts)
            .restart_backoff_base(self.restart_backoff_base)
            .shutdown_grace_period(self.shutdown_grace_period)
    }
}

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Node capacity configuration.
pub struct CapacityConfig {
    pub cpu: u32,
    pub memory_gb: u32,
    pub pods: u32,
}

impl Default for CapacityConfig {
    fn default() -> Self {
        Self {
            cpu: 100,
            memory_gb: 100,
            pods: 100,
        }
    }
}

/// Configuration for the logic layer.
pub struct LogicConfig {
    /// Maximum number of pods allowed in the store.
    pub max_pods: usize,
    /// Default grace period for pod deletion when none is specified.
    pub default_grace_period: Duration,
    /// Node capacity reported to the K8s API server.
    pub capacity: CapacityConfig,
    /// Node address reported as InternalIP.
    pub address: String,
    /// Root directory for resolving relative container `image` paths.
    /// Absolute paths bypass this. Empty means no root (the container
    /// `image` field is used verbatim).
    pub images_dir: PathBuf,
}

impl Default for LogicConfig {
    fn default() -> Self {
        Self {
            max_pods: 100,
            default_grace_period: Duration::from_secs(30),
            capacity: CapacityConfig::default(),
            address: "127.0.0.1".to_string(),
            images_dir: PathBuf::new(),
        }
    }
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Errors returned by logic layer operations.
#[derive(Debug, thiserror::Error)]
pub enum LogicError {
    /// Pod not found by namespace + name.
    #[error("pod {namespace}/{name} not found")]
    NotFound { namespace: String, name: String },
    /// A pod with this namespace + name already exists.
    #[error("pod {namespace}/{name} already exists")]
    Conflict { namespace: String, name: String },
    /// Store is at capacity.
    #[error("pod capacity exceeded (max {max})")]
    CapacityExceeded { max: usize },
    /// Pod admission rejected the spec.
    #[error("admission rejected: {0}")]
    Admission(#[from] crate::pod::admission::AdmissionError),
    /// Internal error (executor failure, etc.).
    #[error("internal error: {0}")]
    Internal(String),
}

// ---------------------------------------------------------------------------
// PodRuntime — per-pod runtime state owned by the logic layer
// ---------------------------------------------------------------------------

/// Per-pod runtime state. A pod begins life as [`PodRuntime::Preparing`]
/// while the preparer task waits for missing configmaps/secrets and
/// builds the running spec. Once preparation succeeds the entry is
/// replaced (atomically, under the runtimes write lock) with
/// [`PodRuntime::Running`] which carries the executor's kill/log
/// handles. If preparation fails or times out the entry stays as
/// `Preparing` with a finished task until `delete_pod` removes it.
enum PodRuntime {
    Preparing {
        /// Fires when the preparer should abort (delete or shutdown).
        cancel_tx: watch::Sender<()>,
        /// True once `delete_pod` has been called for this pod.
        deleted: Arc<AtomicBool>,
        /// Preparer task. Kept so `wait_for_pods` can poll completion.
        task: JoinHandle<()>,
    },
    Running {
        kill_handle: PodKillHandle,
        log_handle: PodLogHandle,
        /// True once `delete_pod` has been called for this pod.
        deleted: Arc<AtomicBool>,
        /// Monitor task that processes executor events and updates the
        /// store. Kept alive — aborted if the runtime is dropped.
        _monitor: JoinHandle<()>,
    },
}

impl PodRuntime {
    fn deleted(&self) -> &Arc<AtomicBool> {
        match self {
            Self::Preparing { deleted, .. } | Self::Running { deleted, .. } => deleted,
        }
    }

    fn is_finished(&self) -> bool {
        match self {
            Self::Preparing { task, .. } => task.is_finished(),
            Self::Running { _monitor, .. } => _monitor.is_finished(),
        }
    }
}

// ---------------------------------------------------------------------------
// RunnerTask — pod-runner-level background task
// ---------------------------------------------------------------------------

/// Context handed to a registered [`RunnerTask`] when it's spawned.
///
/// Unlike [`crate::pod::executor::PodTaskContext`], this is per-runner
/// (not per-pod): there is no pod spec or pod-level kill signal. The
/// task runs once for the lifetime of the [`PodLogic`] instance.
#[derive(Clone)]
pub struct RunnerTaskContext {
    /// The pod-runner's working directory (set via
    /// [`PodLogic::runner_work_dir`]). Empty when not configured.
    pub work_dir: PathBuf,
    /// Fires when the pod runner is shutting down. Tasks should exit
    /// promptly once observed.
    pub runner_shutdown: watch::Receiver<()>,
}

/// A user-registered task that runs for the entire pod-runner lifetime.
///
/// Tasks are registered on [`PodLogic`] via
/// [`PodLogic::register_runner_task`] and are spawned eagerly (at
/// registration time). They are expected to exit when
/// [`RunnerTaskContext::runner_shutdown`] fires.
///
/// On clean shutdown, call [`PodLogic::wait_for_runner_tasks`] to await
/// graceful exit with a timeout. If [`PodLogic`] is dropped before
/// tasks exit, the underlying [`JoinSet`] aborts them.
#[async_trait]
pub trait RunnerTask: Send + Sync {
    /// Run the task. The task should exit when
    /// [`RunnerTaskContext::runner_shutdown`] fires.
    async fn run(&self, ctx: RunnerTaskContext);
}

// ---------------------------------------------------------------------------
// PodLogic
// ---------------------------------------------------------------------------

/// Pod logic layer — owns pod lifecycles, wires store and executor.
pub struct PodLogic {
    config: Arc<LogicConfig>,
    cleanup_config: Arc<CleanupConfig>,
    store: Arc<dyn PodStore>,
    executor_provider: Arc<dyn ExecutorProvider>,
    resolvers: Arc<ResolverSet>,
    runtimes: Arc<RwLock<HashMap<PodId, PodRuntime>>>,
    metrics: Option<Arc<Metrics>>,
    shutdown_tx: watch::Sender<()>,
    /// Subscribed at construction so reap timers can be cancelled by
    /// shutdown regardless of when they are scheduled.
    shutdown_rx: watch::Receiver<()>,
    /// Tasks registered via [`PodLogic::register_task`]. A snapshot of
    /// this Vec is passed to every pod executor at spawn time.
    tasks: Arc<Vec<Arc<dyn PodTask>>>,
    /// Pod-runner working directory passed to every [`RunnerTask`] via
    /// [`RunnerTaskContext::work_dir`].
    runner_work_dir: PathBuf,
    /// JoinSet of spawned [`RunnerTask`] handles. Wrapped in `StdMutex`
    /// so the builder methods (which take `&mut self`) can still spawn
    /// into it without giving up `Self` ownership. Dropping the
    /// `JoinSet` aborts all surviving tasks.
    runner_task_set: StdMutex<JoinSet<()>>,
}

impl PodLogic {
    pub fn new(
        config: impl Into<LogicConfig>,
        cleanup_config: CleanupConfig,
        store: Arc<dyn PodStore>,
        executor_provider: Arc<dyn ExecutorProvider>,
        resolvers: ResolverSet,
        shutdown_tx: watch::Sender<()>,
    ) -> Self {
        Self {
            config: Arc::new(config.into()),
            cleanup_config: Arc::new(cleanup_config),
            store,
            executor_provider,
            resolvers: Arc::new(resolvers),
            runtimes: Arc::new(RwLock::new(HashMap::new())),
            metrics: None,
            // Subscribed here rather than on demand: a receiver created
            // after shutdown is signalled cannot observe it, and reap
            // timers must be cancellable whenever they are scheduled.
            shutdown_rx: shutdown_tx.subscribe(),
            shutdown_tx,
            tasks: Arc::new(Vec::new()),
            runner_work_dir: PathBuf::new(),
            runner_task_set: StdMutex::new(JoinSet::new()),
        }
    }

    /// Wait for all running pod monitor tasks to complete.
    ///
    /// Call after sending the shutdown signal. Polls until every active
    /// monitor has finished (markers, deadlines, inline reap). Naturally
    /// completed pods keep their runtime entry so logs stay accessible
    /// — those have `_monitor.is_finished()` and don't block shutdown.
    /// Times out after `timeout` to avoid hanging on stuck pods.
    pub async fn wait_for_pods(&self, timeout: Duration) {
        // Preparer cancel channels are otherwise only fired by
        // delete_pod. Fire them for every pod still preparing so a
        // pod stuck waiting for a missing resource (infinite
        // resolver backoff) cannot hold shutdown until the timeout.
        {
            let runtimes = self.runtimes.read().unwrap();
            for runtime in runtimes.values() {
                if let PodRuntime::Preparing { cancel_tx, .. } = runtime {
                    let _ = cancel_tx.send(());
                }
            }
        }

        let deadline = tokio::time::Instant::now() + timeout;
        let mut last_logged = 0;
        loop {
            let count = self
                .runtimes
                .read()
                .unwrap()
                .values()
                .filter(|r| !r.is_finished())
                .count();
            if count == 0 {
                info!("All pod monitors completed");
                return;
            }
            if tokio::time::Instant::now() >= deadline {
                warn!(
                    remaining = count,
                    "Timeout waiting for pod monitors, proceeding with shutdown"
                );
                return;
            }
            if count != last_logged {
                info!(remaining = count, "Waiting for pod monitors to complete");
                last_logged = count;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// Set the metrics instance for instrumentation.
    pub fn with_metrics(mut self, metrics: Arc<Metrics>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// Register a [`PodTask`] that runs alongside every pod for the
    /// duration of its execution.
    ///
    /// Each registered task gets its own tokio task per pod, started when
    /// the executor spawns the pod and signalled to stop (via the
    /// per-pod kill watch) when the orchestrator finishes. The
    /// pod-runner shutdown signal is also exposed; tasks should exit
    /// promptly when either fires. See [`PodTask`] and
    /// [`crate::pod::executor::PodTaskContext`] for the contract.
    ///
    /// Call this on the builder before `create_pod` is invoked; pods
    /// created before registration won't receive the task.
    pub fn register_task(mut self, task: Arc<dyn PodTask>) -> Self {
        Arc::make_mut(&mut self.tasks).push(task);
        self
    }

    /// Set the working directory passed to every [`RunnerTask`] via
    /// [`RunnerTaskContext::work_dir`].
    ///
    /// Call before [`register_runner_task`](Self::register_runner_task);
    /// tasks already registered get the value that was set at the time
    /// of their registration (empty `PathBuf` if never set).
    pub fn runner_work_dir(mut self, dir: PathBuf) -> Self {
        self.runner_work_dir = dir;
        self
    }

    /// Register a [`RunnerTask`] that runs for the lifetime of this
    /// pod-runner.
    ///
    /// The task is spawned immediately with a context containing the
    /// runner working directory (see
    /// [`runner_work_dir`](Self::runner_work_dir)) and a fresh subscriber
    /// of the runner shutdown signal. Call
    /// [`wait_for_runner_tasks`](Self::wait_for_runner_tasks) after
    /// firing shutdown to await graceful exit; dropping `PodLogic`
    /// aborts any still-running tasks.
    pub fn register_runner_task(self, task: Arc<dyn RunnerTask>) -> Self {
        let ctx = RunnerTaskContext {
            work_dir: self.runner_work_dir.clone(),
            runner_shutdown: self.shutdown_tx.subscribe(),
        };
        self.runner_task_set
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .spawn(async move { task.run(ctx).await });
        self
    }

    /// Wait for all registered runner tasks to finish, up to `timeout`.
    ///
    /// Call after sending the shutdown signal. Returns when every task
    /// has exited or when `timeout` elapses. Tasks still running after
    /// the timeout are NOT aborted here — they remain owned by the
    /// inner [`JoinSet`] and are aborted only when `PodLogic` is
    /// dropped.
    pub async fn wait_for_runner_tasks(&self, timeout: Duration) {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            // Try to reap any already-finished tasks. Use try_join_next
            // so we don't block while holding the StdMutex.
            let finished = {
                let mut guard = self
                    .runner_task_set
                    .lock()
                    .unwrap_or_else(|p| p.into_inner());
                if guard.is_empty() {
                    return;
                }
                guard.try_join_next().is_some()
            };
            if finished {
                continue;
            }
            if tokio::time::Instant::now() >= deadline {
                let remaining = self
                    .runner_task_set
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .len();
                warn!(
                    remaining,
                    "Timeout waiting for runner tasks, proceeding with shutdown",
                );
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// Create a pod.
    ///
    /// Admits the spec (rejects unsupported features), checks
    /// uniqueness/capacity, stores the pod in **Pending**, and spawns a
    /// background preparer task. Returns the initial Pending snapshot.
    /// The executor is spawned only after the preparer finishes — see
    /// [`prepare_and_run`] for the full flow.
    pub async fn create_pod(&self, mut spec: PodSpec) -> Result<PodSnapshot, LogicError> {
        let create_start = std::time::Instant::now();
        let id = PodId::from_spec(&spec);
        stamp_creation_time(&mut spec, chrono::Utc::now());

        // Admission — reject unsupported features (e.g. `optional: true`
        // on configmap/secret references). See pod::admission for the
        // full list of rules.
        crate::pod::admission::admit(&spec)?;

        // Identity reconciliation. A pod's (namespace, name) is its
        // identity in kube; the UID only distinguishes successive
        // incarnations. The control plane reuses a name only after the
        // prior pod has been deleted, so an existing entry under the same
        // name means one of:
        //   - same UID  → a redelivery of this same pod (resync).
        //     Idempotent: return the current snapshot, don't start a
        //     second run.
        //   - diff UID + terminal → a stale incarnation whose delete went
        //     missing upstream. Evict it and run the new pod.
        //   - diff UID + live → a live pod with the same name still
        //     exists. Never run two pods with the same name at once:
        //     terminate the old one and reject the new pod with a
        //     (retryable) conflict — the next attempt proceeds once the
        //     old pod has been reaped.
        // A different-UID collision always means a delete went missing
        // upstream, so warn to keep it visible.
        if let Some(existing) = self
            .store
            .get_by_name(&id.namespace, &id.name, SpecView::Running)
            .await
        {
            if existing.id.uid == id.uid {
                debug!(pod = %id, "Create for already-known pod (same UID); returning current snapshot");
                return Ok(existing);
            }
            if existing.status.phase.is_terminal() {
                warn!(
                    pod = %id,
                    stale_uid = %existing.id.uid,
                    stale_phase = ?existing.status.phase,
                    "Evicting stale terminal pod to make room for the new incarnation"
                );
                self.delete_pod_inner(existing, None, std::time::Instant::now())
                    .await?;
            } else {
                warn!(
                    pod = %id,
                    live_uid = %existing.id.uid,
                    live_phase = ?existing.status.phase,
                    "Live pod with same name still exists; terminating it and rejecting the new pod (retry once reaped)"
                );
                self.delete_pod_inner(existing, None, std::time::Instant::now())
                    .await?;
                return Err(LogicError::Conflict {
                    namespace: id.namespace,
                    name: id.name,
                });
            }
        }

        // Capacity counts only non-terminal pods: Succeeded/Failed pods hold no
        // resources, so they must not consume the capacity budget (otherwise an
        // accumulation of completed-but-undeleted pods would wrongly block new
        // admissions — they linger in the store until the control plane's
        // delete arrives, which it may do late, out of order, or never).
        if self.store.count_non_terminal().await >= self.config.max_pods {
            warn!(pod = %id, max = self.config.max_pods, "Capacity exceeded");
            return Err(LogicError::CapacityExceeded {
                max: self.config.max_pods,
            });
        }

        // A log id label that is not a Kubernetes name cannot be a path
        // component; the pod is filed under its own name instead. Said once,
        // here, rather than failing the pod over where its logs go.
        let log_id_label = &self.executor_provider.layout().log_id_label;
        if let Some(value) = spec.metadata.labels.get(log_id_label)
            && !crate::store::layout::is_kube_name(value)
        {
            warn!(
                pod = %id,
                label = %log_id_label,
                value = %value,
                "Log id label is not a valid Kubernetes name; filing the pod's logs under its name",
            );
        }

        // Claim the pod's dirs and record its retention policy before the
        // store writes its first status into the state dir — a pod replayed
        // under this id after a restart finds its previous attempt's dirs.
        reaper::claim_pod_dirs(
            &self.executor_provider.work_dir(&id),
            &self.executor_provider.state_dir(&spec),
            &self.cleanup_config.ttls,
            &spec.metadata.annotations,
            chrono::Utc::now(),
        );

        // Store the pod in Pending. The running_spec starts equal to the
        // original; the preparer overwrites it with the rewritten spec
        // once preparation succeeds.
        let original_spec = Arc::new(spec);
        let initial_status = build_initial_status(&original_spec);
        self.store
            .put(
                id.clone(),
                Arc::clone(&original_spec),
                Arc::clone(&original_spec),
                initial_status,
            )
            .await;
        {
            // Tightly scoped: EnteredSpan is !Send, so it must not be
            // held across an await.
            let _span = info_span!("pod", pod = %id).entered();
            info!("Pod accepted");
        }

        // Per-pod cancel channel — fires on delete_pod or shutdown.
        let (cancel_tx, cancel_rx) = watch::channel(());
        let deleted = Arc::new(AtomicBool::new(false));

        // Spawn preparer task. It owns the only path that mutates
        // self.runtimes for this pod (swap Preparing → Running on
        // success), keeping the lock-held regions short.
        let prep_span = info_span!("prepare", pod = %id);
        let task = tokio::spawn(
            prepare_and_run(
                id.clone(),
                Arc::clone(&original_spec),
                Arc::clone(&self.store),
                Arc::clone(&self.executor_provider),
                Arc::clone(&self.cleanup_config),
                Arc::clone(&self.resolvers),
                Arc::clone(&self.config),
                Arc::clone(&self.runtimes),
                self.metrics.clone(),
                Arc::clone(&deleted),
                self.shutdown_tx.clone(),
                cancel_rx,
                Arc::clone(&self.tasks),
            )
            .instrument(prep_span),
        );

        // Register Preparing runtime entry so delete_pod can find it.
        self.runtimes.write().unwrap().insert(
            id.clone(),
            PodRuntime::Preparing {
                cancel_tx,
                deleted,
                task,
            },
        );

        // Metrics
        if let Some(ref m) = self.metrics {
            m.pods_created_total.inc();
            m.pods_running
                .set(self.runtimes.read().unwrap().len() as i64);
            m.pod_creation_seconds
                .observe(create_start.elapsed().as_secs_f64());
        }

        match self.store.get(&id, SpecView::Running).await {
            Some(snap) => Ok(snap),
            None => {
                self.store.remove(&id).await;
                Err(LogicError::Internal(
                    "pod disappeared after put".to_string(),
                ))
            }
        }
    }

    /// Delete a pod.
    ///
    /// - **Not found**: returns `Ok(())` (idempotent).
    /// - **Terminal** (Succeeded/Failed): removed from store immediately.
    /// - **Running**: kills containers and marks `DisruptionTarget`.
    ///   The monitor task handles the phase transition when containers exit.
    pub async fn delete_pod(
        &self,
        namespace: &str,
        name: &str,
        grace_period: Option<Duration>,
    ) -> Result<(), LogicError> {
        let delete_start = std::time::Instant::now();
        let snap = match self
            .store
            .get_by_name(namespace, name, SpecView::Running)
            .await
        {
            Some(snap) => snap,
            None => {
                debug!(namespace, name, "Delete: pod not found (ok, idempotent)");
                return Ok(());
            }
        };

        // Attach the pod context to every log via .instrument() rather
        // than an .entered() guard — EnteredSpan is !Send, so holding
        // one across an .await makes the whole future !Send and would
        // prevent the axum handler from using this function.
        let span = info_span!("pod", pod = %snap.id);
        self.delete_pod_inner(snap, grace_period, delete_start)
            .instrument(span)
            .await
    }

    async fn delete_pod_inner(
        &self,
        snap: PodSnapshot,
        grace_period: Option<Duration>,
        delete_start: std::time::Instant,
    ) -> Result<(), LogicError> {
        // Effective grace: an explicit delete `gracePeriod` wins; otherwise
        // the pod's own `terminationGracePeriodSeconds`; otherwise the
        // runner's configured default.
        let grace = grace_period
            .or_else(|| snap.spec.termination_grace_period())
            .unwrap_or(self.config.default_grace_period);

        info!(phase = ?snap.status.phase, "Pod deleted");

        // Terminal — the pod leaves the runner, which starts its delete
        // clock. The monitor has already stamped the outcome and the reap
        // deadline by the time we get here.
        //
        // The pod dir is left for the reaper: `log/` and `store/` are the
        // record of what this pod did, and deleting them the moment the
        // control plane stops asking about the pod is what made a failure
        // unexplainable. `evict` drops the index entry but keeps the record.
        if snap.status.phase.is_terminal() {
            debug!(phase = ?snap.status.phase, "Evicting terminal pod");
            // A terminal pod's containers are gone, but a process that
            // escaped a container's process group can still be running,
            // and this path signals nothing else — the executor is done
            // and its handles are dropped. The sweep is the only thing
            // that can act here.
            sweep::kill_orphans(&sweep::pod_prefix(&snap.id.name, &snap.id.uid)).await;
            self.evict_pod(&snap).await;
            // Mark deleted so if the monitor is still finishing, it skips
            // its own eviction (store entry already dropped above).
            if let Some(runtime) = self.runtimes.read().unwrap().get(&snap.id) {
                runtime.deleted().store(true, Ordering::Relaxed);
            }
            self.runtimes.write().unwrap().remove(&snap.id);
            return Ok(());
        }

        // Cancel the preparer (if still preparing) or kill via runtime
        // (if already running). The map read lock is dropped before any
        // .await — we extract the bits we need then act on them.
        enum Action {
            CancelPreparer(watch::Sender<()>),
            KillRunning(PodKillHandle),
            None,
        }

        let action = {
            let runtimes = self.runtimes.read().unwrap();
            match runtimes.get(&snap.id) {
                Some(runtime) => {
                    if runtime.deleted().load(Ordering::Relaxed) {
                        debug!("Pod already marked for deletion");
                        return Ok(());
                    }
                    runtime.deleted().store(true, Ordering::Relaxed);
                    match runtime {
                        PodRuntime::Preparing { cancel_tx, .. } => {
                            Action::CancelPreparer(cancel_tx.clone())
                        }
                        PodRuntime::Running { kill_handle, .. } => {
                            Action::KillRunning(kill_handle.clone())
                        }
                    }
                }
                None => {
                    warn!("No runtime found for non-terminal pod");
                    Action::None
                }
            }
        };

        match action {
            Action::CancelPreparer(tx) => {
                debug!("Cancelling pod preparer");
                let _ = tx.send(());
                // The preparer task observes `deleted` after its
                // prepare_pod call returns and exits without spawning the
                // executor, so nothing else will ever finalize this dir.
                // A pod deleted mid-preparation is usually one that was
                // stuck there, and its partially resolved volumes are the
                // evidence — so it is retained on the error TTLs rather
                // than left unmarked, which no reaper would ever look at.
                let data_dir = self.executor_provider.work_dir(&snap.id);
                let now = chrono::Utc::now();
                let reap_on = reaper::finalize_pod(
                    &data_dir,
                    &self.executor_provider.state_dir(&snap.spec),
                    reaper::Outcome::Failed,
                    &self.cleanup_config.ttls,
                    now,
                );
                reaper::schedule_deadline(
                    &data_dir,
                    reaper::ScheduledReap::DataDir,
                    reap_on,
                    now,
                    &self.shutdown_rx,
                );
                self.evict_pod(&snap).await;
                self.runtimes.write().unwrap().remove(&snap.id);
                return Ok(());
            }
            Action::KillRunning(kh) => {
                debug!(grace_period = ?grace, "Killing pod");
                kh.kill(grace);
                // Mark disruption target on the LIVE lifecycle + status
                // so the next emitted event carries it forward.
                kh.mark_disruption_target().await;
            }
            Action::None => {}
        }

        // Also write to the store immediately so clients that poll
        // before the next event arrives see the disruption condition.
        // The next emitted event will overwrite with the same condition
        // (since we also set it on the live status).
        let mut status = snap.status.clone();
        status.mark_disruption_target();
        self.store.update_status(&snap.id, status).await;

        if let Some(ref m) = self.metrics {
            m.pods_deleted_total.inc();
            m.pod_deletion_seconds
                .observe(delete_start.elapsed().as_secs_f64());
        }

        Ok(())
    }

    /// Drop a pod from the runner: start its delete clock, drop the store
    /// index entry, and apply any deadline that is already due.
    ///
    /// The delete clock runs from here rather than from completion because
    /// until this point the runner still answers queries about the pod, so
    /// its dir is live data.
    async fn evict_pod(&self, snap: &PodSnapshot) {
        let id = &snap.id;
        let state_dir = self.executor_provider.state_dir(&snap.spec);
        let now = chrono::Utc::now();
        let delete_on = reaper::mark_pod_evicted(&state_dir, &self.cleanup_config.ttls, now);
        self.store.evict(id).await;
        // A zero TTL acts here; anything later waits out its own deadline.
        reaper::apply_deadline(&state_dir, reaper::ScheduledReap::StateDir, now);
        reaper::schedule_deadline(
            &state_dir,
            reaper::ScheduledReap::StateDir,
            delete_on,
            now,
            &self.shutdown_rx,
        );
    }

    /// Pick up the pod dirs a previous incarnation of the runner left,
    /// applying or scheduling every deadline they carry. Call once at
    /// startup, before the runner admits pods.
    pub fn recover_pod_dirs(&self) -> reaper::RecoveryStats {
        reaper::recover_pod_dirs(
            self.executor_provider.layout(),
            &self.cleanup_config.ttls,
            chrono::Utc::now(),
            &self.shutdown_rx,
        )
    }

    /// Get a pod by namespace and name.
    pub async fn get_pod(
        &self,
        namespace: &str,
        name: &str,
        view: SpecView,
    ) -> Result<PodSnapshot, LogicError> {
        // Use .instrument() instead of .entered() — EnteredSpan is
        // !Send and would make this async fn's future !Send.
        async {
            self.store
                .get_by_name(namespace, name, view)
                .await
                .ok_or_else(|| {
                    trace!("Pod not found");
                    LogicError::NotFound {
                        namespace: namespace.to_string(),
                        name: name.to_string(),
                    }
                })
        }
        .instrument(trace_span!("logic_get", namespace, name))
        .await
    }

    /// Get only the status of a pod.
    pub async fn get_pod_status(
        &self,
        namespace: &str,
        name: &str,
    ) -> Result<PodStatus, LogicError> {
        async {
            self.get_pod(namespace, name, SpecView::Running)
                .await
                .map(|snap| snap.status)
        }
        .instrument(trace_span!("logic_get_status", namespace, name))
        .await
    }

    /// List all pods.
    pub async fn list_pods(&self, view: SpecView) -> Vec<PodSnapshot> {
        async {
            let pods = self.store.list(view).await;
            trace!(count = pods.len(), "Listed pods");
            pods
        }
        .instrument(trace_span!("logic_list"))
        .await
    }

    /// Return the current phase of every pod as lightweight entries.
    ///
    /// Designed for the virtual-kubelet's sync loop — one bulk call
    /// per tick returns all statuses, eliminating per-pod HTTP calls.
    pub async fn list_pod_statuses(&self) -> Vec<crate::spec::api::PodStatusEntry> {
        async {
            let pods = self.store.list(SpecView::Running).await;
            trace!(count = pods.len(), "Listed pod statuses");
            pods.into_iter()
                .map(|snap| crate::spec::api::PodStatusEntry {
                    namespace: snap.id.namespace,
                    name: snap.id.name,
                    // Carry the uid so the VK can distinguish a stale
                    // incarnation lingering under a reused name from the
                    // current one. See PodStatusEntry docs.
                    uid: snap.id.uid,
                    phase: snap.status.phase.to_string(),
                    status: snap.status,
                })
                .collect()
        }
        .instrument(trace_span!("logic_list_statuses"))
        .await
    }

    /// List pods in a specific namespace.
    pub async fn list_pods_in_namespace(
        &self,
        namespace: &str,
        view: SpecView,
    ) -> Vec<PodSnapshot> {
        async {
            let pods = self.store.list_in_namespace(namespace, view).await;
            trace!(count = pods.len(), "Listed pods in namespace");
            pods
        }
        .instrument(trace_span!("logic_list_ns", namespace))
        .await
    }

    /// Stream container logs for a pod.
    ///
    /// Returns an `mpsc::Receiver` that yields log chunks as `Bytes`.
    /// The receiver can be converted to an axum streaming response.
    ///
    /// Delegates to [`PodLogHandle::stream()`] which encapsulates all
    /// log file resolution and streaming details.
    pub async fn stream_container_logs(
        &self,
        namespace: &str,
        name: &str,
        container: Option<&str>,
        options: ContainerLogOptions,
    ) -> Result<mpsc::Receiver<Result<Bytes, std::io::Error>>, LogicError> {
        // Verify the pod exists
        let snap = self.get_pod(namespace, name, SpecView::Running).await?;

        let runtimes = self.runtimes.read().unwrap();
        let runtime = runtimes
            .get(&snap.id)
            .ok_or_else(|| LogicError::Internal("no runtime for pod".to_string()))?;

        match runtime {
            PodRuntime::Preparing { .. } => Err(LogicError::Internal(
                "pod is still being prepared (waiting for configmap/secret); no container logs available yet"
                    .to_string(),
            )),
            PodRuntime::Running { log_handle, .. } => log_handle
                .stream(container, options)
                .map_err(|e| LogicError::Internal(e.to_string())),
        }
    }

    /// Stream the logs of every pod filed under a log pod id, oldest first,
    /// from the pod-state dirs — whether or not the runner still knows the
    /// pods. See [`pod_logs`] and [`crate::store::layout`].
    pub fn stream_pod_logs(
        &self,
        log_pod_id: &str,
        options: pod_logs::PodLogsOptions,
    ) -> Result<mpsc::Receiver<Result<Bytes, std::io::Error>>, pod_logs::PodLogsError> {
        pod_logs::stream_pod_logs(self.executor_provider.layout(), log_pod_id, options)
    }

    /// Node capacity from configuration.
    pub fn get_capacity(&self) -> NodeCapacity {
        NodeCapacity {
            cpu: self.config.capacity.cpu.to_string(),
            memory: format!("{}Gi", self.config.capacity.memory_gb),
            pods: self.config.capacity.pods.to_string(),
        }
    }

    /// Node conditions (hardcoded healthy status with current timestamps).
    pub fn get_node_conditions(&self) -> Vec<NodeCondition> {
        let now = chrono::Utc::now().to_rfc3339();
        vec![
            NodeCondition {
                condition_type: "Ready".to_string(),
                status: "True".to_string(),
                last_heartbeat_time: now.clone(),
                last_transition_time: now.clone(),
                reason: REASON_KUBELET_READY.to_string(),
                message: "kubelet is ready.".to_string(),
            },
            NodeCondition {
                condition_type: "MemoryPressure".to_string(),
                status: "False".to_string(),
                last_heartbeat_time: now.clone(),
                last_transition_time: now.clone(),
                reason: REASON_KUBELET_SUFFICIENT_MEMORY.to_string(),
                message: "kubelet has sufficient memory available".to_string(),
            },
            NodeCondition {
                condition_type: "DiskPressure".to_string(),
                status: "False".to_string(),
                last_heartbeat_time: now.clone(),
                last_transition_time: now,
                reason: REASON_KUBELET_NO_DISK_PRESSURE.to_string(),
                message: "kubelet has no disk pressure".to_string(),
            },
        ]
    }

    /// Node addresses (InternalIP from configuration).
    pub fn get_node_addresses(&self) -> Vec<NodeAddress> {
        vec![NodeAddress {
            address_type: "InternalIP".to_string(),
            address: self.config.address.clone(),
        }]
    }
}

// ---------------------------------------------------------------------------
// Spec preparation
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Initial status
// ---------------------------------------------------------------------------

/// Build the initial [`PodStatus`] for a newly created pod.
/// Give a pod with no usable creation time the admission time as one.
///
/// The creation time names the pod's incarnation dir, so every admitted pod
/// must carry one, and it must be fixed once: every later path is derived
/// from the spec. The API server always sets it; a pod submitted around it
/// (`kpr`) gets the admission time, so a replay of such a pod after a
/// restart starts a new incarnation dir rather than continuing its own.
fn stamp_creation_time(spec: &mut PodSpec, now: chrono::DateTime<chrono::Utc>) {
    if spec.metadata.incarnation_stamp().is_none() {
        spec.metadata.creation_timestamp =
            Some(now.to_rfc3339_opts(chrono::SecondsFormat::Secs, true));
    }
}

fn build_initial_status(spec: &PodSpec) -> PodStatus {
    let mut status = PodStatus {
        phase: PodPhase::Pending,
        start_time: chrono::Utc::now().to_rfc3339(),
        ..Default::default()
    };

    status.mark_scheduled();

    if spec.spec.init_containers.is_empty() {
        status.mark_initialized();
    } else {
        status.mark_not_initialized(REASON_INIT_CONTAINERS_RUNNING);
    }

    status.mark_containers_not_ready(REASON_CONTAINERS_CREATING);

    // Init container statuses
    for c in &spec.spec.init_containers {
        status
            .init_container_statuses
            .push(ContainerStatus::new_waiting(&c.name));
    }

    // Container statuses
    for c in &spec.spec.containers {
        status
            .container_statuses
            .push(ContainerStatus::new_waiting(&c.name));
    }

    status
}

// ---------------------------------------------------------------------------
// Monitor task
// ---------------------------------------------------------------------------

/// Background task that processes executor events and updates the store.
///
/// Runs until the pod reaches a terminal state or the executor channel
/// closes. On completion, removes the runtime entry so future deletes
/// just clean up the store.
// Launcher-style: every parameter is an independent value the caller
// already has, so bundling them into a struct moves the list rather
// than shortening it.
#[allow(clippy::too_many_arguments)]
async fn monitor_pod(
    mut handle: PodHandle,
    store: Arc<dyn PodStore>,
    executor_provider: Arc<dyn ExecutorProvider>,
    cleanup_config: Arc<CleanupConfig>,
    runtimes: Arc<RwLock<HashMap<PodId, PodRuntime>>>,
    metrics: Option<Arc<Metrics>>,
    id: PodId,
    state_dir: PathBuf,
    deleted: Arc<AtomicBool>,
    mut shutdown: watch::Receiver<()>,
) {
    // The outcome the pod completed with, taken from the Completed event
    // rather than the store so it can't be disturbed by a concurrent delete.
    let mut final_outcome: Option<reaper::Outcome> = None;

    // Cloned at entry, before the loop below can observe a shutdown: a
    // receiver that has already seen it would leave reap timers uncancellable.
    let reap_shutdown = shutdown.clone();

    loop {
        let event = tokio::select! {
            result = handle.wait() => result,
            _ = shutdown.changed() => {
                debug!("Shutdown signal received in monitor, waiting for pod to finish");
                // Don't break — let the orchestrator finish. It already
                // received the shutdown signal and will kill containers.
                // Just continue the event loop to process final events.
                continue;
            }
        };
        match event {
            Ok(event) => {
                match &event.kind {
                    PodEventKind::ContainerStarted { name, role } => {
                        debug!(container = name.as_str(), role = %role, "Container started");
                        if let Some(ref m) = metrics {
                            m.containers_started_total.inc();
                            m.containers_running.inc();
                        }
                    }
                    PodEventKind::ContainerSucceeded { name } => {
                        debug!(container = name.as_str(), "Container succeeded");
                        if let Some(ref m) = metrics {
                            m.containers_running.dec();
                        }
                    }
                    PodEventKind::ContainerFailed { name, exit_code } => {
                        // Sidecars are killed when the pod completes, so
                        // a nonzero exit is their normal termination.
                        let is_sidecar = handle
                            .pod()
                            .spec
                            .init_containers
                            .iter()
                            .any(|c| c.name == *name && c.is_sidecar());
                        if is_sidecar {
                            debug!(container = name.as_str(), exit_code, "Sidecar terminated");
                        } else {
                            warn!(container = name.as_str(), exit_code, "Container failed");
                        }
                        if let Some(ref m) = metrics {
                            m.containers_failed_total.inc();
                            m.containers_running.dec();
                        }
                    }
                    PodEventKind::ContainerBackOff {
                        name,
                        restart_count,
                        delay,
                    } => {
                        warn!(
                            container = name.as_str(),
                            restart_count,
                            delay_ms = delay.as_millis() as u64,
                            "Container backing off before restarting",
                        );
                    }
                    PodEventKind::Completed { phase } => {
                        if *phase == crate::spec::constants::PodPhase::Failed {
                            warn!(
                                phase = ?phase,
                                reason = %event.status.message,
                                "Pod completed",
                            );
                        } else {
                            info!(phase = ?phase, "Pod completed");
                        }
                        if let Some(ref m) = metrics {
                            if *phase == crate::spec::constants::PodPhase::Failed {
                                m.pods_failed_total.inc();
                            }
                            m.pods_running.dec();
                        }
                    }
                    PodEventKind::PhaseChanged { to, .. } => {
                        if *to == crate::spec::constants::PodPhase::Running {
                            info!(phase = ?to, "Pod running");
                        } else {
                            debug!(phase = ?to, "Phase changed");
                        }
                        if let Some(ref m) = metrics
                            && *to == crate::spec::constants::PodPhase::Running
                        {
                            m.pods_started_total.inc();
                        }
                    }
                    _ => {
                        debug!(kind = ?event.kind, phase = ?event.status.phase, "Event received");
                    }
                }
                let completed = match event.kind {
                    PodEventKind::Completed { phase } => {
                        final_outcome = reaper::Outcome::from_phase(phase);
                        true
                    }
                    _ => false,
                };
                store.update_status(&id, event.status).await;
                if completed {
                    // Let the orchestrator's post-completion cleanup run
                    // before we drop the handle. The orchestrator's
                    // wrapper waits on registered PodTasks (with
                    // shutdown_grace_period as a bound) so they can
                    // finish their post-loop work — e.g. the
                    // DiskSizePodTask's final scan. Without this wait,
                    // PodHandle::Drop would abort the orchestrator
                    // before any of that runs.
                    handle.wait_for_finish().await;
                    break;
                }
            }
            Err(e) => {
                warn!(error = %e, "Event stream ended unexpectedly");
                break;
            }
        }
    }

    // Stamp the outcome and start the reap clock, then act on it — a reap
    // TTL of 0s reclaims the data dirs here rather than waiting for a scan.
    // This runs after the orchestrator's post-completion tasks (the
    // disk-usage final scan among them) so the reported size still reflects
    // what the pod used.
    //
    // A pod whose event stream died without a Completed event never reached
    // a terminal phase, so it gets no marker — the next run's recovery
    // finalizes those.
    let data_dir = executor_provider.work_dir(&id);
    if let Some(outcome) = final_outcome {
        let now = chrono::Utc::now();
        let reap_on =
            reaper::finalize_pod(&data_dir, &state_dir, outcome, &cleanup_config.ttls, now);
        reaper::apply_deadline(&data_dir, reaper::ScheduledReap::DataDir, now);
        reaper::schedule_deadline(
            &data_dir,
            reaper::ScheduledReap::DataDir,
            reap_on,
            now,
            &reap_shutdown,
        );
    }

    // If the pod was deleted while it ran, it has already left the runner,
    // so its delete clock starts now. A natural completion keeps everything
    // until the control plane deletes the pod.
    if deleted.load(Ordering::Relaxed) {
        debug!("Evicting deleted pod");
        let now = chrono::Utc::now();
        let delete_on = reaper::mark_pod_evicted(&state_dir, &cleanup_config.ttls, now);
        store.evict(&id).await;
        reaper::apply_deadline(&state_dir, reaper::ScheduledReap::StateDir, now);
        reaper::schedule_deadline(
            &state_dir,
            reaper::ScheduledReap::StateDir,
            delete_on,
            now,
            &reap_shutdown,
        );
        runtimes.write().unwrap().remove(&id);
    }

    debug!("Monitor task finished");
}

// ---------------------------------------------------------------------------
// Preparer task
// ---------------------------------------------------------------------------

/// Build a Failed status for a pod that never made it past preparation.
/// Sets `phase = Failed`, attaches the reason/message, and leaves container
/// statuses empty (no container ever started).
fn build_failed_prep_status(reason: &str, message: &str) -> PodStatus {
    let now = chrono::Utc::now().to_rfc3339();
    let mut status = PodStatus {
        phase: PodPhase::Failed,
        message: message.to_string(),
        start_time: now.clone(),
        last_transition_time: now,
        ..Default::default()
    };
    status.mark_scheduled();
    status.mark_not_initialized(reason);
    status.mark_containers_not_ready(reason);
    status
}

/// Background task that prepares the pod and, on success, spawns the
/// executor + monitor (replacing this pod's `PodRuntime::Preparing` entry
/// with `PodRuntime::Running`).
///
/// Failure modes:
/// - **Timeout** (`activeDeadlineSeconds` exceeded while waiting for a
///   referenced configmap/secret) — pod transitions to
///   `Failed`/`DeadlineExceeded`.
/// - **Cancelled** by `delete_pod` — the task exits silently; the caller
///   has already removed the store and runtime entries.
/// - **Other** preparation error (parse error, executor spawn failure,
///   an executable path too long for Windows, etc.) — pod transitions to
///   `Failed`/`PodPreparationFailed`, with the full error as the status
///   message. A too-long executable path is logged at error level.
#[allow(clippy::too_many_arguments)]
async fn prepare_and_run(
    id: PodId,
    original_spec: Arc<PodSpec>,
    store: Arc<dyn PodStore>,
    executor_provider: Arc<dyn ExecutorProvider>,
    cleanup_config: Arc<CleanupConfig>,
    resolvers: Arc<ResolverSet>,
    config: Arc<LogicConfig>,
    runtimes: Arc<RwLock<HashMap<PodId, PodRuntime>>>,
    metrics: Option<Arc<Metrics>>,
    deleted: Arc<AtomicBool>,
    shutdown_tx: watch::Sender<()>,
    mut cancel_rx: watch::Receiver<()>,
    tasks: Arc<Vec<Arc<dyn PodTask>>>,
) {
    let pod_work_dir = executor_provider.work_dir(&id);
    let pod_state_dir = executor_provider.state_dir(&original_spec);
    let timeout = original_spec
        .spec
        .active_deadline_seconds
        .map(Duration::from_secs);

    debug!(
        timeout_secs = ?timeout.map(|d| d.as_secs()),
        "Starting pod preparation",
    );

    // For the failure paths below, which finalize the pod dir and so need a
    // reap timer. Subscribed now, well before any shutdown.
    let reap_shutdown = shutdown_tx.subscribe();

    let prepare_result = prepare_pod(
        &original_spec,
        &pod_work_dir,
        &id.namespace,
        &resolvers,
        &config.images_dir,
        timeout,
        &mut cancel_rx,
    )
    .await;

    // If delete_pod fired the cancel while we were preparing, exit
    // silently — the delete path has already cleaned store + runtimes.
    if deleted.load(Ordering::Relaxed) {
        debug!("Preparation aborted by deletion");
        return;
    }

    let prepared = match prepare_result {
        Ok(p) => p,
        Err(err) => {
            // A cancelled poll with `deleted` unset means shutdown
            // fired the cancel (wait_for_pods) — exit without
            // marking the pod Failed.
            if err.is_cancelled() {
                info!("Preparation aborted by shutdown");
                return;
            }
            // Classify and surface as Failed in the store.
            let reason = if err.is_timeout() {
                warn!(error = %err, "Pod preparation timed out");
                REASON_DEADLINE_EXCEEDED
            } else if let PrepareError::ExecutableTooLong {
                container, path, ..
            } = &err
            {
                error!(
                    container = %container,
                    path = %path,
                    error = %err,
                    "Pod preparation failed: executable path too long for Windows",
                );
                REASON_POD_PREPARATION_FAILED
            } else {
                warn!(error = %err, "Pod preparation failed");
                REASON_POD_PREPARATION_FAILED
            };
            let status = build_failed_prep_status(reason, &err.to_string());
            store.update_status(&id, status).await;
            // A pod that dies here never ran a container, so its dir holds
            // only the partially resolved volumes — which is exactly what
            // says where preparation stopped. Finalize it like any other
            // failure so it is retained and reaped on the error TTLs.
            let now = chrono::Utc::now();
            let reap_on = reaper::finalize_pod(
                &pod_work_dir,
                &pod_state_dir,
                reaper::Outcome::Failed,
                &cleanup_config.ttls,
                now,
            );
            reaper::schedule_deadline(
                &pod_work_dir,
                reaper::ScheduledReap::DataDir,
                reap_on,
                now,
                &reap_shutdown,
            );
            if let Some(ref m) = metrics {
                m.pods_failed_total.inc();
            }
            return;
        }
    };

    // Re-store with the prepared running spec and a fresh initial status.
    let running_spec = Arc::new(prepared);
    let initial_status = build_initial_status(&running_spec);
    store
        .put(
            id.clone(),
            Arc::clone(&original_spec),
            Arc::clone(&running_spec),
            initial_status,
        )
        .await;

    // Spawn the executor for the prepared spec.
    let mut executor = executor_provider.create(Arc::clone(&running_spec), &id);
    for task in tasks.iter() {
        executor = executor.register_task(Arc::clone(task));
    }
    let exec_shutdown = shutdown_tx.subscribe();
    let handle = match executor.spawn(exec_shutdown).await {
        Ok(h) => h,
        Err(e) => {
            warn!(error = %e, "Executor spawn failed");
            let status = build_failed_prep_status(
                REASON_POD_PREPARATION_FAILED,
                &format!("executor spawn failed: {}", e),
            );
            store.update_status(&id, status).await;
            let now = chrono::Utc::now();
            let reap_on = reaper::finalize_pod(
                &pod_work_dir,
                &pod_state_dir,
                reaper::Outcome::Failed,
                &cleanup_config.ttls,
                now,
            );
            reaper::schedule_deadline(
                &pod_work_dir,
                reaper::ScheduledReap::DataDir,
                reap_on,
                now,
                &reap_shutdown,
            );
            if let Some(ref m) = metrics {
                m.pods_failed_total.inc();
            }
            return;
        }
    };

    let kill_handle = handle.kill_handle();
    let log_handle = handle.log_handle();

    // Root span: the monitor outlives preparation — don't nest
    // the pod's runtime logs under the prepare{...} span.
    let monitor_span = info_span!(parent: None, "pod", pod = %id);
    let monitor_shutdown = shutdown_tx.subscribe();
    let monitor = tokio::spawn(
        monitor_pod(
            handle,
            Arc::clone(&store),
            Arc::clone(&executor_provider),
            Arc::clone(&cleanup_config),
            Arc::clone(&runtimes),
            metrics,
            id.clone(),
            pod_state_dir,
            Arc::clone(&deleted),
            monitor_shutdown,
        )
        .instrument(monitor_span),
    );

    // Swap Preparing → Running. The preparer's own JoinHandle (held by
    // the Preparing variant we're replacing) is dropped here — tokio
    // detaches the task rather than aborting, so this fn keeps running
    // to completion just fine.
    runtimes.write().unwrap().insert(
        id.clone(),
        PodRuntime::Running {
            kill_handle,
            log_handle,
            deleted,
            _monitor: monitor,
        },
    );

    debug!("Pod prepared and executor spawned");
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::pod::{Container, PodMetadata, PodSpecBody};
    use crate::store::memory::MemoryStore;

    fn make_spec(namespace: &str, name: &str, uid: &str) -> PodSpec {
        PodSpec {
            metadata: PodMetadata {
                namespace: namespace.to_string(),
                name: name.to_string(),
                uid: uid.to_string(),
                ..Default::default()
            },
            spec: PodSpecBody::default(),
            ..Default::default()
        }
    }

    use std::sync::atomic::AtomicU64;
    static TEST_COUNTER: AtomicU64 = AtomicU64::new(0);

    /// The state dir of the only incarnation of pod `name`.
    fn only_state_dir(provider: &dyn ExecutorProvider, name: &str) -> PathBuf {
        let name_dir = provider.layout().log_pod_dir(name);
        let mut dirs: Vec<PathBuf> = std::fs::read_dir(&name_dir)
            .unwrap_or_else(|e| panic!("{}: {e}", name_dir.display()))
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.is_dir())
            .collect();
        assert_eq!(dirs.len(), 1, "one incarnation expected: {dirs:?}");
        dirs.pop().unwrap()
    }

    fn make_provider() -> Arc<DefaultExecutorProvider> {
        crate::testutils::init_test_tracing();
        let n = TEST_COUNTER.fetch_add(1, Ordering::Relaxed);
        let base = std::env::temp_dir().join(format!(
            "pod-runner-rs2-test-logic-{}-{}",
            std::process::id(),
            n,
        ));
        Arc::new(DefaultExecutorProvider {
            layout: PodLayout::new(&base),
            pid_dir: base.join("pids"),
            propagate_env: true,
            die_with_parent: true,
            max_restarts: 0,
            restart_backoff_base: Duration::from_secs(2),
            shutdown_grace_period: Duration::from_secs(30),
        })
    }

    fn test_cleanup_config() -> CleanupConfig {
        CleanupConfig::default()
    }

    fn make_logic() -> PodLogic {
        let store = Arc::new(MemoryStore::new());
        let (shutdown_tx, _) = watch::channel(());
        PodLogic::new(
            LogicConfig::default(),
            test_cleanup_config(),
            store,
            make_provider(),
            ResolverSet::default(),
            shutdown_tx,
        )
    }

    // -- build_initial_status --

    #[test]
    fn initial_status_no_init_containers() {
        let spec = make_spec("default", "app", "uid-1");
        let status = build_initial_status(&spec);

        assert_eq!(status.phase, PodPhase::Pending);
        assert!(!status.start_time.is_empty());
        assert!(status.is_condition_true(CONDITION_POD_SCHEDULED));
        assert!(status.is_condition_true(CONDITION_INITIALIZED));
        assert!(!status.is_condition_true(CONDITION_CONTAINERS_READY));
        assert!(!status.is_condition_true(CONDITION_READY));
        assert!(status.container_statuses.is_empty());
        assert!(status.init_container_statuses.is_empty());
    }

    #[test]
    fn initial_status_with_containers() {
        let mut spec = make_spec("default", "app", "uid-1");
        spec.spec.containers.push(Container {
            name: "web".to_string(),
            command: vec!["echo".to_string()],
            ..Default::default()
        });
        spec.spec.containers.push(Container {
            name: "sidecar".to_string(),
            command: vec!["echo".to_string()],
            ..Default::default()
        });

        let status = build_initial_status(&spec);

        assert_eq!(status.container_statuses.len(), 2);
        assert_eq!(status.container_statuses[0].name, "web");
        assert_eq!(status.container_statuses[1].name, "sidecar");
        assert!(!status.container_statuses[0].started);
        assert!(!status.container_statuses[0].ready);
    }

    #[test]
    fn initial_status_with_init_containers() {
        let mut spec = make_spec("default", "app", "uid-1");
        spec.spec.init_containers.push(Container {
            name: "setup".to_string(),
            command: vec!["echo".to_string()],
            ..Default::default()
        });
        spec.spec.containers.push(Container {
            name: "main".to_string(),
            command: vec!["echo".to_string()],
            ..Default::default()
        });

        let status = build_initial_status(&spec);

        assert!(!status.is_condition_true(CONDITION_INITIALIZED));
        assert_eq!(status.init_container_statuses.len(), 1);
        assert_eq!(status.init_container_statuses[0].name, "setup");
        assert_eq!(status.container_statuses.len(), 1);
        assert_eq!(status.container_statuses[0].name, "main");
    }

    // -- LogicError --

    #[test]
    fn error_display() {
        let e = LogicError::NotFound {
            namespace: "default".to_string(),
            name: "app".to_string(),
        };
        assert_eq!(e.to_string(), "pod default/app not found");

        let e = LogicError::Conflict {
            namespace: "default".to_string(),
            name: "app".to_string(),
        };
        assert_eq!(e.to_string(), "pod default/app already exists");

        let e = LogicError::CapacityExceeded { max: 10 };
        assert_eq!(e.to_string(), "pod capacity exceeded (max 10)");

        let e = LogicError::Internal("boom".to_string());
        assert_eq!(e.to_string(), "internal error: boom");
    }

    // -- sync logic tests (no executor) --

    #[tokio::test]
    async fn get_pod_not_found() {
        let logic = make_logic();
        let err = logic
            .get_pod("default", "nope", SpecView::Running)
            .await
            .unwrap_err();
        assert!(matches!(err, LogicError::NotFound { .. }));
    }

    #[tokio::test]
    async fn get_pod_status_not_found() {
        let logic = make_logic();
        let err = logic.get_pod_status("default", "nope").await.unwrap_err();
        assert!(matches!(err, LogicError::NotFound { .. }));
    }

    #[tokio::test]
    async fn list_pods_empty() {
        let logic = make_logic();
        assert!(logic.list_pods(SpecView::Running).await.is_empty());
        assert!(logic.list_pods(SpecView::Original).await.is_empty());
    }

    #[tokio::test]
    async fn list_pods_in_namespace_empty() {
        let logic = make_logic();
        assert!(
            logic
                .list_pods_in_namespace("default", SpecView::Running)
                .await
                .is_empty()
        );
    }

    #[tokio::test]
    async fn delete_pod_not_found_is_ok() {
        let logic = make_logic();
        assert!(logic.delete_pod("default", "nope", None).await.is_ok());
    }

    #[test]
    fn default_config() {
        let config = LogicConfig::default();
        assert_eq!(config.max_pods, 100);
        assert_eq!(config.default_grace_period, Duration::from_secs(30));
        assert_eq!(config.capacity.cpu, 100);
        assert_eq!(config.capacity.memory_gb, 100);
        assert_eq!(config.capacity.pods, 100);
        assert_eq!(config.address, "127.0.0.1");
    }

    // -- DefaultExecutorProvider --

    #[test]
    fn executor_provider_creates_directories() {
        let base = std::env::temp_dir().join(format!(
            "pod-runner-rs2-test-provider-{}",
            std::process::id(),
        ));
        let provider = DefaultExecutorProvider {
            layout: PodLayout::new(&base),
            pid_dir: base.join("pids"),
            propagate_env: true,
            die_with_parent: true,
            max_restarts: 0,
            restart_backoff_base: Duration::from_secs(2),
            shutdown_grace_period: Duration::from_secs(30),
        };
        let spec = Arc::new(make_spec("default", "app", "uid-1"));
        let id = PodId::from_spec(&spec);

        let _executor = provider.create(Arc::clone(&spec), &id);

        // Directories should exist after create()
        assert!(provider.work_dir(&id).exists());
        assert!(provider.log_dir(&spec).unwrap().exists());
        assert!(
            provider
                .log_dir(&spec)
                .unwrap()
                .starts_with(provider.state_dir(&spec)),
            "logs are part of the pod's record, not its data",
        );
    }

    #[test]
    fn executor_provider_log_dir() {
        let provider = make_provider();
        let mut spec = make_spec("default", "app", "uid-1");
        spec.metadata.creation_timestamp = Some("2026-10-19T14:30:05Z".into());

        let log_dir = provider.log_dir(&spec).unwrap();
        assert!(
            log_dir.ends_with(
                std::path::Path::new("app")
                    .join("20261019T143005Z_app")
                    .join("log")
            ),
            "{log_dir:?}"
        );
    }

    #[test]
    fn stamp_creation_time_fills_only_a_missing_or_malformed_one() {
        let now = chrono::DateTime::parse_from_rfc3339("2026-10-19T14:30:05.123Z")
            .unwrap()
            .with_timezone(&chrono::Utc);

        let mut spec = make_spec("default", "app", "uid-1");
        stamp_creation_time(&mut spec, now);
        assert_eq!(
            spec.metadata.creation_timestamp.as_deref(),
            Some("2026-10-19T14:30:05Z")
        );

        spec.metadata.creation_timestamp = Some("garbage".into());
        stamp_creation_time(&mut spec, now);
        assert_eq!(
            spec.metadata.creation_timestamp.as_deref(),
            Some("2026-10-19T14:30:05Z")
        );

        spec.metadata.creation_timestamp = Some("2026-01-01T00:00:00Z".into());
        stamp_creation_time(&mut spec, now);
        assert_eq!(
            spec.metadata.creation_timestamp.as_deref(),
            Some("2026-01-01T00:00:00Z"),
            "the API server's value is kept"
        );
    }

    // -- get_capacity / get_node_conditions / get_node_addresses --

    #[test]
    fn get_capacity_from_config() {
        let logic = make_logic();
        let cap = logic.get_capacity();
        assert_eq!(cap.cpu, "100");
        assert_eq!(cap.memory, "100Gi");
        assert_eq!(cap.pods, "100");
    }

    #[test]
    fn get_node_conditions_returns_three() {
        let logic = make_logic();
        let conditions = logic.get_node_conditions();
        assert_eq!(conditions.len(), 3);
        assert_eq!(conditions[0].condition_type, "Ready");
        assert_eq!(conditions[0].status, "True");
        assert_eq!(conditions[1].condition_type, "MemoryPressure");
        assert_eq!(conditions[1].status, "False");
        assert_eq!(conditions[2].condition_type, "DiskPressure");
        assert_eq!(conditions[2].status, "False");
        // Timestamps should be populated
        assert!(!conditions[0].last_heartbeat_time.is_empty());
    }

    #[test]
    fn get_node_addresses_from_config() {
        let logic = make_logic();
        let addrs = logic.get_node_addresses();
        assert_eq!(addrs.len(), 1);
        assert_eq!(addrs[0].address_type, "InternalIP");
        assert_eq!(addrs[0].address, "127.0.0.1");
    }

    // -- async tests with real executor --

    mod async_tests {
        use super::*;
        use crate::server::config::POD_WORK_SUBDIR;
        use crate::testutils::shell_cmd;
        #[cfg(windows)]
        const SLEEP_60: &str = "ping -n 61 127.0.0.1 > NUL";
        #[cfg(not(windows))]
        const SLEEP_60: &str = "sleep 60";

        fn make_pod_spec(name: &str, cmd: &str) -> PodSpec {
            PodSpec {
                metadata: PodMetadata {
                    namespace: "default".to_string(),
                    name: name.to_string(),
                    uid: format!("uid-{}", name),
                    ..Default::default()
                },
                spec: PodSpecBody {
                    containers: vec![Container {
                        name: "main".to_string(),
                        command: shell_cmd(cmd),
                        ..Default::default()
                    }],
                    ..Default::default()
                },
                ..Default::default()
            }
        }

        fn make_pod_spec_ns(namespace: &str, name: &str, cmd: &str) -> PodSpec {
            let mut spec = make_pod_spec(name, cmd);
            spec.metadata.namespace = namespace.to_string();
            spec
        }

        fn make_pod_spec_uid(name: &str, uid: &str, cmd: &str) -> PodSpec {
            let mut spec = make_pod_spec(name, cmd);
            spec.metadata.uid = uid.to_string();
            spec
        }

        fn make_logic_with_store() -> (PodLogic, Arc<MemoryStore>, watch::Sender<()>) {
            let store = Arc::new(MemoryStore::new());
            let provider = make_provider();
            let (shutdown_tx, _) = watch::channel(());
            let logic = PodLogic::new(
                LogicConfig::default(),
                test_cleanup_config(),
                Arc::clone(&store) as Arc<dyn PodStore>,
                provider,
                ResolverSet::default(),
                shutdown_tx.clone(),
            );
            (logic, store, shutdown_tx)
        }

        /// Wait until a pod reaches a specific phase within `BUDGET`. A pod
        /// that ends in another terminal phase fails at once.
        ///
        /// Polling goes on to `PATIENCE`, past the budget, so a failure
        /// tells a slow pod (reached the phase, only late) from a stuck one
        /// (never reached it). It reports what the polling saw: each phase
        /// and when it was first seen, the slowest `get_pod`, the latest
        /// wakeup of the poll sleep (a starved runtime wakes late), and the
        /// last status.
        async fn wait_for_phase(logic: &PodLogic, name: &str, target: PodPhase) {
            const POLL: Duration = Duration::from_millis(50);
            const BUDGET: Duration = Duration::from_secs(10);
            const PATIENCE: Duration = Duration::from_secs(120);
            let started = std::time::Instant::now();
            let mut seen: Vec<(u128, PodPhase)> = Vec::new();
            let mut polls = 0u32;
            let mut slowest_get = Duration::ZERO;
            let mut latest_wakeup = Duration::ZERO;
            let mut last_error = None;
            let mut last = None;
            let result = tokio::time::timeout(PATIENCE, async {
                loop {
                    let slept = std::time::Instant::now();
                    tokio::time::sleep(POLL).await;
                    latest_wakeup = latest_wakeup.max(slept.elapsed().saturating_sub(POLL));
                    polls += 1;
                    let asked = std::time::Instant::now();
                    let snap = logic.get_pod("default", name, SpecView::Running).await;
                    slowest_get = slowest_get.max(asked.elapsed());
                    match snap {
                        Ok(snap) => {
                            let phase = snap.status.phase;
                            if seen.last().map(|(_, p)| *p) != Some(phase) {
                                seen.push((started.elapsed().as_millis(), phase));
                            }
                            if phase == target {
                                return true;
                            }
                            last = Some(snap.status);
                            if phase.is_terminal() {
                                return false;
                            }
                        }
                        Err(e) => last_error = Some(e.to_string()),
                    }
                }
            })
            .await;
            let elapsed = started.elapsed();
            let outcome = match result {
                Ok(true) if elapsed <= BUDGET => return,
                Ok(true) => format!("reached {target:?} only beyond the {BUDGET:?} budget"),
                Ok(false) => format!("ended before reaching {target:?}"),
                Err(_) => format!("never reached {target:?} within {PATIENCE:?}"),
            };
            panic!(
                "Pod '{name}' {outcome}, after {} ms and {polls} polls\n\
                 phases first seen at (ms, phase): {seen:?}\n\
                 slowest get_pod: {slowest_get:?}; latest poll wakeup: {latest_wakeup:?} late\n\
                 last get_pod error: {last_error:?}\n\
                 last status: {last:#?}",
                elapsed.as_millis(),
            );
        }

        /// Wait until a pod reaches any terminal phase, with timeout.
        async fn wait_for_terminal(logic: &PodLogic, name: &str) -> PodPhase {
            let result = tokio::time::timeout(Duration::from_secs(10), async {
                loop {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    match logic.get_pod("default", name, SpecView::Running).await {
                        Ok(snap) if snap.status.phase.is_terminal() => {
                            return snap.status.phase;
                        }
                        Err(_) => {
                            // Pod was removed from store (deleted pod completed)
                            return PodPhase::Failed;
                        }
                        _ => {}
                    }
                }
            })
            .await;
            result.unwrap_or_else(|_| panic!("Timed out waiting for pod '{}' to terminate", name))
        }

        /// Wait until a pod is removed from the store, with timeout.
        async fn wait_for_removal(store: &MemoryStore, name: &str) {
            let id = PodId::new("default", name, format!("uid-{}", name));
            let result = tokio::time::timeout(Duration::from_secs(10), async {
                loop {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    if !store.contains(&id).await {
                        return;
                    }
                }
            })
            .await;
            result.unwrap_or_else(|_| panic!("Timed out waiting for pod '{}' to be removed", name));
        }

        // -- create_pod --

        #[tokio::test]
        async fn create_pod_returns_pending_snapshot() {
            let (logic, _, _) = make_logic_with_store();

            let snap = logic
                .create_pod(make_pod_spec("app", "exit 0"))
                .await
                .unwrap();
            assert_eq!(snap.id.name, "app");
            assert_eq!(snap.id.namespace, "default");
            assert_eq!(snap.status.phase, PodPhase::Pending);
            assert!(snap.status.is_condition_true(CONDITION_POD_SCHEDULED));
            assert!(snap.status.is_condition_true(CONDITION_INITIALIZED));
            assert!(!snap.status.is_condition_true(CONDITION_CONTAINERS_READY));
            assert!(!snap.status.start_time.is_empty());
            assert_eq!(snap.status.container_statuses.len(), 1);
            assert_eq!(snap.status.container_statuses[0].name, "main");
        }

        #[tokio::test]
        async fn create_pod_stores_both_spec_variants() {
            let (logic, _, _) = make_logic_with_store();
            logic
                .create_pod(make_pod_spec("app", "sleep 60"))
                .await
                .unwrap();

            let original = logic
                .get_pod("default", "app", SpecView::Original)
                .await
                .unwrap();
            let running = logic
                .get_pod("default", "app", SpecView::Running)
                .await
                .unwrap();

            // Identity prepare_spec means both are equivalent
            assert_eq!(original.spec.metadata.name, "app");
            assert_eq!(running.spec.metadata.name, "app");
        }

        #[tokio::test]
        async fn create_pod_same_uid_is_idempotent() {
            let (logic, store, _) = make_logic_with_store();
            // Same (namespace, name, uid) redelivered — a resync, not a
            // second incarnation. The second create returns the current
            // snapshot without starting another run.
            let first = logic
                .create_pod(make_pod_spec("app", "sleep 60"))
                .await
                .unwrap();
            let second = logic
                .create_pod(make_pod_spec("app", "sleep 60"))
                .await
                .unwrap();
            assert_eq!(first.id, second.id);
            assert_eq!(store.len().await, 1);
        }

        #[tokio::test]
        async fn create_pod_same_name_new_uid_rejected_when_live() {
            let (logic, _, _) = make_logic_with_store();
            logic
                .create_pod(make_pod_spec("app", "sleep 60"))
                .await
                .unwrap();

            // Same name, different uid, old pod still live → reject
            // (retryable). The old pod is terminated so a later retry can
            // proceed once it has been reaped.
            let err = logic
                .create_pod(make_pod_spec_uid("app", "uid-app-2", "sleep 60"))
                .await
                .unwrap_err();
            assert!(matches!(err, LogicError::Conflict { .. }));
        }

        #[tokio::test]
        async fn create_pod_evicts_stale_terminal_same_name() {
            let (logic, store, _) = make_logic_with_store();
            // First incarnation runs to completion but is never deleted.
            logic
                .create_pod(make_pod_spec("app", "exit 0"))
                .await
                .unwrap();
            wait_for_terminal(&logic, "app").await;
            tokio::time::sleep(Duration::from_millis(100)).await;

            // New incarnation, same name, different uid: the stale
            // terminal entry is evicted and the new pod starts.
            let snap = logic
                .create_pod(make_pod_spec_uid("app", "uid-app-2", "sleep 60"))
                .await
                .unwrap();
            assert_eq!(snap.id.uid, "uid-app-2");
            assert_eq!(snap.status.phase, PodPhase::Pending);

            // Only the new incarnation remains under this name.
            let got = logic
                .get_pod("default", "app", SpecView::Running)
                .await
                .unwrap();
            assert_eq!(got.id.uid, "uid-app-2");
            assert_eq!(store.len().await, 1);
        }

        #[tokio::test]
        async fn create_pod_capacity_exceeded() {
            let store = Arc::new(MemoryStore::new());
            let (shutdown_tx, _) = watch::channel(());
            let config = LogicConfig {
                max_pods: 1,
                ..Default::default()
            };
            let provider = make_provider();
            let logic = PodLogic::new(
                config,
                test_cleanup_config(),
                Arc::clone(&store) as Arc<dyn PodStore>,
                provider,
                ResolverSet::default(),
                shutdown_tx,
            );

            logic
                .create_pod(make_pod_spec("first", "sleep 60"))
                .await
                .unwrap();
            let err = logic
                .create_pod(make_pod_spec("second", "sleep 60"))
                .await
                .unwrap_err();
            assert!(matches!(err, LogicError::CapacityExceeded { max: 1 }));
        }

        #[tokio::test]
        async fn terminal_pods_do_not_count_against_capacity() {
            let store = Arc::new(MemoryStore::new());
            let (shutdown_tx, _) = watch::channel(());
            let config = LogicConfig {
                max_pods: 1,
                ..Default::default()
            };
            let provider = make_provider();
            let logic = PodLogic::new(
                config,
                test_cleanup_config(),
                Arc::clone(&store) as Arc<dyn PodStore>,
                provider,
                ResolverSet::default(),
                shutdown_tx,
            );

            // First pod runs to completion and is never deleted, so it lingers
            // in the store in a terminal phase (as it would between completion
            // and the control plane's delete).
            logic
                .create_pod(make_pod_spec("first", "exit 0"))
                .await
                .unwrap();
            wait_for_terminal(&logic, "first").await;

            // With max_pods=1 the old store.len() gate would reject this. A
            // terminal pod holds no resources, so the second pod is admitted.
            let snap = logic
                .create_pod(make_pod_spec("second", "sleep 60"))
                .await
                .unwrap();
            assert_eq!(snap.status.phase, PodPhase::Pending);

            // The terminal pod is still in the store — it was skipped by the
            // capacity count, not removed.
            assert_eq!(store.len().await, 2);
            assert_eq!(store.count_non_terminal().await, 1);
        }

        #[tokio::test]
        async fn list_pod_statuses_carries_uid() {
            let (logic, _, _) = make_logic_with_store();
            logic
                .create_pod(make_pod_spec("app", "sleep 60"))
                .await
                .unwrap();

            // The uid must be present in the bulk status feed: the VK sync
            // loop relies on it to distinguish a stale incarnation from the
            // current one under a reused name.
            let entries = logic.list_pod_statuses().await;
            assert_eq!(entries.len(), 1);
            assert_eq!(entries[0].name, "app");
            assert_eq!(entries[0].uid, "uid-app");
        }

        // -- monitor: status transitions --

        #[tokio::test]
        async fn monitor_transitions_to_running() {
            let (logic, _, _) = make_logic_with_store();
            logic
                .create_pod(make_pod_spec("app", "sleep 60"))
                .await
                .unwrap();

            wait_for_phase(&logic, "app", PodPhase::Running).await;

            let snap = logic
                .get_pod("default", "app", SpecView::Running)
                .await
                .unwrap();
            assert_eq!(snap.status.phase, PodPhase::Running);
        }

        #[tokio::test]
        async fn monitor_transitions_to_succeeded() {
            let (logic, _, _) = make_logic_with_store();
            logic
                .create_pod(make_pod_spec("app", "exit 0"))
                .await
                .unwrap();

            let phase = wait_for_terminal(&logic, "app").await;
            assert_eq!(phase, PodPhase::Succeeded);
        }

        #[tokio::test]
        async fn monitor_transitions_to_failed() {
            let (logic, _, _) = make_logic_with_store();
            logic
                .create_pod(make_pod_spec("app", "exit 1"))
                .await
                .unwrap();

            let phase = wait_for_terminal(&logic, "app").await;
            assert_eq!(phase, PodPhase::Failed);
        }

        #[tokio::test]
        async fn monitor_keeps_runtime_after_natural_completion() {
            let (logic, _, _) = make_logic_with_store();
            logic
                .create_pod(make_pod_spec("app", "exit 0"))
                .await
                .unwrap();
            let id = PodId::new("default", "app", "uid-app");

            // Runtime exists while running
            assert!(logic.runtimes.read().unwrap().contains_key(&id));

            wait_for_terminal(&logic, "app").await;
            tokio::time::sleep(Duration::from_millis(100)).await;

            // Runtime survives natural completion (logs still accessible)
            assert!(logic.runtimes.read().unwrap().contains_key(&id));

            // delete_pod cleans it up
            logic.delete_pod("default", "app", None).await.unwrap();
            assert!(!logic.runtimes.read().unwrap().contains_key(&id));
        }

        // -- monitor: data dir reaping --

        /// Wait until `path` disappears, with timeout. Returns whether it did.
        // Matches wait_for_presence: this waits on a directory tree being
        // removed, which on a loaded Windows box is slower still than a file
        // appearing.
        async fn wait_for_absence(path: &std::path::Path) -> bool {
            tokio::time::timeout(Duration::from_secs(30), async {
                loop {
                    if !path.exists() {
                        return;
                    }
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            })
            .await
            .is_ok()
        }

        /// Wait until `path` appears, with timeout. Returns whether it did.
        // Longer than the phase waits: this one polls the filesystem rather
        // than in-memory state, and a loaded Windows box running the whole
        // suite in parallel has been seen to take more than ten seconds to
        // make the marker visible.
        async fn wait_for_presence(path: &std::path::Path) -> bool {
            tokio::time::timeout(Duration::from_secs(30), async {
                loop {
                    if path.exists() {
                        return;
                    }
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            })
            .await
            .is_ok()
        }

        #[tokio::test]
        async fn incarnations_of_a_name_get_their_own_state_dirs() {
            let (logic, _, _) = make_logic_with_store();
            let layout = logic.executor_provider.layout().clone();

            let mut first = make_pod_spec_uid("app", "uid-1", "echo first");
            first.metadata.creation_timestamp = Some("2026-10-19T14:00:00Z".into());
            let first_state = layout.state_dir(&first);
            logic.create_pod(first).await.unwrap();
            assert_eq!(wait_for_terminal(&logic, "app").await, PodPhase::Succeeded);
            logic.delete_pod("default", "app", None).await.unwrap();

            let mut second = make_pod_spec_uid("app", "uid-2", "echo second");
            second.metadata.creation_timestamp = Some("2026-10-19T15:00:00Z".into());
            let second_state = layout.state_dir(&second);
            logic.create_pod(second).await.unwrap();
            assert_eq!(wait_for_terminal(&logic, "app").await, PodPhase::Succeeded);

            assert_eq!(first_state.parent(), second_state.parent());
            assert_eq!(first_state.file_name().unwrap(), "20261019T140000Z_app");
            assert_eq!(second_state.file_name().unwrap(), "20261019T150000Z_app");
            for (dir, out) in [(&first_state, "first"), (&second_state, "second")] {
                let log = dir
                    .join(crate::server::config::POD_LOG_SUBDIR)
                    .join("main")
                    .join("0.log");
                assert!(wait_for_presence(&log).await, "{}", log.display());
                let content = std::fs::read_to_string(&log).unwrap();
                assert!(content.contains(out), "{}: {content}", log.display());
            }
            assert!(
                first_state.join(DELETE_ON_FILE).exists(),
                "the deleted incarnation's delete clock runs"
            );
            assert!(
                !second_state.join(DELETE_ON_FILE).exists(),
                "the live incarnation's does not"
            );
        }

        #[tokio::test]
        async fn a_pod_without_a_creation_time_is_stamped_at_admission() {
            let (logic, _, _) = make_logic_with_store();
            let spec = make_pod_spec("app", "sleep 60");
            assert!(spec.metadata.creation_timestamp.is_none());

            let snap = logic.create_pod(spec).await.unwrap();

            let stamp = snap
                .spec
                .metadata
                .incarnation_stamp()
                .expect("stamped at admission");
            assert!(
                logic
                    .executor_provider
                    .layout()
                    .state_dir_at("app", &format!("{stamp}_app"))
                    .join(CLEANUP_TTLS_FILE)
                    .exists(),
                "the incarnation dir is named by the stamp"
            );
        }

        #[tokio::test]
        async fn monitor_reaps_data_dirs_of_succeeded_pod() {
            let (logic, _, _) = make_logic_with_store();
            logic
                .create_pod(make_pod_spec("app", "exit 0"))
                .await
                .unwrap();
            let id = PodId::new("default", "app", "uid-app");

            assert_eq!(wait_for_terminal(&logic, "app").await, PodPhase::Succeeded);

            // Default reap-on-success is 0s, and the inline pass runs after
            // the orchestrator's post-completion tasks, so poll instead of
            // assuming it lands immediately.
            let data_dir = logic.executor_provider.work_dir(&id);
            let state_dir = only_state_dir(logic.executor_provider.as_ref(), &id.name);
            assert!(
                wait_for_absence(&data_dir).await,
                "the data dir should be reaped as soon as the pod succeeds",
            );
            assert!(
                only_state_dir(logic.executor_provider.as_ref(), &id.name)
                    .join(crate::server::config::POD_LOG_SUBDIR)
                    .exists(),
                "container logs survive the reap — they are the record",
            );
            assert!(
                state_dir.join(SUCCEEDED_MARKER).exists(),
                "the state dir survives the reap",
            );
        }

        #[tokio::test]
        async fn monitor_keeps_data_dirs_of_failed_pod() {
            let (logic, _, _) = make_logic_with_store();
            logic
                .create_pod(make_pod_spec("app", "exit 1"))
                .await
                .unwrap();
            let id = PodId::new("default", "app", "uid-app");

            assert_eq!(wait_for_terminal(&logic, "app").await, PodPhase::Failed);

            // The marker is written immediately before the inline reap pass —
            // once it is there, a reap would already have happened.
            let pod_dir = logic.executor_provider.work_dir(&id);
            assert!(wait_for_presence(&pod_dir.join(FAILED_MARKER)).await);
            assert!(
                pod_dir.join(POD_WORK_SUBDIR).exists(),
                "the default 1h reap-on-error keeps work/ for debugging",
            );
        }

        #[tokio::test]
        async fn reap_ttl_annotation_defers_reaping_a_succeeded_pod() {
            let (logic, _, _) = make_logic_with_store();
            let mut spec = make_pod_spec("app", "exit 0");
            spec.metadata.annotations.insert(
                ANN_POD_DATA_REAP_TTL_ON_POD_SUCCESS.to_string(),
                "1h".to_string(),
            );
            logic.create_pod(spec).await.unwrap();
            let id = PodId::new("default", "app", "uid-app");

            assert_eq!(wait_for_terminal(&logic, "app").await, PodPhase::Succeeded);

            let pod_dir = logic.executor_provider.work_dir(&id);
            assert!(wait_for_presence(&pod_dir.join(SUCCEEDED_MARKER)).await);
            assert!(
                pod_dir.join(POD_WORK_SUBDIR).exists(),
                "the annotation defers the reap by an hour",
            );
            let reap_on = reaper::read_deadline(&pod_dir, REAP_ON_FILE).expect("reap deadline");
            assert!(!reap_on.is_due(chrono::Utc::now()), "{reap_on:?}");
        }

        #[tokio::test]
        async fn never_reap_ttl_keeps_data_dirs_indefinitely() {
            let (logic, _, _) = make_logic_with_store();
            let mut spec = make_pod_spec("app", "exit 0");
            spec.metadata.annotations.insert(
                ANN_POD_DATA_REAP_TTL_ON_POD_SUCCESS.to_string(),
                TTL_NEVER.to_string(),
            );
            logic.create_pod(spec).await.unwrap();
            let id = PodId::new("default", "app", "uid-app");

            assert_eq!(wait_for_terminal(&logic, "app").await, PodPhase::Succeeded);

            let pod_dir = logic.executor_provider.work_dir(&id);
            assert!(wait_for_presence(&pod_dir.join(SUCCEEDED_MARKER)).await);
            assert_eq!(
                reaper::read_deadline(&pod_dir, REAP_ON_FILE),
                Some(reaper::Deadline::Never),
            );
            assert!(pod_dir.join(POD_WORK_SUBDIR).exists(), "never means never");
        }

        // -- cleanup TTLs --

        #[tokio::test]
        async fn cleanup_ttls_file_written_at_pod_start() {
            let (logic, _, _) = make_logic_with_store();
            logic
                .create_pod(make_pod_spec("app", "sleep 60"))
                .await
                .unwrap();
            let id = PodId::new("default", "app", "uid-app");

            // Written at admission, before the pod is prepared, so it is
            // there while the pod still runs — a pod that dies in
            // preparation has a policy too. Each half goes in the dir it
            // governs.
            let reap: ttl::ReapTtls =
                ttl::read_cleanup_ttls(&logic.executor_provider.work_dir(&id))
                    .expect("reap TTLs readable");
            assert_eq!(
                reap.reap_ttl_on_success.raw,
                DEFAULT_POD_DATA_REAP_TTL_ON_POD_SUCCESS
            );
            assert_eq!(reap.reap_ttl_on_success.source, ttl::TtlSource::Config);
            assert!(!reap.written_at.is_empty());

            let delete: ttl::DeleteTtls =
                ttl::read_cleanup_ttls(&only_state_dir(logic.executor_provider.as_ref(), &id.name))
                    .expect("delete TTLs readable");
            assert_eq!(
                delete.delete_ttl_on_failure.raw,
                DEFAULT_POD_DIR_DELETE_TTL_POD_FAILURE_ON_POD_DELETE
            );
        }

        #[tokio::test]
        async fn cleanup_ttls_file_records_annotation_override() {
            let (logic, _, _) = make_logic_with_store();
            let mut spec = make_pod_spec("app", "exit 0");
            spec.metadata.annotations.insert(
                ANN_POD_DATA_REAP_TTL_ON_POD_FAILURE.to_string(),
                "45m".to_string(),
            );
            logic.create_pod(spec).await.unwrap();
            let id = PodId::new("default", "app", "uid-app");

            let ttls: ttl::ReapTtls =
                ttl::read_cleanup_ttls(&logic.executor_provider.work_dir(&id))
                    .expect("cleanup TTLs must record the annotated value");
            assert_eq!(ttls.reap_ttl_on_failure.raw, "45m");
            assert_eq!(ttls.reap_ttl_on_failure.secs, Some(2700));
            assert_eq!(ttls.reap_ttl_on_failure.source, ttl::TtlSource::Annotation);
            // Only the annotated value is overridden.
            assert_eq!(ttls.reap_ttl_on_success.source, ttl::TtlSource::Config);
        }

        // -- finalization: every terminal path stamps the pod dir --

        #[tokio::test]
        async fn succeeded_pod_is_finalized_as_succeeded() {
            let (logic, _, _) = make_logic_with_store();
            logic
                .create_pod(make_pod_spec("app", "exit 0"))
                .await
                .unwrap();
            let id = PodId::new("default", "app", "uid-app");
            assert_eq!(wait_for_terminal(&logic, "app").await, PodPhase::Succeeded);

            let state_dir = only_state_dir(logic.executor_provider.as_ref(), &id.name);
            assert!(wait_for_presence(&state_dir.join(SUCCEEDED_MARKER)).await);
            assert!(!state_dir.join(FAILED_MARKER).exists());
            // Default reap-on-success is 0s, so the inline pass removes the
            // data dir at completion.
            assert!(wait_for_absence(&logic.executor_provider.work_dir(&id)).await);
            assert!(
                !state_dir.join(DELETE_ON_FILE).exists(),
                "the delete clock starts when the pod leaves the runner",
            );
        }

        #[tokio::test]
        async fn failed_pod_is_finalized_as_failed() {
            let (logic, _, _) = make_logic_with_store();
            logic
                .create_pod(make_pod_spec("app", "exit 1"))
                .await
                .unwrap();
            let id = PodId::new("default", "app", "uid-app");
            assert_eq!(wait_for_terminal(&logic, "app").await, PodPhase::Failed);

            let pod_dir = logic.executor_provider.work_dir(&id);
            assert!(wait_for_presence(&pod_dir.join(FAILED_MARKER)).await);
            assert!(!pod_dir.join(SUCCEEDED_MARKER).exists());
            assert!(
                only_state_dir(logic.executor_provider.as_ref(), &id.name)
                    .join(FAILED_MARKER)
                    .exists(),
                "the state dir is marked too",
            );
            // Default reap-on-error is 1h out.
            let reap_on = reaper::read_deadline(&pod_dir, REAP_ON_FILE).expect("reap deadline");
            assert!(!reap_on.is_due(chrono::Utc::now()), "{reap_on:?}");
        }

        #[tokio::test]
        async fn pod_that_fails_preparation_is_finalized() {
            let secret_dir = temp_dir("finalize-prep-failure");
            let (logic, _, _) = make_logic_with_secret_dir(secret_dir);

            // Never-appearing secret with a 1s deadline: the pod fails
            // before any container runs, so nothing ever creates log/.
            logic
                .create_pod(pod_with_secret_ref("never", "missing-secret", Some(1)))
                .await
                .unwrap();
            let id = PodId::new("default", "never", "uid-never");
            assert_eq!(wait_for_terminal(&logic, "never").await, PodPhase::Failed);

            let pod_dir = logic.executor_provider.work_dir(&id);
            assert!(
                wait_for_presence(&pod_dir.join(FAILED_MARKER)).await,
                "a pod that dies in preparation still gets a retention policy",
            );
            assert!(reaper::read_deadline(&pod_dir, REAP_ON_FILE).is_some());
            assert!(
                !only_state_dir(logic.executor_provider.as_ref(), &id.name)
                    .join(crate::server::config::POD_LOG_SUBDIR)
                    .exists(),
                "no container ran, so there is no log dir — the store is the record",
            );
        }

        #[tokio::test]
        async fn pod_deleted_while_preparing_is_finalized() {
            let secret_dir = temp_dir("finalize-delete-preparing");
            let (logic, _, _) = make_logic_with_secret_dir(secret_dir);

            // No deadline: the preparer waits on the missing secret until
            // the delete cancels it.
            logic
                .create_pod(pod_with_secret_ref("stuck", "missing-secret", None))
                .await
                .unwrap();
            let id = PodId::new("default", "stuck", "uid-stuck");
            let pod_dir = logic.executor_provider.work_dir(&id);
            // The policy is written at admission, before the preparer starts.
            assert!(pod_dir.join(CLEANUP_TTLS_FILE).exists());

            logic.delete_pod("default", "stuck", None).await.unwrap();

            assert!(
                wait_for_presence(&pod_dir.join(FAILED_MARKER)).await,
                "a dir left unmarked would never be looked at by the reaper",
            );
            assert!(reaper::read_deadline(&pod_dir, REAP_ON_FILE).is_some());
        }

        // -- get_pod --

        #[tokio::test]
        async fn get_pod_both_views() {
            let (logic, _, _) = make_logic_with_store();
            logic
                .create_pod(make_pod_spec("app", "sleep 60"))
                .await
                .unwrap();

            let running = logic
                .get_pod("default", "app", SpecView::Running)
                .await
                .unwrap();
            let original = logic
                .get_pod("default", "app", SpecView::Original)
                .await
                .unwrap();

            assert_eq!(running.id, original.id);
            assert_eq!(running.status.phase, original.status.phase);
        }

        #[tokio::test]
        async fn get_pod_status_returns_current() {
            let (logic, _, _) = make_logic_with_store();
            logic
                .create_pod(make_pod_spec("app", "sleep 60"))
                .await
                .unwrap();

            let status = logic.get_pod_status("default", "app").await.unwrap();
            assert_eq!(status.phase, PodPhase::Pending);

            wait_for_phase(&logic, "app", PodPhase::Running).await;

            let status = logic.get_pod_status("default", "app").await.unwrap();
            assert_eq!(status.phase, PodPhase::Running);
        }

        // -- list_pods --

        #[tokio::test]
        async fn list_pods_multiple() {
            let (logic, _, _) = make_logic_with_store();
            logic
                .create_pod(make_pod_spec("a", "sleep 60"))
                .await
                .unwrap();
            logic
                .create_pod(make_pod_spec("b", "sleep 60"))
                .await
                .unwrap();
            logic
                .create_pod(make_pod_spec("c", "sleep 60"))
                .await
                .unwrap();

            assert_eq!(logic.list_pods(SpecView::Running).await.len(), 3);
            assert_eq!(logic.list_pods(SpecView::Original).await.len(), 3);
        }

        #[tokio::test]
        async fn list_pods_in_namespace_filters() {
            let (logic, _, _) = make_logic_with_store();
            logic
                .create_pod(make_pod_spec_ns("ns1", "a", "sleep 60"))
                .await
                .unwrap();
            logic
                .create_pod(make_pod_spec_ns("ns1", "b", "sleep 60"))
                .await
                .unwrap();
            logic
                .create_pod(make_pod_spec_ns("ns2", "c", "sleep 60"))
                .await
                .unwrap();

            assert_eq!(
                logic
                    .list_pods_in_namespace("ns1", SpecView::Running)
                    .await
                    .len(),
                2
            );
            assert_eq!(
                logic
                    .list_pods_in_namespace("ns2", SpecView::Running)
                    .await
                    .len(),
                1
            );
            assert_eq!(
                logic
                    .list_pods_in_namespace("ns3", SpecView::Running)
                    .await
                    .len(),
                0
            );
        }

        // -- delete_pod --

        #[tokio::test]
        async fn delete_not_found_is_ok() {
            let (logic, _, _) = make_logic_with_store();
            assert!(logic.delete_pod("default", "nope", None).await.is_ok());
        }

        #[tokio::test]
        async fn delete_terminal_pod_removes_from_store() {
            let (logic, store, _) = make_logic_with_store();
            logic
                .create_pod(make_pod_spec("app", "exit 0"))
                .await
                .unwrap();

            wait_for_terminal(&logic, "app").await;
            assert_eq!(store.len().await, 1);

            logic.delete_pod("default", "app", None).await.unwrap();
            assert_eq!(store.len().await, 0);
            assert!(
                logic
                    .get_pod("default", "app", SpecView::Running)
                    .await
                    .is_err()
            );
        }

        #[tokio::test]
        async fn delete_running_pod_kills_and_removes() {
            let (logic, store, _) = make_logic_with_store();
            logic
                .create_pod(make_pod_spec("app", "sleep 60"))
                .await
                .unwrap();

            wait_for_phase(&logic, "app", PodPhase::Running).await;

            logic
                .delete_pod("default", "app", Some(Duration::from_secs(1)))
                .await
                .unwrap();

            // Pod is removed from store after completion
            wait_for_removal(&store, "app").await;
            assert_eq!(store.len().await, 0);
        }

        #[tokio::test]
        async fn delete_uses_default_grace_period() {
            let store = Arc::new(MemoryStore::new());
            let (shutdown_tx, _) = watch::channel(());
            let config = LogicConfig {
                default_grace_period: Duration::from_secs(1),
                ..Default::default()
            };
            let provider = make_provider();
            let logic = PodLogic::new(
                config,
                test_cleanup_config(),
                Arc::clone(&store) as Arc<dyn PodStore>,
                provider,
                ResolverSet::default(),
                shutdown_tx,
            );
            logic
                .create_pod(make_pod_spec("app", "sleep 60"))
                .await
                .unwrap();

            wait_for_phase(&logic, "app", PodPhase::Running).await;

            // Delete with None uses default_grace_period
            logic.delete_pod("default", "app", None).await.unwrap();
            wait_for_removal(&store, "app").await;
        }

        // A container that ignores SIGTERM and only dies on SIGKILL — so the
        // time to terminate (and thus removal) tracks the grace the kill
        // path used. `wait_for_removal`'s 10s timeout is the discriminator:
        // it only completes if the *short* grace was applied.
        const SIGTERM_PROOF: &str = "trap '' TERM; while true; do sleep 0.2; done";

        fn pod_spec_grace(name: &str, cmd: &str, grace_secs: Option<u64>) -> PodSpec {
            let mut spec = make_pod_spec(name, cmd);
            spec.spec.termination_grace_period_seconds = grace_secs;
            spec
        }

        #[tokio::test]
        async fn delete_uses_pod_termination_grace_when_unspecified() {
            // Runner default grace is 30s, but the pod asks for 1s. A delete
            // with no explicit gracePeriod must use the pod's 1s — otherwise
            // the SIGTERM-ignoring container would survive ~30s and removal
            // would time out.
            let store = Arc::new(MemoryStore::new());
            let (shutdown_tx, _) = watch::channel(());
            let config = LogicConfig {
                default_grace_period: Duration::from_secs(30),
                ..Default::default()
            };
            let provider = make_provider();
            let logic = PodLogic::new(
                config,
                test_cleanup_config(),
                Arc::clone(&store) as Arc<dyn PodStore>,
                provider,
                ResolverSet::default(),
                shutdown_tx,
            );
            logic
                .create_pod(pod_spec_grace("app", SIGTERM_PROOF, Some(1)))
                .await
                .unwrap();

            wait_for_phase(&logic, "app", PodPhase::Running).await;

            logic.delete_pod("default", "app", None).await.unwrap();
            // Completes only if the pod's 1s grace (not the 30s default) was used.
            wait_for_removal(&store, "app").await;
        }

        #[tokio::test]
        async fn delete_explicit_grace_overrides_pod_termination_grace() {
            // Pod asks for 30s, but an explicit delete gracePeriod of 1s
            // must win — otherwise removal would time out.
            let (logic, store, _) = make_logic_with_store();
            logic
                .create_pod(pod_spec_grace("app", SIGTERM_PROOF, Some(30)))
                .await
                .unwrap();

            wait_for_phase(&logic, "app", PodPhase::Running).await;

            logic
                .delete_pod("default", "app", Some(Duration::from_secs(1)))
                .await
                .unwrap();
            wait_for_removal(&store, "app").await;
        }

        #[tokio::test]
        async fn delete_idempotent_disruption() {
            let (logic, _, _) = make_logic_with_store();
            logic
                .create_pod(make_pod_spec("app", "sleep 60"))
                .await
                .unwrap();

            wait_for_phase(&logic, "app", PodPhase::Running).await;

            // First delete marks disruption
            logic
                .delete_pod("default", "app", Some(Duration::from_secs(1)))
                .await
                .unwrap();
            let snap = logic
                .get_pod("default", "app", SpecView::Running)
                .await
                .unwrap();
            assert!(snap.status.is_condition_true(CONDITION_DISRUPTION_TARGET));

            // Second delete is a no-op (no error, no change)
            logic
                .delete_pod("default", "app", Some(Duration::from_secs(1)))
                .await
                .unwrap();
        }

        #[tokio::test]
        async fn delete_already_removed_is_ok() {
            let (logic, _, _) = make_logic_with_store();
            logic
                .create_pod(make_pod_spec("app", "exit 0"))
                .await
                .unwrap();

            wait_for_terminal(&logic, "app").await;

            // Delete once removes
            logic.delete_pod("default", "app", None).await.unwrap();
            // Delete again: not found, still ok
            logic.delete_pod("default", "app", None).await.unwrap();
        }

        // -- multiple pods --

        #[tokio::test]
        async fn multiple_pods_independent_lifecycle() {
            let (logic, store, _) = make_logic_with_store();

            // Create three pods: fast exit, slow, failing
            logic
                .create_pod(make_pod_spec("fast", "exit 0"))
                .await
                .unwrap();
            logic
                .create_pod(make_pod_spec("slow", SLEEP_60))
                .await
                .unwrap();
            logic
                .create_pod(make_pod_spec("fail", "exit 1"))
                .await
                .unwrap();

            assert_eq!(store.len().await, 3);

            // Fast and fail complete on their own
            let fast_phase = wait_for_terminal(&logic, "fast").await;
            assert_eq!(fast_phase, PodPhase::Succeeded);

            let fail_phase = wait_for_terminal(&logic, "fail").await;
            assert_eq!(fail_phase, PodPhase::Failed);

            // Slow is still running
            wait_for_phase(&logic, "slow", PodPhase::Running).await;

            // Kill slow — deleted pods are removed automatically
            logic
                .delete_pod("default", "slow", Some(Duration::from_secs(1)))
                .await
                .unwrap();
            wait_for_removal(&store, "slow").await;

            // Clean up terminal pods (natural completions stay in store)
            logic.delete_pod("default", "fast", None).await.unwrap();
            logic.delete_pod("default", "fail", None).await.unwrap();

            assert_eq!(store.len().await, 0);
        }

        // -- shutdown --

        #[tokio::test]
        async fn shutdown_terminates_running_pods() {
            let (logic, _, shutdown_tx) = make_logic_with_store();
            logic
                .create_pod(make_pod_spec("a", "sleep 60"))
                .await
                .unwrap();
            logic
                .create_pod(make_pod_spec("b", "sleep 60"))
                .await
                .unwrap();

            wait_for_phase(&logic, "a", PodPhase::Running).await;
            wait_for_phase(&logic, "b", PodPhase::Running).await;

            let _ = shutdown_tx.send(());

            wait_for_terminal(&logic, "a").await;
            wait_for_terminal(&logic, "b").await;
        }

        // -- container logs --

        #[tokio::test]
        async fn stream_logs_after_completion() {
            let (logic, _, _) = make_logic_with_store();
            logic
                .create_pod(make_pod_spec("logger", "echo hello-world"))
                .await
                .unwrap();

            wait_for_terminal(&logic, "logger").await;

            let mut rx = logic
                .stream_container_logs(
                    "default",
                    "logger",
                    Some("main"),
                    ContainerLogOptions::default(),
                )
                .await
                .unwrap();

            let mut output = String::new();
            while let Some(Ok(chunk)) = rx.recv().await {
                output.push_str(&String::from_utf8_lossy(&chunk));
            }
            assert!(output.contains("hello-world"), "got: {}", output);
        }

        #[tokio::test]
        async fn stream_logs_with_tail_lines() {
            let (logic, _, _) = make_logic_with_store();
            // Print 10 lines.
            #[cfg(windows)]
            const TEN_LINES: &str = "for %i in (1 2 3 4 5 6 7 8 9 10) do @echo line-%i";
            #[cfg(not(windows))]
            const TEN_LINES: &str = "for i in 1 2 3 4 5 6 7 8 9 10; do echo line-$i; done";

            logic
                .create_pod(make_pod_spec("tailer", TEN_LINES))
                .await
                .unwrap();

            wait_for_terminal(&logic, "tailer").await;

            let mut rx = logic
                .stream_container_logs(
                    "default",
                    "tailer",
                    Some("main"),
                    ContainerLogOptions {
                        tail_lines: Some(3),
                        ..Default::default()
                    },
                )
                .await
                .unwrap();

            let mut output = String::new();
            while let Some(Ok(chunk)) = rx.recv().await {
                output.push_str(&String::from_utf8_lossy(&chunk));
            }
            let lines: Vec<&str> = output.trim().lines().collect();
            assert_eq!(lines.len(), 3, "expected 3 lines, got: {:?}", lines);
            assert!(lines[0].contains("line-8"), "got: {:?}", lines);
            assert!(lines[2].contains("line-10"), "got: {:?}", lines);
        }

        #[tokio::test]
        async fn stream_logs_follow_stops_on_terminal() {
            let (logic, _, _) = make_logic_with_store();
            logic
                .create_pod(make_pod_spec("follower", "echo follow-me && sleep 1"))
                .await
                .unwrap();

            // create_pod returns while the pod is still Pending (the
            // preparer task hasn't promoted it to Running yet). Wait for
            // Running so the log handle is available before streaming.
            wait_for_phase(&logic, "follower", PodPhase::Running).await;

            // Start following from the Running state.
            let mut rx = logic
                .stream_container_logs(
                    "default",
                    "follower",
                    None,
                    ContainerLogOptions {
                        follow: true,
                        ..Default::default()
                    },
                )
                .await
                .unwrap();

            // Collect all output — should complete when pod terminates
            let result = tokio::time::timeout(Duration::from_secs(10), async {
                let mut output = String::new();
                while let Some(Ok(chunk)) = rx.recv().await {
                    output.push_str(&String::from_utf8_lossy(&chunk));
                }
                output
            })
            .await;

            let output = result.expect("follow should stop when pod terminates");
            assert!(output.contains("follow-me"), "got: {}", output);
        }

        #[tokio::test]
        async fn stream_logs_pod_not_found() {
            let (logic, _, _) = make_logic_with_store();
            let err = logic
                .stream_container_logs("default", "nope", None, ContainerLogOptions::default())
                .await
                .unwrap_err();
            assert!(matches!(err, LogicError::NotFound { .. }));
        }

        // -- Phase 4: preparer with secret/configmap waits --

        use crate::pod::resolver::secret::FileSecretResolver;
        use crate::spec::pod::{EnvVar, EnvVarSource, KeyRef};

        fn temp_dir(name: &str) -> std::path::PathBuf {
            let n = TEST_COUNTER.fetch_add(1, Ordering::Relaxed);
            let d = std::env::temp_dir().join(format!(
                "pod-runner-rs2-test-prep-{}-{}-{}",
                std::process::id(),
                name,
                n,
            ));
            let _ = std::fs::remove_dir_all(&d);
            std::fs::create_dir_all(&d).unwrap();
            d
        }

        /// Build a logic whose store writes specs and statuses into each
        /// pod dir, as the server does. Returns the provider too so tests
        /// can look at what survives on disk.
        fn make_logic_with_file_store(
            secret_dir: std::path::PathBuf,
        ) -> (PodLogic, Arc<DefaultExecutorProvider>, watch::Sender<()>) {
            use crate::store::file::{FileStore, FileStoreConfig};
            use crate::store::multiplex::MultiplexStore;

            let provider = make_provider();
            let file_store = FileStore::new(FileStoreConfig {
                layout: provider.layout.clone(),
                write_running_spec: false,
            });
            let store: Arc<dyn PodStore> = Arc::new(MultiplexStore::new(
                Arc::new(MemoryStore::new()),
                Arc::new(file_store),
            ));
            let (shutdown_tx, _) = watch::channel(());
            let resolvers = ResolverSet {
                secret_resolver: Some(Box::new(FileSecretResolver::new(secret_dir).unwrap())),
                ..Default::default()
            };
            let logic = PodLogic::new(
                LogicConfig::default(),
                test_cleanup_config(),
                store,
                Arc::clone(&provider) as Arc<dyn ExecutorProvider>,
                resolvers,
                shutdown_tx.clone(),
            );
            (logic, provider, shutdown_tx)
        }

        /// The status snapshots the file store wrote for a pod.
        fn status_files(pod_dir: &std::path::Path) -> Vec<String> {
            let status_dir = pod_dir
                .join(crate::server::config::POD_STORE_SUBDIR)
                .join("status");
            let Ok(entries) = std::fs::read_dir(&status_dir) else {
                return Vec::new();
            };
            entries
                .filter_map(|e| e.ok())
                .filter_map(|e| std::fs::read_to_string(e.path()).ok())
                .collect()
        }

        #[tokio::test]
        async fn deleting_a_prep_failed_pod_keeps_the_reason_on_disk() {
            let secret_dir = temp_dir("keep-reason");
            let (logic, provider, _tx) = make_logic_with_file_store(secret_dir);

            // Fails in preparation, so no container ever runs and there is
            // no log dir: store/ is the entire record of why.
            logic
                .create_pod(pod_with_secret_ref("never", "missing-secret", Some(1)))
                .await
                .unwrap();
            let id = PodId::new("default", "never", "uid-never");
            assert_eq!(wait_for_terminal(&logic, "never").await, PodPhase::Failed);

            let pod_dir = only_state_dir(provider.as_ref(), &id.name);
            assert!(wait_for_presence(&pod_dir.join(FAILED_MARKER)).await);

            // The control plane deletes the pod, as it does for every
            // terminal pod. That must not take the reason with it.
            logic.delete_pod("default", "never", None).await.unwrap();

            assert!(
                logic.get_pod_status("default", "never").await.is_err(),
                "the pod is gone from the API",
            );
            let statuses = status_files(&pod_dir);
            assert!(
                !statuses.is_empty(),
                "status snapshots must survive the delete",
            );
            assert!(
                statuses.iter().any(|s| s.contains("timed out")),
                "the failure reason must survive the delete, got: {statuses:?}",
            );
            assert!(
                pod_dir
                    .join(crate::server::config::POD_STORE_SUBDIR)
                    .join("original-spec.json")
                    .exists(),
                "the spec must survive the delete",
            );
        }

        #[tokio::test]
        async fn deleting_a_succeeded_pod_keeps_its_logs_until_the_delete_deadline() {
            let secret_dir = temp_dir("keep-logs");
            let (logic, provider, _tx) = make_logic_with_file_store(secret_dir);

            logic
                .create_pod(make_pod_spec("app", "echo hello"))
                .await
                .unwrap();
            let id = PodId::new("default", "app", "uid-app");
            assert_eq!(wait_for_terminal(&logic, "app").await, PodPhase::Succeeded);
            let pod_dir = only_state_dir(provider.as_ref(), &id.name);
            assert!(wait_for_presence(&pod_dir.join(SUCCEEDED_MARKER)).await);

            logic.delete_pod("default", "app", None).await.unwrap();

            // Default delete-on-success is 1h out, so the record stays.
            assert!(pod_dir.exists(), "the state dir outlives the delete");
            assert!(
                only_state_dir(provider.as_ref(), &id.name)
                    .join(crate::server::config::POD_LOG_SUBDIR)
                    .exists(),
                "container logs outlive the delete",
            );
            assert!(
                pod_dir
                    .join(crate::server::config::POD_STORE_SUBDIR)
                    .join("original-spec.json")
                    .exists(),
            );
            let delete_on =
                reaper::read_deadline(&pod_dir, DELETE_ON_FILE).expect("delete deadline");
            assert!(
                !delete_on.is_due(chrono::Utc::now()),
                "the delete clock starts at eviction: {delete_on:?}",
            );
        }

        #[tokio::test]
        async fn zero_delete_ttl_removes_the_pod_dir_at_eviction() {
            let secret_dir = temp_dir("delete-now");
            let (logic, provider, _tx) = make_logic_with_file_store(secret_dir);

            let mut spec = make_pod_spec("app", "exit 0");
            spec.metadata.annotations.insert(
                ANN_POD_DIR_DELETE_TTL_POD_SUCCESS_ON_POD_DELETE.to_string(),
                "0s".to_string(),
            );
            logic.create_pod(spec).await.unwrap();
            let id = PodId::new("default", "app", "uid-app");
            assert_eq!(wait_for_terminal(&logic, "app").await, PodPhase::Succeeded);
            let pod_dir = only_state_dir(provider.as_ref(), &id.name);
            assert!(wait_for_presence(&pod_dir.join(SUCCEEDED_MARKER)).await);

            logic.delete_pod("default", "app", None).await.unwrap();

            assert!(
                !pod_dir.exists(),
                "a 0s delete TTL is applied inline, not on the next scan",
            );
        }

        /// Build a logic with a file-backed SecretResolver rooted at
        /// `secret_dir`. Used by the preparer tests that need a real
        /// resolver wait path.
        fn make_logic_with_secret_dir(
            secret_dir: std::path::PathBuf,
        ) -> (PodLogic, Arc<MemoryStore>, watch::Sender<()>) {
            let store = Arc::new(MemoryStore::new());
            let provider = make_provider();
            let (shutdown_tx, _) = watch::channel(());
            let resolvers = ResolverSet {
                secret_resolver: Some(Box::new(FileSecretResolver::new(secret_dir).unwrap())),
                ..Default::default()
            };
            let logic = PodLogic::new(
                LogicConfig::default(),
                test_cleanup_config(),
                Arc::clone(&store) as Arc<dyn PodStore>,
                provider,
                resolvers,
                shutdown_tx.clone(),
            );
            (logic, store, shutdown_tx)
        }

        /// Pod spec that references a secret via valueFrom + a short
        /// activeDeadlineSeconds. Used by the timeout-transitions test.
        fn pod_with_secret_ref(
            name: &str,
            secret_name: &str,
            active_deadline_seconds: Option<u64>,
        ) -> PodSpec {
            PodSpec {
                metadata: PodMetadata {
                    namespace: "default".to_string(),
                    name: name.to_string(),
                    uid: format!("uid-{}", name),
                    ..Default::default()
                },
                spec: PodSpecBody {
                    containers: vec![Container {
                        name: "main".to_string(),
                        command: shell_cmd("echo ok"),
                        env: vec![EnvVar {
                            name: "PASS".to_string(),
                            value: None,
                            value_from: Some(EnvVarSource {
                                secret_key_ref: Some(KeyRef {
                                    name: secret_name.to_string(),
                                    key: "pass".to_string(),
                                    optional: None,
                                }),
                                ..Default::default()
                            }),
                        }],
                        ..Default::default()
                    }],
                    active_deadline_seconds,
                    ..Default::default()
                },
                ..Default::default()
            }
        }

        #[tokio::test]
        async fn missing_secret_with_deadline_transitions_to_failed() {
            let secret_dir = temp_dir("missing-secret-deadline");
            let (logic, _, _) = make_logic_with_secret_dir(secret_dir);

            // Reference a secret that doesn't exist. activeDeadlineSeconds=1
            // bounds the preparer wait so the test completes quickly.
            let pod = pod_with_secret_ref("never", "missing-secret", Some(1));
            let snap = logic.create_pod(pod).await.unwrap();
            // create_pod returns Pending while the preparer runs in
            // the background.
            assert_eq!(snap.status.phase, PodPhase::Pending);

            let phase = wait_for_terminal(&logic, "never").await;
            assert_eq!(phase, PodPhase::Failed);

            let status = logic.get_pod_status("default", "never").await.unwrap();
            assert!(
                status.message.contains("timed out"),
                "status message should mention timeout, got: {}",
                status.message,
            );
            let init_cond = status
                .conditions
                .iter()
                .find(|c| c.condition_type == CONDITION_INITIALIZED)
                .expect("Initialized condition should be set");
            assert_eq!(
                init_cond.message, REASON_DEADLINE_EXCEEDED,
                "Initialized condition should carry the DeadlineExceeded reason",
            );
        }

        #[cfg(windows)]
        #[tokio::test]
        async fn executable_path_too_long_for_windows_fails_preparation() {
            let (logic, _, _) = make_logic_with_store();
            let mut pod = make_pod_spec("long", "exit 0");
            pod.spec.containers[0].command = vec![format!(
                "$(KM_CONTAINER_ROOT)/{}/x.exe",
                "a".repeat(crate::pod::prepare::WINDOWS_EXECUTABLE_PATH_LIMIT)
            )];
            pod.spec.containers[0].args.clear();
            logic.create_pod(pod).await.unwrap();

            assert_eq!(wait_for_terminal(&logic, "long").await, PodPhase::Failed);
            let status = logic.get_pod_status("default", "long").await.unwrap();
            assert!(
                status.message.contains("MAX_PATH") && status.message.contains("'main'"),
                "the status message carries the full error, got: {}",
                status.message,
            );
            let init_cond = status
                .conditions
                .iter()
                .find(|c| c.condition_type == CONDITION_INITIALIZED)
                .expect("Initialized condition should be set");
            assert_eq!(init_cond.message, REASON_POD_PREPARATION_FAILED);
        }

        #[tokio::test]
        async fn shutdown_cancels_stuck_preparer() {
            let secret_dir = temp_dir("shutdown-cancels-preparer");
            let (logic, _, shutdown_tx) = make_logic_with_secret_dir(secret_dir);

            // No activeDeadlineSeconds — the resolver backoff would
            // otherwise retry forever waiting for the missing secret.
            let pod = pod_with_secret_ref("stuck", "missing-secret", None);
            logic.create_pod(pod).await.unwrap();

            // Let the preparer enter the resolver poll loop.
            tokio::time::sleep(Duration::from_millis(200)).await;

            // Shutdown: wait_for_pods fires the preparer cancels, so
            // it must return well before its own timeout.
            let _ = shutdown_tx.send(());
            let start = std::time::Instant::now();
            logic.wait_for_pods(Duration::from_secs(10)).await;
            assert!(
                start.elapsed() < Duration::from_secs(5),
                "wait_for_pods should return promptly once the preparer is cancelled, took {:?}",
                start.elapsed(),
            );
        }

        #[tokio::test]
        async fn secret_appearing_during_wait_lets_pod_run() {
            let secret_dir = temp_dir("late-secret");
            let (logic, _, _) = make_logic_with_secret_dir(secret_dir.clone());

            // Generous deadline (30s) so the secret has time to appear.
            let pod = pod_with_secret_ref("late", "late-secret", Some(30));
            logic.create_pod(pod).await.unwrap();

            // Materialize the secret after a short delay — long enough
            // that the preparer has already failed at least once and is
            // in its first backoff (500ms).
            let writer = tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(800)).await;
                std::fs::write(
                    secret_dir.join("late-secret.yaml"),
                    "stringData:\n  pass: top-secret\n",
                )
                .unwrap();
            });

            let phase = wait_for_terminal(&logic, "late").await;
            writer.await.unwrap();
            assert_eq!(phase, PodPhase::Succeeded);
        }

        #[tokio::test]
        async fn delete_during_preparation_removes_pod() {
            let secret_dir = temp_dir("delete-during-prep");
            let (logic, store, _) = make_logic_with_secret_dir(secret_dir);

            // No deadline — the preparer waits indefinitely for the
            // missing secret. delete_pod must cancel it.
            let pod = pod_with_secret_ref("waiter", "absent", None);
            logic.create_pod(pod).await.unwrap();

            // Give the preparer a moment to enter its first wait.
            tokio::time::sleep(Duration::from_millis(100)).await;

            logic
                .delete_pod("default", "waiter", Some(Duration::from_secs(1)))
                .await
                .unwrap();
            wait_for_removal(&store, "waiter").await;
            assert_eq!(store.len().await, 0);
        }

        #[tokio::test]
        async fn pod_logic_register_task_propagates_to_created_pods() {
            use std::sync::atomic::{AtomicU32, Ordering};

            use crate::pod::executor::{PodTask, PodTaskContext};

            struct Counter {
                started: Arc<AtomicU32>,
            }

            #[async_trait::async_trait]
            impl PodTask for Counter {
                async fn run(&self, mut ctx: PodTaskContext) {
                    self.started.fetch_add(1, Ordering::SeqCst);
                    // Exit promptly once the orchestrator fires kill_watch.
                    let _ = ctx.pod_kill.changed().await;
                }
            }

            let started = Arc::new(AtomicU32::new(0));
            let store = Arc::new(MemoryStore::new());
            let (shutdown_tx, _) = watch::channel(());
            let logic = PodLogic::new(
                LogicConfig::default(),
                test_cleanup_config(),
                Arc::clone(&store) as Arc<dyn PodStore>,
                make_provider(),
                ResolverSet::default(),
                shutdown_tx.clone(),
            )
            .register_task(Arc::new(Counter {
                started: Arc::clone(&started),
            }));

            // Two pods on the same PodLogic — both should pick up the
            // registered task.
            logic
                .create_pod(make_pod_spec("a", "exit 0"))
                .await
                .unwrap();
            logic
                .create_pod(make_pod_spec("b", "exit 0"))
                .await
                .unwrap();
            wait_for_terminal(&logic, "a").await;
            wait_for_terminal(&logic, "b").await;

            assert_eq!(
                started.load(Ordering::SeqCst),
                2,
                "Counter task should have run once per pod",
            );
        }

        #[tokio::test]
        async fn create_pod_rejects_optional_true() {
            let (logic, _, _) = make_logic_with_store();
            let mut pod = make_pod_spec("rejected", "exit 0");
            pod.spec.containers[0].env.push(EnvVar {
                name: "X".to_string(),
                value: None,
                value_from: Some(EnvVarSource {
                    secret_key_ref: Some(KeyRef {
                        name: "s".to_string(),
                        key: "k".to_string(),
                        optional: Some(true),
                    }),
                    ..Default::default()
                }),
            });
            let err = logic.create_pod(pod).await.unwrap_err();
            match err {
                LogicError::Admission(ref ae) => assert!(
                    ae.to_string().contains("optional"),
                    "expected admission error mentioning 'optional', got: {}",
                    ae,
                ),
                other => panic!("expected Admission error, got {:?}", other),
            }
        }

        #[tokio::test]
        async fn stream_logs_while_preparing_errors() {
            let secret_dir = temp_dir("stream-while-prep");
            let (logic, _, _) = make_logic_with_secret_dir(secret_dir);

            let pod = pod_with_secret_ref("prep", "never-appears", None);
            logic.create_pod(pod).await.unwrap();

            // Pod stays in Preparing — stream should error out cleanly
            // rather than panic.
            tokio::time::sleep(Duration::from_millis(50)).await;
            let err = logic
                .stream_container_logs("default", "prep", None, ContainerLogOptions::default())
                .await
                .unwrap_err();
            match err {
                LogicError::Internal(msg) => {
                    assert!(msg.contains("being prepared"), "got: {}", msg);
                }
                other => panic!("expected Internal error, got {:?}", other),
            }

            // Clean up so the test runtime doesn't hang on the spawned
            // preparer task.
            logic
                .delete_pod("default", "prep", Some(Duration::from_secs(1)))
                .await
                .unwrap();
        }

        // -- runner tasks ---------------------------------------------------

        use std::sync::atomic::{AtomicBool, AtomicU32};

        /// Records work_dir, signals when spawned, exits on shutdown.
        struct RunnerObserver {
            spawned: Arc<AtomicBool>,
            shutdown_observed: Arc<AtomicBool>,
            recorded_work_dir: Arc<tokio::sync::Mutex<Option<PathBuf>>>,
        }

        #[async_trait]
        impl RunnerTask for RunnerObserver {
            async fn run(&self, mut ctx: RunnerTaskContext) {
                self.spawned.store(true, Ordering::SeqCst);
                *self.recorded_work_dir.lock().await = Some(ctx.work_dir.clone());
                let _ = ctx.runner_shutdown.changed().await;
                self.shutdown_observed.store(true, Ordering::SeqCst);
            }
        }

        struct RunnerCounter {
            spawn_count: Arc<AtomicU32>,
        }

        #[async_trait]
        impl RunnerTask for RunnerCounter {
            async fn run(&self, mut ctx: RunnerTaskContext) {
                self.spawn_count.fetch_add(1, Ordering::SeqCst);
                let _ = ctx.runner_shutdown.changed().await;
            }
        }

        /// Hangs forever — used to exercise the wait_for_runner_tasks
        /// timeout path and the Drop-aborts-tasks path.
        struct RunnerHang {
            aborted: Arc<AtomicBool>,
        }

        #[async_trait]
        impl RunnerTask for RunnerHang {
            async fn run(&self, _ctx: RunnerTaskContext) {
                struct Guard(Arc<AtomicBool>);
                impl Drop for Guard {
                    fn drop(&mut self) {
                        self.0.store(true, Ordering::SeqCst);
                    }
                }
                let _g = Guard(Arc::clone(&self.aborted));
                std::future::pending::<()>().await;
            }
        }

        #[tokio::test]
        async fn runner_task_receives_work_dir_and_observes_shutdown() {
            let store = Arc::new(MemoryStore::new());
            let (shutdown_tx, _) = watch::channel(());
            let spawned = Arc::new(AtomicBool::new(false));
            let shutdown_observed = Arc::new(AtomicBool::new(false));
            let recorded = Arc::new(tokio::sync::Mutex::new(None));

            let task = Arc::new(RunnerObserver {
                spawned: Arc::clone(&spawned),
                shutdown_observed: Arc::clone(&shutdown_observed),
                recorded_work_dir: Arc::clone(&recorded),
            });

            let work_dir = PathBuf::from("/tmp/runner-test");
            let logic = PodLogic::new(
                LogicConfig::default(),
                test_cleanup_config(),
                store,
                make_provider(),
                ResolverSet::default(),
                shutdown_tx.clone(),
            )
            .runner_work_dir(work_dir.clone())
            .register_runner_task(task);

            // The task is spawned eagerly. Give it a tick to start.
            tokio::time::sleep(Duration::from_millis(50)).await;
            assert!(spawned.load(Ordering::SeqCst));
            assert_eq!(recorded.lock().await.as_ref().unwrap(), &work_dir);
            assert!(!shutdown_observed.load(Ordering::SeqCst));

            // Fire shutdown — task should observe it and exit.
            let _ = shutdown_tx.send(());
            logic.wait_for_runner_tasks(Duration::from_secs(2)).await;

            assert!(shutdown_observed.load(Ordering::SeqCst));
        }

        #[tokio::test]
        async fn multiple_runner_tasks_all_spawn() {
            let store = Arc::new(MemoryStore::new());
            let (shutdown_tx, _) = watch::channel(());
            let count = Arc::new(AtomicU32::new(0));

            let logic = PodLogic::new(
                LogicConfig::default(),
                test_cleanup_config(),
                store,
                make_provider(),
                ResolverSet::default(),
                shutdown_tx.clone(),
            )
            .register_runner_task(Arc::new(RunnerCounter {
                spawn_count: Arc::clone(&count),
            }))
            .register_runner_task(Arc::new(RunnerCounter {
                spawn_count: Arc::clone(&count),
            }))
            .register_runner_task(Arc::new(RunnerCounter {
                spawn_count: Arc::clone(&count),
            }));

            tokio::time::sleep(Duration::from_millis(100)).await;
            assert_eq!(count.load(Ordering::SeqCst), 3);

            let _ = shutdown_tx.send(());
            logic.wait_for_runner_tasks(Duration::from_secs(2)).await;
        }

        #[tokio::test]
        async fn wait_for_runner_tasks_times_out_on_hang() {
            let store = Arc::new(MemoryStore::new());
            let (shutdown_tx, _) = watch::channel(());
            let aborted = Arc::new(AtomicBool::new(false));

            let logic = PodLogic::new(
                LogicConfig::default(),
                test_cleanup_config(),
                store,
                make_provider(),
                ResolverSet::default(),
                shutdown_tx.clone(),
            )
            .register_runner_task(Arc::new(RunnerHang {
                aborted: Arc::clone(&aborted),
            }));

            tokio::time::sleep(Duration::from_millis(50)).await;
            let _ = shutdown_tx.send(());

            // wait_for_runner_tasks does NOT abort on its own — it just
            // gives up after the timeout. The task should still be
            // running afterwards.
            let start = std::time::Instant::now();
            logic
                .wait_for_runner_tasks(Duration::from_millis(200))
                .await;
            assert!(
                start.elapsed() >= Duration::from_millis(150),
                "wait should have honored the timeout",
            );
            assert!(
                !aborted.load(Ordering::SeqCst),
                "task should still be running after timeout"
            );

            // Drop logic — JoinSet drop aborts the task.
            drop(logic);
            tokio::time::sleep(Duration::from_millis(100)).await;
            assert!(
                aborted.load(Ordering::SeqCst),
                "task should be aborted on PodLogic drop"
            );
        }

        #[tokio::test]
        async fn dropping_pod_logic_aborts_runner_tasks() {
            let store = Arc::new(MemoryStore::new());
            let (shutdown_tx, _) = watch::channel(());
            let aborted = Arc::new(AtomicBool::new(false));

            let logic = PodLogic::new(
                LogicConfig::default(),
                test_cleanup_config(),
                store,
                make_provider(),
                ResolverSet::default(),
                shutdown_tx,
            )
            .register_runner_task(Arc::new(RunnerHang {
                aborted: Arc::clone(&aborted),
            }));

            tokio::time::sleep(Duration::from_millis(50)).await;
            assert!(!aborted.load(Ordering::SeqCst));

            drop(logic);
            tokio::time::sleep(Duration::from_millis(100)).await;
            assert!(aborted.load(Ordering::SeqCst));
        }

        #[tokio::test]
        async fn no_runner_tasks_wait_returns_immediately() {
            let store = Arc::new(MemoryStore::new());
            let (shutdown_tx, _) = watch::channel(());
            let logic = PodLogic::new(
                LogicConfig::default(),
                test_cleanup_config(),
                store,
                make_provider(),
                ResolverSet::default(),
                shutdown_tx,
            );

            let start = std::time::Instant::now();
            logic.wait_for_runner_tasks(Duration::from_secs(5)).await;
            assert!(
                start.elapsed() < Duration::from_millis(200),
                "wait should have returned immediately when no tasks registered",
            );
        }
    }
}
