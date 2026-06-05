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

pub mod cleanup;
pub mod purge;

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use bytes::Bytes;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tracing::{debug, debug_span, info, info_span, warn, Instrument};

pub use crate::pod::container_log::ContainerLogOptions;
use crate::metrics::Metrics;
use crate::pod::executor::{PodEventKind, PodExecutor, PodHandle, PodKillHandle, PodLogHandle};
use crate::pod::prepare::prepare_pod;
pub use crate::pod::resolver::ResolverSet;
use crate::spec::constants::*;
use crate::spec::node::{NodeAddress, NodeCapacity, NodeCondition};
use crate::spec::pod::PodSpec;
use crate::spec::status::{ContainerStatus, PodStatus};
use crate::store::{PodId, PodSnapshot, PodStore, SpecView};

pub use cleanup::CleanupConfig;

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

    /// Working directory for a pod.
    fn work_dir(&self, id: &PodId) -> PathBuf;

    /// Log directory for a pod's containers, if logging is configured.
    fn log_dir(&self, id: &PodId) -> Option<PathBuf>;
}

/// Default executor provider with configurable base directories.
///
/// Per-pod layout: `<pods_dir>/<pod_id>/{work/<container>/, log/<container>/}`.
pub struct DefaultExecutorProvider {
    /// Base directory for all pods: `<workdir>/run/pods/`.
    /// Each pod gets `<pods_dir>/<pod_full_id>/`.
    pub pods_dir: PathBuf,
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
    fn work_dir(&self, id: &PodId) -> PathBuf {
        self.pods_dir.join(id.full())
    }

    fn create(&self, pod: Arc<PodSpec>, id: &PodId) -> PodExecutor {
        let pod_dir = self.pods_dir.join(id.full());
        let log_dir = pod_dir.join(crate::server::config::POD_LOG_SUBDIR);

        // Ensure directories exist before the executor tries to use them
        if let Err(e) = std::fs::create_dir_all(&pod_dir) {
            warn!(pod = %id, error = %e, "Failed to create pod directory");
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

    fn log_dir(&self, id: &PodId) -> Option<PathBuf> {
        Some(self.pods_dir.join(id.full()).join(crate::server::config::POD_LOG_SUBDIR))
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
#[derive(Debug)]
pub enum LogicError {
    /// Pod not found by namespace + name.
    NotFound { namespace: String, name: String },
    /// A pod with this namespace + name already exists.
    Conflict { namespace: String, name: String },
    /// Store is at capacity.
    CapacityExceeded { max: usize },
    /// Internal error (executor failure, etc.).
    Internal(String),
}

impl std::fmt::Display for LogicError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotFound { namespace, name } => {
                write!(f, "pod {}/{} not found", namespace, name)
            }
            Self::Conflict { namespace, name } => {
                write!(f, "pod {}/{} already exists", namespace, name)
            }
            Self::CapacityExceeded { max } => {
                write!(f, "pod capacity exceeded (max {})", max)
            }
            Self::Internal(msg) => write!(f, "internal error: {}", msg),
        }
    }
}

impl std::error::Error for LogicError {}

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
            shutdown_tx,
        }
    }

    /// Wait for all running pod monitor tasks to complete.
    ///
    /// Call after sending the shutdown signal. Polls until every active
    /// monitor has finished (cleanup, sentinels, archive). Naturally
    /// completed pods keep their runtime entry so logs stay accessible
    /// — those have `_monitor.is_finished()` and don't block shutdown.
    /// Times out after `timeout` to avoid hanging on stuck pods.
    pub async fn wait_for_pods(&self, timeout: Duration) {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let count = self.runtimes.read().unwrap()
                .values()
                .filter(|r| !r.is_finished())
                .count();
            if count == 0 {
                info!("All pod monitors completed");
                return;
            }
            if tokio::time::Instant::now() >= deadline {
                warn!(remaining = count, "Timeout waiting for pod monitors, proceeding with shutdown");
                return;
            }
            debug!(remaining = count, "Waiting for pod monitors to complete");
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// Set the metrics instance for instrumentation.
    pub fn with_metrics(mut self, metrics: Arc<Metrics>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// Create a pod.
    ///
    /// Admits the spec (rejects unsupported features), checks
    /// uniqueness/capacity, stores the pod in **Pending**, and spawns a
    /// background preparer task. Returns the initial Pending snapshot.
    /// The executor is spawned only after the preparer finishes — see
    /// [`prepare_and_run`] for the full flow.
    pub async fn create_pod(&self, spec: PodSpec) -> Result<PodSnapshot, LogicError> {
        let create_start = std::time::Instant::now();
        let id = PodId::from_spec(&spec);

        // Admission — reject unsupported features (e.g. `optional: true`
        // on configmap/secret references). See pod::admission for the
        // full list of rules.
        crate::pod::admission::admit(&spec).map_err(LogicError::Internal)?;

        // Uniqueness
        if self.store.get_by_name(&id.namespace, &id.name, SpecView::Running).await.is_some() {
            warn!(pod = %id, "Pod already exists");
            return Err(LogicError::Conflict {
                namespace: id.namespace,
                name: id.name,
            });
        }

        // Capacity
        if self.store.len().await >= self.config.max_pods {
            warn!(pod = %id, max = self.config.max_pods, "Capacity exceeded");
            return Err(LogicError::CapacityExceeded { max: self.config.max_pods });
        }

        // Store the pod in Pending. The running_spec starts equal to the
        // original; the preparer overwrites it with the rewritten spec
        // once preparation succeeds.
        let original_spec = Arc::new(spec);
        let initial_status = build_initial_status(&original_spec);
        self.store.put(
            id.clone(),
            Arc::clone(&original_spec),
            Arc::clone(&original_spec),
            initial_status,
        ).await;
        info!(pod = %id, "Pod stored in Pending; spawning preparer");

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
            )
            .instrument(prep_span),
        );

        // Register Preparing runtime entry so delete_pod can find it.
        self.runtimes.write().unwrap().insert(
            id.clone(),
            PodRuntime::Preparing { cancel_tx, deleted, task },
        );

        // Metrics
        if let Some(ref m) = self.metrics {
            m.pods_created_total.inc();
            m.pods_running.set(self.runtimes.read().unwrap().len() as i64);
            m.pod_creation_seconds.observe(create_start.elapsed().as_secs_f64());
        }

        match self.store.get(&id, SpecView::Running).await {
            Some(snap) => Ok(snap),
            None => {
                self.store.remove(&id).await;
                Err(LogicError::Internal("pod disappeared after put".to_string()))
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
        let snap = match self.store.get_by_name(namespace, name, SpecView::Running).await {
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
        let span = info_span!("logic_delete", pod = %snap.id);
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
        let grace = grace_period.unwrap_or(self.config.default_grace_period);

        // Terminal — run cleanup and remove runtime.
        // The monitor task has already processed the Completed event and
        // written the FINISHED sentinel by the time we reach here.
        if snap.status.phase.is_terminal() {
            info!(phase = ?snap.status.phase, "Cleaning up terminal pod");
            cleanup::cleanup_pod(
                &snap.id,
                self.store.as_ref(),
                self.executor_provider.as_ref(),
                &self.cleanup_config,
            ).await;
            // Mark deleted so if the monitor is still finishing, it skips
            // its own cleanup (store entry already removed above).
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
                info!("Cancelling pod preparer");
                let _ = tx.send(());
                // The preparer task observes `deleted` after its
                // prepare_pod call returns and exits without spawning the
                // executor. The store entry is removed below.
                self.store.remove(&snap.id).await;
                self.runtimes.write().unwrap().remove(&snap.id);
                return Ok(());
            }
            Action::KillRunning(kh) => {
                info!(grace_period = ?grace, "Killing pod");
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
            m.pod_deletion_seconds.observe(delete_start.elapsed().as_secs_f64());
        }

        Ok(())
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
            self.store.get_by_name(namespace, name, view).await.ok_or_else(|| {
                debug!("Pod not found");
                LogicError::NotFound {
                    namespace: namespace.to_string(),
                    name: name.to_string(),
                }
            })
        }
        .instrument(debug_span!("logic_get", namespace, name))
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
        .instrument(debug_span!("logic_get_status", namespace, name))
        .await
    }

    /// List all pods.
    pub async fn list_pods(&self, view: SpecView) -> Vec<PodSnapshot> {
        async {
            let pods = self.store.list(view).await;
            debug!(count = pods.len(), "Listed pods");
            pods
        }
        .instrument(debug_span!("logic_list"))
        .await
    }

    /// Return the current phase of every pod as lightweight entries.
    ///
    /// Designed for the virtual-kubelet's sync loop — one bulk call
    /// per tick returns all statuses, eliminating per-pod HTTP calls.
    pub async fn list_pod_statuses(&self) -> Vec<crate::spec::api::PodStatusEntry> {
        async {
            let pods = self.store.list(SpecView::Running).await;
            debug!(count = pods.len(), "Listed pod statuses");
            pods.into_iter()
                .map(|snap| crate::spec::api::PodStatusEntry {
                    namespace: snap.id.namespace,
                    name: snap.id.name,
                    phase: snap.status.phase.to_string(),
                    status: snap.status,
                })
                .collect()
        }
        .instrument(debug_span!("logic_list_statuses"))
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
            debug!(count = pods.len(), "Listed pods in namespace");
            pods
        }
        .instrument(debug_span!("logic_list_ns", namespace))
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
        let runtime = runtimes.get(&snap.id).ok_or_else(|| {
            LogicError::Internal("no runtime for pod".to_string())
        })?;

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
        status.init_container_statuses.push(ContainerStatus::new_waiting(&c.name));
    }

    // Container statuses
    for c in &spec.spec.containers {
        status.container_statuses.push(ContainerStatus::new_waiting(&c.name));
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
async fn monitor_pod(
    mut handle: PodHandle,
    store: Arc<dyn PodStore>,
    executor_provider: Arc<dyn ExecutorProvider>,
    cleanup_config: Arc<CleanupConfig>,
    runtimes: Arc<RwLock<HashMap<PodId, PodRuntime>>>,
    metrics: Option<Arc<Metrics>>,
    id: PodId,
    deleted: Arc<AtomicBool>,
    mut shutdown: watch::Receiver<()>,
) {
    loop {
        let event = tokio::select! {
            result = handle.wait() => result,
            _ = shutdown.changed() => {
                info!("Shutdown signal received in monitor, waiting for pod to finish");
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
                        info!(container = name.as_str(), role = %role, "Container started");
                        if let Some(ref m) = metrics {
                            m.containers_started_total.inc();
                            m.containers_running.inc();
                        }
                    }
                    PodEventKind::ContainerSucceeded { name } => {
                        info!(container = name.as_str(), "Container succeeded");
                        if let Some(ref m) = metrics {
                            m.containers_running.dec();
                        }
                    }
                    PodEventKind::ContainerFailed { name, exit_code } => {
                        info!(container = name.as_str(), exit_code, "Container failed");
                        if let Some(ref m) = metrics {
                            m.containers_failed_total.inc();
                            m.containers_running.dec();
                        }
                    }
                    PodEventKind::Completed { phase } => {
                        info!(phase = ?phase, "Pod completed");
                        if let Some(ref m) = metrics {
                            if *phase == crate::spec::constants::PodPhase::Failed {
                                m.pods_failed_total.inc();
                            }
                            m.pods_running.dec();
                        }
                    }
                    PodEventKind::PhaseChanged { to, .. } => {
                        debug!(phase = ?to, "Phase changed");
                        if let Some(ref m) = metrics
                            && *to == crate::spec::constants::PodPhase::Running {
                                m.pods_started_total.inc();
                            }
                    }
                    _ => {
                        debug!(kind = ?event.kind, phase = ?event.status.phase, "Event received");
                    }
                }
                let completed = matches!(event.kind, PodEventKind::Completed { .. });
                store.update_status(&id, event.status).await;
                if completed {
                    break;
                }
            }
            Err(e) => {
                warn!(error = %e, "Event stream ended unexpectedly");
                break;
            }
        }
    }

    // Write FINISHED sentinel for all completed pods. This marks the
    // work dir for the purge daemon regardless of keep policy.
    let work_dir = executor_provider.work_dir(&id);
    if work_dir.exists() {
        let sentinel = work_dir.join(FINISHED_SENTINEL);
        if let Err(e) = std::fs::write(&sentinel, chrono::Utc::now().to_rfc3339()) {
            warn!(error = %e, "Failed to write FINISHED sentinel");
        }
    }

    // If the pod was marked for deletion, perform full cleanup.
    // Natural completions keep everything so logs can still be read
    // until delete_pod is called.
    if deleted.load(Ordering::Relaxed) {
        info!("Cleaning up deleted pod");
        cleanup::cleanup_pod(
            &id,
            store.as_ref(),
            executor_provider.as_ref(),
            &cleanup_config,
        ).await;
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
///   etc.) — pod transitions to `Failed`/`PodPreparationFailed`.
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
) {
    let pod_work_dir = executor_provider.work_dir(&id);
    let timeout = original_spec
        .spec
        .active_deadline_seconds
        .map(Duration::from_secs);

    debug!(
        pod = %id,
        timeout_secs = ?timeout.map(|d| d.as_secs()),
        "Starting pod preparation",
    );

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
        info!(pod = %id, "Preparation aborted by deletion");
        return;
    }

    let prepared = match prepare_result {
        Ok(p) => p,
        Err(msg) => {
            // Classify and surface as Failed in the store.
            let reason = if msg.contains("timed out") {
                warn!(pod = %id, error = %msg, "Pod preparation timed out");
                REASON_DEADLINE_EXCEEDED
            } else {
                warn!(pod = %id, error = %msg, "Pod preparation failed");
                REASON_POD_PREPARATION_FAILED
            };
            let status = build_failed_prep_status(reason, &msg);
            store.update_status(&id, status).await;
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
    let executor = executor_provider.create(Arc::clone(&running_spec), &id);
    let exec_shutdown = shutdown_tx.subscribe();
    let handle = match executor.spawn(exec_shutdown).await {
        Ok(h) => h,
        Err(e) => {
            warn!(pod = %id, error = %e, "Executor spawn failed");
            let status = build_failed_prep_status(
                REASON_POD_PREPARATION_FAILED,
                &format!("executor spawn failed: {}", e),
            );
            store.update_status(&id, status).await;
            if let Some(ref m) = metrics {
                m.pods_failed_total.inc();
            }
            return;
        }
    };

    let kill_handle = handle.kill_handle();
    let log_handle = handle.log_handle();

    let monitor_span = info_span!("monitor", pod = %id);
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

    info!(pod = %id, "Pod prepared and executor spawned");
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

    fn make_provider() -> Arc<DefaultExecutorProvider> {
        let n = TEST_COUNTER.fetch_add(1, Ordering::Relaxed);
        let base = std::env::temp_dir().join(format!(
            "pod-runner-rs2-test-logic-{}-{}",
            std::process::id(),
            n,
        ));
        Arc::new(DefaultExecutorProvider {
            pods_dir: base.join("pods"),
            pid_dir: base.join("pids"),
            propagate_env: true,
            die_with_parent: true,
            max_restarts: 0,
            restart_backoff_base: Duration::from_secs(2),
            shutdown_grace_period: Duration::from_secs(30),
        })
    }

    /// Build a CleanupConfig with paths under a temp directory so tests
    /// don't write to the repository root.
    fn test_cleanup_config() -> CleanupConfig {
        let n = TEST_COUNTER.fetch_add(1, Ordering::Relaxed);
        let base = std::env::temp_dir().join(format!(
            "pod-runner-rs2-test-cleanup-{}-{}",
            std::process::id(),
            n,
        ));
        CleanupConfig {
            pods_dir: base.join("pods"),
            ..CleanupConfig::default()
        }
    }

    fn make_logic() -> PodLogic {
        let store = Arc::new(MemoryStore::new());
        let (shutdown_tx, _) = watch::channel(());
        PodLogic::new(LogicConfig::default(), test_cleanup_config(), store, make_provider(), ResolverSet::default(), shutdown_tx)
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
        let err = logic.get_pod("default", "nope", SpecView::Running).await.unwrap_err();
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
        assert!(logic.list_pods_in_namespace("default", SpecView::Running).await.is_empty());
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
            pods_dir: base.join("pods"),
            pid_dir: base.join("pids"),
            propagate_env: true,
            die_with_parent: true,
            max_restarts: 0,
            restart_backoff_base: Duration::from_secs(2),
            shutdown_grace_period: Duration::from_secs(30),
        };
        let spec = Arc::new(make_spec("default", "app", "uid-1"));
        let id = PodId::from_spec(&spec);

        let _executor = provider.create(spec, &id);

        // Directories should exist after create()
        assert!(provider.pods_dir.join(id.full()).exists());
        assert!(provider.log_dir(&id).unwrap().exists());
    }

    #[test]
    fn executor_provider_log_dir() {
        let provider = make_provider();
        let id = PodId::new("default", "app", "uid-1");

        let log_dir = provider.log_dir(&id).unwrap();
        assert!(log_dir.ends_with("default_app_uid-1/log"));
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
        use crate::testutils::shell_cmd;

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

        /// Wait until a pod reaches a specific phase, with timeout.
        async fn wait_for_phase(logic: &PodLogic, name: &str, target: PodPhase) {
            let result = tokio::time::timeout(Duration::from_secs(10), async {
                loop {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    if let Ok(snap) = logic.get_pod("default", name, SpecView::Running).await {
                        if snap.status.phase == target {
                            return;
                        }
                    }
                }
            })
            .await;
            assert!(
                result.is_ok(),
                "Timed out waiting for pod '{}' to reach {:?}",
                name,
                target,
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
            result.expect(&format!("Timed out waiting for pod '{}' to terminate", name))
        }

        /// Wait until a pod is removed from the store, with timeout.
        async fn wait_for_removal(store: &MemoryStore, name: &str) {
            let id = PodId::new("default", name, &format!("uid-{}", name));
            let result = tokio::time::timeout(Duration::from_secs(10), async {
                loop {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    if !store.contains(&id).await {
                        return;
                    }
                }
            })
            .await;
            result.expect(&format!("Timed out waiting for pod '{}' to be removed", name));
        }

        // -- create_pod --

        #[tokio::test]
        async fn create_pod_returns_pending_snapshot() {
            let (logic, _, _) = make_logic_with_store();

            let snap = logic.create_pod(make_pod_spec("app", "exit 0")).await.unwrap();
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
            logic.create_pod(make_pod_spec("app", "sleep 60")).await.unwrap();

            let original = logic.get_pod("default", "app", SpecView::Original).await.unwrap();
            let running = logic.get_pod("default", "app", SpecView::Running).await.unwrap();

            // Identity prepare_spec means both are equivalent
            assert_eq!(original.spec.metadata.name, "app");
            assert_eq!(running.spec.metadata.name, "app");
        }

        #[tokio::test]
        async fn create_pod_duplicate_rejected() {
            let (logic, _, _) = make_logic_with_store();
            logic.create_pod(make_pod_spec("app", "sleep 60")).await.unwrap();

            let err = logic.create_pod(make_pod_spec("app", "sleep 60")).await.unwrap_err();
            assert!(matches!(err, LogicError::Conflict { .. }));
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
            let logic = PodLogic::new(config, test_cleanup_config(), Arc::clone(&store) as Arc<dyn PodStore>, provider, ResolverSet::default(), shutdown_tx);

            logic.create_pod(make_pod_spec("first", "sleep 60")).await.unwrap();
            let err = logic.create_pod(make_pod_spec("second", "sleep 60")).await.unwrap_err();
            assert!(matches!(err, LogicError::CapacityExceeded { max: 1 }));
        }

        // -- monitor: status transitions --

        #[tokio::test]
        async fn monitor_transitions_to_running() {
            let (logic, _, _) = make_logic_with_store();
            logic.create_pod(make_pod_spec("app", "sleep 60")).await.unwrap();

            wait_for_phase(&logic, "app", PodPhase::Running).await;

            let snap = logic.get_pod("default", "app", SpecView::Running).await.unwrap();
            assert_eq!(snap.status.phase, PodPhase::Running);
        }

        #[tokio::test]
        async fn monitor_transitions_to_succeeded() {
            let (logic, _, _) = make_logic_with_store();
            logic.create_pod(make_pod_spec("app", "exit 0")).await.unwrap();

            let phase = wait_for_terminal(&logic, "app").await;
            assert_eq!(phase, PodPhase::Succeeded);
        }

        #[tokio::test]
        async fn monitor_transitions_to_failed() {
            let (logic, _, _) = make_logic_with_store();
            logic.create_pod(make_pod_spec("app", "exit 1")).await.unwrap();

            let phase = wait_for_terminal(&logic, "app").await;
            assert_eq!(phase, PodPhase::Failed);
        }

        #[tokio::test]
        async fn monitor_keeps_runtime_after_natural_completion() {
            let (logic, _, _) = make_logic_with_store();
            logic.create_pod(make_pod_spec("app", "exit 0")).await.unwrap();
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

        #[tokio::test]
        async fn monitor_writes_finished_sentinel() {
            let (logic, _, _) = make_logic_with_store();
            logic.create_pod(make_pod_spec("app", "exit 0")).await.unwrap();
            let id = PodId::new("default", "app", "uid-app");

            wait_for_terminal(&logic, "app").await;
            tokio::time::sleep(Duration::from_millis(100)).await;

            let work_dir = logic.executor_provider.work_dir(&id);
            let sentinel = work_dir.join(FINISHED_SENTINEL);
            assert!(sentinel.exists(), "FINISHED sentinel should be written on completion");
        }

        // -- get_pod --

        #[tokio::test]
        async fn get_pod_both_views() {
            let (logic, _, _) = make_logic_with_store();
            logic.create_pod(make_pod_spec("app", "sleep 60")).await.unwrap();

            let running = logic.get_pod("default", "app", SpecView::Running).await.unwrap();
            let original = logic.get_pod("default", "app", SpecView::Original).await.unwrap();

            assert_eq!(running.id, original.id);
            assert_eq!(running.status.phase, original.status.phase);
        }

        #[tokio::test]
        async fn get_pod_status_returns_current() {
            let (logic, _, _) = make_logic_with_store();
            logic.create_pod(make_pod_spec("app", "sleep 60")).await.unwrap();

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
            logic.create_pod(make_pod_spec("a", "sleep 60")).await.unwrap();
            logic.create_pod(make_pod_spec("b", "sleep 60")).await.unwrap();
            logic.create_pod(make_pod_spec("c", "sleep 60")).await.unwrap();

            assert_eq!(logic.list_pods(SpecView::Running).await.len(), 3);
            assert_eq!(logic.list_pods(SpecView::Original).await.len(), 3);
        }

        #[tokio::test]
        async fn list_pods_in_namespace_filters() {
            let (logic, _, _) = make_logic_with_store();
            logic.create_pod(make_pod_spec_ns("ns1", "a", "sleep 60")).await.unwrap();
            logic.create_pod(make_pod_spec_ns("ns1", "b", "sleep 60")).await.unwrap();
            logic.create_pod(make_pod_spec_ns("ns2", "c", "sleep 60")).await.unwrap();

            assert_eq!(logic.list_pods_in_namespace("ns1", SpecView::Running).await.len(), 2);
            assert_eq!(logic.list_pods_in_namespace("ns2", SpecView::Running).await.len(), 1);
            assert_eq!(logic.list_pods_in_namespace("ns3", SpecView::Running).await.len(), 0);
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
            logic.create_pod(make_pod_spec("app", "exit 0")).await.unwrap();

            wait_for_terminal(&logic, "app").await;
            assert_eq!(store.len().await, 1);

            logic.delete_pod("default", "app", None).await.unwrap();
            assert_eq!(store.len().await, 0);
            assert!(logic.get_pod("default", "app", SpecView::Running).await.is_err());
        }

        #[tokio::test]
        async fn delete_running_pod_kills_and_removes() {
            let (logic, store, _) = make_logic_with_store();
            logic.create_pod(make_pod_spec("app", "sleep 60")).await.unwrap();

            wait_for_phase(&logic, "app", PodPhase::Running).await;

            logic.delete_pod("default", "app", Some(Duration::from_secs(1))).await.unwrap();

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
            let logic = PodLogic::new(config, test_cleanup_config(), Arc::clone(&store) as Arc<dyn PodStore>, provider, ResolverSet::default(), shutdown_tx);
            logic.create_pod(make_pod_spec("app", "sleep 60")).await.unwrap();

            wait_for_phase(&logic, "app", PodPhase::Running).await;

            // Delete with None uses default_grace_period
            logic.delete_pod("default", "app", None).await.unwrap();
            wait_for_removal(&store, "app").await;
        }

        #[tokio::test]
        async fn delete_idempotent_disruption() {
            let (logic, _, _) = make_logic_with_store();
            logic.create_pod(make_pod_spec("app", "sleep 60")).await.unwrap();

            wait_for_phase(&logic, "app", PodPhase::Running).await;

            // First delete marks disruption
            logic.delete_pod("default", "app", Some(Duration::from_secs(1))).await.unwrap();
            let snap = logic.get_pod("default", "app", SpecView::Running).await.unwrap();
            assert!(snap.status.is_condition_true(CONDITION_DISRUPTION_TARGET));

            // Second delete is a no-op (no error, no change)
            logic.delete_pod("default", "app", Some(Duration::from_secs(1))).await.unwrap();
        }

        #[tokio::test]
        async fn delete_already_removed_is_ok() {
            let (logic, _, _) = make_logic_with_store();
            logic.create_pod(make_pod_spec("app", "exit 0")).await.unwrap();

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
            logic.create_pod(make_pod_spec("fast", "exit 0")).await.unwrap();
            logic.create_pod(make_pod_spec("slow", "sleep 60")).await.unwrap();
            logic.create_pod(make_pod_spec("fail", "exit 1")).await.unwrap();

            assert_eq!(store.len().await, 3);

            // Fast and fail complete on their own
            let fast_phase = wait_for_terminal(&logic, "fast").await;
            assert_eq!(fast_phase, PodPhase::Succeeded);

            let fail_phase = wait_for_terminal(&logic, "fail").await;
            assert_eq!(fail_phase, PodPhase::Failed);

            // Slow is still running
            wait_for_phase(&logic, "slow", PodPhase::Running).await;

            // Kill slow — deleted pods are removed automatically
            logic.delete_pod("default", "slow", Some(Duration::from_secs(1))).await.unwrap();
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
            logic.create_pod(make_pod_spec("a", "sleep 60")).await.unwrap();
            logic.create_pod(make_pod_spec("b", "sleep 60")).await.unwrap();

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
            logic.create_pod(make_pod_spec("logger", "echo hello-world")).await.unwrap();

            wait_for_terminal(&logic, "logger").await;

            let mut rx = logic
                .stream_container_logs(
                    "default",
                    "logger",
                    Some("main"),
                    ContainerLogOptions::default(),
                ).await.unwrap();

            let mut output = String::new();
            while let Some(Ok(chunk)) = rx.recv().await {
                output.push_str(&String::from_utf8_lossy(&chunk));
            }
            assert!(output.contains("hello-world"), "got: {}", output);
        }

        #[tokio::test]
        async fn stream_logs_with_tail_lines() {
            let (logic, _, _) = make_logic_with_store();
            // Print 10 lines
            logic
                .create_pod(make_pod_spec("tailer", "for i in 1 2 3 4 5 6 7 8 9 10; do echo line-$i; done"))
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
                ).await.unwrap();

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
                ).await.unwrap();

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
                .stream_container_logs("default", "nope", None, ContainerLogOptions::default()).await.unwrap_err();
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

            logic.delete_pod("default", "waiter", Some(Duration::from_secs(1))).await.unwrap();
            wait_for_removal(&store, "waiter").await;
            assert_eq!(store.len().await, 0);
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
                LogicError::Internal(msg) => assert!(
                    msg.contains("optional"),
                    "expected admission error mentioning 'optional', got: {}",
                    msg,
                ),
                other => panic!("expected Internal admission error, got {:?}", other),
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
                .stream_container_logs(
                    "default",
                    "prep",
                    None,
                    ContainerLogOptions::default(),
                )
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
            logic.delete_pod("default", "prep", Some(Duration::from_secs(1))).await.unwrap();
        }
    }
}
