// Copyright 2026 Tabsdata Inc.

//! Disk-usage tasks for pods and the pod runner.
//!
//! Both task types use [`crate::utils::dir_size::scan_dir`] to compute
//! allocated bytes under a directory tree. Each call walks the tree
//! fresh and writes a single `_size` file at the root of the scan; no
//! per-subtree caching.
//!
//! - [`DiskSizePodTask`] is a [`PodTask`] that scans the pod's working
//!   directory on three occasions: once immediately when the task
//!   starts (the **initial** scan), every `interval` thereafter, and
//!   once more when `pod_kill` or `runner_shutdown` fires (the
//!   **final** scan).
//! - [`DiskSizeRunnerTask`] is a [`RunnerTask`] with the same
//!   initial / periodic / final pattern against the pod-runner's
//!   working directory.
//!
//! Each scan emits a structured tracing event at `INFO` with two
//! fields — `mb` (allocated size as MiB with three decimals) and
//! `blocks` (in 512-byte units per
//! [`crate::utils::dir_size::BLOCK_BYTES`]) — and a distinct
//! message per scan kind (`initial`, periodic, `final`). The scan
//! also writes `_size` (JSON: `{"bytes":N,"blocks":M}`) at the
//! directory it was called on.

use std::path::PathBuf;
use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::watch;
use tokio::time::{self, MissedTickBehavior};
use tracing::{info, warn};

use super::{RunnerTask, RunnerTaskContext};
use crate::pod::executor::{PodTask, PodTaskContext};
use crate::utils::dir_size::scan_dir;

/// Default cadence between scans.
pub const DEFAULT_SCAN_INTERVAL: Duration = Duration::from_secs(60);

/// Bytes-per-MiB divisor used to format the `mb` log field.
const MIB: f64 = 1024.0 * 1024.0;

/// Format `bytes` as MiB with three decimals (e.g. `0.027`).
fn mib(bytes: u64) -> String {
    format!("{:.3}", bytes as f64 / MIB)
}

// ---------------------------------------------------------------------------
// Shared scan loop
// ---------------------------------------------------------------------------

/// Per-scan-kind log messages. Pod and runner tasks differ only in
/// what they print; the loop logic itself is shared.
struct ScanLabels {
    initial_ok: &'static str,
    initial_err: &'static str,
    periodic_ok: &'static str,
    periodic_err: &'static str,
    final_ok: &'static str,
    final_err: &'static str,
}

const POD_LABELS: ScanLabels = ScanLabels {
    initial_ok: "Pod initial disk utilization",
    initial_err: "Pod initial disk usage scan failed",
    periodic_ok: "Pod disk utilization",
    periodic_err: "Pod disk usage scan failed",
    final_ok: "Pod final disk utilization",
    final_err: "Pod final disk usage scan failed",
};

const RUNNER_LABELS: ScanLabels = ScanLabels {
    initial_ok: "Pod-runner initial total disk utilization",
    initial_err: "Pod-runner initial disk usage scan failed",
    periodic_ok: "Pod-runner total disk utilization",
    periodic_err: "Pod-runner disk usage scan failed",
    final_ok: "Pod-runner final total disk utilization",
    final_err: "Pod-runner final disk usage scan failed",
};

/// Perform a single scan and emit a log line.
async fn one_scan(
    work_dir: &std::path::Path,
    cancel: &mut watch::Receiver<()>,
    ok_msg: &'static str,
    err_msg: &'static str,
) {
    match scan_dir(work_dir, cancel).await {
        Ok(s) => info!(mb = %mib(s.bytes), blocks = s.blocks, "{}", ok_msg),
        Err(e) => warn!(error = %e, "{}", err_msg),
    }
}

/// Run the initial → periodic → final scan sequence under `work_dir`.
///
/// `pod_kill` is the per-pod kill signal (only the pod task uses it;
/// the runner task passes `None`). Both tasks observe
/// `runner_shutdown` as a break signal and also clone it as the
/// in-flight cancel for periodic scans. The final scan uses a fresh
/// un-fired watch so it can complete even after shutdown has fired.
async fn run_scan_loop(
    work_dir: PathBuf,
    interval: Duration,
    labels: &ScanLabels,
    mut pod_kill: Option<watch::Receiver<bool>>,
    mut runner_shutdown: watch::Receiver<()>,
) {
    if work_dir.as_os_str().is_empty() {
        // No work dir configured — nothing to scan.
        return;
    }

    // Initial scan.
    {
        let mut cancel = runner_shutdown.clone();
        one_scan(
            &work_dir,
            &mut cancel,
            labels.initial_ok,
            labels.initial_err,
        )
        .await;
    }

    let mut ticker = time::interval(interval);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
    // tokio's interval fires the first tick immediately; skip it
    // since we just did the initial scan above.
    ticker.tick().await;

    loop {
        tokio::select! {
            _ = async {
                // If pod_kill is configured, await it; otherwise pend
                // forever so this arm never fires. `changed()` is
                // cancel-safe: a cancelled future does not advance
                // the "seen" pointer.
                if let Some(rx) = pod_kill.as_mut() {
                    let _ = rx.changed().await;
                } else {
                    std::future::pending::<()>().await;
                }
            } => break,
            _ = runner_shutdown.changed() => break,
            _ = ticker.tick() => {
                let mut cancel = runner_shutdown.clone();
                one_scan(&work_dir, &mut cancel, labels.periodic_ok, labels.periodic_err).await;
            }
        }
    }

    // Final scan. Fresh, un-fired cancel so this scan can complete
    // even when runner_shutdown has already fired.
    let (_keep_tx, mut never_fires) = watch::channel(());
    one_scan(
        &work_dir,
        &mut never_fires,
        labels.final_ok,
        labels.final_err,
    )
    .await;
}

// ---------------------------------------------------------------------------
// DiskSizePodTask
// ---------------------------------------------------------------------------

/// Scans the pod's working directory on task start (initial),
/// periodically (every `interval`), and once more when `pod_kill` or
/// `runner_shutdown` fires (final) so the reported size reflects the
/// latest state on completion.
pub struct DiskSizePodTask {
    interval: Duration,
}

impl DiskSizePodTask {
    /// Create with the default [`DEFAULT_SCAN_INTERVAL`].
    pub fn new() -> Self {
        Self {
            interval: DEFAULT_SCAN_INTERVAL,
        }
    }

    /// Create with a custom scan interval (mainly for tests).
    pub fn with_interval(interval: Duration) -> Self {
        Self { interval }
    }
}

impl Default for DiskSizePodTask {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl PodTask for DiskSizePodTask {
    async fn run(&self, ctx: PodTaskContext) {
        // Pod context (`pod=<ns>/<name>`) is supplied by the
        // orchestrator's pod span which wraps this task — no need to
        // attach it as an event field.
        run_scan_loop(
            ctx.work_dir,
            self.interval,
            &POD_LABELS,
            Some(ctx.pod_kill),
            ctx.runner_shutdown,
        )
        .await
    }
}

// ---------------------------------------------------------------------------
// DiskSizeRunnerTask
// ---------------------------------------------------------------------------

/// Scans the pod-runner's working directory on task start (initial),
/// periodically (every `interval`), and once more when
/// `runner_shutdown` fires (final).
pub struct DiskSizeRunnerTask {
    interval: Duration,
}

impl DiskSizeRunnerTask {
    /// Create with the default [`DEFAULT_SCAN_INTERVAL`].
    pub fn new() -> Self {
        Self {
            interval: DEFAULT_SCAN_INTERVAL,
        }
    }

    /// Create with a custom scan interval (mainly for tests).
    pub fn with_interval(interval: Duration) -> Self {
        Self { interval }
    }
}

impl Default for DiskSizeRunnerTask {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl RunnerTask for DiskSizeRunnerTask {
    async fn run(&self, ctx: RunnerTaskContext) {
        run_scan_loop(
            ctx.work_dir,
            self.interval,
            &RUNNER_LABELS,
            None,
            ctx.runner_shutdown,
        )
        .await
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use std::sync::Arc;

    use crate::spec::pod::{PodMetadata, PodSpec, PodSpecBody};

    fn make_pod() -> Arc<PodSpec> {
        Arc::new(PodSpec {
            metadata: PodMetadata {
                namespace: "default".to_string(),
                name: "test-pod".to_string(),
                uid: "uid-1".to_string(),
                ..Default::default()
            },
            spec: PodSpecBody::default(),
            ..Default::default()
        })
    }

    fn make_tmp_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "pod-runner-rs2-disk-usage-{}-{}",
            name,
            std::process::id(),
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[tokio::test]
    async fn pod_task_writes_size_file_on_final_scan() {
        let work_dir = make_tmp_dir("pod-final");
        std::fs::write(work_dir.join("payload"), vec![0u8; 4096]).unwrap();

        let (pod_kill_tx, pod_kill_rx) = watch::channel(false);
        let (_shutdown_tx, runner_shutdown) = watch::channel(());

        let ctx = PodTaskContext {
            pod: make_pod(),
            work_dir: work_dir.clone(),
            pod_kill: pod_kill_rx,
            runner_shutdown,
        };

        let task = DiskSizePodTask::with_interval(Duration::from_secs(3600));
        let handle = tokio::spawn(async move { task.run(ctx).await });

        // Give the task a moment to enter the loop, then trigger pod_kill.
        tokio::time::sleep(Duration::from_millis(50)).await;
        let _ = pod_kill_tx.send(true);

        // Task should run final scan and exit.
        tokio::time::timeout(Duration::from_secs(5), handle)
            .await
            .expect("task did not finish in time")
            .expect("task panicked");

        assert!(work_dir.join("_size").exists(), "_size should be written");
        // _size is JSON: `{"bytes":N,"blocks":M}`. Confirm both fields
        // are present and non-zero.
        let raw = std::fs::read_to_string(work_dir.join("_size")).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&raw).unwrap();
        let bytes = parsed["bytes"].as_u64().unwrap();
        let blocks = parsed["blocks"].as_u64().unwrap();
        assert!(bytes > 0, "scanned bytes should be > 0");
        assert!(blocks > 0, "scanned blocks should be > 0");

        let _ = std::fs::remove_dir_all(&work_dir);
    }

    #[tokio::test]
    async fn pod_task_empty_work_dir_returns_immediately() {
        let (_pod_kill_tx, pod_kill_rx) = watch::channel(false);
        let (_shutdown_tx, runner_shutdown) = watch::channel(());
        let ctx = PodTaskContext {
            pod: make_pod(),
            work_dir: PathBuf::new(),
            pod_kill: pod_kill_rx,
            runner_shutdown,
        };

        let task = DiskSizePodTask::new();
        let start = std::time::Instant::now();
        task.run(ctx).await;
        // No work_dir → no scans, no waiting.
        assert!(start.elapsed() < Duration::from_millis(200));
    }

    #[tokio::test]
    async fn pod_task_exits_on_runner_shutdown() {
        let work_dir = make_tmp_dir("pod-shutdown");
        let (_pod_kill_tx, pod_kill_rx) = watch::channel(false);
        let (shutdown_tx, runner_shutdown) = watch::channel(());

        let ctx = PodTaskContext {
            pod: make_pod(),
            work_dir: work_dir.clone(),
            pod_kill: pod_kill_rx,
            runner_shutdown,
        };

        let task = DiskSizePodTask::with_interval(Duration::from_secs(3600));
        let handle = tokio::spawn(async move { task.run(ctx).await });

        tokio::time::sleep(Duration::from_millis(50)).await;
        let _ = shutdown_tx.send(());

        tokio::time::timeout(Duration::from_secs(5), handle)
            .await
            .expect("task did not finish on runner_shutdown")
            .expect("task panicked");

        let _ = std::fs::remove_dir_all(&work_dir);
    }

    #[tokio::test]
    async fn pod_task_writes_size_only_at_root_not_subdirs() {
        let work_dir = make_tmp_dir("pod-no-subtree-cache");
        let sub = work_dir.join("sub");
        std::fs::create_dir_all(&sub).unwrap();
        std::fs::write(sub.join("payload"), vec![0u8; 4096]).unwrap();

        let (pod_kill_tx, pod_kill_rx) = watch::channel(false);
        let (_shutdown_tx, runner_shutdown) = watch::channel(());
        let ctx = PodTaskContext {
            pod: make_pod(),
            work_dir: work_dir.clone(),
            pod_kill: pod_kill_rx,
            runner_shutdown,
        };

        let task = DiskSizePodTask::with_interval(Duration::from_secs(3600));
        let handle = tokio::spawn(async move { task.run(ctx).await });

        tokio::time::sleep(Duration::from_millis(50)).await;
        let _ = pod_kill_tx.send(true);

        tokio::time::timeout(Duration::from_secs(5), handle)
            .await
            .expect("task did not finish in time")
            .expect("task panicked");

        assert!(
            work_dir.join("_size").exists(),
            "root _size should be written"
        );
        assert!(
            !sub.join("_size").exists(),
            "subdir _size should NOT be written (no subtree caching)",
        );

        let _ = std::fs::remove_dir_all(&work_dir);
    }

    #[tokio::test]
    async fn runner_task_writes_size_file_on_final_scan() {
        let work_dir = make_tmp_dir("runner-final");
        std::fs::write(work_dir.join("payload"), vec![0u8; 4096]).unwrap();

        let (shutdown_tx, runner_shutdown) = watch::channel(());
        let ctx = RunnerTaskContext {
            work_dir: work_dir.clone(),
            runner_shutdown,
        };

        let task = DiskSizeRunnerTask::with_interval(Duration::from_secs(3600));
        let handle = tokio::spawn(async move { task.run(ctx).await });

        tokio::time::sleep(Duration::from_millis(50)).await;
        let _ = shutdown_tx.send(());

        tokio::time::timeout(Duration::from_secs(5), handle)
            .await
            .expect("task did not finish in time")
            .expect("task panicked");

        assert!(work_dir.join("_size").exists());
        let _ = std::fs::remove_dir_all(&work_dir);
    }

    #[tokio::test]
    async fn runner_task_empty_work_dir_returns_immediately() {
        let (_shutdown_tx, runner_shutdown) = watch::channel(());
        let ctx = RunnerTaskContext {
            work_dir: PathBuf::new(),
            runner_shutdown,
        };
        let task = DiskSizeRunnerTask::new();
        let start = std::time::Instant::now();
        task.run(ctx).await;
        assert!(start.elapsed() < Duration::from_millis(200));
    }

    #[tokio::test]
    async fn pod_task_runs_initial_scan_before_first_tick() {
        let work_dir = make_tmp_dir("pod-initial");
        std::fs::write(work_dir.join("payload"), vec![0u8; 4096]).unwrap();

        let (_pod_kill_tx, pod_kill_rx) = watch::channel(false);
        let (_shutdown_tx, runner_shutdown) = watch::channel(());

        let ctx = PodTaskContext {
            pod: make_pod(),
            work_dir: work_dir.clone(),
            pod_kill: pod_kill_rx,
            runner_shutdown,
        };

        // Use a huge interval so periodic ticks never fire during the
        // test; only the initial scan can produce a `_size` file here.
        let task = DiskSizePodTask::with_interval(Duration::from_secs(3600));
        let _handle = tokio::spawn(async move { task.run(ctx).await });

        // Poll for the `_size` file appearing. The initial scan runs
        // unconditionally before the loop, so it should land quickly.
        let size_path = work_dir.join("_size");
        let mut found = false;
        for _ in 0..50 {
            if size_path.exists() {
                found = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(
            found,
            "initial scan should write _size before the first interval tick"
        );

        let _ = std::fs::remove_dir_all(&work_dir);
    }

    #[tokio::test]
    async fn runner_task_runs_initial_scan_before_first_tick() {
        let work_dir = make_tmp_dir("runner-initial");
        std::fs::write(work_dir.join("payload"), vec![0u8; 4096]).unwrap();

        let (_shutdown_tx, runner_shutdown) = watch::channel(());
        let ctx = RunnerTaskContext {
            work_dir: work_dir.clone(),
            runner_shutdown,
        };

        let task = DiskSizeRunnerTask::with_interval(Duration::from_secs(3600));
        let _handle = tokio::spawn(async move { task.run(ctx).await });

        let size_path = work_dir.join("_size");
        let mut found = false;
        for _ in 0..50 {
            if size_path.exists() {
                found = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(
            found,
            "initial scan should write _size before the first interval tick"
        );

        let _ = std::fs::remove_dir_all(&work_dir);
    }

    #[test]
    fn mib_formats_three_decimals() {
        assert_eq!(mib(0), "0.000");
        assert_eq!(mib(1024 * 1024), "1.000");
        assert_eq!(mib(1024 * 1024 + 512 * 1024), "1.500");
        // 28672 bytes = 0.02734375 MiB → rounds to "0.027".
        assert_eq!(mib(28672), "0.027");
    }
}
