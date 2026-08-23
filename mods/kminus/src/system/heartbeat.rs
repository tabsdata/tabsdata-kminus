// Copyright 2026 Tabsdata Inc.

//! Component heartbeat files: the readiness and liveness channel between
//! km and the components it supervises.
//!
//! Every component rewrites its own file once per heartbeat interval, and
//! only after verifying itself end to end — the pod-runner calls its own
//! `/capacity`, the control-plane probes etcd, the API server and
//! kube-controller-manager, the virtual-kubelet checks its node is still
//! registered. A fresh file therefore means "serving", not merely
//! "process alive", which a bound socket cannot distinguish. km reads two
//! signals out of it:
//!
//! * the file appearing at all is the readiness gate for starting the
//!   next component;
//! * its mtime staying fresh is the liveness signal the monitor watches.
//!
//! km itself speaks neither HTTP nor TLS to any component: it stats a file.

use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use serde::Deserialize;
use tracing::{info, warn};

use crate::system::pid::{BIN_KM, DATA_DIR};

/// Filename of a component's heartbeat file, under `<workdir>/data/<bin>/`.
pub const HEARTBEAT_FILE: &str = "heartbeat.json";

/// The only status km treats as healthy. Anything else is the component
/// reporting its own trouble, and counts as a miss.
pub const STATUS_READY: &str = "ready";

/// Slack added to the heartbeat interval before a poll counts as a miss,
/// so a beat that is merely late does not trip the counter.
const HEARTBEAT_GRACE: Duration = Duration::from_secs(2);

/// Path of `name`'s heartbeat file: `<workdir>/data/<name>/heartbeat.json`.
pub fn heartbeat_file(workdir: &Path, name: &str) -> PathBuf {
    workdir.join(DATA_DIR).join(name).join(HEARTBEAT_FILE)
}

/// How old a heartbeat may get before a poll counts it as missed.
pub fn stale_after(interval: Duration) -> Duration {
    interval + HEARTBEAT_GRACE
}

/// Filename of km's own ready marker, under `<workdir>/data/km/`.
pub const KM_READY_FILE: &str = "ready";

/// Path of km's ready marker.
///
/// Only its modification time carries meaning, so it is a marker rather than
/// a heartbeat: km rewrites the timestamp on every supervision round where
/// the whole stack is healthy, and stops the moment it is not. `km status`
/// reads nothing but the age, and ages it with [`stale_after`] against the
/// same `intervalSecs` km supervises on — so the two cannot drift apart the
/// way a second opinion about readiness would.
pub fn ready_file(workdir: &Path) -> PathBuf {
    workdir.join(DATA_DIR).join(BIN_KM).join(KM_READY_FILE)
}

/// Stamp the ready marker with the current time, creating it if needed.
///
/// Best-effort: failing to record readiness must not bring down a stack that
/// is, in fact, ready. A marker that stops being stamped goes stale on its
/// own, which is the same thing `km status` would conclude anyway.
pub fn touch_ready(workdir: &Path) {
    let path = ready_file(workdir);
    if let Some(parent) = path.parent()
        && let Err(e) = fs::create_dir_all(parent)
    {
        warn!(dir = %parent.display(), error = %e, "Cannot create the ready marker directory");
        return;
    }
    // Rewriting the file is the portable way to set mtime to now; the
    // contents are never read.
    if let Err(e) = fs::write(&path, b"ready\n") {
        warn!(file = %path.display(), error = %e, "Cannot stamp the ready marker");
    }
}

/// How long ago km last reported the stack ready, or `None` when it never
/// has. A marker dated in the future — a backwards clock step — is read as
/// fresh rather than impossibly old, matching [`read`].
pub fn ready_age(workdir: &Path) -> Option<Duration> {
    let modified = fs::metadata(ready_file(workdir)).ok()?.modified().ok()?;
    Some(modified.elapsed().unwrap_or_default())
}

/// Remove the ready marker, so a stopped instance cannot read as ready and
/// the next start cannot inherit this run's answer.
pub fn clear_ready(workdir: &Path) {
    let path = ready_file(workdir);
    if path.exists() {
        let _ = fs::remove_file(&path);
    }
}

/// What a component wrote into its heartbeat file. `detail` carries the
/// component's own explanation whenever `status` is not [`STATUS_READY`].
#[derive(Debug, Deserialize)]
pub struct Heartbeat {
    #[serde(default)]
    pub status: String,
    #[serde(default)]
    pub detail: String,
}

/// Read a heartbeat file, returning its age and contents. `None` if it is
/// missing, unreadable or unparseable — all of which count as a miss.
pub fn read(path: &Path) -> Option<(Duration, Heartbeat)> {
    let modified = fs::metadata(path).ok()?.modified().ok()?;
    // A backwards clock step puts mtime in the future; call that fresh
    // rather than stale, so a time jump cannot tear down a live stack.
    let age = modified.elapsed().unwrap_or_default();
    let text = fs::read_to_string(path).ok()?;
    // JSON is a subset of YAML, so km's existing YAML parser reads it.
    let beat: Heartbeat = serde_yaml::from_str(&text).ok()?;
    Some((age, beat))
}

/// Whether a component reports itself ready, at any age. This is the
/// startup gate: the file's existence is the signal, not its freshness.
pub fn is_ready(path: &Path) -> bool {
    matches!(read(path), Some((_, beat)) if beat.status == STATUS_READY)
}

/// Age of a component's last beat, for status reporting.
pub fn age(path: &Path) -> Option<Duration> {
    read(path).map(|(age, _)| age)
}

/// How often the startup gate re-checks a component's heartbeat file.
const READY_POLL_INTERVAL: Duration = Duration::from_millis(250);

/// Remove a stale heartbeat file before (re)starting its component, so
/// the previous run's beat cannot satisfy this run's readiness gate.
pub fn clear(path: &Path) {
    if path.exists() {
        let _ = fs::remove_file(path);
    }
}

/// Block until `path` reports ready or `timeout` elapses. Returns false
/// on timeout, leaving the caller to log and tear down.
pub fn wait_until_ready(name: &str, path: &Path, timeout: Duration) -> bool {
    let started = Instant::now();
    while started.elapsed() < timeout {
        if is_ready(path) {
            info!(
                component = name,
                elapsed_ms = started.elapsed().as_millis() as u64,
                "Component reported ready"
            );
            return true;
        }
        std::thread::sleep(READY_POLL_INTERVAL);
    }
    false
}

/// Outcome of a single [`HeartbeatMonitor::poll`].
#[derive(Debug, PartialEq, Eq)]
pub enum Beat {
    /// Fresh and ready; the miss counter was reset.
    Healthy,
    /// Missed, still under the limit.
    Missed { misses: u32 },
    /// The limit is reached: treat the component as dead. Carries what the
    /// last poll observed, so the failure can be reported with a cause
    /// rather than just a count.
    Failed { misses: u32, reason: String },
}

/// Consecutive-miss counter for one component, polled once per interval.
pub struct HeartbeatMonitor {
    name: &'static str,
    path: PathBuf,
    stale_after: Duration,
    max_missed: u32,
    misses: u32,
}

impl HeartbeatMonitor {
    pub fn new(name: &'static str, path: PathBuf, stale_after: Duration, max_missed: u32) -> Self {
        Self {
            name,
            path,
            stale_after,
            max_missed,
            misses: 0,
        }
    }

    /// Poll once. A missing, stale, unparseable or not-ready file is a
    /// miss, logged with the running count; a good beat clears the count.
    pub fn poll(&mut self) -> Beat {
        match self.miss_reason() {
            None => {
                self.misses = 0;
                Beat::Healthy
            }
            Some(reason) => {
                self.misses += 1;
                warn!(
                    component = self.name,
                    missed = self.misses,
                    max_missed = self.max_missed,
                    "Missed heartbeat ({}/{}): {}",
                    self.misses,
                    self.max_missed,
                    reason
                );
                if self.misses >= self.max_missed {
                    Beat::Failed {
                        misses: self.misses,
                        reason,
                    }
                } else {
                    Beat::Missed {
                        misses: self.misses,
                    }
                }
            }
        }
    }

    /// Why this poll is a miss, or `None` when the beat is good.
    fn miss_reason(&self) -> Option<String> {
        let Some((age, beat)) = read(&self.path) else {
            return Some(format!("no readable heartbeat at {}", self.path.display()));
        };
        if beat.status != STATUS_READY {
            return Some(format!("reported '{}': {}", beat.status, beat.detail));
        }
        if age > self.stale_after {
            return Some(format!(
                "last beat {}s ago, over the {}s limit",
                age.as_secs(),
                self.stale_after.as_secs()
            ));
        }
        None
    }
}

#[cfg(test)]
mod tests;
