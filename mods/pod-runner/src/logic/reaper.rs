// Copyright 2026 Tabsdata Inc.

//! Pod dir reaping — the marker and deadline files, and the daemon that
//! acts on them.
//!
//! When a pod reaches a terminal phase its dir gets an outcome marker
//! ([`SUCCEEDED_MARKER`] or [`FAILED_MARKER`], content the completion time)
//! and a [`REAP_ON_FILE`] holding the absolute instant its data dirs become
//! reapable. [`DELETE_ON_FILE`] is written later, when the pod leaves the
//! pod runner, since the delete clock starts there rather than at
//! completion.
//!
//! Deadlines are stored as absolute instants rather than durations so the
//! reaper never has to re-derive them: it compares `now` against a file's
//! contents and acts. A deadline of [`TTL_NEVER`] is written out literally,
//! so "never expires" is distinguishable from "the file is missing".
//!
//! Two things can happen to a finished pod dir:
//!
//! - **reap** — `work/` and `volumes/` are deleted. They hold everything the
//!   containers produced and are usually the bulk of the footprint, while
//!   `log/` and `store/` — the record of what the pod did and why it ended —
//!   stay behind.
//! - **delete** — the whole dir goes, record included.
//!
//! [`reap_pod_dir`] applies both rules inline whenever a pod is finalized or
//! evicted, so a zero TTL acts at once. A deadline in the future instead gets
//! a task of its own via [`schedule_deadline`] — there is no scanning daemon.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use tokio::sync::watch;
use tracing::{Instrument, debug, debug_span, info, warn};

use super::ttl::{
    CleanupTtls, Ttl, TtlConfig, read_cleanup_ttls, resolve_cleanup_ttls, write_cleanup_ttls,
};
use crate::pod::resolver::volume::VOLUMES_DIR;
use crate::server::config::POD_WORK_SUBDIR;
use crate::spec::constants::{
    DELETE_ON_FILE, FAILED_MARKER, PodPhase, REAP_ON_FILE, REAPED_MARKER, SUCCEEDED_MARKER,
    TTL_NEVER,
};
use crate::store::PodId;
use crate::utils::dir_size::SIZE_FILE;

// ---------------------------------------------------------------------------
// CleanupConfig
// ---------------------------------------------------------------------------

/// Configuration for pod dir reaping.
#[derive(Debug, Clone)]
pub struct CleanupConfig {
    /// Cleanup TTLs: when a finished pod's data dirs are reaped, and when
    /// its pod dir is deleted. Resolved per pod (config plus annotations)
    /// and recorded in the pod dir at pod start.
    pub ttls: TtlConfig,
    /// Pods directory (`<workdir>/run/pods/`). Pod dirs live here; the
    /// `run_<stamp>/pods/` dirs beside it hold previous runs'.
    pub pods_dir: PathBuf,
}

impl Default for CleanupConfig {
    fn default() -> Self {
        use crate::server::config::{PODS_SUBDIR, RUN_DIR};
        Self {
            ttls: TtlConfig::default(),
            pods_dir: PathBuf::from(RUN_DIR).join(PODS_SUBDIR),
        }
    }
}

// ---------------------------------------------------------------------------
// Outcome
// ---------------------------------------------------------------------------

/// Which terminal outcome a pod reached — the half of the retention policy
/// that picks between the `PodSuccess` and `PodFailure` TTLs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    /// The pod succeeded.
    Succeeded,
    /// The pod failed, was killed, or never got far enough to run.
    Failed,
}

impl Outcome {
    /// The outcome for a terminal phase, or `None` if the phase is not
    /// terminal.
    pub fn from_phase(phase: PodPhase) -> Option<Self> {
        match phase {
            PodPhase::Succeeded => Some(Outcome::Succeeded),
            PodPhase::Failed => Some(Outcome::Failed),
            _ => None,
        }
    }

    /// The marker file recording this outcome.
    pub fn marker(&self) -> &'static str {
        match self {
            Outcome::Succeeded => SUCCEEDED_MARKER,
            Outcome::Failed => FAILED_MARKER,
        }
    }

    /// Whether this is the success outcome.
    pub fn succeeded(&self) -> bool {
        matches!(self, Outcome::Succeeded)
    }
}

// ---------------------------------------------------------------------------
// Deadline files
// ---------------------------------------------------------------------------

/// The contents of a deadline file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Deadline {
    /// The literal [`TTL_NEVER`] — the guarded files are never removed.
    Never,
    /// The instant at which the guarded files become removable.
    At(DateTime<Utc>),
}

impl Deadline {
    /// Whether this deadline has passed at `now`. `Never` never has.
    pub fn is_due(&self, now: DateTime<Utc>) -> bool {
        match self {
            Deadline::Never => false,
            Deadline::At(t) => now >= *t,
        }
    }

    /// The instant, or `None` for `Never` — for computing the next wake-up.
    pub fn instant(&self) -> Option<DateTime<Utc>> {
        match self {
            Deadline::Never => None,
            Deadline::At(t) => Some(*t),
        }
    }

    /// The file contents for this deadline.
    fn encode(&self) -> String {
        match self {
            Deadline::Never => TTL_NEVER.to_string(),
            Deadline::At(t) => t.to_rfc3339(),
        }
    }

    /// The deadline for `ttl` measured from `origin`.
    fn of(ttl: Ttl, origin: DateTime<Utc>) -> Self {
        match ttl.deadline(origin) {
            Some(t) => Deadline::At(t),
            None => Deadline::Never,
        }
    }
}

/// Write a deadline file into the pod dir.
pub fn write_deadline(
    pod_dir: &Path,
    file: &str,
    ttl: Ttl,
    origin: DateTime<Utc>,
) -> io::Result<Deadline> {
    let deadline = Deadline::of(ttl, origin);
    fs::write(pod_dir.join(file), deadline.encode())?;
    Ok(deadline)
}

/// Read a deadline file. `None` means the file is absent — for
/// [`DELETE_ON_FILE`] that means the pod has not left the runner yet.
/// Unreadable or malformed contents also yield `None` with a warning: an
/// ambiguous deadline must never be treated as due.
pub fn read_deadline(pod_dir: &Path, file: &str) -> Option<Deadline> {
    let path = pod_dir.join(file);
    let raw = match fs::read_to_string(&path) {
        Ok(raw) => raw,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return None,
        Err(e) => {
            warn!(path = ?path, error = %e, "Failed to read deadline file");
            return None;
        }
    };
    let raw = raw.trim();
    if raw == TTL_NEVER {
        return Some(Deadline::Never);
    }
    match DateTime::parse_from_rfc3339(raw) {
        Ok(t) => Some(Deadline::At(t.with_timezone(&Utc))),
        Err(e) => {
            warn!(path = ?path, value = raw, error = %e, "Malformed deadline file");
            None
        }
    }
}

// ---------------------------------------------------------------------------
// Outcome markers
// ---------------------------------------------------------------------------

/// Read the outcome marker from a pod dir, with the completion time it
/// records. `None` means the pod has not been finalized.
pub fn read_outcome(pod_dir: &Path) -> Option<(Outcome, Option<DateTime<Utc>>)> {
    for outcome in [Outcome::Succeeded, Outcome::Failed] {
        let path = pod_dir.join(outcome.marker());
        let Ok(raw) = fs::read_to_string(&path) else {
            continue;
        };
        let completed_at = match DateTime::parse_from_rfc3339(raw.trim()) {
            Ok(t) => Some(t.with_timezone(&Utc)),
            Err(e) => {
                warn!(path = ?path, error = %e, "Malformed outcome marker");
                None
            }
        };
        return Some((outcome, completed_at));
    }
    None
}

// ---------------------------------------------------------------------------
// Finalization
// ---------------------------------------------------------------------------

/// Mark a pod dir as finished: write the outcome marker and the reap
/// deadline, measured from `completed_at`.
///
/// Idempotent — a dir that already carries an outcome marker is left alone,
/// so the monitor and a concurrent delete can both call this and the first
/// one's timestamps stand.
///
/// The TTLs come from the pod's own [`crate::spec::constants::CLEANUP_TTLS_FILE`],
/// written at pod start. If that file is missing the values are resolved
/// from `fallback` config and written out, so the dir always ends up
/// carrying a policy — but per-pod annotations are lost, hence the warning.
pub fn finalize_pod(
    pod_dir: &Path,
    outcome: Outcome,
    fallback: &TtlConfig,
    completed_at: DateTime<Utc>,
) -> Option<Deadline> {
    if !pod_dir.is_dir() {
        debug!(dir = ?pod_dir, "No pod dir to finalize");
        return None;
    }
    if let Some((existing, _)) = read_outcome(pod_dir) {
        debug!(dir = ?pod_dir, ?existing, "Pod dir already finalized");
        return None;
    }

    let ttls = cleanup_ttls_or_fallback(pod_dir, fallback, completed_at);

    if let Err(e) = fs::write(pod_dir.join(outcome.marker()), completed_at.to_rfc3339()) {
        warn!(dir = ?pod_dir, error = %e, "Failed to write outcome marker");
    }
    match write_deadline(
        pod_dir,
        REAP_ON_FILE,
        ttls.reap_ttl(outcome.succeeded()),
        completed_at,
    ) {
        Ok(deadline) => {
            debug!(dir = ?pod_dir, ?outcome, ?deadline, "Pod dir finalized");
            Some(deadline)
        }
        Err(e) => {
            warn!(dir = ?pod_dir, error = %e, "Failed to write reap deadline");
            None
        }
    }
}

/// Record that the pod has left the pod runner, starting the delete clock.
///
/// The outcome comes from the marker in the dir, so eviction needs no
/// in-memory state: a pod that finished before a restart can still be
/// evicted correctly. Idempotent — an existing deadline is left alone.
pub fn mark_pod_evicted(
    pod_dir: &Path,
    fallback: &TtlConfig,
    evicted_at: DateTime<Utc>,
) -> Option<Deadline> {
    if !pod_dir.is_dir() {
        debug!(dir = ?pod_dir, "No pod dir to mark evicted");
        return None;
    }
    if pod_dir.join(DELETE_ON_FILE).exists() {
        debug!(dir = ?pod_dir, "Pod dir already marked evicted");
        return None;
    }
    let Some((outcome, _)) = read_outcome(pod_dir) else {
        warn!(dir = ?pod_dir, "No outcome marker, cannot start the delete clock");
        return None;
    };

    let ttls = cleanup_ttls_or_fallback(pod_dir, fallback, evicted_at);
    match write_deadline(
        pod_dir,
        DELETE_ON_FILE,
        ttls.delete_ttl(outcome.succeeded()),
        evicted_at,
    ) {
        Ok(deadline) => {
            debug!(dir = ?pod_dir, ?deadline, "Pod dir marked evicted");
            Some(deadline)
        }
        Err(e) => {
            warn!(dir = ?pod_dir, error = %e, "Failed to write delete deadline");
            None
        }
    }
}

/// The pod's own TTLs, or config-derived ones written out on the spot.
fn cleanup_ttls_or_fallback(
    pod_dir: &Path,
    fallback: &TtlConfig,
    now: DateTime<Utc>,
) -> CleanupTtls {
    if let Some(ttls) = read_cleanup_ttls(pod_dir) {
        return ttls;
    }
    warn!(
        dir = ?pod_dir,
        "No cleanup TTLs for pod, falling back to configured values",
    );
    let ttls = resolve_cleanup_ttls(fallback, &Default::default(), &now.to_rfc3339());
    if let Err(e) = write_cleanup_ttls(pod_dir, &ttls) {
        warn!(dir = ?pod_dir, error = %e, "Failed to write fallback cleanup TTLs");
    }
    ttls
}

// ---------------------------------------------------------------------------
// Reaping
// ---------------------------------------------------------------------------

/// What [`reap_pod_dir`] did to a pod dir.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReapAction {
    /// No outcome marker: the pod is still live, or never got finalized.
    NotFinished,
    /// A deadline exists but has not been reached.
    Waiting,
    /// The data dirs were deleted.
    Reaped,
    /// The whole pod dir was deleted.
    Deleted,
}

/// The result of evaluating one pod dir.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReapResult {
    /// What was done.
    pub action: ReapAction,
    /// The earliest deadline still ahead, for the caller to schedule.
    pub next_deadline: Option<DateTime<Utc>>,
}

/// Delete the pod's data dirs — `work/` and `volumes/` — plus the stale
/// disk-usage cache, leaving `log/`, `store/` and the markers in place.
///
/// `remove_dir_all` does not follow symlinks, so `hostPath` volumes
/// (symlinks under `volumes/`) and container mount points (symlinks under
/// `work/<container>/`) are unlinked without touching what they point at.
fn reap_data_dirs(pod_dir: &Path) {
    for subdir in [POD_WORK_SUBDIR, VOLUMES_DIR] {
        let dir = pod_dir.join(subdir);
        if !dir.exists() {
            continue;
        }
        match fs::remove_dir_all(&dir) {
            Ok(()) => debug!(dir = ?dir, "Pod data dir reaped"),
            Err(e) => warn!(error = %e, dir = ?dir, "Failed to reap pod data dir"),
        }
    }
    // The cached size describes a footprint that no longer exists.
    let _ = fs::remove_file(pod_dir.join(SIZE_FILE));
}

/// Reap the data dirs and leave the audit trail: the reap deadline is
/// consumed and a [`REAPED_MARKER`] takes its place, so nothing repeats the
/// work and a later reader can see where the data dirs went.
fn reap_data_dirs_and_mark(pod_dir: &Path, now: DateTime<Utc>) {
    if !pod_dir.is_dir() {
        // The whole dir went first — its delete deadline came due before
        // this reap did.
        return;
    }
    reap_data_dirs(pod_dir);
    let _ = fs::remove_file(pod_dir.join(REAP_ON_FILE));
    if let Err(e) = fs::write(pod_dir.join(REAPED_MARKER), now.to_rfc3339()) {
        warn!(error = %e, dir = ?pod_dir, "Failed to write reaped marker");
    }
}

/// How many times [`delete_pod_dir`] walks the dir before giving up. A
/// racing reap refills it at most once — it writes one marker and then
/// finishes — so the second walk is the one that succeeds and the third is
/// margin.
const DELETE_ATTEMPTS: u32 = 3;

/// Delete the whole pod dir, record included. Returns whether it went.
fn delete_pod_dir(pod_dir: &Path) -> bool {
    delete_pod_dir_with(pod_dir, |dir| fs::remove_dir_all(dir))
}

/// [`delete_pod_dir`] over an injectable removal, so the retry is testable
/// without having to win a race against a real walk.
///
/// `ENOTEMPTY` means an entry appeared while the walk was in flight. Since
/// `readdir` batches, anything created after the last batch was read is
/// missed by the walk and only surfaces when the final `rmdir` refuses. The
/// only writer that can be racing here is this pod's own reap task: both
/// deadlines of one pod come due together when it is deleted while still
/// live, and the reap drops a [`REAPED_MARKER`] into a dir this call had
/// already emptied. It writes that marker once and then finishes, so walking
/// again converges rather than chasing a moving target.
///
/// Left unhandled this stranded the pod dir holding nothing but that marker:
/// nothing reclaims it later, because the walk took the outcome marker that
/// [`reap_pod_dir`] needs and [`reap_previous_run_pod_dirs`] skips a dir that is
/// already marked reaped — and the marker it kept is a false record, claiming
/// `log/` and `store/` were preserved when the walk had destroyed them.
fn delete_pod_dir_with(pod_dir: &Path, remove: impl Fn(&Path) -> io::Result<()>) -> bool {
    for attempt in 1..=DELETE_ATTEMPTS {
        match remove(pod_dir) {
            Ok(()) => {
                debug!(dir = ?pod_dir, "Pod dir deleted");
                return true;
            }
            // Already gone — nothing to report.
            Err(e) if e.kind() == io::ErrorKind::NotFound => return false,
            Err(e) if e.kind() == io::ErrorKind::DirectoryNotEmpty && attempt < DELETE_ATTEMPTS => {
                debug!(
                    dir = ?pod_dir,
                    attempt,
                    "Pod dir refilled while being deleted, walking it again",
                );
            }
            Err(e) => {
                warn!(error = %e, dir = ?pod_dir, "Failed to delete pod dir");
                return false;
            }
        }
    }
    false
}

/// Apply the reap and delete deadlines to one pod dir.
///
/// Delete is checked first: when both deadlines are due there is no point
/// reaping a dir that is about to go.
pub fn reap_pod_dir(pod_dir: &Path, now: DateTime<Utc>) -> ReapResult {
    let no_deadline = |action| ReapResult {
        action,
        next_deadline: None,
    };

    if read_outcome(pod_dir).is_none() {
        // Either the pod is still running, or it died without ever being
        // finalized. Nothing here is safe to remove on a schedule.
        return no_deadline(ReapAction::NotFinished);
    }

    let delete_on = read_deadline(pod_dir, DELETE_ON_FILE);
    if let Some(deadline) = delete_on
        && deadline.is_due(now)
    {
        return match delete_pod_dir(pod_dir) {
            true => no_deadline(ReapAction::Deleted),
            false => no_deadline(ReapAction::Waiting),
        };
    }

    // Not yet evicted, or evicted with a deadline still ahead.
    let mut next = delete_on.and_then(|d| d.instant());

    let reap_on = read_deadline(pod_dir, REAP_ON_FILE);
    let mut action = ReapAction::Waiting;
    if let Some(deadline) = reap_on {
        if deadline.is_due(now) {
            reap_data_dirs_and_mark(pod_dir, now);
            action = ReapAction::Reaped;
        } else {
            next = earliest(next, deadline.instant());
        }
    }

    ReapResult {
        action,
        next_deadline: next,
    }
}

/// The earlier of two optional instants.
fn earliest(a: Option<DateTime<Utc>>, b: Option<DateTime<Utc>>) -> Option<DateTime<Utc>> {
    match (a, b) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (a, None) => a,
        (None, b) => b,
    }
}

/// Reap the data dirs of every pod in a previous run's `pods/` dir.
///
/// These pods belonged to a previous incarnation of the runner. Nothing
/// will resume them and nothing will read `work/` or `volumes/` again, so
/// the bulk of the footprint goes at startup rather than waiting out each
/// pod's own reap deadline. This also covers pods the runner never got to
/// finalize — a `SIGKILL`ed runner leaves dirs with no marker at all, and
/// those would otherwise sit untouched.
///
/// `log/` and `store/` stay: that record is what makes a crashed run
/// diagnosable, and it goes when its run dir is purged.
///
/// An explicit `never` reap deadline is honoured — it means someone asked to
/// keep the data dirs for inspection, and a restart is no reason to overrule
/// that.
///
/// Returns how many pod dirs were reaped.
pub fn reap_previous_run_pod_dirs(pods_dir: &Path, now: DateTime<Utc>) -> u32 {
    let Ok(entries) = fs::read_dir(pods_dir) else {
        return 0;
    };

    let mut reaped = 0u32;
    for entry in entries.filter_map(|e| e.ok()) {
        let pod_dir = entry.path();
        if !pod_dir.is_dir() {
            continue;
        }
        if read_deadline(&pod_dir, REAP_ON_FILE) == Some(Deadline::Never) {
            debug!(dir = ?pod_dir, "Rotated pod dir kept: reap TTL is never");
            continue;
        }
        if pod_dir.join(REAPED_MARKER).exists() {
            continue;
        }

        reap_data_dirs_and_mark(&pod_dir, now);
        reaped += 1;
    }

    if reaped > 0 {
        info!(reaped, dir = ?pods_dir, "Reaped rotated pod dirs");
    }
    reaped
}

// ---------------------------------------------------------------------------
// Deadline timers
// ---------------------------------------------------------------------------

/// What a scheduled deadline does when it comes due.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScheduledReap {
    /// Reap `work/` and `volumes/`, keeping the record — the [`REAP_ON_FILE`]
    /// deadline.
    DataDirs,
    /// Delete the whole pod dir — the [`DELETE_ON_FILE`] deadline.
    PodDir,
}

/// Wait out a pod dir deadline and act on it.
///
/// One task per deadline, sleeping until it comes due. There is no scanning
/// daemon: a deadline is known the moment it is written, so the runner waits
/// on it directly instead of re-reading every finished pod dir to rediscover
/// what it already knew. A deadline already due needs no task — the caller's
/// inline [`reap_pod_dir`] has just acted on it — and [`Deadline::Never`]
/// never comes due.
///
/// The task is the authority on its own deadline: when the sleep elapses it
/// acts, without re-reading the file it came from. The deadline files stay
/// on disk as the record of what was scheduled, but nothing reads them back
/// to decide. Both actions tolerate a dir that is already gone, which is
/// what makes the two deadlines of one pod independent.
///
/// The sleep is raced against `shutdown`, so a pending deadline never holds
/// the runner's exit, and whatever it would have done is dropped. The
/// runner keeps no pod state between runs: the next run reclaims the whole
/// previous run at startup via [`reap_previous_run_pod_dirs`].
///
/// `shutdown` must be a receiver that already existed when shutdown was
/// signalled — a `subscribe()` afterwards cannot observe it — which is why
/// callers pass one they are already holding.
pub fn schedule_deadline(
    pod_dir: &Path,
    action: ScheduledReap,
    deadline: Option<Deadline>,
    now: DateTime<Utc>,
    shutdown: &watch::Receiver<()>,
) {
    let Some(deadline) = deadline else {
        return;
    };
    // Already due — the caller's inline reap covered it. `is_due` rather than
    // a negative-duration check, so a deadline landing exactly on `now` (a
    // `0s` TTL, whose deadline *is* the completion instant) needs no task.
    if deadline.is_due(now) {
        return;
    }
    let Some(instant) = deadline.instant() else {
        // `never` — nothing to wait for.
        return;
    };
    let Ok(delay) = (instant - now).to_std() else {
        return;
    };

    // Same `pod=<name>_<uid>` identity as every other pod-scoped log line,
    // recovered from the dir name.
    let pod = pod_dir
        .file_name()
        .and_then(|n| n.to_str())
        .and_then(PodId::from_full)
        .map(|id| id.to_string())
        .unwrap_or_default();

    let pod_dir = pod_dir.to_path_buf();
    let mut shutdown = shutdown.clone();
    tokio::spawn(
        async move {
            debug!(dir = ?pod_dir, ?action, ?instant, "Deadline scheduled");
            tokio::select! {
                _ = tokio::time::sleep(delay) => {}
                _ = shutdown.changed() => {
                    debug!(dir = ?pod_dir, ?action, "Deadline dropped by shutdown");
                    return;
                }
            }
            match action {
                ScheduledReap::DataDirs => reap_data_dirs_and_mark(&pod_dir, Utc::now()),
                ScheduledReap::PodDir => {
                    delete_pod_dir(&pod_dir);
                }
            }
        }
        // Root span: the task outlives the scope that scheduled it, so it
        // must not inherit that scope's pod span.
        .instrument(debug_span!(parent: None, "reap", pod = %pod)),
    );
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logic::ttl::{TtlSource, TtlSpec};
    use crate::spec::constants::{
        ANN_POD_DATA_REAP_TTL_ON_POD_FAILURE, ANN_POD_DIR_DELETE_TTL_POD_FAILURE_ON_POD_DELETE,
        CLEANUP_TTLS_FILE,
    };
    use std::collections::HashMap;
    use std::time::Duration;

    fn tmp(name: &str) -> std::path::PathBuf {
        let dir =
            std::env::temp_dir().join(format!("pod-runner-reaper-{}-{}", name, std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn at(rfc3339: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(rfc3339)
            .unwrap()
            .with_timezone(&Utc)
    }

    const T0: &str = "2026-08-04T09:00:00+00:00";

    /// Write the pod's TTLs as a pod start would, with optional annotations.
    fn plant_ttls(pod_dir: &Path, annotations: &[(&str, &str)]) {
        let ttls = resolve_cleanup_ttls(
            &TtlConfig::default(),
            &annotations
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect::<HashMap<_, _>>(),
            T0,
        );
        write_cleanup_ttls(pod_dir, &ttls).unwrap();
    }

    // -- Outcome --

    #[test]
    fn outcome_from_terminal_phases_only() {
        assert_eq!(
            Outcome::from_phase(PodPhase::Succeeded),
            Some(Outcome::Succeeded)
        );
        assert_eq!(Outcome::from_phase(PodPhase::Failed), Some(Outcome::Failed));
        assert_eq!(Outcome::from_phase(PodPhase::Running), None);
        assert_eq!(Outcome::from_phase(PodPhase::Pending), None);
    }

    #[test]
    fn outcome_markers_are_distinct() {
        assert_eq!(Outcome::Succeeded.marker(), SUCCEEDED_MARKER);
        assert_eq!(Outcome::Failed.marker(), FAILED_MARKER);
        assert!(Outcome::Succeeded.succeeded());
        assert!(!Outcome::Failed.succeeded());
    }

    // -- Deadline --

    #[test]
    fn deadline_due_only_after_the_instant() {
        let d = Deadline::At(at(T0));
        assert!(!d.is_due(at("2026-08-04T08:59:59+00:00")));
        assert!(d.is_due(at(T0)), "due exactly at the instant");
        assert!(d.is_due(at("2026-08-04T09:00:01+00:00")));
    }

    #[test]
    fn never_is_never_due() {
        assert!(!Deadline::Never.is_due(at("2099-01-01T00:00:00+00:00")));
        assert_eq!(Deadline::Never.instant(), None);
    }

    #[test]
    fn write_then_read_deadline_round_trips() {
        let dir = tmp("deadline-round-trip");
        let written =
            write_deadline(&dir, REAP_ON_FILE, Ttl::parse("1h").unwrap(), at(T0)).unwrap();
        assert_eq!(written, Deadline::At(at("2026-08-04T10:00:00+00:00")));
        assert_eq!(read_deadline(&dir, REAP_ON_FILE), Some(written));
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn write_never_deadline_writes_the_literal() {
        let dir = tmp("deadline-never");
        let written = write_deadline(&dir, REAP_ON_FILE, Ttl::Never, at(T0)).unwrap();
        assert_eq!(written, Deadline::Never);
        assert_eq!(
            fs::read_to_string(dir.join(REAP_ON_FILE)).unwrap(),
            TTL_NEVER,
            "never is written out literally, not as an absent file",
        );
        assert_eq!(read_deadline(&dir, REAP_ON_FILE), Some(Deadline::Never));
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn zero_ttl_deadline_is_the_origin() {
        let dir = tmp("deadline-zero");
        let written =
            write_deadline(&dir, REAP_ON_FILE, Ttl::parse("0s").unwrap(), at(T0)).unwrap();
        assert_eq!(written, Deadline::At(at(T0)));
        assert!(written.is_due(at(T0)), "0s is due immediately");
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn read_missing_deadline_is_none() {
        let dir = tmp("deadline-missing");
        assert_eq!(read_deadline(&dir, DELETE_ON_FILE), None);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn read_malformed_deadline_is_none() {
        let dir = tmp("deadline-malformed");
        fs::write(dir.join(REAP_ON_FILE), "tomorrow-ish").unwrap();
        assert_eq!(
            read_deadline(&dir, REAP_ON_FILE),
            None,
            "an ambiguous deadline must not be treated as due",
        );
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn read_deadline_tolerates_trailing_newline() {
        let dir = tmp("deadline-newline");
        fs::write(dir.join(REAP_ON_FILE), format!("{}\n", T0)).unwrap();
        assert_eq!(
            read_deadline(&dir, REAP_ON_FILE),
            Some(Deadline::At(at(T0)))
        );
        let _ = fs::remove_dir_all(&dir);
    }

    // -- finalize_pod --

    #[test]
    fn finalize_writes_marker_and_reap_deadline() {
        let dir = tmp("finalize-success");
        plant_ttls(&dir, &[]);

        finalize_pod(&dir, Outcome::Succeeded, &TtlConfig::default(), at(T0));

        assert_eq!(
            fs::read_to_string(dir.join(SUCCEEDED_MARKER)).unwrap(),
            T0,
            "marker records the completion time",
        );
        assert!(!dir.join(FAILED_MARKER).exists(), "markers are exclusive");
        // Default reap-on-success is 0s, so the deadline is the completion.
        assert_eq!(
            read_deadline(&dir, REAP_ON_FILE),
            Some(Deadline::At(at(T0)))
        );
        assert!(
            !dir.join(DELETE_ON_FILE).exists(),
            "the delete clock starts at eviction, not completion",
        );
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn finalize_failed_uses_the_error_ttl() {
        let dir = tmp("finalize-failed");
        plant_ttls(&dir, &[]);

        finalize_pod(&dir, Outcome::Failed, &TtlConfig::default(), at(T0));

        assert!(dir.join(FAILED_MARKER).exists());
        assert!(!dir.join(SUCCEEDED_MARKER).exists());
        // Default reap-on-error is 1h.
        assert_eq!(
            read_deadline(&dir, REAP_ON_FILE),
            Some(Deadline::At(at("2026-08-04T10:00:00+00:00")))
        );
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn finalize_honors_the_pods_own_annotated_ttl() {
        let dir = tmp("finalize-annotated");
        plant_ttls(&dir, &[(ANN_POD_DATA_REAP_TTL_ON_POD_FAILURE, "15m")]);

        finalize_pod(&dir, Outcome::Failed, &TtlConfig::default(), at(T0));

        assert_eq!(
            read_deadline(&dir, REAP_ON_FILE),
            Some(Deadline::At(at("2026-08-04T09:15:00+00:00"))),
            "the annotation on the pod, not the runner default",
        );
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn finalize_writes_never_for_a_never_ttl() {
        let dir = tmp("finalize-never");
        plant_ttls(&dir, &[(ANN_POD_DATA_REAP_TTL_ON_POD_FAILURE, TTL_NEVER)]);

        finalize_pod(&dir, Outcome::Failed, &TtlConfig::default(), at(T0));

        assert_eq!(read_deadline(&dir, REAP_ON_FILE), Some(Deadline::Never));
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn finalize_is_idempotent() {
        let dir = tmp("finalize-twice");
        plant_ttls(&dir, &[]);

        finalize_pod(&dir, Outcome::Failed, &TtlConfig::default(), at(T0));
        // A concurrent delete finalizing later must not move the clock or
        // change the recorded outcome.
        finalize_pod(
            &dir,
            Outcome::Succeeded,
            &TtlConfig::default(),
            at("2026-08-04T12:00:00+00:00"),
        );

        assert!(dir.join(FAILED_MARKER).exists(), "first outcome stands");
        assert!(!dir.join(SUCCEEDED_MARKER).exists());
        assert_eq!(
            fs::read_to_string(dir.join(FAILED_MARKER)).unwrap(),
            T0,
            "first timestamp stands",
        );
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn finalize_falls_back_to_config_without_a_ttls_file() {
        let dir = tmp("finalize-no-ttls");
        // No plant_ttls: simulate a pod whose TTL file never got written.
        let fallback = TtlConfig {
            reap_on_failure: TtlSpec::parse("5m").unwrap(),
            ..TtlConfig::default()
        };

        finalize_pod(&dir, Outcome::Failed, &fallback, at(T0));

        assert_eq!(
            read_deadline(&dir, REAP_ON_FILE),
            Some(Deadline::At(at("2026-08-04T09:05:00+00:00")))
        );
        assert!(
            dir.join(CLEANUP_TTLS_FILE).exists(),
            "the fallback policy is written out so the dir carries one",
        );
        let ttls = read_cleanup_ttls(&dir).unwrap();
        assert_eq!(ttls.reap_ttl_on_failure.source, TtlSource::Config);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn finalize_skips_a_missing_pod_dir() {
        let dir = tmp("finalize-missing").join("gone");
        finalize_pod(&dir, Outcome::Failed, &TtlConfig::default(), at(T0));
        assert!(!dir.exists(), "no dir is created for a pod that has none");
        let _ = fs::remove_dir_all(dir.parent().unwrap());
    }

    // -- read_outcome --

    #[test]
    fn read_outcome_returns_marker_and_time() {
        let dir = tmp("read-outcome");
        plant_ttls(&dir, &[]);
        finalize_pod(&dir, Outcome::Succeeded, &TtlConfig::default(), at(T0));

        assert_eq!(read_outcome(&dir), Some((Outcome::Succeeded, Some(at(T0)))));
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn read_outcome_of_unfinalized_dir_is_none() {
        let dir = tmp("read-outcome-none");
        assert_eq!(read_outcome(&dir), None);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn read_outcome_survives_a_malformed_timestamp() {
        let dir = tmp("read-outcome-bad-time");
        fs::write(dir.join(FAILED_MARKER), "whenever").unwrap();
        assert_eq!(
            read_outcome(&dir),
            Some((Outcome::Failed, None)),
            "the outcome still counts even if the time is unreadable",
        );
        let _ = fs::remove_dir_all(&dir);
    }

    // -- mark_pod_evicted --

    #[test]
    fn eviction_starts_the_delete_clock() {
        let dir = tmp("evict");
        plant_ttls(&dir, &[]);
        finalize_pod(&dir, Outcome::Succeeded, &TtlConfig::default(), at(T0));

        // Evicted two hours after completion: the delete deadline runs from
        // the eviction, not from the completion.
        let evicted = at("2026-08-04T11:00:00+00:00");
        mark_pod_evicted(&dir, &TtlConfig::default(), evicted);

        assert_eq!(
            read_deadline(&dir, DELETE_ON_FILE),
            Some(Deadline::At(at("2026-08-04T12:00:00+00:00"))),
            "1h delete-on-success measured from eviction",
        );
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn eviction_uses_the_outcome_from_the_marker() {
        let dir = tmp("evict-failed");
        plant_ttls(&dir, &[]);
        finalize_pod(&dir, Outcome::Failed, &TtlConfig::default(), at(T0));

        mark_pod_evicted(&dir, &TtlConfig::default(), at(T0));

        // Default delete-on-error is 24h.
        assert_eq!(
            read_deadline(&dir, DELETE_ON_FILE),
            Some(Deadline::At(at("2026-08-05T09:00:00+00:00")))
        );
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn eviction_writes_never_for_a_never_ttl() {
        let dir = tmp("evict-never");
        plant_ttls(
            &dir,
            &[(ANN_POD_DIR_DELETE_TTL_POD_FAILURE_ON_POD_DELETE, TTL_NEVER)],
        );
        finalize_pod(&dir, Outcome::Failed, &TtlConfig::default(), at(T0));

        mark_pod_evicted(&dir, &TtlConfig::default(), at(T0));

        assert_eq!(read_deadline(&dir, DELETE_ON_FILE), Some(Deadline::Never));
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn eviction_is_idempotent() {
        let dir = tmp("evict-twice");
        plant_ttls(&dir, &[]);
        finalize_pod(&dir, Outcome::Succeeded, &TtlConfig::default(), at(T0));

        mark_pod_evicted(&dir, &TtlConfig::default(), at(T0));
        mark_pod_evicted(&dir, &TtlConfig::default(), at("2026-08-04T20:00:00+00:00"));

        assert_eq!(
            read_deadline(&dir, DELETE_ON_FILE),
            Some(Deadline::At(at("2026-08-04T10:00:00+00:00"))),
            "the first eviction's clock stands",
        );
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn eviction_without_an_outcome_marker_is_skipped() {
        let dir = tmp("evict-unfinalized");
        plant_ttls(&dir, &[]);

        mark_pod_evicted(&dir, &TtlConfig::default(), at(T0));

        assert!(
            !dir.join(DELETE_ON_FILE).exists(),
            "a pod that never finished has no outcome to pick a TTL by",
        );
        let _ = fs::remove_dir_all(&dir);
    }

    // -- reap_pod_dir --

    /// A pod dir with data dirs, logs, a store and a size cache, as a real
    /// one has by the time it finishes.
    fn plant_pod_dir(pod_dir: &Path, annotations: &[(&str, &str)]) {
        fs::create_dir_all(pod_dir.join(POD_WORK_SUBDIR).join("main")).unwrap();
        fs::create_dir_all(pod_dir.join(VOLUMES_DIR).join("data")).unwrap();
        fs::create_dir_all(
            pod_dir
                .join(crate::server::config::POD_LOG_SUBDIR)
                .join("main"),
        )
        .unwrap();
        fs::write(
            pod_dir
                .join(crate::server::config::POD_LOG_SUBDIR)
                .join("main")
                .join("0.log"),
            "output",
        )
        .unwrap();
        fs::create_dir_all(pod_dir.join(crate::server::config::POD_STORE_SUBDIR)).unwrap();
        fs::write(
            pod_dir
                .join(crate::server::config::POD_STORE_SUBDIR)
                .join("original-spec.json"),
            "{}",
        )
        .unwrap();
        fs::write(pod_dir.join(SIZE_FILE), "{}").unwrap();
        plant_ttls(pod_dir, annotations);
    }

    fn data_dirs_present(pod_dir: &Path) -> bool {
        pod_dir.join(POD_WORK_SUBDIR).exists() || pod_dir.join(VOLUMES_DIR).exists()
    }

    fn record_present(pod_dir: &Path) -> bool {
        pod_dir.join(crate::server::config::POD_LOG_SUBDIR).exists()
            && pod_dir
                .join(crate::server::config::POD_STORE_SUBDIR)
                .exists()
    }

    #[test]
    fn unfinished_pod_dir_is_left_alone() {
        let dir = tmp("reap-unfinished");
        plant_pod_dir(&dir, &[]);

        // No outcome marker: the pod may still be running.
        let result = reap_pod_dir(&dir, at("2099-01-01T00:00:00+00:00"));

        assert_eq!(result.action, ReapAction::NotFinished);
        assert!(data_dirs_present(&dir), "a live pod keeps its data dirs");
        assert_eq!(result.next_deadline, None);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn reap_zero_ttl_takes_data_dirs_and_keeps_the_record() {
        let dir = tmp("reap-zero");
        plant_pod_dir(&dir, &[]);
        // Default reap-on-success is 0s.
        finalize_pod(&dir, Outcome::Succeeded, &TtlConfig::default(), at(T0));

        // finalize_pod does not reap; the caller's inline pass does.
        let result = reap_pod_dir(&dir, at(T0));

        assert_eq!(result.action, ReapAction::Reaped);
        assert!(!data_dirs_present(&dir), "work/ and volumes/ are gone");
        assert!(record_present(&dir), "log/ and store/ survive the reap");
        assert!(
            !dir.join(SIZE_FILE).exists(),
            "the stale size cache is gone"
        );
        assert!(dir.join(REAPED_MARKER).exists());
        assert!(
            !dir.join(REAP_ON_FILE).exists(),
            "the deadline is consumed once acted on",
        );
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn reap_waits_until_the_deadline() {
        let dir = tmp("reap-waits");
        plant_pod_dir(&dir, &[]);
        // Default reap-on-error is 1h.
        finalize_pod(&dir, Outcome::Failed, &TtlConfig::default(), at(T0));

        let result = reap_pod_dir(&dir, at("2026-08-04T09:59:59+00:00"));

        assert_eq!(result.action, ReapAction::Waiting);
        assert!(data_dirs_present(&dir));
        assert_eq!(
            result.next_deadline,
            Some(at("2026-08-04T10:00:00+00:00")),
            "the pending deadline is reported for scheduling",
        );
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn reap_happens_once_the_deadline_passes() {
        let dir = tmp("reap-due");
        plant_pod_dir(&dir, &[]);
        finalize_pod(&dir, Outcome::Failed, &TtlConfig::default(), at(T0));

        let result = reap_pod_dir(&dir, at("2026-08-04T10:00:01+00:00"));

        assert_eq!(result.action, ReapAction::Reaped);
        assert!(!data_dirs_present(&dir));
        assert!(record_present(&dir), "the archive-less record must survive");
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn reap_is_not_repeated() {
        let dir = tmp("reap-once");
        plant_pod_dir(&dir, &[]);
        finalize_pod(&dir, Outcome::Succeeded, &TtlConfig::default(), at(T0));

        assert_eq!(reap_pod_dir(&dir, at(T0)).action, ReapAction::Reaped);
        let reaped_at = fs::read_to_string(dir.join(REAPED_MARKER)).unwrap();

        let again = reap_pod_dir(&dir, at("2026-08-04T09:30:00+00:00"));

        assert_eq!(again.action, ReapAction::Waiting, "no second reap");
        assert_eq!(
            fs::read_to_string(dir.join(REAPED_MARKER)).unwrap(),
            reaped_at,
            "the original reap time stands",
        );
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn never_reap_ttl_is_never_reaped() {
        let dir = tmp("reap-never");
        plant_pod_dir(&dir, &[(ANN_POD_DATA_REAP_TTL_ON_POD_FAILURE, TTL_NEVER)]);
        finalize_pod(&dir, Outcome::Failed, &TtlConfig::default(), at(T0));

        let result = reap_pod_dir(&dir, at("2099-01-01T00:00:00+00:00"));

        assert_eq!(result.action, ReapAction::Waiting);
        assert!(data_dirs_present(&dir), "never means never");
        assert_eq!(
            result.next_deadline, None,
            "nothing to schedule for a never deadline",
        );
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn delete_removes_the_whole_pod_dir() {
        let dir = tmp("delete-due");
        plant_pod_dir(&dir, &[]);
        finalize_pod(&dir, Outcome::Succeeded, &TtlConfig::default(), at(T0));
        mark_pod_evicted(&dir, &TtlConfig::default(), at(T0));

        // Default delete-on-success is 1h from eviction.
        let result = reap_pod_dir(&dir, at("2026-08-04T10:00:00+00:00"));

        assert_eq!(result.action, ReapAction::Deleted);
        assert!(!dir.exists(), "the record goes with the dir");
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn delete_waits_until_the_deadline() {
        let dir = tmp("delete-waits");
        plant_pod_dir(&dir, &[]);
        finalize_pod(&dir, Outcome::Failed, &TtlConfig::default(), at(T0));
        mark_pod_evicted(&dir, &TtlConfig::default(), at(T0));

        // Reap at 1h, delete at 24h: the data dirs go, the record stays.
        let result = reap_pod_dir(&dir, at("2026-08-04T11:00:00+00:00"));

        assert_eq!(result.action, ReapAction::Reaped);
        assert!(dir.exists());
        assert!(!data_dirs_present(&dir));
        assert!(
            record_present(&dir),
            "the record outlives the data dirs — this is what makes a \
             failure explainable after the disk is reclaimed",
        );
        assert_eq!(
            result.next_deadline,
            Some(at("2026-08-05T09:00:00+00:00")),
            "the delete deadline is what remains pending",
        );
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn delete_wins_when_both_deadlines_are_due() {
        let dir = tmp("delete-wins");
        plant_pod_dir(&dir, &[]);
        finalize_pod(&dir, Outcome::Failed, &TtlConfig::default(), at(T0));
        mark_pod_evicted(&dir, &TtlConfig::default(), at(T0));

        // Past both the 1h reap and the 24h delete: no point reaping a dir
        // that is about to go.
        let result = reap_pod_dir(&dir, at("2026-08-06T00:00:00+00:00"));

        assert_eq!(result.action, ReapAction::Deleted);
        assert!(!dir.exists());
        assert!(
            !dir.join(REAPED_MARKER).exists(),
            "no wasted reap before the delete",
        );
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn never_delete_ttl_keeps_the_dir_forever() {
        let dir = tmp("delete-never");
        plant_pod_dir(
            &dir,
            &[(ANN_POD_DIR_DELETE_TTL_POD_FAILURE_ON_POD_DELETE, TTL_NEVER)],
        );
        finalize_pod(&dir, Outcome::Failed, &TtlConfig::default(), at(T0));
        mark_pod_evicted(&dir, &TtlConfig::default(), at(T0));

        let result = reap_pod_dir(&dir, at("2099-01-01T00:00:00+00:00"));

        // The reap still happens; only the delete is disabled.
        assert_eq!(result.action, ReapAction::Reaped);
        assert!(dir.exists(), "never means never");
        assert!(record_present(&dir));
        assert_eq!(result.next_deadline, None);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn unevicted_pod_dir_is_never_deleted() {
        let dir = tmp("delete-unevicted");
        plant_pod_dir(&dir, &[]);
        finalize_pod(&dir, Outcome::Succeeded, &TtlConfig::default(), at(T0));
        // No mark_pod_evicted: the pod is still in the runner.

        let result = reap_pod_dir(&dir, at("2099-01-01T00:00:00+00:00"));

        assert_eq!(result.action, ReapAction::Reaped);
        assert!(
            dir.exists(),
            "the delete clock has not started, so the dir stays",
        );
        assert!(record_present(&dir));
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn malformed_deadline_is_never_acted_on() {
        let dir = tmp("reap-malformed");
        plant_pod_dir(&dir, &[]);
        finalize_pod(&dir, Outcome::Failed, &TtlConfig::default(), at(T0));
        fs::write(dir.join(REAP_ON_FILE), "soon").unwrap();
        fs::write(dir.join(DELETE_ON_FILE), "later").unwrap();

        let result = reap_pod_dir(&dir, at("2099-01-01T00:00:00+00:00"));

        assert_eq!(result.action, ReapAction::Waiting);
        assert!(dir.exists(), "an unreadable deadline never deletes");
        assert!(
            data_dirs_present(&dir),
            "an unreadable deadline never reaps"
        );
        let _ = fs::remove_dir_all(&dir);
    }

    // -- reap_previous_run_pod_dirs --

    #[test]
    fn rotation_reaps_finished_pods_and_keeps_the_record() {
        let root = tmp("rotate-finished");
        let pods = root.join("run.0").join("pods");
        let pod_dir = pods.join("default_app_uid-1");
        plant_pod_dir(&pod_dir, &[]);
        // Failed, so its own reap deadline is an hour out.
        finalize_pod(&pod_dir, Outcome::Failed, &TtlConfig::default(), at(T0));

        assert_eq!(reap_previous_run_pod_dirs(&pods, at(T0)), 1);

        assert!(
            !data_dirs_present(&pod_dir),
            "rotation reaps regardless of the pod's own deadline",
        );
        assert!(record_present(&pod_dir), "log/ and store/ survive rotation");
        assert!(pod_dir.join(REAPED_MARKER).exists());
        assert!(!pod_dir.join(REAP_ON_FILE).exists());
        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn rotation_reaps_pods_that_never_finished() {
        let root = tmp("rotate-unfinished");
        let pods = root.join("run.0").join("pods");
        let pod_dir = pods.join("default_killed_uid-1");
        // No marker at all: the runner was killed while this pod ran.
        plant_pod_dir(&pod_dir, &[]);

        assert_eq!(reap_previous_run_pod_dirs(&pods, at(T0)), 1);

        assert!(
            !data_dirs_present(&pod_dir),
            "a pod the runner never finalized still gets its data dirs back",
        );
        assert!(record_present(&pod_dir));
        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn rotation_honors_a_never_reap_deadline() {
        let root = tmp("rotate-never");
        let pods = root.join("run.0").join("pods");
        let pod_dir = pods.join("default_keep_uid-1");
        plant_pod_dir(
            &pod_dir,
            &[(ANN_POD_DATA_REAP_TTL_ON_POD_FAILURE, TTL_NEVER)],
        );
        finalize_pod(&pod_dir, Outcome::Failed, &TtlConfig::default(), at(T0));

        assert_eq!(reap_previous_run_pod_dirs(&pods, at(T0)), 0);

        assert!(
            data_dirs_present(&pod_dir),
            "a restart must not overrule an explicit never",
        );
        assert!(!pod_dir.join(REAPED_MARKER).exists());
        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn rotation_skips_already_reaped_pods() {
        let root = tmp("rotate-already");
        let pods = root.join("run.0").join("pods");
        let pod_dir = pods.join("default_done_uid-1");
        plant_pod_dir(&pod_dir, &[]);
        finalize_pod(&pod_dir, Outcome::Succeeded, &TtlConfig::default(), at(T0));
        assert_eq!(reap_pod_dir(&pod_dir, at(T0)).action, ReapAction::Reaped);
        let reaped_at = fs::read_to_string(pod_dir.join(REAPED_MARKER)).unwrap();

        assert_eq!(
            reap_previous_run_pod_dirs(&pods, at("2026-08-04T12:00:00+00:00")),
            0,
        );

        assert_eq!(
            fs::read_to_string(pod_dir.join(REAPED_MARKER)).unwrap(),
            reaped_at,
            "the original reap time stands",
        );
        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn rotation_tolerates_a_missing_pods_dir() {
        let root = tmp("rotate-missing");
        assert_eq!(
            reap_previous_run_pod_dirs(&root.join("run.0/pods"), at(T0)),
            0
        );
        let _ = fs::remove_dir_all(&root);
    }

    // -- delete_pod_dir retry --

    /// The reap's marker landing in the tail of the delete's walk must not
    /// strand the dir: the second walk takes it. Injected rather than raced,
    /// so this covers the retry on every run and every platform.
    #[test]
    fn delete_walks_again_when_a_racing_reap_refills_the_dir() {
        let dir = tmp("delete-refilled");
        plant_pod_dir(&dir, &[]);

        let attempts = std::cell::Cell::new(0u32);
        let deleted = delete_pod_dir_with(&dir, |pod_dir| {
            attempts.set(attempts.get() + 1);
            if attempts.get() == 1 {
                // Exactly what the racing reap leaves behind: the walk
                // emptied the dir, then the marker landed in it, so the
                // final `rmdir` refuses.
                for entry in fs::read_dir(pod_dir).unwrap().filter_map(|e| e.ok()) {
                    let path = entry.path();
                    if path.is_dir() {
                        fs::remove_dir_all(&path).unwrap();
                    } else {
                        fs::remove_file(&path).unwrap();
                    }
                }
                fs::write(pod_dir.join(REAPED_MARKER), "stamp").unwrap();
                return Err(io::Error::from(io::ErrorKind::DirectoryNotEmpty));
            }
            fs::remove_dir_all(pod_dir)
        });

        assert!(deleted, "the retry must finish what the first walk started");
        assert_eq!(attempts.get(), 2, "one refill needs exactly one retry");
        assert!(!dir.exists(), "the pod dir must not survive as a shell");
    }

    /// Bounded, so a dir being refilled faster than it can be walked fails
    /// loudly instead of spinning.
    #[test]
    fn delete_gives_up_after_a_bounded_number_of_walks() {
        let dir = tmp("delete-refilled-forever");
        plant_pod_dir(&dir, &[]);

        let attempts = std::cell::Cell::new(0u32);
        let deleted = delete_pod_dir_with(&dir, |_| {
            attempts.set(attempts.get() + 1);
            Err(io::Error::from(io::ErrorKind::DirectoryNotEmpty))
        });

        assert!(!deleted);
        assert_eq!(attempts.get(), DELETE_ATTEMPTS, "bounded, not a spin");
        let _ = fs::remove_dir_all(&dir);
    }

    /// A dir already taken by its own reap's delete, or by rotation, is not
    /// worth a second walk.
    #[test]
    fn delete_of_an_absent_dir_is_not_retried() {
        let attempts = std::cell::Cell::new(0u32);
        let deleted = delete_pod_dir_with(&tmp("delete-absent"), |_| {
            attempts.set(attempts.get() + 1);
            Err(io::Error::from(io::ErrorKind::NotFound))
        });

        assert!(!deleted);
        assert_eq!(attempts.get(), 1, "a dir already gone needs no retry");
    }

    // -- schedule_deadline --

    /// The replacement for the reaping daemon: a deadline still ahead is
    /// waited out by its own task, with no scan in between.
    #[tokio::test(start_paused = true)]
    async fn a_scheduled_reap_acts_when_its_deadline_comes_due() {
        let pod_dir = tmp("schedule-reap").join("default_app_uid-1");
        plant_pod_dir(&pod_dir, &[]);
        let (_tx, rx) = watch::channel(());

        let deadline = Utc::now() + chrono::Duration::hours(1);
        schedule_deadline(
            &pod_dir,
            ScheduledReap::DataDirs,
            Some(Deadline::At(deadline)),
            Utc::now(),
            &rx,
        );
        assert!(
            pod_dir.join(POD_WORK_SUBDIR).exists(),
            "the data dirs must survive until the deadline",
        );

        // Paused time: the task's sleep is the only thing being waited on.
        tokio::time::sleep(Duration::from_secs(3601)).await;
        tokio::task::yield_now().await;

        assert!(
            !pod_dir.join(POD_WORK_SUBDIR).exists(),
            "the data dirs must be reaped once the deadline comes due",
        );
        assert!(
            pod_dir.join(REAPED_MARKER).exists(),
            "a reap must leave its audit trail",
        );
        assert!(pod_dir.is_dir(), "the record must stay behind");
        let _ = fs::remove_dir_all(pod_dir.parent().unwrap());
    }

    #[tokio::test(start_paused = true)]
    async fn a_scheduled_delete_removes_the_whole_pod_dir() {
        let pod_dir = tmp("schedule-delete").join("default_app_uid-1");
        plant_pod_dir(&pod_dir, &[]);
        let (_tx, rx) = watch::channel(());

        schedule_deadline(
            &pod_dir,
            ScheduledReap::PodDir,
            Some(Deadline::At(Utc::now() + chrono::Duration::hours(1))),
            Utc::now(),
            &rx,
        );
        tokio::time::sleep(Duration::from_secs(3601)).await;
        tokio::task::yield_now().await;

        assert!(!pod_dir.exists(), "the whole pod dir must go");
        let _ = fs::remove_dir_all(pod_dir.parent().unwrap());
    }

    /// A pending deadline must never hold the runner's exit. What it would
    /// have done is dropped — the next run reclaims it at rotation.
    #[tokio::test(start_paused = true)]
    async fn shutdown_drops_a_pending_deadline() {
        let pod_dir = tmp("schedule-shutdown").join("default_app_uid-1");
        plant_pod_dir(&pod_dir, &[]);
        let (tx, rx) = watch::channel(());

        schedule_deadline(
            &pod_dir,
            ScheduledReap::PodDir,
            Some(Deadline::At(Utc::now() + chrono::Duration::hours(1))),
            Utc::now(),
            &rx,
        );

        let _ = tx.send(());
        tokio::time::sleep(Duration::from_secs(7200)).await;
        tokio::task::yield_now().await;

        assert!(
            pod_dir.exists(),
            "a deadline cancelled by shutdown must not act",
        );
        let _ = fs::remove_dir_all(pod_dir.parent().unwrap());
    }

    /// `never`, an absent deadline, and one already due all need no task:
    /// the first two never come due, and the caller's inline reap has
    /// already handled the third.
    #[tokio::test(start_paused = true)]
    async fn deadlines_needing_no_task_schedule_none() {
        let root = tmp("schedule-none");
        let (_tx, rx) = watch::channel(());
        let now = Utc::now();

        for (name, deadline) in [
            ("never", Some(Deadline::Never)),
            ("absent", None),
            (
                "due",
                Some(Deadline::At(Utc::now() - chrono::Duration::hours(1))),
            ),
            // A `0s` TTL: the deadline *is* the instant it was scheduled at,
            // and the caller's inline reap has already acted on it.
            ("exactly-due", Some(Deadline::At(now))),
        ] {
            let pod_dir = root.join(format!("default_{}_uid-1", name));
            plant_pod_dir(&pod_dir, &[]);
            schedule_deadline(&pod_dir, ScheduledReap::PodDir, deadline, now, &rx);
            tokio::time::sleep(Duration::from_secs(7200)).await;
            tokio::task::yield_now().await;
            assert!(pod_dir.exists(), "{name} should not have been scheduled");
        }
        let _ = fs::remove_dir_all(&root);
    }

    /// A pod deleted while it is still running starts both clocks at the
    /// same instant, so equal TTLs put the two deadlines microseconds apart
    /// and their tasks act at the same moment on different workers. The reap
    /// must not be able to strand the delete: `remove_dir_all` reports
    /// `ENOTEMPTY` when the reap's marker lands after the walk emptied the
    /// dir but before the final `rmdir`, which left the pod dir behind
    /// holding nothing but [`REAPED_MARKER`] — a dir no later pass reclaims,
    /// and a marker claiming the record was kept when the walk had destroyed
    /// it.
    ///
    /// Real time and several workers on purpose: the interleaving needs the
    /// two blocking walks to genuinely overlap, which a current-thread
    /// runtime cannot produce — it runs each task's sync block to
    /// completion. Many pods and a wide dir widen the window enough to hit
    /// it reliably.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_reap_and_delete_leaves_no_pod_dir_behind() {
        /// Pods racing at once — one is enough to hit the window sometimes,
        /// this many makes it near-certain.
        const RACING_PODS: usize = 40;
        /// Filler entries per pod dir. `readdir` batches, so an entry created
        /// in the tail of a walk is missed and only surfaces at `rmdir` —
        /// this many entries makes that tail wide enough to aim at (~10ms of
        /// walk on a warm APFS volume).
        const FILLER_ENTRIES: usize = 400;
        /// How far out the delete deadline is set.
        const DUE_IN_MS: i64 = 50;
        /// How far the reap wakes after the delete, placing its marker write
        /// about three quarters of the way through the walk — measured to
        /// strand an unprotected delete a third to a half of the time.
        const REAP_LAG_MS: i64 = 8;

        let root = tmp("schedule-race");
        let (_tx, rx) = watch::channel(());
        let pod_dirs: Vec<PathBuf> = (0..RACING_PODS)
            .map(|i| root.join(format!("default_app_uid-{i}")))
            .collect();

        for pod_dir in &pod_dirs {
            // As the incident's pods were: deleted while still preparing, so
            // no `work/` or `volumes/` were ever created and the reap has
            // only its marker to write. Filler entries lengthen the delete's
            // walk so that write lands before its final `rmdir`.
            plant_ttls(pod_dir, &[]);
            for f in 0..FILLER_ENTRIES {
                fs::write(pod_dir.join(format!("filler-{f}")), "x").unwrap();
            }

            // The incident's two deadlines were 341µs apart, but timer
            // granularity expired them in the same tick and the delete got a
            // worker first — its walk was already running when the reap woke
            // and stamped its marker 2.3ms before the `rmdir` failed. The
            // offsets here reproduce that interleaving directly instead of
            // relying on which task the scheduler happens to pick.
            let now = Utc::now();
            let delete_at = now + chrono::Duration::milliseconds(DUE_IN_MS);
            let reap_at = delete_at + chrono::Duration::milliseconds(REAP_LAG_MS);
            schedule_deadline(
                pod_dir,
                ScheduledReap::DataDirs,
                Some(Deadline::At(reap_at)),
                now,
                &rx,
            );
            schedule_deadline(
                pod_dir,
                ScheduledReap::PodDir,
                Some(Deadline::At(delete_at)),
                now,
                &rx,
            );
        }

        // Real time here: wait out the deadlines and both walks.
        tokio::time::sleep(Duration::from_millis(1500)).await;

        let stranded: Vec<String> = pod_dirs
            .iter()
            .filter(|d| d.exists())
            .map(|d| {
                let left: Vec<String> = fs::read_dir(d)
                    .map(|entries| {
                        entries
                            .filter_map(|e| e.ok())
                            .map(|e| e.file_name().to_string_lossy().into_owned())
                            .collect()
                    })
                    .unwrap_or_default();
                format!("{} -> {left:?}", d.display())
            })
            .collect();

        assert!(
            stranded.is_empty(),
            "a concurrent reap must not strand the delete; {} of {RACING_PODS} left behind: {stranded:#?}",
            stranded.len(),
        );
        let _ = fs::remove_dir_all(&root);
    }
}
