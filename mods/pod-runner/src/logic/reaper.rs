// Copyright 2026 Tabsdata Inc.

//! Pod dir reaping — the marker and deadline files, and the tasks that act
//! on them.
//!
//! A pod's footprint is two dirs (see [`crate::store::layout`]):
//!
//! - the **data dir**, `pod-run/<uid>/` — `work/` and `volumes/`,
//!   everything the containers produced and usually the bulk of the disk.
//!   It goes on the **reap** deadline, measured from pod completion, and
//!   its `pod-run/<pod_id>` link goes with it.
//! - the **state dir**, `pod-state/<bucket>/<pod_name>/<stamp>/` — `log/`
//!   and `store/`, the record of what one incarnation of the pod did and why
//!   it ended. It goes on
//!   the **delete** deadline, measured from the moment the pod leaves the
//!   pod runner, since until then the runner still answers queries about
//!   it.
//!
//! Each dir carries everything its own clock needs, so the two never have
//! to consult each other: its [`CLEANUP_TTLS_FILE`], an outcome marker
//! ([`SUCCEEDED_MARKER`] or [`FAILED_MARKER`], content the completion
//! time), and its deadline file — [`REAP_ON_FILE`] in the data dir, written
//! at completion; [`DELETE_ON_FILE`] in the state dir, written at eviction.
//!
//! Deadlines are stored as absolute instants rather than durations so the
//! reaper never has to re-derive them: it compares `now` against a file's
//! contents and acts. A deadline of [`TTL_NEVER`] is written out literally,
//! so "never expires" is distinguishable from "the file is missing".
//!
//! Acting on a deadline removes the whole dir it lives in. [`apply_deadline`]
//! runs inline whenever a pod is finalized or evicted, so a zero TTL acts at
//! once; a deadline in the future gets a task of its own via
//! [`schedule_deadline`] — there is no scanning daemon. Because the files
//! are the whole policy, a restart loses nothing: [`recover_pod_dirs`]
//! re-reads them at startup and picks up every deadline where it was.

use std::fs;
use std::io;
use std::path::Path;

use chrono::{DateTime, Utc};
use tokio::sync::watch;
use tracing::{Instrument, debug, debug_span, info, warn};

use super::ttl::{
    DeleteTtls, ReapTtls, Ttl, TtlConfig, read_cleanup_ttls, resolve_delete_ttls,
    resolve_reap_ttls, write_cleanup_ttls,
};
use crate::spec::constants::{
    DELETE_ON_FILE, FAILED_MARKER, PodPhase, REAP_ON_FILE, SUCCEEDED_MARKER, TTL_NEVER,
};
use crate::store::layout::{PodLayout, remove_run_links_to, run_dir_pod};

// ---------------------------------------------------------------------------
// CleanupConfig
// ---------------------------------------------------------------------------

/// Configuration for pod dir reaping.
#[derive(Debug, Clone, Default)]
pub struct CleanupConfig {
    /// Cleanup TTLs: when a finished pod's data dir is reaped, and when its
    /// state dir is deleted. Resolved per pod (config plus annotations) and
    /// recorded in each dir at pod start.
    pub ttls: TtlConfig,
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
    /// The literal [`TTL_NEVER`] — the guarded dir is never removed.
    Never,
    /// The instant at which the guarded dir becomes removable.
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

/// Write a deadline file into a pod dir.
pub fn write_deadline(
    dir: &Path,
    file: &str,
    ttl: Ttl,
    origin: DateTime<Utc>,
) -> io::Result<Deadline> {
    let deadline = Deadline::of(ttl, origin);
    fs::write(dir.join(file), deadline.encode())?;
    Ok(deadline)
}

/// Read a deadline file. `None` means the file is absent — for
/// [`DELETE_ON_FILE`] that means the pod has not left the runner yet.
/// Unreadable or malformed contents also yield `None` with a warning: an
/// ambiguous deadline must never be treated as due.
pub fn read_deadline(dir: &Path, file: &str) -> Option<Deadline> {
    let path = dir.join(file);
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

/// Which of a pod's two deadlines — and so which of its two dirs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScheduledReap {
    /// The data dir, on the [`REAP_ON_FILE`] deadline.
    DataDir,
    /// The state dir, on the [`DELETE_ON_FILE`] deadline.
    StateDir,
}

impl ScheduledReap {
    /// The deadline file guarding this dir.
    pub fn deadline_file(&self) -> &'static str {
        match self {
            ScheduledReap::DataDir => REAP_ON_FILE,
            ScheduledReap::StateDir => DELETE_ON_FILE,
        }
    }
}

// ---------------------------------------------------------------------------
// Outcome markers
// ---------------------------------------------------------------------------

/// Read the outcome marker from a pod dir, with the completion time it
/// records. `None` means the pod has not been finalized.
pub fn read_outcome(dir: &Path) -> Option<(Outcome, Option<DateTime<Utc>>)> {
    for outcome in [Outcome::Succeeded, Outcome::Failed] {
        let path = dir.join(outcome.marker());
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

/// Write the outcome marker into `dir`, unless it already carries one.
/// Returns whether a marker was written.
fn write_outcome(dir: &Path, outcome: Outcome, completed_at: DateTime<Utc>) -> bool {
    if !dir.is_dir() {
        debug!(dir = ?dir, "No pod dir to mark finished");
        return false;
    }
    if let Some((existing, _)) = read_outcome(dir) {
        debug!(dir = ?dir, ?existing, "Pod dir already finalized");
        return false;
    }
    match fs::write(dir.join(outcome.marker()), completed_at.to_rfc3339()) {
        Ok(()) => true,
        Err(e) => {
            warn!(dir = ?dir, error = %e, "Failed to write outcome marker");
            false
        }
    }
}

// ---------------------------------------------------------------------------
// Pod start
// ---------------------------------------------------------------------------

/// Claim a pod's dirs at pod start and record its retention policy in each.
///
/// Both dirs outlive the runner, so a pod admitted under an id that has
/// been here before — replayed after a restart, typically — finds what its
/// previous attempt left:
///
/// - the **data dir** is dead weight from the previous attempt and would
///   collide with this one's, so it is removed outright;
/// - the **state dir** is kept, so the previous attempt's logs and status
///   history stay readable, but its outcome marker and delete deadline are
///   cleared: the pod is live again, and a pending delete task re-checks
///   the deadline file before acting, so clearing it disarms that task.
///
/// The TTLs are resolved and written before anything else can fail: a pod
/// that dies in preparation still needs a retention policy, and writing it
/// here means each dir carries its own — unaffected by a later config edit
/// or a runner restart.
pub fn claim_pod_dirs(
    data_dir: &Path,
    state_dir: &Path,
    config: &TtlConfig,
    annotations: &std::collections::HashMap<String, String>,
    now: DateTime<Utc>,
) {
    if data_dir.exists() {
        warn!(dir = ?data_dir, "Removing pod data dir left by a previous attempt");
        delete_pod_dir(data_dir);
    }
    for stale in [SUCCEEDED_MARKER, FAILED_MARKER, DELETE_ON_FILE] {
        match fs::remove_file(state_dir.join(stale)) {
            Ok(()) => debug!(dir = ?state_dir, file = stale, "Cleared stale state marker"),
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => {
                warn!(dir = ?state_dir, file = stale, error = %e, "Failed to clear stale state marker")
            }
        }
    }

    let written_at = now.to_rfc3339();
    if let Err(e) = write_cleanup_ttls(
        data_dir,
        &resolve_reap_ttls(config, annotations, &written_at),
    ) {
        warn!(dir = ?data_dir, error = %e, "Failed to write reap TTLs");
    }
    if let Err(e) = write_cleanup_ttls(
        state_dir,
        &resolve_delete_ttls(config, annotations, &written_at),
    ) {
        warn!(dir = ?state_dir, error = %e, "Failed to write delete TTLs");
    }
}

// ---------------------------------------------------------------------------
// Finalization
// ---------------------------------------------------------------------------

/// Mark a pod as finished: write the outcome marker into both dirs, and
/// the reap deadline, measured from `completed_at`, into the data dir.
/// Returns the reap deadline when one was written.
///
/// Idempotent — a dir that already carries an outcome marker is left alone,
/// so the monitor and a concurrent delete can both call this and the first
/// one's timestamps stand.
pub fn finalize_pod(
    data_dir: &Path,
    state_dir: &Path,
    outcome: Outcome,
    fallback: &TtlConfig,
    completed_at: DateTime<Utc>,
) -> Option<Deadline> {
    write_outcome(state_dir, outcome, completed_at);
    finalize_data_dir(data_dir, outcome, fallback, completed_at)
}

/// The data-dir half of [`finalize_pod`].
///
/// The TTLs come from the dir's own [`ReapTtls`], written at pod start. If
/// that file is missing they are resolved from `fallback` config and
/// written out, so the dir always ends up carrying a policy — but per-pod
/// annotations are lost, hence the warning.
fn finalize_data_dir(
    data_dir: &Path,
    outcome: Outcome,
    fallback: &TtlConfig,
    completed_at: DateTime<Utc>,
) -> Option<Deadline> {
    if !write_outcome(data_dir, outcome, completed_at) {
        return None;
    }
    let ttls = reap_ttls_or_fallback(data_dir, fallback, completed_at);
    match write_deadline(
        data_dir,
        REAP_ON_FILE,
        ttls.ttl(outcome.succeeded()),
        completed_at,
    ) {
        Ok(deadline) => {
            debug!(dir = ?data_dir, ?outcome, ?deadline, "Pod data dir finalized");
            Some(deadline)
        }
        Err(e) => {
            warn!(dir = ?data_dir, error = %e, "Failed to write reap deadline");
            None
        }
    }
}

/// Record that the pod has left the pod runner, starting the delete clock
/// on its state dir. Returns the delete deadline when one was written.
///
/// The outcome comes from the marker in the dir, so eviction needs no
/// in-memory state: a pod that finished before a restart can still be
/// evicted correctly. Idempotent — an existing deadline is left alone.
pub fn mark_pod_evicted(
    state_dir: &Path,
    fallback: &TtlConfig,
    evicted_at: DateTime<Utc>,
) -> Option<Deadline> {
    if !state_dir.is_dir() {
        debug!(dir = ?state_dir, "No pod state dir to mark evicted");
        return None;
    }
    if state_dir.join(DELETE_ON_FILE).exists() {
        debug!(dir = ?state_dir, "Pod state dir already marked evicted");
        return None;
    }
    let Some((outcome, _)) = read_outcome(state_dir) else {
        warn!(dir = ?state_dir, "No outcome marker, cannot start the delete clock");
        return None;
    };

    let ttls = delete_ttls_or_fallback(state_dir, fallback, evicted_at);
    match write_deadline(
        state_dir,
        DELETE_ON_FILE,
        ttls.ttl(outcome.succeeded()),
        evicted_at,
    ) {
        Ok(deadline) => {
            debug!(dir = ?state_dir, ?deadline, "Pod state dir marked evicted");
            Some(deadline)
        }
        Err(e) => {
            warn!(dir = ?state_dir, error = %e, "Failed to write delete deadline");
            None
        }
    }
}

/// The data dir's own reap TTLs, or config-derived ones written on the spot.
fn reap_ttls_or_fallback(dir: &Path, fallback: &TtlConfig, now: DateTime<Utc>) -> ReapTtls {
    if let Some(ttls) = read_cleanup_ttls(dir) {
        return ttls;
    }
    warn!(dir = ?dir, "No reap TTLs for pod, falling back to configured values");
    let ttls = resolve_reap_ttls(fallback, &Default::default(), &now.to_rfc3339());
    if let Err(e) = write_cleanup_ttls(dir, &ttls) {
        warn!(dir = ?dir, error = %e, "Failed to write fallback reap TTLs");
    }
    ttls
}

/// The state dir's own delete TTLs, or config-derived ones written on the
/// spot.
fn delete_ttls_or_fallback(dir: &Path, fallback: &TtlConfig, now: DateTime<Utc>) -> DeleteTtls {
    if let Some(ttls) = read_cleanup_ttls(dir) {
        return ttls;
    }
    warn!(dir = ?dir, "No delete TTLs for pod, falling back to configured values");
    let ttls = resolve_delete_ttls(fallback, &Default::default(), &now.to_rfc3339());
    if let Err(e) = write_cleanup_ttls(dir, &ttls) {
        warn!(dir = ?dir, error = %e, "Failed to write fallback delete TTLs");
    }
    ttls
}

// ---------------------------------------------------------------------------
// Removal
// ---------------------------------------------------------------------------

/// How many times [`delete_pod_dir`] walks the dir before giving up.
const DELETE_ATTEMPTS: u32 = 3;

/// Delete a whole pod dir. Returns whether it went.
///
/// `remove_dir_all` does not follow symlinks, so `hostPath` volumes
/// (symlinks under `volumes/`) and container mount points (symlinks under
/// `work/<container>/`) are unlinked without touching what they point at.
fn delete_pod_dir(dir: &Path) -> bool {
    delete_pod_dir_with(dir, |dir| fs::remove_dir_all(dir))
}

/// [`delete_pod_dir`] over an injectable removal, so the retry is testable
/// without having to win a race against a real walk.
///
/// `ENOTEMPTY` means an entry appeared while the walk was in flight. Since
/// `readdir` batches, anything created after the last batch was read is
/// missed by the walk and only surfaces when the final `rmdir` refuses —
/// a late status snapshot landing in a state dir whose `0s` delete deadline
/// came due, for one. Walking again takes it.
fn delete_pod_dir_with(dir: &Path, remove: impl Fn(&Path) -> io::Result<()>) -> bool {
    for attempt in 1..=DELETE_ATTEMPTS {
        match remove(dir) {
            Ok(()) => {
                debug!(dir = ?dir, "Pod dir deleted");
                return true;
            }
            // Already gone — nothing to report.
            Err(e) if e.kind() == io::ErrorKind::NotFound => return false,
            Err(e) if e.kind() == io::ErrorKind::DirectoryNotEmpty && attempt < DELETE_ATTEMPTS => {
                debug!(
                    dir = ?dir,
                    attempt,
                    "Pod dir refilled while being deleted, walking it again",
                );
            }
            Err(e) => {
                warn!(error = %e, dir = ?dir, "Failed to delete pod dir");
                return false;
            }
        }
    }
    false
}

/// Remove a pod dir whose deadline came due. Returns whether it went.
///
/// A state dir's parent is the dir all incarnations of its pod name share;
/// it goes with the last of them. Only ever with a plain `rmdir`, which
/// refuses a dir that is not empty: a new incarnation can be claiming its
/// own stamp dir in there at this very moment, and either it lands first
/// and the `rmdir` fails, or the `rmdir` lands first and the claim creates
/// the parent again.
fn remove_on_deadline(dir: &Path, kind: ScheduledReap) -> bool {
    if !delete_pod_dir(dir) {
        return false;
    }
    if kind == ScheduledReap::DataDir {
        remove_run_links_to(dir);
    }
    if kind == ScheduledReap::StateDir
        && let Some(name_dir) = dir.parent()
    {
        match fs::remove_dir(name_dir) {
            Ok(()) => debug!(dir = ?name_dir, "Last incarnation gone, pod name dir removed"),
            Err(e) => debug!(dir = ?name_dir, error = %e, "Pod name dir kept"),
        }
    }
    true
}

/// What [`apply_deadline`] did to a pod dir.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReapAction {
    /// No outcome marker: the pod is still live, or never got finalized.
    NotFinished,
    /// The deadline is absent, `never`, not reached yet, or the removal
    /// failed.
    Waiting,
    /// The dir was removed.
    Removed,
}

/// The result of evaluating one pod dir.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReapResult {
    /// What was done.
    pub action: ReapAction,
    /// The deadline still ahead, for the caller to schedule.
    pub next_deadline: Option<Deadline>,
}

/// Apply a pod dir's deadline: remove the dir if it is due, or report the
/// deadline still ahead.
pub fn apply_deadline(dir: &Path, kind: ScheduledReap, now: DateTime<Utc>) -> ReapResult {
    let result = |action, next_deadline| ReapResult {
        action,
        next_deadline,
    };

    if read_outcome(dir).is_none() {
        // Either the pod is still running, or it died without ever being
        // finalized. Nothing here is safe to remove on a schedule.
        return result(ReapAction::NotFinished, None);
    }
    match read_deadline(dir, kind.deadline_file()) {
        Some(deadline) if deadline.is_due(now) => match remove_on_deadline(dir, kind) {
            true => result(ReapAction::Removed, None),
            false => result(ReapAction::Waiting, None),
        },
        Some(Deadline::Never) | None => result(ReapAction::Waiting, None),
        ahead => result(ReapAction::Waiting, ahead),
    }
}

// ---------------------------------------------------------------------------
// Deadline timers
// ---------------------------------------------------------------------------

/// Wait out a pod dir deadline and act on it.
///
/// One task per deadline, sleeping until it comes due. There is no scanning
/// daemon: a deadline is known the moment it is written, so the runner waits
/// on it directly instead of re-reading every finished pod dir to rediscover
/// what it already knew. A deadline already due needs no task — the caller's
/// inline [`apply_deadline`] has just acted on it — and [`Deadline::Never`]
/// never comes due.
///
/// When the sleep elapses the task re-reads the deadline file and acts only
/// if it still holds the deadline it was scheduled for. Pod dirs outlive a
/// pod's stay in the runner, so the dir can be claimed again while the task
/// sleeps — a pod replayed under the same id — and [`claim_pod_dirs`]
/// clears the deadline file precisely so a pending task stands down rather
/// than removing a live pod's dir. The check compares the deadline itself,
/// not the clock, so wall-clock steps cannot make a due task skip its
/// work.
///
/// The sleep is raced against `shutdown`, so a pending deadline never holds
/// the runner's exit. Nothing is lost: the deadline is on disk, and the next
/// run's [`recover_pod_dirs`] schedules it again.
///
/// `shutdown` must be a receiver that already existed when shutdown was
/// signalled — a `subscribe()` afterwards cannot observe it — which is why
/// callers pass one they are already holding.
pub fn schedule_deadline(
    dir: &Path,
    kind: ScheduledReap,
    deadline: Option<Deadline>,
    now: DateTime<Utc>,
    shutdown: &watch::Receiver<()>,
) {
    let Some(deadline) = deadline else {
        return;
    };
    // Already due — the caller's inline pass covered it. `is_due` rather than
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

    // The pod identity, recovered from the dir: `<name>_<uid>` like every
    // other pod-scoped log line for a data dir, from its link, and
    // `<name>/<stamp>` for an incarnation's state dir, which carries no uid.
    let file_name = |d: &Path| {
        d.file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_default()
    };
    let pod = match kind {
        ScheduledReap::DataDir => run_dir_pod(dir),
        ScheduledReap::StateDir => format!(
            "{}/{}",
            dir.parent().map(file_name).unwrap_or_default(),
            file_name(dir)
        ),
    };

    let dir = dir.to_path_buf();
    let mut shutdown = shutdown.clone();
    tokio::spawn(
        async move {
            debug!(dir = ?dir, ?kind, ?instant, "Deadline scheduled");
            tokio::select! {
                _ = tokio::time::sleep(delay) => {}
                _ = shutdown.changed() => {
                    debug!(dir = ?dir, ?kind, "Deadline dropped by shutdown");
                    return;
                }
            }
            act_on_scheduled(&dir, kind, deadline);
        }
        // Root span: the task outlives the scope that scheduled it, so it
        // must not inherit that scope's pod span.
        .instrument(debug_span!(parent: None, "reap", pod = %pod)),
    );
}

/// Remove `dir` if it is still finished and its deadline file still holds
/// `scheduled`. Returns whether it went.
fn act_on_scheduled(dir: &Path, kind: ScheduledReap, scheduled: Deadline) -> bool {
    if read_outcome(dir).is_none() {
        debug!(dir = ?dir, ?kind, "Deadline stood down: pod dir no longer finished");
        return false;
    }
    let current = read_deadline(dir, kind.deadline_file());
    if current != Some(scheduled) {
        debug!(dir = ?dir, ?kind, ?current, "Deadline stood down: superseded");
        return false;
    }
    remove_on_deadline(dir, kind)
}

// ---------------------------------------------------------------------------
// Startup recovery
// ---------------------------------------------------------------------------

/// What [`recover_pod_dirs`] found.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RecoveryStats {
    /// Dirs whose deadline had passed and that were removed.
    pub removed: u32,
    /// Dirs whose deadline is still ahead and got a task.
    pub scheduled: u32,
    /// Dirs kept with no task: a `never` deadline, or a removal that failed.
    pub kept: u32,
}

/// Pick up every pod dir a previous incarnation of the runner left.
///
/// Must run before the runner admits pods: every dir on disk then belongs
/// to the past, and none is being claimed.
///
/// - A **data dir** with no outcome marker belonged to a pod the previous
///   run never finalized — killed, or stopped before its pod finished.
///   Nothing will resume it, so it is finalized as failed now and reaped on
///   its failure TTL, keeping what it left inspectable for that long.
/// - A **state dir** with no outcome marker is marked failed the same way;
///   one with no delete deadline belonged to a pod still in the runner when
///   it went down. The runner keeps no pods across a restart, so that pod
///   has left it, and its delete clock starts now.
///
/// Every deadline is then applied — removed if due, scheduled if ahead —
/// exactly as at finalization and eviction. A pod replayed after the
/// restart claims its dirs back through [`claim_pod_dirs`], which stands
/// the scheduled tasks down.
pub fn recover_pod_dirs(
    layout: &PodLayout,
    fallback: &TtlConfig,
    now: DateTime<Utc>,
    shutdown: &watch::Receiver<()>,
) -> RecoveryStats {
    let mut stats = RecoveryStats::default();

    for dir in layout.list_run_dirs() {
        if read_outcome(&dir).is_none() {
            finalize_data_dir(&dir, Outcome::Failed, fallback, now);
        }
        recover_one(&dir, ScheduledReap::DataDir, now, shutdown, &mut stats);
    }
    let pruned = layout.prune_run_links();
    if pruned > 0 {
        debug!(pruned, "Pruned links to data dirs that are gone");
    }
    for dir in layout.list_state_dirs() {
        write_outcome(&dir, Outcome::Failed, now);
        mark_pod_evicted(&dir, fallback, now);
        recover_one(&dir, ScheduledReap::StateDir, now, shutdown, &mut stats);
    }

    info!(
        removed = stats.removed,
        scheduled = stats.scheduled,
        kept = stats.kept,
        "Recovered pod dirs",
    );
    stats
}

fn recover_one(
    dir: &Path,
    kind: ScheduledReap,
    now: DateTime<Utc>,
    shutdown: &watch::Receiver<()>,
    stats: &mut RecoveryStats,
) {
    let result = apply_deadline(dir, kind, now);
    match (result.action, result.next_deadline) {
        (ReapAction::Removed, _) => stats.removed += 1,
        (_, Some(deadline)) => {
            schedule_deadline(dir, kind, Some(deadline), now, shutdown);
            stats.scheduled += 1;
        }
        _ => stats.kept += 1,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logic::ttl::{TtlSource, TtlSpec};
    use crate::pod::resolver::volume::VOLUMES_DIR;
    use crate::server::config::{POD_LOG_SUBDIR, POD_STORE_SUBDIR, POD_WORK_SUBDIR};
    use crate::spec::constants::{
        ANN_POD_DATA_REAP_TTL_ON_POD_FAILURE, ANN_POD_DIR_DELETE_TTL_POD_FAILURE_ON_POD_DELETE,
        CLEANUP_TTLS_FILE,
    };
    use crate::spec::pod::{PodMetadata, PodSpec};
    use crate::store::PodId;
    use std::collections::HashMap;
    use std::path::PathBuf;
    use std::time::Duration;

    fn tmp(name: &str) -> PathBuf {
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

    fn ann(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    /// A pod spec as admission leaves it: stamped with its creation time.
    fn pod_spec(name: &str, uid: &str) -> PodSpec {
        PodSpec {
            metadata: PodMetadata {
                namespace: "default".into(),
                name: name.into(),
                uid: uid.into(),
                creation_timestamp: Some(T0.into()),
                ..Default::default()
            },
            ..Default::default()
        }
    }

    /// A pod's two dirs under a fresh layout.
    struct Pod {
        root: PathBuf,
        layout: PodLayout,
        spec: PodSpec,
    }

    impl Pod {
        fn new(name: &str) -> Self {
            let root = tmp(name);
            Self {
                layout: PodLayout::new(&root),
                spec: pod_spec("app", "uid-1"),
                root,
            }
        }

        fn data(&self) -> PathBuf {
            self.layout.run_dir(&PodId::from_spec(&self.spec))
        }

        fn state(&self) -> PathBuf {
            self.layout.state_dir(&self.spec)
        }

        /// Claim the dirs as a pod start does, then fill them as a pod that
        /// ran has them.
        fn start(&self, annotations: &[(&str, &str)]) {
            claim_pod_dirs(
                &self.data(),
                &self.state(),
                &TtlConfig::default(),
                &ann(annotations),
                at(T0),
            );
            fs::create_dir_all(self.data().join(POD_WORK_SUBDIR).join("main")).unwrap();
            fs::create_dir_all(self.data().join(VOLUMES_DIR).join("data")).unwrap();
            let log = self.state().join(POD_LOG_SUBDIR).join("main");
            fs::create_dir_all(&log).unwrap();
            fs::write(log.join("0.log"), "output").unwrap();
            let store = self.state().join(POD_STORE_SUBDIR);
            fs::create_dir_all(&store).unwrap();
            fs::write(store.join("original-spec.json"), "{}").unwrap();
        }

        fn finalize(&self, outcome: Outcome, completed_at: &str) -> Option<Deadline> {
            finalize_pod(
                &self.data(),
                &self.state(),
                outcome,
                &TtlConfig::default(),
                at(completed_at),
            )
        }

        fn evict(&self, evicted_at: &str) -> Option<Deadline> {
            mark_pod_evicted(&self.state(), &TtlConfig::default(), at(evicted_at))
        }
    }

    impl Drop for Pod {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.root);
        }
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

    // -- claim_pod_dirs --

    #[test]
    fn claim_writes_each_half_of_the_policy_into_its_own_dir() {
        let pod = Pod::new("claim-split");
        pod.start(&[(ANN_POD_DIR_DELETE_TTL_POD_FAILURE_ON_POD_DELETE, "90m")]);

        let reap: ReapTtls = read_cleanup_ttls(&pod.data()).unwrap();
        assert_eq!(reap.reap_ttl_on_success.source, TtlSource::Config);
        let delete: DeleteTtls = read_cleanup_ttls(&pod.state()).unwrap();
        assert_eq!(delete.delete_ttl_on_failure.raw, "90m");
        assert_eq!(delete.delete_ttl_on_failure.source, TtlSource::Annotation);

        let data_json = fs::read_to_string(pod.data().join(CLEANUP_TTLS_FILE)).unwrap();
        assert!(!data_json.contains("deleteTtl"), "{data_json}");
        let state_json = fs::read_to_string(pod.state().join(CLEANUP_TTLS_FILE)).unwrap();
        assert!(!state_json.contains("reapTtl"), "{state_json}");
    }

    #[test]
    fn claim_replaces_a_previous_attempts_data_dir() {
        let pod = Pod::new("claim-data");
        pod.start(&[]);
        fs::write(
            pod.data()
                .join(POD_WORK_SUBDIR)
                .join("main")
                .join("leftover"),
            "x",
        )
        .unwrap();

        claim_pod_dirs(
            &pod.data(),
            &pod.state(),
            &TtlConfig::default(),
            &HashMap::new(),
            at(T0),
        );

        assert!(pod.data().join(CLEANUP_TTLS_FILE).exists(), "fresh policy");
        assert!(
            !pod.data().join(POD_WORK_SUBDIR).exists(),
            "a previous attempt's data must not leak into this one",
        );
    }

    #[test]
    fn claim_keeps_a_previous_attempts_record_but_rearms_it() {
        let pod = Pod::new("claim-state");
        pod.start(&[]);
        pod.finalize(Outcome::Failed, T0);
        pod.evict(T0);

        claim_pod_dirs(
            &pod.data(),
            &pod.state(),
            &TtlConfig::default(),
            &HashMap::new(),
            at(T0),
        );

        assert!(
            pod.state()
                .join(POD_LOG_SUBDIR)
                .join("main")
                .join("0.log")
                .exists(),
            "the previous attempt's logs stay readable",
        );
        assert!(pod.state().join(POD_STORE_SUBDIR).exists());
        assert_eq!(read_outcome(&pod.state()), None, "the pod is live again");
        assert_eq!(read_deadline(&pod.state(), DELETE_ON_FILE), None);
    }

    // -- finalize_pod --

    #[test]
    fn finalize_marks_both_dirs_and_writes_the_reap_deadline() {
        let pod = Pod::new("finalize-success");
        pod.start(&[]);

        let deadline = pod.finalize(Outcome::Succeeded, T0);

        // Default reap-on-success is 0s.
        assert_eq!(deadline, Some(Deadline::At(at(T0))));
        for dir in [pod.data(), pod.state()] {
            let (outcome, completed_at) = read_outcome(&dir).unwrap();
            assert_eq!(outcome, Outcome::Succeeded);
            assert_eq!(completed_at, Some(at(T0)));
        }
        assert_eq!(read_deadline(&pod.data(), REAP_ON_FILE), deadline);
        assert!(
            !pod.state().join(REAP_ON_FILE).exists(),
            "the reap deadline belongs to the data dir only",
        );
        assert!(
            !pod.state().join(DELETE_ON_FILE).exists(),
            "the delete clock starts at eviction, not completion",
        );
    }

    #[test]
    fn finalize_failed_uses_the_error_ttl() {
        let pod = Pod::new("finalize-failed");
        pod.start(&[]);
        let deadline = pod.finalize(Outcome::Failed, T0);
        // Default reap-on-failure is 1h.
        assert_eq!(
            deadline,
            Some(Deadline::At(at("2026-08-04T10:00:00+00:00")))
        );
    }

    #[test]
    fn finalize_honors_the_pods_own_annotated_ttl() {
        let pod = Pod::new("finalize-annotated");
        pod.start(&[(ANN_POD_DATA_REAP_TTL_ON_POD_FAILURE, "15m")]);
        let deadline = pod.finalize(Outcome::Failed, T0);
        assert_eq!(
            deadline,
            Some(Deadline::At(at("2026-08-04T09:15:00+00:00")))
        );
    }

    #[test]
    fn finalize_writes_never_for_a_never_ttl() {
        let pod = Pod::new("finalize-never");
        pod.start(&[(ANN_POD_DATA_REAP_TTL_ON_POD_FAILURE, TTL_NEVER)]);
        assert_eq!(pod.finalize(Outcome::Failed, T0), Some(Deadline::Never));
        assert_eq!(
            read_deadline(&pod.data(), REAP_ON_FILE),
            Some(Deadline::Never)
        );
    }

    #[test]
    fn finalize_is_idempotent() {
        let pod = Pod::new("finalize-idempotent");
        pod.start(&[]);
        pod.finalize(Outcome::Failed, T0);

        // A second finalization — different outcome, later — changes nothing.
        let again = pod.finalize(Outcome::Succeeded, "2026-08-04T09:30:00+00:00");

        assert_eq!(again, None);
        for dir in [pod.data(), pod.state()] {
            assert_eq!(read_outcome(&dir), Some((Outcome::Failed, Some(at(T0)))));
            assert!(!dir.join(SUCCEEDED_MARKER).exists());
        }
        assert_eq!(
            read_deadline(&pod.data(), REAP_ON_FILE),
            Some(Deadline::At(at("2026-08-04T10:00:00+00:00")))
        );
    }

    #[test]
    fn finalize_falls_back_to_config_without_a_ttls_file() {
        let pod = Pod::new("finalize-fallback");
        pod.start(&[]);
        fs::remove_file(pod.data().join(CLEANUP_TTLS_FILE)).unwrap();
        let fallback = TtlConfig {
            reap_on_failure: TtlSpec::parse("10m").unwrap(),
            ..TtlConfig::default()
        };

        let deadline = finalize_pod(
            &pod.data(),
            &pod.state(),
            Outcome::Failed,
            &fallback,
            at(T0),
        );

        assert_eq!(
            deadline,
            Some(Deadline::At(at("2026-08-04T09:10:00+00:00")))
        );
        let written: ReapTtls = read_cleanup_ttls(&pod.data()).unwrap();
        assert_eq!(
            written.reap_ttl_on_failure.raw, "10m",
            "fallback written out"
        );
    }

    #[test]
    fn finalize_without_a_data_dir_still_marks_the_record() {
        let pod = Pod::new("finalize-no-data");
        pod.start(&[]);
        fs::remove_dir_all(pod.data()).unwrap();

        assert_eq!(pod.finalize(Outcome::Failed, T0), None);
        assert!(!pod.data().exists(), "finalize must not recreate it");
        assert_eq!(read_outcome(&pod.state()).unwrap().0, Outcome::Failed);
    }

    #[test]
    fn read_outcome_of_unfinalized_dir_is_none() {
        let dir = tmp("outcome-none");
        assert_eq!(read_outcome(&dir), None);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn read_outcome_survives_a_malformed_timestamp() {
        let dir = tmp("outcome-malformed");
        fs::write(dir.join(FAILED_MARKER), "garbage").unwrap();
        assert_eq!(read_outcome(&dir), Some((Outcome::Failed, None)));
        let _ = fs::remove_dir_all(&dir);
    }

    // -- mark_pod_evicted --

    #[test]
    fn eviction_starts_the_delete_clock() {
        let pod = Pod::new("evict");
        pod.start(&[]);
        pod.finalize(Outcome::Succeeded, T0);

        let deadline = pod.evict("2026-08-04T12:00:00+00:00");

        // Default delete-on-success is 1h, from eviction.
        assert_eq!(
            deadline,
            Some(Deadline::At(at("2026-08-04T13:00:00+00:00")))
        );
        assert_eq!(read_deadline(&pod.state(), DELETE_ON_FILE), deadline);
    }

    #[test]
    fn eviction_uses_the_outcome_from_the_marker() {
        let pod = Pod::new("evict-failed");
        pod.start(&[]);
        pod.finalize(Outcome::Failed, T0);
        // Default delete-on-failure is 24h.
        assert_eq!(
            pod.evict(T0),
            Some(Deadline::At(at("2026-08-05T09:00:00+00:00")))
        );
    }

    #[test]
    fn eviction_writes_never_for_a_never_ttl() {
        let pod = Pod::new("evict-never");
        pod.start(&[(ANN_POD_DIR_DELETE_TTL_POD_FAILURE_ON_POD_DELETE, TTL_NEVER)]);
        pod.finalize(Outcome::Failed, T0);
        assert_eq!(pod.evict(T0), Some(Deadline::Never));
    }

    #[test]
    fn eviction_is_idempotent() {
        let pod = Pod::new("evict-idempotent");
        pod.start(&[]);
        pod.finalize(Outcome::Succeeded, T0);
        let first = pod.evict(T0);
        assert_eq!(pod.evict("2026-08-04T20:00:00+00:00"), None);
        assert_eq!(read_deadline(&pod.state(), DELETE_ON_FILE), first);
    }

    #[test]
    fn eviction_without_an_outcome_marker_is_skipped() {
        let pod = Pod::new("evict-unfinished");
        pod.start(&[]);
        assert_eq!(pod.evict(T0), None);
        assert!(!pod.state().join(DELETE_ON_FILE).exists());
    }

    // -- apply_deadline --

    #[test]
    fn unfinished_dirs_are_left_alone() {
        let pod = Pod::new("apply-unfinished");
        pod.start(&[]);
        let later = at("2099-01-01T00:00:00+00:00");

        for (dir, kind) in [
            (pod.data(), ScheduledReap::DataDir),
            (pod.state(), ScheduledReap::StateDir),
        ] {
            let result = apply_deadline(&dir, kind, later);
            assert_eq!(result.action, ReapAction::NotFinished);
            assert_eq!(result.next_deadline, None);
            assert!(dir.exists(), "a live pod keeps its dirs");
        }
    }

    #[test]
    fn zero_reap_ttl_removes_the_data_dir_and_keeps_the_record() {
        let pod = Pod::new("apply-reap-zero");
        pod.start(&[]);
        pod.finalize(Outcome::Succeeded, T0);

        let result = apply_deadline(&pod.data(), ScheduledReap::DataDir, at(T0));

        assert_eq!(result.action, ReapAction::Removed);
        assert!(!pod.data().exists(), "the whole data dir goes");
        assert!(
            pod.state().join(POD_LOG_SUBDIR).exists()
                && pod.state().join(POD_STORE_SUBDIR).exists(),
            "log/ and store/ live elsewhere and survive the reap",
        );
    }

    #[test]
    fn reap_waits_until_the_deadline() {
        let pod = Pod::new("apply-reap-wait");
        pod.start(&[]);
        pod.finalize(Outcome::Failed, T0);

        let result = apply_deadline(
            &pod.data(),
            ScheduledReap::DataDir,
            at("2026-08-04T09:59:59+00:00"),
        );
        assert_eq!(result.action, ReapAction::Waiting);
        assert_eq!(
            result.next_deadline,
            Some(Deadline::At(at("2026-08-04T10:00:00+00:00")))
        );
        assert!(pod.data().join(POD_WORK_SUBDIR).exists());

        let result = apply_deadline(
            &pod.data(),
            ScheduledReap::DataDir,
            at("2026-08-04T10:00:01+00:00"),
        );
        assert_eq!(result.action, ReapAction::Removed);
        assert!(!pod.data().exists());
    }

    #[test]
    fn never_reap_ttl_is_never_reaped() {
        let pod = Pod::new("apply-reap-never");
        pod.start(&[(ANN_POD_DATA_REAP_TTL_ON_POD_FAILURE, TTL_NEVER)]);
        pod.finalize(Outcome::Failed, T0);

        let result = apply_deadline(
            &pod.data(),
            ScheduledReap::DataDir,
            at("2099-01-01T00:00:00+00:00"),
        );
        assert_eq!(result.action, ReapAction::Waiting);
        assert_eq!(result.next_deadline, None, "nothing to schedule");
        assert!(pod.data().join(POD_WORK_SUBDIR).exists());
    }

    #[test]
    fn delete_removes_the_state_dir_once_due() {
        let pod = Pod::new("apply-delete");
        pod.start(&[]);
        pod.finalize(Outcome::Succeeded, T0);
        pod.evict(T0);

        let result = apply_deadline(
            &pod.state(),
            ScheduledReap::StateDir,
            at("2026-08-04T09:59:59+00:00"),
        );
        assert_eq!(result.action, ReapAction::Waiting);
        assert!(pod.state().exists());

        let result = apply_deadline(
            &pod.state(),
            ScheduledReap::StateDir,
            at("2026-08-04T10:00:00+00:00"),
        );
        assert_eq!(result.action, ReapAction::Removed);
        assert!(!pod.state().exists(), "the whole record goes");
    }

    #[test]
    fn the_last_incarnation_takes_the_name_dir_with_it() {
        let pod = Pod::new("apply-name-dir");
        pod.start(&[]);
        pod.finalize(Outcome::Succeeded, T0);
        pod.evict(T0);
        let name_dir = pod.state().parent().unwrap().to_path_buf();

        apply_deadline(
            &pod.state(),
            ScheduledReap::StateDir,
            at("2026-08-04T10:00:00+00:00"),
        );

        assert!(!name_dir.exists(), "nothing left under the pod name");
        assert!(
            name_dir.parent().unwrap().exists(),
            "the bucket stays: other names share it"
        );
    }

    #[test]
    fn an_older_incarnation_leaves_the_newer_one_alone() {
        let pod = Pod::new("apply-two-incarnations");
        pod.start(&[]);
        pod.finalize(Outcome::Succeeded, T0);
        pod.evict(T0);

        // A newer incarnation of the same name, still running.
        let mut newer = pod_spec("app", "uid-2");
        newer.metadata.creation_timestamp = Some("2026-08-04T09:30:00+00:00".into());
        let newer_state = pod.layout.state_dir(&newer);
        claim_pod_dirs(
            &pod.layout.run_dir(&PodId::from_spec(&newer)),
            &newer_state,
            &TtlConfig::default(),
            &HashMap::new(),
            at(T0),
        );
        assert_eq!(newer_state.parent(), pod.state().parent());

        let result = apply_deadline(
            &pod.state(),
            ScheduledReap::StateDir,
            at("2026-08-04T10:00:00+00:00"),
        );

        assert_eq!(result.action, ReapAction::Removed);
        assert!(!pod.state().exists());
        assert!(newer_state.exists(), "the newer incarnation's record stays");
        assert!(newer_state.join(CLEANUP_TTLS_FILE).exists());
    }

    #[test]
    fn unevicted_state_dir_is_never_deleted() {
        let pod = Pod::new("apply-unevicted");
        pod.start(&[]);
        pod.finalize(Outcome::Succeeded, T0);

        let result = apply_deadline(
            &pod.state(),
            ScheduledReap::StateDir,
            at("2099-01-01T00:00:00+00:00"),
        );
        assert_eq!(result.action, ReapAction::Waiting);
        assert!(
            pod.state().exists(),
            "the runner still answers queries about a pod it has not let go",
        );
    }

    #[test]
    fn the_two_clocks_are_independent() {
        let pod = Pod::new("apply-independent");
        // Data kept forever, record deleted at once.
        pod.start(&[
            (ANN_POD_DATA_REAP_TTL_ON_POD_FAILURE, TTL_NEVER),
            (ANN_POD_DIR_DELETE_TTL_POD_FAILURE_ON_POD_DELETE, "0s"),
        ]);
        pod.finalize(Outcome::Failed, T0);
        pod.evict(T0);

        let now = at("2026-08-04T09:00:01+00:00");
        assert_eq!(
            apply_deadline(&pod.state(), ScheduledReap::StateDir, now).action,
            ReapAction::Removed
        );
        assert_eq!(
            apply_deadline(&pod.data(), ScheduledReap::DataDir, now).action,
            ReapAction::Waiting
        );
        assert!(pod.data().join(POD_WORK_SUBDIR).exists());
    }

    #[test]
    fn malformed_deadline_is_never_acted_on() {
        let pod = Pod::new("apply-malformed");
        pod.start(&[]);
        pod.finalize(Outcome::Failed, T0);
        fs::write(pod.data().join(REAP_ON_FILE), "soon").unwrap();

        let result = apply_deadline(
            &pod.data(),
            ScheduledReap::DataDir,
            at("2099-01-01T00:00:00+00:00"),
        );
        assert_eq!(result.action, ReapAction::Waiting);
        assert!(pod.data().exists());
    }

    // -- delete_pod_dir retry --

    /// A file landing in the tail of the walk must not strand the dir: the
    /// second walk takes it. Injected rather than raced, so this covers the
    /// retry on every run and every platform.
    #[test]
    fn delete_walks_again_when_the_dir_is_refilled() {
        let pod = Pod::new("delete-refilled");
        pod.start(&[]);
        let dir = pod.state();

        let attempts = std::cell::Cell::new(0u32);
        let deleted = delete_pod_dir_with(&dir, |dir| {
            attempts.set(attempts.get() + 1);
            if attempts.get() == 1 {
                // The walk emptied the dir, then a late write landed in it,
                // so the final `rmdir` refuses.
                fs::remove_dir_all(dir).unwrap();
                fs::create_dir_all(dir).unwrap();
                fs::write(dir.join("late.json"), "{}").unwrap();
                return Err(io::Error::from(io::ErrorKind::DirectoryNotEmpty));
            }
            fs::remove_dir_all(dir)
        });

        assert!(deleted, "the retry must finish what the first walk started");
        assert_eq!(attempts.get(), 2, "one refill needs exactly one retry");
        assert!(!dir.exists(), "the pod dir must not survive as a shell");
    }

    /// Bounded, so a dir being refilled faster than it can be walked fails
    /// loudly instead of spinning.
    #[test]
    fn delete_gives_up_after_a_bounded_number_of_walks() {
        let pod = Pod::new("delete-refilled-forever");
        pod.start(&[]);

        let attempts = std::cell::Cell::new(0u32);
        let deleted = delete_pod_dir_with(&pod.state(), |_| {
            attempts.set(attempts.get() + 1);
            Err(io::Error::from(io::ErrorKind::DirectoryNotEmpty))
        });

        assert!(!deleted);
        assert_eq!(attempts.get(), DELETE_ATTEMPTS, "bounded, not a spin");
    }

    #[test]
    fn delete_of_an_absent_dir_is_not_retried() {
        let attempts = std::cell::Cell::new(0u32);
        let deleted = delete_pod_dir_with(&tmp("delete-absent").join("gone"), |_| {
            attempts.set(attempts.get() + 1);
            Err(io::Error::from(io::ErrorKind::NotFound))
        });

        assert!(!deleted);
        assert_eq!(attempts.get(), 1, "a dir already gone needs no retry");
    }

    // -- schedule_deadline --

    /// A deadline still ahead is waited out by its own task, with no scan
    /// in between.
    #[tokio::test(start_paused = true)]
    async fn a_scheduled_reap_acts_when_its_deadline_comes_due() {
        let pod = Pod::new("schedule-reap");
        pod.start(&[]);
        let now = Utc::now();
        let deadline = finalize_pod(
            &pod.data(),
            &pod.state(),
            Outcome::Failed,
            &TtlConfig::default(),
            now,
        );
        let (_tx, rx) = watch::channel(());

        schedule_deadline(&pod.data(), ScheduledReap::DataDir, deadline, now, &rx);
        assert!(
            pod.data().exists(),
            "the data dir must survive until the deadline"
        );

        // Paused time: the task's sleep is the only thing being waited on.
        tokio::time::sleep(Duration::from_secs(3601)).await;
        tokio::task::yield_now().await;

        assert!(!pod.data().exists(), "the data dir must go once due");
        assert!(pod.state().exists(), "the record must stay behind");
    }

    #[tokio::test(start_paused = true)]
    async fn a_scheduled_delete_removes_the_state_dir() {
        let pod = Pod::new("schedule-delete");
        pod.start(&[]);
        let now = Utc::now();
        pod.finalize(Outcome::Succeeded, T0);
        let deadline = mark_pod_evicted(&pod.state(), &TtlConfig::default(), now);
        let (_tx, rx) = watch::channel(());

        schedule_deadline(&pod.state(), ScheduledReap::StateDir, deadline, now, &rx);
        tokio::time::sleep(Duration::from_secs(3601)).await;
        tokio::task::yield_now().await;

        assert!(!pod.state().exists(), "the whole state dir must go");
    }

    /// A pod replayed under the same id claims its dirs back while the
    /// previous attempt's tasks still sleep. Those tasks must stand down,
    /// not remove the live pod's dirs.
    #[tokio::test(start_paused = true)]
    async fn a_reclaimed_dir_stands_its_pending_tasks_down() {
        let pod = Pod::new("schedule-reclaimed");
        pod.start(&[]);
        let now = Utc::now();
        let reap_on = finalize_pod(
            &pod.data(),
            &pod.state(),
            Outcome::Failed,
            &TtlConfig::default(),
            now,
        );
        let delete_on = mark_pod_evicted(&pod.state(), &TtlConfig::default(), now);
        let (_tx, rx) = watch::channel(());
        schedule_deadline(&pod.data(), ScheduledReap::DataDir, reap_on, now, &rx);
        schedule_deadline(&pod.state(), ScheduledReap::StateDir, delete_on, now, &rx);

        // Replayed: claimed and running again.
        pod.start(&[]);

        tokio::time::sleep(Duration::from_secs(2 * 86_400)).await;
        tokio::task::yield_now().await;

        assert!(
            pod.data().join(POD_WORK_SUBDIR).exists(),
            "live data dir kept"
        );
        assert!(
            pod.state().join(POD_LOG_SUBDIR).exists(),
            "live record kept"
        );
    }

    /// A later deadline written over the scheduled one supersedes it; the
    /// earlier task must not act on the later one's behalf.
    #[tokio::test(start_paused = true)]
    async fn a_superseded_deadline_stands_down() {
        let pod = Pod::new("schedule-superseded");
        pod.start(&[]);
        let now = Utc::now();
        let reap_on = finalize_pod(
            &pod.data(),
            &pod.state(),
            Outcome::Failed,
            &TtlConfig::default(),
            now,
        );
        let (_tx, rx) = watch::channel(());
        schedule_deadline(&pod.data(), ScheduledReap::DataDir, reap_on, now, &rx);
        write_deadline(&pod.data(), REAP_ON_FILE, Ttl::Never, now).unwrap();

        tokio::time::sleep(Duration::from_secs(7200)).await;
        tokio::task::yield_now().await;

        assert!(pod.data().exists(), "never, written later, wins");
    }

    /// A pending deadline must never hold the runner's exit. What it would
    /// have done is dropped — the next run's recovery schedules it again.
    #[tokio::test(start_paused = true)]
    async fn shutdown_drops_a_pending_deadline() {
        let pod = Pod::new("schedule-shutdown");
        pod.start(&[]);
        let now = Utc::now();
        pod.finalize(Outcome::Succeeded, T0);
        let deadline = mark_pod_evicted(&pod.state(), &TtlConfig::default(), now);
        let (tx, rx) = watch::channel(());

        schedule_deadline(&pod.state(), ScheduledReap::StateDir, deadline, now, &rx);

        let _ = tx.send(());
        tokio::time::sleep(Duration::from_secs(7200)).await;
        tokio::task::yield_now().await;

        assert!(
            pod.state().exists(),
            "a deadline cancelled by shutdown must not act"
        );
    }

    /// `never`, an absent deadline, and one already due all need no task:
    /// the first two never come due, and the caller's inline pass has
    /// already handled the third.
    #[tokio::test(start_paused = true)]
    async fn deadlines_needing_no_task_schedule_none() {
        let (_tx, rx) = watch::channel(());
        let now = Utc::now();

        for (name, deadline) in [
            ("never", Some(Deadline::Never)),
            ("absent", None),
            (
                "due",
                Some(Deadline::At(Utc::now() - chrono::Duration::hours(1))),
            ),
            // A `0s` TTL: the deadline *is* the instant it was scheduled at.
            ("exactly-due", Some(Deadline::At(now))),
        ] {
            let pod = Pod::new(&format!("schedule-none-{name}"));
            pod.start(&[]);
            pod.finalize(Outcome::Succeeded, T0);
            if let Some(d) = deadline {
                fs::write(pod.state().join(DELETE_ON_FILE), d.encode()).unwrap();
            }
            schedule_deadline(&pod.state(), ScheduledReap::StateDir, deadline, now, &rx);
            tokio::time::sleep(Duration::from_secs(7200)).await;
            tokio::task::yield_now().await;
            assert!(
                pod.state().exists(),
                "{name} should not have been scheduled"
            );
        }
    }

    // -- recover_pod_dirs --

    /// What a runner that went down mid-flight leaves: a pod still running
    /// (no markers anywhere) and a finished pod the control plane had not
    /// deleted yet (markers, no delete deadline).
    #[tokio::test(start_paused = true)]
    async fn recovery_starts_the_clocks_a_restart_would_have_lost() {
        let base = tmp("recover-clocks");
        let layout = PodLayout::new(&base);
        let running = pod_spec("running", "uid-1");
        let finished = pod_spec("finished", "uid-2");
        for id in [&running, &finished] {
            claim_pod_dirs(
                &layout.run_dir(&PodId::from_spec(id)),
                &layout.state_dir(id),
                &TtlConfig::default(),
                &HashMap::new(),
                at(T0),
            );
            fs::create_dir_all(layout.run_dir(&PodId::from_spec(id)).join(POD_WORK_SUBDIR))
                .unwrap();
        }
        let now = Utc::now();
        finalize_pod(
            &layout.run_dir(&PodId::from_spec(&finished)),
            &layout.state_dir(&finished),
            Outcome::Succeeded,
            &TtlConfig::default(),
            now,
        );
        // Default reap-on-success is 0s, applied inline at completion.
        apply_deadline(
            &layout.run_dir(&PodId::from_spec(&finished)),
            ScheduledReap::DataDir,
            now,
        );
        assert!(!layout.run_dir(&PodId::from_spec(&finished)).exists());

        let (_tx, rx) = watch::channel(());
        let stats = recover_pod_dirs(&layout, &TtlConfig::default(), now, &rx);

        // The running pod: finalized failed — data kept 1h, record 24h.
        assert_eq!(
            read_outcome(&layout.run_dir(&PodId::from_spec(&running)))
                .unwrap()
                .0,
            Outcome::Failed
        );
        assert_eq!(
            read_outcome(&layout.state_dir(&running)).unwrap().0,
            Outcome::Failed
        );
        assert_eq!(
            read_deadline(&layout.run_dir(&PodId::from_spec(&running)), REAP_ON_FILE),
            Some(Deadline::At(now + chrono::Duration::hours(1)))
        );
        assert_eq!(
            read_deadline(&layout.state_dir(&running), DELETE_ON_FILE),
            Some(Deadline::At(now + chrono::Duration::hours(24)))
        );
        // The finished pod keeps its outcome; its delete clock starts now.
        assert_eq!(
            read_outcome(&layout.state_dir(&finished)).unwrap().0,
            Outcome::Succeeded
        );
        assert_eq!(
            read_deadline(&layout.state_dir(&finished), DELETE_ON_FILE),
            Some(Deadline::At(now + chrono::Duration::hours(1)))
        );
        assert_eq!(
            stats,
            RecoveryStats {
                removed: 0,
                scheduled: 3,
                kept: 0
            }
        );

        // And the scheduled tasks act.
        tokio::time::sleep(Duration::from_secs(3601)).await;
        tokio::task::yield_now().await;
        assert!(!layout.run_dir(&PodId::from_spec(&running)).exists());
        assert!(!layout.state_dir(&finished).exists());
        assert!(layout.state_dir(&running).exists(), "24h not up yet");

        let _ = fs::remove_dir_all(&base);
    }

    #[test]
    fn a_reaped_data_dir_takes_its_link_and_recovery_prunes_orphans() {
        let base = tmp("reap-link");
        let layout = PodLayout::new(&base);
        let reaped = PodId::from_spec(&pod_spec("reaped", "uid-1"));
        let orphan = PodId::new("default", "orphan", "uid-2");
        for id in [&reaped, &orphan] {
            fs::create_dir_all(layout.run_dir(id)).unwrap();
            layout.link_run_dir(id).unwrap();
        }
        fs::remove_dir_all(layout.run_dir(&orphan)).unwrap();
        let now = at(T0);
        finalize_data_dir(
            &layout.run_dir(&reaped),
            Outcome::Succeeded,
            &TtlConfig::default(),
            now,
        );

        let (_tx, rx) = watch::channel(());
        let stats = recover_pod_dirs(&layout, &TtlConfig::default(), now, &rx);

        assert_eq!(stats.removed, 1);
        assert!(!layout.run_dir(&reaped).exists());
        assert!(fs::symlink_metadata(layout.run_link(&reaped)).is_err());
        assert!(fs::symlink_metadata(layout.run_link(&orphan)).is_err());

        let _ = fs::remove_dir_all(&base);
    }

    #[tokio::test(start_paused = true)]
    async fn recovery_removes_what_came_due_while_the_runner_was_down() {
        let pod = Pod::new("recover-due");
        pod.start(&[]);
        pod.finalize(Outcome::Failed, T0);
        pod.evict(T0);

        let (_tx, rx) = watch::channel(());
        let stats = recover_pod_dirs(
            &pod.layout,
            &TtlConfig::default(),
            at("2026-08-06T00:00:00+00:00"),
            &rx,
        );

        assert!(!pod.data().exists());
        assert!(!pod.state().exists());
        assert_eq!(stats.removed, 2);
    }

    #[tokio::test(start_paused = true)]
    async fn recovery_honours_never() {
        let pod = Pod::new("recover-never");
        pod.start(&[
            (ANN_POD_DATA_REAP_TTL_ON_POD_FAILURE, TTL_NEVER),
            (ANN_POD_DIR_DELETE_TTL_POD_FAILURE_ON_POD_DELETE, TTL_NEVER),
        ]);
        pod.finalize(Outcome::Failed, T0);
        pod.evict(T0);

        let (_tx, rx) = watch::channel(());
        let stats = recover_pod_dirs(
            &pod.layout,
            &TtlConfig::default(),
            at("2099-01-01T00:00:00+00:00"),
            &rx,
        );

        assert!(pod.data().join(POD_WORK_SUBDIR).exists());
        assert!(pod.state().join(POD_LOG_SUBDIR).exists());
        assert_eq!(stats.kept, 2);
    }

    #[tokio::test(start_paused = true)]
    async fn recovery_of_an_empty_layout_is_a_no_op() {
        let (_tx, rx) = watch::channel(());
        let layout = PodLayout::new(&tmp("recover-empty").join("missing"));
        assert_eq!(
            recover_pod_dirs(&layout, &TtlConfig::default(), Utc::now(), &rx),
            RecoveryStats::default()
        );
    }
}
