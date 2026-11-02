// Copyright 2026 Tabsdata Inc.

//! Process and PID-file helpers: PID files, liveness, cross-platform
//! spawn flags, stderr capture, graceful and forceful termination.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use kcommon::{identity, signal};
use tracing::{info, warn};

pub const DATA_DIR: &str = "data";
pub const PIDS_DIR: &str = "pids";
pub const BIN_KM: &str = "km";

/// Directory under which PID files for `bin` are stored:
/// `<workdir>/data/<bin>/pids/`.
pub fn pids_dir(workdir: &Path, bin: &str) -> PathBuf {
    workdir.join(DATA_DIR).join(bin).join(PIDS_DIR)
}

/// Write a PID file at `<dir>/<pid>.<name>` recording the identity of the
/// process, not merely its number.
///
/// The content is `<pid>@<start-stamp>`, which is what lets a later reader
/// tell "that process is still running" from "something now holds that
/// number". These files outlive the run that wrote them — that is their
/// purpose — so by the time anything reads one, the operating system has
/// had every opportunity to hand the number to somebody else.
pub fn write_pid(dir: &Path, name: &str, pid: u32) {
    if let Err(e) = fs::create_dir_all(dir) {
        warn!(dir = %dir.display(), error = %e, "Cannot create PID directory");
    }
    let path = dir.join(format!("{}.{}", pid, name));
    let content = match identity::process_start_time(pid) {
        Ok(Some(secs)) => identity::owner_identity(pid, secs),
        Err(error) => {
            warn!(
                name,
                pid,
                %error,
                "Cannot read the process table; PID file will not be verifiable"
            );
            format!("{}{}{}", pid, identity::OWNER_SEP, identity::UNKNOWN_STAMP)
        }
        Ok(None) => {
            // Only reachable if the process went away between spawning and
            // this call. Recorded unverifiably on purpose: a reader will
            // treat it as stale, which for a process that is already gone
            // is the right answer.
            warn!(
                name,
                pid, "Cannot read start time; PID file will not be verifiable"
            );
            format!("{}{}{}", pid, identity::OWNER_SEP, identity::UNKNOWN_STAMP)
        }
    };
    if let Err(e) = fs::write(&path, content) {
        // This file is what `recorded` — the single-instance guard and the
        // status/stop commands — reads. Running without it is running
        // unobserved, which must not happen silently.
        warn!(name, pid, path = %path.display(), error = %e, "Cannot write PID file");
    }
}

/// What a PID file says about its subject.
#[derive(Debug, PartialEq, Eq)]
pub enum Recorded {
    /// No file for this name.
    Absent,
    /// A file, and the process it recorded is still that same process.
    Live(u32),
    /// A file, but its subject is gone — or the number now belongs to
    /// something else, which reads the same way and must, since acting on
    /// it either way would be acting on a stranger.
    Stale(u32),
    /// A file, but the process table cannot be read, so whether its subject
    /// still runs is unknown. Neither started over nor signalled.
    Unknown(u32),
}

/// Read the PID file for `name` and say whether its subject is still
/// there.
///
/// Verified against the recorded start stamp, not just the presence of
/// *a* process with that number. Without that, `km stop` would eventually
/// signal whatever inherited a dead km's PID, and `km start` would refuse
/// to start because a stranger was holding it.
///
/// A file that predates this format, or that cannot be parsed, reads as
/// [`Recorded::Stale`]: unverifiable is not the same as running, and
/// nothing should be signalled on the strength of it.
pub fn recorded(dir: &Path, name: &str) -> Recorded {
    let suffix = format!(".{}", name);
    let Ok(entries) = fs::read_dir(dir) else {
        return Recorded::Absent;
    };
    for entry in entries.flatten() {
        let fname = entry.file_name();
        let fname = fname.to_string_lossy();
        let Some(pid_str) = fname.strip_suffix(&suffix) else {
            continue;
        };
        let Ok(pid) = pid_str.parse::<u32>() else {
            continue;
        };
        let verified = fs::read_to_string(entry.path()).ok().and_then(|content| {
            let (recorded_pid, stamp) = identity::parse_owner(content.trim())?;
            if recorded_pid != pid {
                return Some(Ok(false));
            }
            Some(identity::running_since(pid, stamp))
        });
        return match verified {
            Some(Ok(true)) => Recorded::Live(pid),
            Some(Ok(false)) | None => Recorded::Stale(pid),
            Some(Err(error)) => {
                warn!(name, pid, %error, "Cannot read the process table; PID file unverified");
                Recorded::Unknown(pid)
            }
        };
    }
    Recorded::Absent
}

/// Remove the PID file(s) for `name` from `dir`.
pub fn remove_pid(dir: &Path, name: &str) {
    if let Ok(entries) = fs::read_dir(dir) {
        for entry in entries.flatten() {
            let fname = entry.file_name();
            if fname.to_string_lossy().ends_with(&format!(".{}", name)) {
                remove_file_best_effort(&entry.path(), "PID file");
            }
        }
    }
}

/// Remove a file, warning on any failure except the file already being
/// gone — for hygiene removals where absence is the goal, so best-effort
/// is the right semantics but a persistent failure must not be invisible.
pub fn remove_file_best_effort(path: &Path, what: &str) {
    if let Err(e) = fs::remove_file(path)
        && e.kind() != std::io::ErrorKind::NotFound
    {
        warn!(path = %path.display(), error = %e, "Cannot remove {what}");
    }
}

/// [`remove_file_best_effort`], for a directory tree.
pub fn remove_dir_all_best_effort(path: &Path, what: &str) {
    if let Err(e) = fs::remove_dir_all(path)
        && e.kind() != std::io::ErrorKind::NotFound
    {
        warn!(path = %path.display(), error = %e, "Cannot remove {what}");
    }
}

/// Apply OS-specific creation flags so a spawned subprocess survives the
/// parent's console/terminal closing. On Windows this sets
/// `CREATE_NO_WINDOW | CREATE_NEW_PROCESS_GROUP` — its own hidden console
/// plus group leadership, keeping the invariant that everything this
/// stack spawns on Windows can be addressed by a group CTRL_BREAK and is
/// deaf to console events aimed at anyone else. On Unix this is a no-op.
pub fn apply_detached_flags(command: &mut Command) {
    #[cfg(target_os = "windows")]
    {
        use std::os::windows::process::CommandExt;
        use windows_sys::Win32::System::Threading::{CREATE_NEW_PROCESS_GROUP, CREATE_NO_WINDOW};
        command.creation_flags(CREATE_NO_WINDOW | CREATE_NEW_PROCESS_GROUP);
    }
    #[cfg(not(target_os = "windows"))]
    {
        let _ = command;
    }
}

/// Apply OS-specific creation flags for a long-running subprocess that
/// must survive terminal close and accept targeted graceful shutdown.
/// On Windows this sets `CREATE_NO_WINDOW | CREATE_NEW_PROCESS_GROUP`;
/// on Unix this puts the child in its own process group.
pub fn apply_subprocess_flags(command: &mut Command) {
    #[cfg(target_os = "windows")]
    {
        use std::os::windows::process::CommandExt;
        use windows_sys::Win32::System::Threading::{CREATE_NEW_PROCESS_GROUP, CREATE_NO_WINDOW};
        command.creation_flags(CREATE_NO_WINDOW | CREATE_NEW_PROCESS_GROUP);
    }
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        // Put the child in its own process group (pgid == its own pid).
        // The controlling terminal delivers Ctrl+C as SIGINT to *every*
        // process in km's foreground group; without its own group a child
        // would receive that SIGINT directly and race km's orderly
        // shutdown — exiting before km can walk its process tree and
        // orphaning grandchildren (etcd, kube-apiserver) that escaped to
        // their own groups via setpgid. With its own group, km is the
        // sole signal target and drives shutdown via terminate_process.
        // Mirrors CREATE_NEW_PROCESS_GROUP on Windows; also makes the
        // group-wide SIGKILL in force_kill_tree (kill(-pid)) target the
        // child's own group.
        command.process_group(0);
    }
    #[cfg(not(any(target_os = "windows", unix)))]
    {
        let _ = command;
    }
}

/// Filename extension of a component's captured standard error, giving
/// `<logdir>/<bin>.err`.
const STDERR_LOG_EXT: &str = "err";

/// Path [`capture_stderr`] redirects `bin`'s standard error to.
pub fn stderr_log(logdir: &Path, bin: &str) -> PathBuf {
    logdir.join(format!("{}.{}", bin, STDERR_LOG_EXT))
}

/// Redirect a subprocess's standard error to [`stderr_log`], discarding
/// its standard output.
///
/// Every component logs through its own rotating file, so its standard
/// error is normally empty — which is precisely why it must not be thrown
/// away. A process that dies without logging says why on that stream and
/// nowhere else: a Rust abort prints `memory allocation of N bytes
/// failed`, a panic prints its location and message, a Go runtime failure
/// prints its stack. Discarded, such a death reaches km as a component
/// log that simply stops mid-run, with nothing to say what ended it.
///
/// Truncated on every spawn, so the file describes the current run only.
/// A component failure stops the whole stack rather than restarting it,
/// so the file outlives the failure it recorded.
///
/// Best effort: a file that cannot be opened leaves the stream discarded
/// instead of failing the start, since losing a diagnostic is a smaller
/// harm than refusing to boot over one.
pub fn capture_stderr(command: &mut Command, logdir: &Path, bin: &str) {
    command.stdout(Stdio::null());
    let path = stderr_log(logdir, bin);
    let _ = fs::create_dir_all(logdir);
    match fs::File::create(&path) {
        Ok(file) => {
            command.stderr(Stdio::from(file));
        }
        Err(e) => {
            warn!(
                bin,
                path = %path.display(),
                error = %e,
                "Cannot capture stderr; discarding it",
            );
            command.stderr(Stdio::null());
        }
    }
}

/// Ask the process a PID file records to stop, wait up to `grace` for it
/// to go, then force-kill it and the group it leads.
///
/// Judged by [`recorded`] throughout, so every step acts on an identity
/// rather than a number: nothing is asked of a file whose subject is
/// gone or reused, the wait ends the moment the subject does (a zombie
/// awaiting its reaper counts as gone), and the escalation cannot fire
/// at a recycled PID because the identity was re-verified on the same
/// poll that found it still there.
///
/// This stops the recorded process and its process group. Its wider tree
/// is not this function's job: everything below a km component carries
/// the run's kill token, and the token sweep after a teardown reclaims
/// whatever outlives this.
pub fn stop_recorded(dir: &Path, name: &str, grace: Duration) {
    let pid = match recorded(dir, name) {
        Recorded::Live(pid) => pid,
        Recorded::Unknown(pid) => {
            warn!(
                name,
                pid, "Cannot verify the recorded process; not signalling it"
            );
            return;
        }
        Recorded::Absent | Recorded::Stale(_) => return,
    };
    info!(
        name,
        pid,
        grace_secs = grace.as_secs(),
        "Requesting graceful shutdown"
    );
    signal::request_graceful(pid);

    let deadline = Instant::now() + grace;
    while Instant::now() < deadline {
        if matches!(recorded(dir, name), Recorded::Absent | Recorded::Stale(_)) {
            return;
        }
        std::thread::sleep(Duration::from_millis(100));
    }

    warn!(name, pid, "Grace period expired, killing");
    if signal::leads_own_group(pid) {
        signal::force_kill_group(pid);
    } else {
        signal::force_kill(pid);
    }
}

/// Every process parented (transitively) under `root`, by PPID walk.
#[cfg(target_os = "macos")]
fn collect_descendants(root: u32) -> Vec<u32> {
    use sysinfo::{Pid, ProcessesToUpdate, System};

    let mut system = match System::new() {
        Ok(system) => system,
        Err(error) => {
            warn!(root, %error, "Cannot read the process table; no descendants to kill");
            return Vec::new();
        }
    };
    system.refresh_processes(ProcessesToUpdate::All, true);
    let root_pid = Pid::from_u32(root);
    let mut descendants: Vec<u32> = Vec::new();
    let mut frontier: Vec<Pid> = vec![root_pid];
    while let Some(current) = frontier.pop() {
        for (pid, process) in system.processes() {
            if process.parent() == Some(current) {
                descendants.push(pid.as_u32());
                frontier.push(*pid);
            }
        }
    }
    descendants
}

/// Best-effort: force-kill every process still parented under `root`
/// (transitively), and each target's own group, without touching `root`.
///
/// macOS only, as a backstop for the one thing the kill-token sweep
/// cannot see there: SIP hides an Apple platform binary's environment, so
/// a `/bin/sh` wrapper carries its token unreadably. The gap is one hop
/// wide (children re-expose the token; a simple command leaves no wrapper
/// since the shell execs). Elsewhere the sweep sees everything this would
/// and also reaches reparented processes, which no parent walk can.
#[cfg(target_os = "macos")]
pub fn kill_descendants(root: u32) {
    let descendants = collect_descendants(root);
    #[cfg(unix)]
    {
        use nix::sys::signal::{self, Signal};
        use nix::unistd::Pid as NixPid;
        for pid in &descendants {
            let _ = signal::kill(NixPid::from_raw(-(*pid as i32)), Signal::SIGKILL);
            let _ = signal::kill(NixPid::from_raw(*pid as i32), Signal::SIGKILL);
        }
    }
    #[cfg(target_os = "windows")]
    {
        for pid in &descendants {
            let mut command = Command::new("taskkill.exe");
            apply_detached_flags(&mut command);
            let _ = command
                .arg("/PID")
                .arg(pid.to_string())
                .arg("/T")
                .arg("/F")
                .status();
        }
    }
}

/// Walk every PID file in `dir`; if its subject is still running — the
/// same process the file recorded, verified by identity — stop it
/// (graceful, then force). Remove the file either way.
///
/// These files are km's own identity-stamped records from a previous run,
/// so the numbers in them have had the longest possible time to be
/// reused; the stamp is what makes acting on one safe. Content that does
/// not verify — the old bare-number format included — is never signalled:
/// whatever really is still running from that run carries its kill token,
/// and the startup sweep reclaims it by that instead.
pub fn cleanup_stale_pids(dir: &Path) {
    let entries = match fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(_) => return,
    };
    for entry in entries.flatten() {
        let fname = entry.file_name();
        let fname = fname.to_string_lossy();
        let parts: Vec<&str> = fname.splitn(2, '.').collect();
        if parts.len() != 2 || parts[0].parse::<u32>().is_err() {
            continue;
        }
        let name = parts[1];
        stop_recorded(dir, name, Duration::from_secs(2));
        remove_file_best_effort(&entry.path(), "stale PID file");
    }
}

#[cfg(test)]
mod tests;
