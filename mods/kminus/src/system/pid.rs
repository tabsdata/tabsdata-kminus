// Copyright 2026 Tabsdata Inc.

//! Process and PID-file helpers: PID files, liveness, cross-platform
//! spawn flags, stderr capture, graceful and forceful termination.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use kcommon::identity;
use sysinfo::{Pid, ProcessStatus, ProcessesToUpdate, System};
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
    let _ = fs::create_dir_all(dir);
    let path = dir.join(format!("{}.{}", pid, name));
    let content = match identity::process_start_time(pid) {
        Some(secs) => identity::owner_identity(pid, secs),
        None => {
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
    let _ = fs::write(&path, content);
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
        let verified = fs::read_to_string(entry.path())
            .ok()
            .and_then(|content| {
                let (recorded_pid, stamp) = identity::parse_owner(content.trim())?;
                Some(recorded_pid == pid && identity::running_since(pid, stamp))
            })
            .unwrap_or(false);
        return if verified {
            Recorded::Live(pid)
        } else {
            Recorded::Stale(pid)
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
                let _ = fs::remove_file(entry.path());
            }
        }
    }
}

/// Apply OS-specific creation flags so a spawned subprocess survives the
/// parent's console/terminal closing. On Windows this sets
/// `CREATE_NO_WINDOW`; on Unix this is a no-op.
pub fn apply_detached_flags(command: &mut Command) {
    #[cfg(target_os = "windows")]
    {
        use std::os::windows::process::CommandExt;
        use windows_sys::Win32::System::Threading::CREATE_NO_WINDOW;
        command.creation_flags(CREATE_NO_WINDOW);
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

/// Check whether a process with the given PID is currently running.
pub fn is_alive(pid: u32) -> bool {
    let mut system = System::new();
    system.refresh_processes(ProcessesToUpdate::Some(&[Pid::from_u32(pid)]), true);
    match system.process(Pid::from_u32(pid)) {
        Some(process) => process.status() != ProcessStatus::Zombie,
        None => false,
    }
}

/// Walk the process table by PPID and return every descendant of `root`.
/// Needed because some children (notably kube-apiserver and etcd) call
/// `setpgid` and escape the leader's process group, so a group-wide
/// SIGKILL alone leaves them dangling.
fn collect_descendants(root: u32) -> Vec<u32> {
    let mut system = System::new();
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

/// Send a graceful shutdown request to the process with `pid`, wait up
/// to `grace` for the whole descendant tree, then force-kill anything
/// still alive.
///
/// Unix: `SIGTERM` to the leader, then (after `grace`) `SIGKILL` to the
/// process group and to each descendant by PID. The per-PID pass catches
/// kube-apiserver and etcd, which escape the leader's process group.
/// Windows: `CTRL_BREAK_EVENT` to the process group then (after
/// `grace`) `taskkill /F /T` to terminate the whole tree.
pub fn terminate_process(pid: u32, grace: Duration) {
    let descendants = collect_descendants(pid);
    request_graceful_shutdown(pid);
    if wait_for_tree_exit(pid, &descendants, grace) {
        return;
    }
    force_kill_tree(pid, &descendants);
}

fn request_graceful_shutdown(pid: u32) {
    #[cfg(unix)]
    {
        use nix::sys::signal::{self, Signal};
        use nix::unistd::Pid as NixPid;
        let _ = signal::kill(NixPid::from_raw(pid as i32), Signal::SIGTERM);
    }
    #[cfg(target_os = "windows")]
    {
        use windows_sys::Win32::System::Console::{CTRL_BREAK_EVENT, GenerateConsoleCtrlEvent};
        unsafe {
            GenerateConsoleCtrlEvent(CTRL_BREAK_EVENT, pid);
        }
    }
}

fn wait_for_tree_exit(root: u32, descendants: &[u32], timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    loop {
        let any_alive = is_alive(root) || descendants.iter().any(|pid| is_alive(*pid));
        if !any_alive {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

fn force_kill_tree(root: u32, descendants: &[u32]) {
    #[cfg(unix)]
    {
        use nix::sys::signal::{self, Signal};
        use nix::unistd::Pid as NixPid;
        let _ = signal::kill(NixPid::from_raw(-(root as i32)), Signal::SIGKILL);
        for pid in descendants {
            let _ = signal::kill(NixPid::from_raw(*pid as i32), Signal::SIGKILL);
        }
    }
    #[cfg(target_os = "windows")]
    {
        let _ = descendants;
        let mut command = Command::new("taskkill.exe");
        apply_detached_flags(&mut command);
        let _ = command
            .arg("/PID")
            .arg(root.to_string())
            .arg("/T")
            .arg("/F")
            .status();
    }
}

/// Best-effort: force-kill every process still parented under `root`
/// (transitively), without touching `root` itself. Used as a teardown
/// backstop for subprocesses that were spawned but whose PID files may
/// not have been written yet (e.g. a Ctrl+C in the middle of startup).
/// Each target's own process group is killed too, to catch isolated
/// children that lead their own group.
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

/// Walk every PID file in `dir`; if the recorded process is still alive,
/// terminate it (graceful then force) and remove the file.
pub fn cleanup_stale_pids(dir: &Path) {
    let entries = match fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(_) => return,
    };
    for entry in entries.flatten() {
        let fname = entry.file_name();
        let fname = fname.to_string_lossy();
        let parts: Vec<&str> = fname.splitn(2, '.').collect();
        if parts.len() != 2 {
            continue;
        }
        let Ok(pid) = parts[0].parse::<u32>() else {
            continue;
        };
        let process_name = parts[1];

        if is_alive(pid) {
            warn!(pid, process_name, "Killing stale process");
            terminate_process(pid, Duration::from_secs(2));
        }
        let _ = fs::remove_file(entry.path());
    }
}

/// Send a graceful shutdown request to a child process and wait up to
/// 5 seconds for exit; if it does not exit, force-kill the whole tree.
pub fn kill_child(child: &mut Child, name: &str) {
    let pid = child.id();
    info!(name, pid, "Requesting graceful shutdown");
    terminate_process(pid, Duration::from_secs(5));
    let _ = child.wait();
}

#[cfg(test)]
mod tests;
