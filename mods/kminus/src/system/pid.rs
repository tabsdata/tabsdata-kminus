// Copyright 2026 Tabsdata Inc.

//! Process and PID-file helpers: PID files, liveness, cross-platform
//! spawn flags, graceful and forceful termination.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::time::{Duration, Instant};

use sysinfo::{Pid, System};
use tracing::{info, warn};

pub const DATA_DIR: &str = "data";
pub const PIDS_DIR: &str = "pids";
pub const BIN_KM: &str = "km";

/// Directory under which PID files for `bin` are stored:
/// `<workdir>/data/<bin>/pids/`.
pub fn pids_dir(workdir: &Path, bin: &str) -> PathBuf {
    workdir.join(DATA_DIR).join(bin).join(PIDS_DIR)
}

/// Write a PID file at `<dir>/<pid>.<name>`.
pub fn write_pid(dir: &Path, name: &str, pid: u32) {
    let _ = fs::create_dir_all(dir);
    let path = dir.join(format!("{}.{}", pid, name));
    let _ = fs::write(&path, pid.to_string());
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
/// on Unix this is a no-op.
pub fn apply_subprocess_flags(command: &mut Command) {
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

/// Check whether a process with the given PID is currently running.
pub fn is_alive(pid: u32) -> bool {
    let mut system = System::new();
    system.refresh_processes(
        sysinfo::ProcessesToUpdate::Some(&[Pid::from_u32(pid)]),
        true,
    );
    system.process(Pid::from_u32(pid)).is_some()
}

/// Send a graceful shutdown request to the process with `pid`, wait up
/// to `grace`, then force-kill the whole tree if it has not exited.
///
/// Unix: `SIGTERM` then (after `grace`) `SIGKILL`.
/// Windows: `CTRL_BREAK_EVENT` to the process group then (after
/// `grace`) `taskkill /F /T` to terminate the whole tree.
pub fn terminate_process(pid: u32, grace: Duration) {
    request_graceful_shutdown(pid);
    if wait_for_exit(pid, grace) {
        return;
    }
    force_kill_tree(pid);
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

fn wait_for_exit(pid: u32, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if !is_alive(pid) {
            return true;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    !is_alive(pid)
}

fn force_kill_tree(pid: u32) {
    #[cfg(unix)]
    {
        use nix::sys::signal::{self, Signal};
        use nix::unistd::Pid as NixPid;
        let _ = signal::kill(NixPid::from_raw(pid as i32), Signal::SIGKILL);
    }
    #[cfg(target_os = "windows")]
    {
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
