// Copyright 2026 Tabsdata Inc.

//! PID-file management and process termination.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Child;
use std::time::{Duration, Instant};

use nix::sys::signal::{self, Signal};
use nix::unistd::Pid;
use tracing::{info, warn};

/// Per-component runtime data goes under `<workdir>/data/<component>/`.
pub const DATA_DIR: &str = "data";

/// Each component's PIDs go under `<workdir>/data/<component>/pids/`.
pub const PIDS_DIR: &str = "pids";

/// Binary name for km itself. Used in PID file naming and to derive
/// `<workdir>/data/km/pids/`.
pub const BIN_KM: &str = "km";

/// PID directory for `bin`: `<workdir>/data/<bin>/pids/`.
pub fn pids_dir(workdir: &Path, bin: &str) -> PathBuf {
    workdir.join(DATA_DIR).join(bin).join(PIDS_DIR)
}

/// Write a PID file at `<dir>/<pid>.<name>` containing the PID as text.
/// Creates `dir` if missing. Errors are silently ignored.
pub fn write_pid(dir: &Path, name: &str, pid: u32) {
    let _ = fs::create_dir_all(dir);
    let path = dir.join(format!("{}.{}", pid, name));
    let _ = fs::write(&path, pid.to_string());
}

/// Remove the PID file(s) for the given component name from `dir`.
/// Matches files ending with `.<name>`.
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

/// Walk every PID file in `dir` and SIGTERM/SIGKILL the recorded PID
/// if the process is still alive. Removes each PID file afterwards.
/// Used at startup to clean up after a previous run that crashed
/// without releasing its children.
pub fn cleanup_stale_pids(dir: &Path) {
    let entries = match fs::read_dir(dir) {
        Ok(e) => e,
        Err(_) => return,
    };
    for entry in entries.flatten() {
        let name = entry.file_name();
        let name = name.to_string_lossy();
        let parts: Vec<&str> = name.splitn(2, '.').collect();
        if parts.len() != 2 {
            continue;
        }
        let pid: i32 = match parts[0].parse() {
            Ok(p) => p,
            Err(_) => continue,
        };
        let process_name = parts[1];

        if signal::kill(Pid::from_raw(pid), None).is_ok() {
            warn!(pid, process_name, "Killing stale process");
            let _ = signal::kill(Pid::from_raw(pid), Signal::SIGTERM);
            std::thread::sleep(Duration::from_secs(2));
            if signal::kill(Pid::from_raw(pid), None).is_ok() {
                let _ = signal::kill(Pid::from_raw(pid), Signal::SIGKILL);
                std::thread::sleep(Duration::from_millis(500));
            }
        }
        let _ = fs::remove_file(entry.path());
    }
}

/// Send SIGTERM to a child process and wait up to 5s for exit; if it
/// does not exit, send SIGKILL.
pub fn kill_child(child: &mut Child, name: &str) {
    let pid = child.id() as i32;
    info!(name, pid, "Sending SIGTERM");
    let _ = signal::kill(Pid::from_raw(pid), Signal::SIGTERM);

    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        match child.try_wait() {
            Ok(Some(_)) => return,
            Ok(None) if Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(100));
            }
            _ => break,
        }
    }

    warn!(name, pid, "SIGTERM timeout, sending SIGKILL");
    let _ = signal::kill(Pid::from_raw(pid), Signal::SIGKILL);
    let _ = child.wait();
}

#[cfg(test)]
mod tests;
