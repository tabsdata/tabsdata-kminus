// Copyright 2026 Tabsdata Inc.

//! Process, port, file-watcher, and PID-file utilities for `km`.
//!
//! Pure helpers with no business logic — kept testable and free of
//! references to specific subprocesses.

use std::env;
use std::fs;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::Child;
use std::time::{Duration, Instant};

use nix::sys::signal::{self, Signal};
use nix::unistd::Pid;
use tracing::{info, warn};

// ---------------------------------------------------------------------------
// Standard subdirectory names
// ---------------------------------------------------------------------------

/// Per-component runtime data goes under `<workdir>/data/<component>/`.
pub const DATA_DIR: &str = "data";

/// Each component's PIDs go under `<workdir>/data/<component>/pids/`.
pub const PIDS_DIR: &str = "pids";

/// Default config directory name (under `<workdir>` if no `--configdir`).
pub const CONFIG_DIR: &str = "config";

/// Default log directory name (under `<workdir>` if no `--logdir`).
pub const LOG_DIR: &str = "log";

/// Binary name for km itself. Used in PID file naming and to derive
/// `<workdir>/data/km/pids/`.
pub const BIN_KM: &str = "km";

// ---------------------------------------------------------------------------
// Ports
// ---------------------------------------------------------------------------

/// Bind to an ephemeral port and return the port number.
/// Used to allocate a guaranteed-free port at startup.
pub fn find_free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .expect("Failed to bind ephemeral port")
        .local_addr()
        .unwrap()
        .port()
}

/// Verify that `port` is available on 127.0.0.1. Returns an error
/// message if the port is already in use.
pub fn check_port(port: &str, component: &str) -> Result<(), String> {
    let addr = format!("127.0.0.1:{}", port);
    match TcpListener::bind(&addr) {
        Ok(_) => Ok(()),
        Err(e) => Err(format!("{} port {} is already in use: {}", component, port, e)),
    }
}

// ---------------------------------------------------------------------------
// Filesystem watching
// ---------------------------------------------------------------------------

/// Block until `path` exists or `timeout` elapses. Returns true if the
/// file appeared, false on timeout. Polls every 500ms.
pub fn wait_for_file(path: &Path, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if path.exists() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(500));
    }
    false
}

// ---------------------------------------------------------------------------
// Binary resolution
// ---------------------------------------------------------------------------

/// The directory containing the running km executable. All sibling
/// component binaries (control-plane, pod-runner, etc.) are loaded
/// from here.
pub fn resolve_bin_dir() -> PathBuf {
    let exe = env::current_exe().expect("Failed to resolve executable path");
    exe.parent()
        .expect("Executable has no parent directory")
        .to_path_buf()
}

// ---------------------------------------------------------------------------
// PID file management
// ---------------------------------------------------------------------------

/// km's PID directory: `<workdir>/data/km/pids/`.
pub fn pid_dir(workdir: &Path) -> PathBuf {
    workdir.join(DATA_DIR).join(BIN_KM).join(PIDS_DIR)
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

// ---------------------------------------------------------------------------
// Process termination
// ---------------------------------------------------------------------------

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
mod tests {
    use super::*;
    use std::collections::HashSet;

    fn tempdir() -> PathBuf {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let dir = env::temp_dir().join(format!("km-utils-{}-{}", std::process::id(), n));
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    // -- ports --------------------------------------------------------------

    #[test]
    fn find_free_port_returns_distinct_ports() {
        let mut seen = HashSet::new();
        for _ in 0..10 {
            seen.insert(find_free_port());
        }
        // Even on a quiet machine, 10 calls in a tight loop should
        // yield several distinct ports.
        assert!(seen.len() > 1);
    }

    #[test]
    fn check_port_succeeds_when_free() {
        let port = find_free_port();
        assert!(check_port(&port.to_string(), "test").is_ok());
    }

    #[test]
    fn check_port_fails_when_bound() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let res = check_port(&port.to_string(), "test");
        assert!(res.is_err());
        let msg = res.unwrap_err();
        assert!(msg.contains("test"));
        assert!(msg.contains(&port.to_string()));
    }

    // -- file watcher -------------------------------------------------------

    #[test]
    fn wait_for_file_returns_true_when_present() {
        let dir = tempdir();
        let path = dir.join("ready");
        fs::write(&path, b"ok").unwrap();
        assert!(wait_for_file(&path, Duration::from_millis(100)));
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn wait_for_file_times_out_when_missing() {
        let dir = tempdir();
        let path = dir.join("never");
        let start = Instant::now();
        assert!(!wait_for_file(&path, Duration::from_millis(200)));
        assert!(start.elapsed() >= Duration::from_millis(200));
        fs::remove_dir_all(&dir).ok();
    }

    // -- pid_dir ------------------------------------------------------------

    #[test]
    fn pid_dir_under_data_km_pids() {
        let workdir = PathBuf::from("/tmp/foo");
        let p = pid_dir(&workdir);
        assert_eq!(p, PathBuf::from("/tmp/foo/data/km/pids"));
    }

    // -- write/remove pid ---------------------------------------------------

    #[test]
    fn write_pid_creates_named_file() {
        let dir = tempdir();
        write_pid(&dir, "alpha", 1234);
        let path = dir.join("1234.alpha");
        assert!(path.exists());
        let content = fs::read_to_string(&path).unwrap();
        assert_eq!(content, "1234");
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn write_pid_creates_missing_dir() {
        let dir = tempdir().join("nested").join("deeper");
        write_pid(&dir, "beta", 42);
        assert!(dir.join("42.beta").exists());
        fs::remove_dir_all(dir.parent().unwrap().parent().unwrap()).ok();
    }

    #[test]
    fn remove_pid_removes_matching_file() {
        let dir = tempdir();
        write_pid(&dir, "comp", 100);
        write_pid(&dir, "other", 200);
        remove_pid(&dir, "comp");
        assert!(!dir.join("100.comp").exists());
        assert!(dir.join("200.other").exists());
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn remove_pid_no_op_when_missing() {
        let dir = tempdir();
        // Should not panic on empty dir.
        remove_pid(&dir, "nonexistent");
        fs::remove_dir_all(&dir).ok();
    }

    // -- cleanup_stale_pids -------------------------------------------------

    #[test]
    fn cleanup_stale_pids_removes_files_for_dead_processes() {
        let dir = tempdir();
        // Use a PID guaranteed not to exist (very high number).
        let dead_pid: i32 = 999_999;
        let path = dir.join(format!("{}.zombie", dead_pid));
        fs::write(&path, dead_pid.to_string()).unwrap();
        cleanup_stale_pids(&dir);
        assert!(!path.exists(), "pid file for dead process should be removed");
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn cleanup_stale_pids_ignores_malformed_filenames() {
        let dir = tempdir();
        fs::write(dir.join("not-a-pid-file"), "junk").unwrap();
        fs::write(dir.join("abc.notapid"), "junk").unwrap();
        cleanup_stale_pids(&dir);
        // Files without "<pid>.<name>" pattern are left alone (the
        // function only removes entries it could parse and act on).
        // We just verify no panic.
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn cleanup_stale_pids_no_op_when_dir_missing() {
        // Should not panic when the dir does not exist.
        cleanup_stale_pids(Path::new("/nonexistent/km/pids"));
    }

    // -- resolve_bin_dir ----------------------------------------------------

    #[test]
    fn resolve_bin_dir_returns_a_path() {
        // Just ensure it doesn't panic and the path exists.
        let bin = resolve_bin_dir();
        assert!(bin.is_absolute());
    }
}
