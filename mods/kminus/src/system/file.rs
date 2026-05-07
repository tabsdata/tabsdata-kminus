// Copyright 2026 Tabsdata Inc.

//! Filesystem helpers: standard directory names, path resolution, file watchers.

use std::env;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

/// Default config directory name (under `<workdir>` if no `--configdir`).
pub const CONFIG_DIR: &str = "config";

/// Default log directory name (under `<workdir>` if no `--logdir`).
pub const LOG_DIR: &str = "log";

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

/// The directory containing the running km executable. All sibling
/// component binaries (control-plane, pod-runner, etc.) are loaded
/// from here.
pub fn resolve_bin_dir() -> PathBuf {
    let exe = env::current_exe().expect("Failed to resolve executable path");
    exe.parent()
        .expect("Executable has no parent directory")
        .to_path_buf()
}

#[cfg(test)]
mod tests;
