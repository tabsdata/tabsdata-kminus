// Copyright 2026 Tabsdata Inc.

//! Filesystem helpers: standard directory names, path resolution, file watchers.

use std::env;
use std::path::PathBuf;

/// Default config directory name (under `<workdir>` if no `--configdir`).
pub const CONFIG_DIR: &str = "config";

/// Default log directory name (under `<workdir>` if no `--logdir`).
pub const LOG_DIR: &str = "log";

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
