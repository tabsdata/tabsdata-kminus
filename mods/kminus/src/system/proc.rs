// Copyright 2026 Tabsdata Inc.

//! Helper-binary path resolution.
//!
//! Lookup chain (first hit wins):
//!   1. Same folder as the running km executable.
//!   2. `<km-venv>/<LIBEXEC_DIR>/<TABSDATA_DIR>/<name>`, where `<km-venv>` is
//!      the parent of the km executable's folder (`<venv>/Scripts` on Windows,
//!      `<venv>/bin` on POSIX). Independent of any `python` on `PATH`, so it
//!      resolves against the venv that actually owns km — not whatever
//!      interpreter PATH happens to point at (e.g. a hosted-tool Python on CI).
//!   3. `<sys.prefix>/<LIBEXEC_DIR>/<TABSDATA_DIR>/<name>` (Python wheel
//!      off-PATH install).
//!   4. `<sys.prefix>/<scripts>/<name>` (Python wheel on-PATH —
//!      `bin` on POSIX, `Scripts` on Windows).
//!   5. The system `PATH`.
//!
//! Steps 3 and 4 require a working `python` (or `python3`) on `PATH`;
//! if Python is unavailable, those steps are silently skipped. If
//! nothing matches, the bare name is returned so the caller surfaces a
//! clear spawn-time error rather than a silent miss here.

use std::env;
use std::path::PathBuf;
use std::process::Command;
use std::sync::OnceLock;

use crate::system::file::resolve_bin_dir;

/// Subdirectory under `<sys.prefix>` for Python wheel off-PATH binaries.
/// Must agree with `install_dest` in `tabsdata-os/setup.toml` for the
/// `kubernetes-srv` group.
const LIBEXEC_DIR: &str = "libexec";

/// Per-application namespace under `<sys.prefix>/<LIBEXEC_DIR>/`.
const TABSDATA_DIR: &str = "tabsdata";

/// Python interpreter command names tried in order when probing the
/// active Python prefix. The first one that runs successfully wins.
const PYTHON3: &str = "python3";
const PYTHON: &str = "python";

/// Key passed to Python's `sysconfig.get_path()` to obtain the
/// console-scripts folder (`bin` on POSIX, `Scripts` on Windows).
const SCRIPTS: &str = "scripts";

/// Resolve `name` to a binary path using the documented lookup chain.
pub fn resolve_binary(name: &str) -> PathBuf {
    let exe_name = with_exe_extension(name);

    let sibling_dir = resolve_bin_dir();
    let sibling = sibling_dir.join(&exe_name);
    if sibling.is_file() {
        return sibling;
    }

    if let Some(venv_root) = sibling_dir.parent() {
        let self_libexec = venv_root
            .join(LIBEXEC_DIR)
            .join(TABSDATA_DIR)
            .join(&exe_name);
        if self_libexec.is_file() {
            return self_libexec;
        }
    }

    if let Some(info) = python_prefix_info() {
        let libexec = info
            .prefix
            .join(LIBEXEC_DIR)
            .join(TABSDATA_DIR)
            .join(&exe_name);
        if libexec.is_file() {
            return libexec;
        }
        let bin = info.prefix.join(&info.scripts_basename).join(&exe_name);
        if bin.is_file() {
            return bin;
        }
    }

    if let Some(p) = find_in_path(&exe_name) {
        return p;
    }

    PathBuf::from(exe_name)
}

#[cfg(windows)]
pub(crate) fn with_exe_extension(name: &str) -> String {
    if name.to_ascii_lowercase().ends_with(".exe") {
        name.to_string()
    } else {
        format!("{name}.exe")
    }
}

#[cfg(not(windows))]
pub(crate) fn with_exe_extension(name: &str) -> String {
    name.to_string()
}

pub(crate) fn find_in_path(name: &str) -> Option<PathBuf> {
    let path_var = env::var_os("PATH")?;
    for dir in env::split_paths(&path_var) {
        if dir.as_os_str().is_empty() {
            continue;
        }
        let candidate = dir.join(name);
        if candidate.is_file() {
            return Some(candidate);
        }
    }
    None
}

struct PythonPrefixInfo {
    prefix: PathBuf,
    scripts_basename: String,
}

fn python_prefix_info() -> Option<&'static PythonPrefixInfo> {
    static CACHE: OnceLock<Option<PythonPrefixInfo>> = OnceLock::new();
    CACHE.get_or_init(query_python_prefix_info).as_ref()
}

fn query_python_prefix_info() -> Option<PythonPrefixInfo> {
    for cmd in [PYTHON3, PYTHON] {
        if let Some(info) = run_python(cmd) {
            return Some(info);
        }
    }
    None
}

fn run_python(cmd: &str) -> Option<PythonPrefixInfo> {
    let query = format!(
        "import os, sys, sysconfig; \
         print(sys.prefix); \
         print(os.path.basename(sysconfig.get_path('{SCRIPTS}')))"
    );
    let output = Command::new(with_exe_extension(cmd))
        .arg("-c")
        .arg(&query)
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let stdout = String::from_utf8(output.stdout).ok()?;
    let mut lines = stdout.lines();
    let prefix = lines.next()?.trim();
    let scripts = lines.next()?.trim();
    if prefix.is_empty() || scripts.is_empty() {
        return None;
    }
    Some(PythonPrefixInfo {
        prefix: PathBuf::from(prefix),
        scripts_basename: scripts.to_string(),
    })
}

#[cfg(test)]
mod tests;
