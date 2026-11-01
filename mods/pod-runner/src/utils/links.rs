// Copyright 2026 Tabsdata Inc.

//! Directory symlinks, the same way on every platform.

use std::fs;
use std::io;
use std::path::Path;

/// Create a directory symlink at `link` pointing to `target`.
pub(crate) fn symlink_dir(target: &Path, link: &Path) -> io::Result<()> {
    #[cfg(unix)]
    return std::os::unix::fs::symlink(target, link);
    #[cfg(windows)]
    return std::os::windows::fs::symlink_dir(target, link);
}

/// Remove a directory symlink (best-effort).
pub(crate) fn remove_symlink(link: &Path) {
    #[cfg(unix)]
    let _ = fs::remove_file(link);
    #[cfg(windows)]
    let _ = fs::remove_dir(link).or_else(|_| fs::remove_file(link));
}
