// Copyright 2026 Tabsdata Inc.

//! Directory symlinks, the same way on every platform.

use std::fs;
use std::io;
use std::path::Path;

/// Create a directory symlink at `link` pointing to `target`.
///
/// A Windows symlink needs Developer Mode or elevation. Without them this
/// creates a junction instead, which needs neither but only takes an
/// absolute target, so a relative `target` is resolved from `link`'s
/// folder as the symlink would have resolved it.
pub(crate) fn symlink_dir(target: &Path, link: &Path) -> io::Result<()> {
    #[cfg(unix)]
    return std::os::unix::fs::symlink(target, link);
    #[cfg(windows)]
    return std::os::windows::fs::symlink_dir(target, link).or_else(|error| {
        use windows_sys::Win32::Foundation::ERROR_PRIVILEGE_NOT_HELD;
        if error.raw_os_error() != Some(ERROR_PRIVILEGE_NOT_HELD as i32) {
            return Err(error);
        }
        let folder = link.parent().unwrap_or(Path::new(""));
        junction::create(std::path::absolute(folder.join(target))?, link)
    });
}

/// Remove a directory symlink (best-effort).
pub(crate) fn remove_symlink(link: &Path) {
    #[cfg(unix)]
    let _ = fs::remove_file(link);
    #[cfg(windows)]
    let _ = fs::remove_dir(link).or_else(|_| fs::remove_file(link));
}
