// Copyright 2026 Tabsdata Inc.

//! Test utilities — reusable helpers for unit and integration tests.
//!
//! This module is gated behind `#[cfg(test)]` and is intended to be
//! extracted into a standalone crate for cross-project reuse.
//!
//! # `test_dir!` macro
//!
//! The [`test_dir!`] macro creates persistent test directories whose
//! names are derived from the calling test function's module path.
//! It is only available inside `#[cfg(test)]` modules.
//!
//! ```ignore
//! use crate::testutils::test_dir;
//!
//! #[test]
//! fn test_something() {
//!     let dir = test_dir!();          // …/tests/test_something/
//!     let logs = test_dir!("logs");   // …/tests/test_something/logs/
//! }
//! ```

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::LazyLock;

use chrono::Utc;

// ---------------------------------------------------------------------------
// Session-wide timestamp
// ---------------------------------------------------------------------------

/// Session-wide timestamp, set once per test run. All test directories
/// created during the same `cargo test` invocation share this timestamp.
pub static SESSION_TS: LazyLock<String> = LazyLock::new(|| {
    Utc::now().format("%Y%m%dT%H%M%S%.3f").to_string()
});

// ---------------------------------------------------------------------------
// test_dir! macro
// ---------------------------------------------------------------------------

/// Create a persistent test directory whose name is derived automatically
/// from the calling test function.
///
/// Uses the `type_name` of an inner function defined at the macro call
/// site to capture the full module path of the enclosing function
/// (e.g. `crate::process::executor::tests::test_foo`), then maps `::`
/// separators to `/` so the directory tree mirrors the module tree:
///
/// ```text
/// target/testrun.<ts>/process/executor/tests/test_foo/
/// ```
///
/// An optional subdir argument creates a child directory:
///
/// ```text
/// test_dir!()           → …/tests/test_foo/
/// test_dir!("logs")     → …/tests/test_foo/logs/
/// test_dir!("a/b")      → …/tests/test_foo/a/b/
/// ```
///
/// **Important**: this macro must be called directly from the test
/// function — not from a helper function. When called from a helper,
/// it captures the helper's path instead of the test's. If you need
/// subdirectories in helper code, call `test_dir!()` in the test and
/// pass the resulting `PathBuf` to the helper, then use [`subdir`] to
/// create children.
macro_rules! test_dir {
    () => {{
        // Define a nested function whose type_name includes the caller's path.
        fn _f() {}
        fn _type_name_of<T>(_: T) -> &'static str {
            std::any::type_name::<T>()
        }
        // type_name returns e.g. "pod_runner::process::executor::tests::test_foo::_f"
        // For async tests (tokio::test) it may include "::{{closure}}" segments.
        let full = _type_name_of(_f);
        // Strip the trailing "::_f"
        let mut trimmed = &full[..full.len() - 4];
        // Strip any trailing "::{{closure}}" segments (added by async test wrappers)
        while trimmed.ends_with("::{{closure}}") {
            trimmed = &trimmed[..trimmed.len() - 13];
        }
        // Strip the crate name prefix (first segment)
        let without_crate = match trimmed.find("::") {
            Some(idx) => &trimmed[idx + 2..],
            None => trimmed,
        };
        $crate::testutils::make_test_dir(without_crate)
    }};
    ($subdir:expr) => {{
        $crate::testutils::subdir(&test_dir!(), $subdir)
    }};
}

pub(crate) use test_dir;

// ---------------------------------------------------------------------------
// Persistent test directories
// ---------------------------------------------------------------------------

/// Create a persistent test directory under `target/testrun.<ts>/<path>/`.
///
/// `path` is a `::` or `/` separated module path. `::` separators are
/// converted to `/` so the directory tree mirrors the module tree.
///
/// Prefer the [`test_dir!`] macro which derives the path automatically.
pub fn make_test_dir(path: &str) -> PathBuf {
    let rel = path.replace("::", "/");
    let base = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("target")
        .join(format!("testrun.{}", *SESSION_TS))
        .join(rel);
    fs::create_dir_all(&base).unwrap();
    base
}

/// Create a subdirectory under an existing test directory.
///
/// ```ignore
/// let dir = test_dir!();
/// let logs = subdir(&dir, "logs");
/// let nested = subdir(&dir, "a/b/c");
/// ```
pub fn subdir(base: &Path, name: &str) -> PathBuf {
    let d = base.join(name);
    fs::create_dir_all(&d).unwrap();
    d
}

// ---------------------------------------------------------------------------
// File helpers
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Unique names for K8s resources
// ---------------------------------------------------------------------------

/// Return `<prefix>-<base32(uuid)>` suitable for K8s resource names.
///
/// Mirrors the Python `unique_name()` in `kube_helpers.py`.
pub fn unique_name(prefix: &str) -> String {
    let raw = uuid::Uuid::new_v4().into_bytes();
    let suffix = data_encoding::BASE32_NOPAD.encode(&raw).to_lowercase();
    format!("{}-{}", prefix, suffix)
}

// ---------------------------------------------------------------------------
// Platform-aware shell commands
// ---------------------------------------------------------------------------

/// Return the shell command array for running a command string on the
/// current platform.
///
/// - Unix: `["sh", "-c", cmd]`
/// - Windows: `["cmd", "/C", cmd]`
///
/// Use this in tests to build platform-independent pod specs.
pub fn shell_cmd(cmd: &str) -> Vec<String> {
    if cfg!(windows) {
        vec!["cmd".to_string(), "/C".to_string(), cmd.to_string()]
    } else {
        vec!["sh".to_string(), "-c".to_string(), cmd.to_string()]
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_unique_name_format() {
        let name = unique_name("secret");
        assert!(name.starts_with("secret-"), "got: {}", name);
        // base32 of 16 bytes (UUID) = 26 chars
        let suffix = &name["secret-".len()..];
        assert_eq!(suffix.len(), 26, "suffix: {}", suffix);
        assert!(
            suffix.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit()),
            "suffix not lowercase base32: {}",
            suffix,
        );
    }

    #[test]
    fn test_unique_name_uniqueness() {
        let a = unique_name("test");
        let b = unique_name("test");
        assert_ne!(a, b);
    }
}
