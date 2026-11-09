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
#[cfg(unix)]
use std::path::Path;
use std::path::PathBuf;
use std::sync::LazyLock;
use std::time::Duration;

use chrono::Utc;

// ---------------------------------------------------------------------------
// Session-wide timestamp
// ---------------------------------------------------------------------------

/// Session-wide timestamp, set once per test run. All test directories
/// created during the same `cargo test` invocation share this timestamp.
pub static SESSION_TS: LazyLock<String> =
    LazyLock::new(|| Utc::now().format("%Y%m%dT%H%M%S%.3f").to_string());

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
    ($subdir:expr) => {{ $crate::testutils::subdir(&test_dir!(), $subdir) }};
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
#[cfg(unix)]
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
// Helper process
// ---------------------------------------------------------------------------

/// Environment variable that turns a re-executed test binary into a
/// long-lived helper process, and selects which mode it runs in.
///
/// Tests that need to watch a real process being signalled re-execute the
/// test binary rather than running `sleep` or `sh`. macOS hides the
/// environment of SIP platform binaries even from the same uid —
/// `/bin/sleep` and `/bin/sh` report no environment at all — so anything
/// that reads a spawned process's environment (the orphan sweep in
/// [`crate::process::sweep`]) cannot see through them. A cargo-built
/// binary is visible, like the tarball binaries (postgres, python) that a
/// real container runs.
pub const HELPER_VAR: &str = "TD_TEST_HELPER";

/// Helper mode: sleep, and die on SIGTERM like any ordinary process.
pub const HELPER_SLEEP: &str = "sleep";

/// Helper mode: a whole container life, with no shell or system command, so
/// it runs the same on every platform. Writes [`HELPER_LINES_VAR`] to stdout,
/// pauses [`HELPER_PAUSE_MS_VAR`], then exits with [`HELPER_EXIT_VAR`].
pub const HELPER_SCRIPT: &str = "script";

/// Lines a [`HELPER_SCRIPT`] helper writes, separated by `\n`.
pub const HELPER_LINES_VAR: &str = "TD_TEST_HELPER_LINES";

/// Milliseconds a [`HELPER_SCRIPT`] helper pauses after writing its lines.
pub const HELPER_PAUSE_MS_VAR: &str = "TD_TEST_HELPER_PAUSE_MS";

/// Exit code of a [`HELPER_SCRIPT`] helper.
pub const HELPER_EXIT_VAR: &str = "TD_TEST_HELPER_EXIT";

/// Set to `1` to make a [`HELPER_SCRIPT`] helper ignore the graceful stop,
/// as [`HELPER_IGNORE_STOP`] does.
pub const HELPER_IGNORE_STOP_VAR: &str = "TD_TEST_HELPER_IGNORE_STOP";

/// Helper mode: ignore the graceful stop (SIGTERM, or CTRL_BREAK on
/// Windows), so that only a forced kill ends it.
pub const HELPER_IGNORE_STOP: &str = "ignore-stop";

/// Helper mode: spawn a detached copy of itself in [`HELPER_SLEEP`] mode,
/// then exit immediately — the full `pg_ctl` shape, where the launcher is
/// gone and the daemon is reparented away. Unix only.
#[cfg(unix)]
pub const HELPER_DAEMONIZE_AND_EXIT: &str = "daemonize-and-exit";

/// Helper mode: spawn a plain (non-detached) copy of itself in
/// [`HELPER_SLEEP`] mode, record its PID, then sleep.
///
/// The copy inherits this process's group, which is the shape a server
/// that has daemonised itself takes: a postgres postmaster in its own
/// group, with its backends forked into that same group. Unix only.
#[cfg(unix)]
pub const HELPER_FORK_CHILD: &str = "fork-child";

/// Path a daemonising or forking helper writes its child's PID to, so a
/// test can identify a process it did not spawn itself.
#[cfg(unix)]
pub const HELPER_PID_FILE_VAR: &str = "TD_TEST_HELPER_PID_FILE";

/// Helper mode: spawn a detached copy of itself in [`HELPER_SLEEP`] mode,
/// then sleep.
///
/// The copy calls `setsid()` before exec, which is what `pg_ctl` does to
/// daemonise the postmaster: it lands in its own session and process
/// group, so no signal aimed at this process's group can reach it, and it
/// inherits this process's environment — including any kill token. Unix
/// only; elsewhere the mode just sleeps.
#[cfg(unix)]
pub const HELPER_DAEMONIZE: &str = "daemonize";

/// Substring that selects [`test_helper_process`] when the test binary is
/// re-executed with it as a filter argument.
pub const HELPER_FILTER: &str = "test_helper_process";

/// How long a helper lives if the test that spawned it fails to kill it.
pub const HELPER_TTL: Duration = Duration::from_secs(30);

/// Argv that re-executes this test binary as a helper process. Pair it
/// with [`HELPER_VAR`] set to one of the modes above.
pub fn helper_command() -> Vec<String> {
    vec![
        std::env::current_exe()
            .expect("test binary path")
            .to_string_lossy()
            .into_owned(),
        HELPER_FILTER.to_string(),
    ]
}

/// The helper process described by [`HELPER_VAR`].
///
/// Without that variable this is a no-op, so an ordinary run of the suite
/// passes through it instantly.
#[test]
fn test_helper_process() {
    let Ok(mode) = std::env::var(HELPER_VAR) else {
        return;
    };

    if mode == HELPER_SCRIPT {
        run_helper_script();
    }

    if mode == HELPER_IGNORE_STOP {
        ignore_stop();
    }

    #[cfg(unix)]
    if mode == HELPER_FORK_CHILD {
        let argv = helper_command();
        // No `setsid`: the child stays in this process's group, exactly
        // where a forked postgres backend sits.
        #[allow(clippy::zombie_processes)]
        let child = std::process::Command::new(&argv[0])
            .args(&argv[1..])
            .env(HELPER_VAR, HELPER_SLEEP)
            .spawn()
            .expect("spawn child helper");
        if let Ok(path) = std::env::var(HELPER_PID_FILE_VAR) {
            fs::write(path, child.id().to_string()).expect("write helper pid file");
        }
    }

    #[cfg(unix)]
    if mode == HELPER_DAEMONIZE || mode == HELPER_DAEMONIZE_AND_EXIT {
        use std::os::unix::process::CommandExt;

        let argv = helper_command();
        let mut cmd = std::process::Command::new(&argv[0]);
        cmd.args(&argv[1..]).env(HELPER_VAR, HELPER_SLEEP);
        // SAFETY: `setsid` is async-signal-safe and is the only call made
        // between fork and exec.
        unsafe {
            cmd.pre_exec(|| {
                let _ = nix::unistd::setsid();
                Ok(())
            });
        }
        // Spawn and let go. Never waiting is the point: this process
        // exits while the child keeps running, so the child is reparented
        // away — the same state `pg_ctl` leaves the postmaster in.
        #[allow(clippy::zombie_processes)]
        let child = cmd.spawn().expect("spawn detached helper");

        if let Ok(path) = std::env::var(HELPER_PID_FILE_VAR) {
            fs::write(path, child.id().to_string()).expect("write helper pid file");
        }
        if mode == HELPER_DAEMONIZE_AND_EXIT {
            return;
        }
    }

    std::thread::sleep(HELPER_TTL);
}

#[cfg(unix)]
fn ignore_stop() {
    // SAFETY: the disposition is set before anything in this process
    // relies on SIGTERM; the helper does nothing but sleep.
    unsafe {
        nix::sys::signal::signal(
            nix::sys::signal::Signal::SIGTERM,
            nix::sys::signal::SigHandler::SigIgn,
        )
        .expect("ignore SIGTERM");
    }
}

#[cfg(windows)]
fn ignore_stop() {
    use windows_sys::Win32::System::Console::{
        CTRL_BREAK_EVENT, CTRL_C_EVENT, SetConsoleCtrlHandler,
    };

    unsafe extern "system" fn swallow(event: u32) -> windows_sys::core::BOOL {
        (event == CTRL_BREAK_EVENT || event == CTRL_C_EVENT).into()
    }

    // SAFETY: `swallow` is a plain function that lives for the whole
    // process, as a console control handler must.
    let added = unsafe { SetConsoleCtrlHandler(Some(swallow), 1) };
    assert_ne!(added, 0, "ignore CTRL_BREAK");
}

fn run_helper_script() -> ! {
    use std::io::Write;

    fn number<T: std::str::FromStr>(var: &str) -> Option<T> {
        std::env::var(var).ok()?.parse().ok()
    }

    if number::<u8>(HELPER_IGNORE_STOP_VAR) == Some(1) {
        ignore_stop();
    }
    let lines = std::env::var(HELPER_LINES_VAR).unwrap_or_default();
    // Written to the stdout handle, not with `println!`: libtest captures
    // the macros, and these lines are the container's log.
    let mut out = std::io::stdout().lock();
    for line in lines.split('\n').filter(|line| !line.is_empty()) {
        writeln!(out, "{line}").expect("helper stdout");
    }
    out.flush().expect("helper stdout");
    drop(out);
    std::thread::sleep(Duration::from_millis(
        number(HELPER_PAUSE_MS_VAR).unwrap_or(0),
    ));
    std::process::exit(number(HELPER_EXIT_VAR).unwrap_or(0));
}

/// What a test container does, run by the re-executed test binary instead
/// of a shell, so a pod behaves the same on every platform.
#[derive(Debug, Clone, Default)]
pub struct Script {
    lines: Vec<String>,
    pause: Duration,
    exit: i32,
    ignore_stop: bool,
}

impl Script {
    /// Keeps running until it is killed (or [`HELPER_TTL`] passes).
    pub fn runs() -> Self {
        Self::default().then_pause(HELPER_TTL)
    }

    /// Exits at once with `code`.
    pub fn exits(code: i32) -> Self {
        Self {
            exit: code,
            ..Self::default()
        }
    }

    /// Writes `lines` to stdout and exits with 0.
    pub fn prints<S: Into<String>>(lines: impl IntoIterator<Item = S>) -> Self {
        Self {
            lines: lines.into_iter().map(Into::into).collect(),
            ..Self::default()
        }
    }

    /// Pauses for `pause` before exiting.
    pub fn then_pause(mut self, pause: Duration) -> Self {
        self.pause = pause;
        self
    }

    /// Ignores the graceful stop, so only a forced kill ends it early.
    pub fn ignoring_stop(mut self) -> Self {
        self.ignore_stop = true;
        self
    }

    /// The container command: this test binary, re-executed as a helper.
    pub fn command(&self) -> Vec<String> {
        helper_command()
    }

    /// The container environment that tells the helper what to do.
    pub fn env(&self) -> Vec<crate::spec::pod::EnvVar> {
        [
            (HELPER_VAR, HELPER_SCRIPT.to_string()),
            (HELPER_LINES_VAR, self.lines.join("\n")),
            (HELPER_PAUSE_MS_VAR, self.pause.as_millis().to_string()),
            (HELPER_EXIT_VAR, self.exit.to_string()),
            (
                HELPER_IGNORE_STOP_VAR,
                u8::from(self.ignore_stop).to_string(),
            ),
        ]
        .into_iter()
        .map(|(name, value)| crate::spec::pod::EnvVar {
            name: name.to_string(),
            value: Some(value),
            value_from: None,
        })
        .collect()
    }
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

/// Send pod-runner's and kcommon's debug traces to the test's captured
/// output, timed from the moment it is installed. nextest shows that
/// output only for a failing test, so a failure carries the timeline of
/// what its pods did. Installs once per process; later calls are no-ops.
pub fn init_test_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::new(
            "pod_runner=debug,kcommon=debug",
        ))
        .with_timer(tracing_subscriber::fmt::time::uptime())
        .with_thread_ids(true)
        .with_ansi(false)
        .with_test_writer()
        .try_init();
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
            suffix
                .chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit()),
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
