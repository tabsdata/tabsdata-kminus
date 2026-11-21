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
use std::path::Path;
use std::path::PathBuf;
use std::sync::LazyLock;
use std::sync::atomic::{AtomicU64, Ordering};
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
pub fn subdir(base: &Path, name: &str) -> PathBuf {
    let d = base.join(name);
    fs::create_dir_all(&d).unwrap();
    d
}

/// Create a test directory under `target/testrun.<ts>/<name>/` that no
/// other run, process or call shares, for helpers that cannot call
/// [`test_dir!`] from the test itself. The session timestamp tells runs
/// apart, and the process id and a counter tell processes and calls apart,
/// so a test never finds what an earlier one left behind.
pub fn unique_test_dir(name: &str) -> PathBuf {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    make_test_dir(&format!("{name}/{}-{n}", std::process::id()))
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

/// Set to `1` to make a [`HELPER_SCRIPT`] helper crash (`abort`) instead of
/// exiting: SIGABRT on Unix, NTSTATUS `0xC0000409` on Windows.
pub const HELPER_ABORT_VAR: &str = "TD_TEST_HELPER_ABORT";

/// Lines a [`HELPER_SCRIPT`] helper writes to stderr, separated by `\n`.
pub const HELPER_ERR_LINES_VAR: &str = "TD_TEST_HELPER_ERR_LINES";

/// Comma-separated variables a [`HELPER_SCRIPT`] helper writes out.
pub const HELPER_SHOW_ENV_VAR: &str = "TD_TEST_HELPER_SHOW_ENV";

/// Set to `1` to make a [`HELPER_SCRIPT`] helper write its working directory.
pub const HELPER_SHOW_CWD_VAR: &str = "TD_TEST_HELPER_SHOW_CWD";

/// Milliseconds between the repeated writes of a ticking [`HELPER_SCRIPT`]
/// helper.
pub const HELPER_TICK_MS_VAR: &str = "TD_TEST_HELPER_TICK_MS";

/// File a [`HELPER_SCRIPT`] helper writes the PID of its sleeping child to.
pub const HELPER_CHILD_PID_FILE_VAR: &str = "TD_TEST_HELPER_CHILD_PID_FILE";

/// Port a [`HELPER_SCRIPT`] helper serves HTTP 200 on.
pub const HELPER_LISTEN_VAR: &str = "TD_TEST_HELPER_LISTEN";

/// File a [`HELPER_SCRIPT`] helper creates when it starts.
pub const HELPER_CREATE_VAR: &str = "TD_TEST_HELPER_CREATE";

/// File that must exist when a [`HELPER_SCRIPT`] helper is done, or it
/// exits with 1.
pub const HELPER_REQUIRE_VAR: &str = "TD_TEST_HELPER_REQUIRE";

/// Number of runs a [`HELPER_SCRIPT`] helper fails at once, counted in
/// [`HELPER_RUNS_FILE_VAR`].
pub const HELPER_FAIL_RUNS_VAR: &str = "TD_TEST_HELPER_FAIL_RUNS";

/// File a [`HELPER_SCRIPT`] helper counts its runs in.
pub const HELPER_RUNS_FILE_VAR: &str = "TD_TEST_HELPER_RUNS_FILE";

/// Number of rounds a [`HELPER_SCRIPT`] helper writes its lines in.
pub const HELPER_REPEAT_VAR: &str = "TD_TEST_HELPER_REPEAT";

/// Set to `1` to make a [`HELPER_SCRIPT`] helper write each line in two
/// halves, with the other stream's half in between.
pub const HELPER_INTERLEAVE_VAR: &str = "TD_TEST_HELPER_INTERLEAVE";

/// Set to `1` to make a [`HELPER_SCRIPT`] helper ignore the graceful stop,
/// as [`HELPER_IGNORE_STOP`] does.
pub const HELPER_IGNORE_STOP_VAR: &str = "TD_TEST_HELPER_IGNORE_STOP";

/// Helper mode: ignore the graceful stop (SIGTERM, or CTRL_BREAK on
/// Windows), so that only a forced kill ends it.
pub const HELPER_IGNORE_STOP: &str = "ignore-stop";

/// Helper mode: spawn a detached copy of itself in [`HELPER_SLEEP`] mode,
/// then exit immediately — the full `pg_ctl` shape, where the launcher is
/// gone and the daemon is reparented away.
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
pub const HELPER_PID_FILE_VAR: &str = "TD_TEST_HELPER_PID_FILE";

/// Helper mode: spawn a detached copy of itself in [`HELPER_SLEEP`] mode,
/// then sleep.
///
/// The copy calls `setsid()` before exec, which is what `pg_ctl` does to
/// daemonise the postmaster: it lands in its own session and process
/// group, so no signal aimed at this process's group can reach it, and it
/// inherits this process's environment — including any kill token. On
/// Windows it starts detached, with no console and in a group of its own,
/// which no console event aimed at this process's group can reach either.
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

/// A helper setting: a `NAME=value` argument after [`HELPER_FILTER`], or
/// else the environment variable `NAME`. libtest takes each such argument
/// as one more name filter, which matches no test.
fn setting(name: &str) -> Option<String> {
    let prefix = format!("{name}=");
    std::env::args()
        .skip(2)
        .find_map(|arg| arg.strip_prefix(&prefix).map(str::to_string))
        .or_else(|| std::env::var(name).ok())
}

/// Hosts kcommon's console-event helper in this test binary, so that a
/// graceful stop a test runs on Windows reaches its target instead of
/// waiting out the grace (see `kcommon::signal::ctrl_break_helper_hook`).
#[cfg(windows)]
#[test]
fn kcommon_ctrl_break_helper() {
    kcommon::signal::ctrl_break_helper_hook();
}

/// Give this test process a console, hidden, when it has none, as on a CI
/// runner. Containers then share it, which is what lets a graceful stop's
/// CTRL_BREAK reach them; without one they run windowless and only a forced
/// kill ends them. A no-op on Unix and when a console is already there.
pub fn ensure_console() {
    #[cfg(windows)]
    {
        use windows_sys::Win32::System::Console::{AllocConsole, GetConsoleWindow};
        use windows_sys::Win32::UI::WindowsAndMessaging::{SW_HIDE, ShowWindow};

        // SAFETY: plain console calls on this process; the window handle
        // comes from the console just allocated.
        unsafe {
            if GetConsoleWindow().is_null() && AllocConsole() != 0 {
                ShowWindow(GetConsoleWindow(), SW_HIDE);
            }
        }
    }
}

/// The helper process described by [`HELPER_VAR`].
///
/// Without that setting this is a no-op, so an ordinary run of the suite
/// passes through it instantly.
#[test]
fn test_helper_process() {
    let Some(mode) = setting(HELPER_VAR) else {
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

    if mode == HELPER_DAEMONIZE || mode == HELPER_DAEMONIZE_AND_EXIT {
        let argv = helper_command();
        let mut cmd = std::process::Command::new(&argv[0]);
        cmd.args(&argv[1..]).env(HELPER_VAR, HELPER_SLEEP);
        #[cfg(unix)]
        {
            use std::os::unix::process::CommandExt;
            // SAFETY: `setsid` is async-signal-safe and is the only call
            // made between fork and exec.
            unsafe {
                cmd.pre_exec(|| {
                    let _ = nix::unistd::setsid();
                    Ok(())
                });
            }
        }
        #[cfg(windows)]
        {
            use std::os::windows::process::CommandExt;
            use windows_sys::Win32::System::Threading::{
                CREATE_NEW_PROCESS_GROUP, DETACHED_PROCESS,
            };
            cmd.creation_flags(DETACHED_PROCESS | CREATE_NEW_PROCESS_GROUP);
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
        setting(var)?.parse().ok()
    }
    fn lines(var: &str) -> Vec<String> {
        setting(var)
            .unwrap_or_default()
            .split('\n')
            .filter(|line| !line.is_empty())
            .map(str::to_string)
            .collect()
    }
    fn emit(stream: &mut impl Write, text: &str) {
        if !text.is_empty() {
            stream.write_all(text.as_bytes()).expect("helper output");
            stream.flush().expect("helper output");
        }
    }
    fn halves(line: Option<&String>) -> (&str, String) {
        let Some(line) = line else {
            return ("", String::new());
        };
        let mut mid = line.len() / 2;
        while !line.is_char_boundary(mid) {
            mid += 1;
        }
        (&line[..mid], format!("{}\n", &line[mid..]))
    }
    // Written to the handles, not with `println!`: libtest captures the
    // macros, and these lines are the container's log.
    fn write(lines: &[String], err: &[String], interleaved: bool) {
        let mut out = std::io::stdout().lock();
        let mut stderr = std::io::stderr().lock();
        if interleaved {
            for k in 0..lines.len().max(err.len()) {
                let (out_head, out_tail) = halves(lines.get(k));
                let (err_head, err_tail) = halves(err.get(k));
                emit(&mut out, out_head);
                emit(&mut stderr, err_head);
                emit(&mut out, &out_tail);
                emit(&mut stderr, &err_tail);
            }
            return;
        }
        for line in lines {
            writeln!(out, "{line}").expect("helper stdout");
        }
        out.flush().expect("helper stdout");
        for line in err {
            writeln!(stderr, "{line}").expect("helper stderr");
        }
        stderr.flush().expect("helper stderr");
    }

    if number::<u8>(HELPER_IGNORE_STOP_VAR) == Some(1) {
        ignore_stop();
    }
    if let (Some(failures), Some(path)) = (
        number::<u32>(HELPER_FAIL_RUNS_VAR),
        setting(HELPER_RUNS_FILE_VAR),
    ) {
        let runs = fs::read_to_string(&path)
            .ok()
            .and_then(|text| text.trim().parse::<u32>().ok())
            .unwrap_or(0)
            + 1;
        fs::write(&path, runs.to_string()).expect("write helper runs file");
        if runs <= failures {
            std::process::exit(1);
        }
    }
    if let Some(path) = setting(HELPER_CREATE_VAR) {
        fs::write(path, "").expect("create helper file");
    }
    if let Some(port) = number::<u16>(HELPER_LISTEN_VAR) {
        serve_ok(port);
    }
    if let Some(path) = setting(HELPER_CHILD_PID_FILE_VAR) {
        let argv = helper_command();
        #[allow(clippy::zombie_processes)]
        let child = std::process::Command::new(&argv[0])
            .args(&argv[1..])
            .env(HELPER_VAR, HELPER_SLEEP)
            .spawn()
            .expect("spawn child helper");
        fs::write(path, child.id().to_string()).expect("write child pid file");
    }

    let mut out = Vec::new();
    for name in setting(HELPER_SHOW_ENV_VAR)
        .unwrap_or_default()
        .split(',')
        .filter(|name| !name.is_empty())
    {
        out.push(match std::env::var_os(name) {
            Some(value) => format!("{name}={}", value.to_string_lossy()),
            None => format!("{name} unset"),
        });
    }
    if number::<u8>(HELPER_SHOW_CWD_VAR) == Some(1) {
        let cwd = std::env::current_dir().expect("current dir");
        out.push(cwd.display().to_string());
    }
    out.extend(lines(HELPER_LINES_VAR));
    let err = lines(HELPER_ERR_LINES_VAR);
    let interleaved = number::<u8>(HELPER_INTERLEAVE_VAR) == Some(1);
    let tick = number::<u64>(HELPER_TICK_MS_VAR)
        .filter(|&tick| tick > 0)
        .map(Duration::from_millis);
    let rounds = number::<u64>(HELPER_REPEAT_VAR);
    let in_round = |lines: &[String], round: u64| -> Vec<String> {
        lines
            .iter()
            .map(|line| line.replace("{i}", &round.to_string()))
            .collect()
    };

    let started = std::time::Instant::now();
    let mut round = 1;
    loop {
        match rounds {
            Some(_) => write(&in_round(&out, round), &in_round(&err, round), interleaved),
            None => write(&out, &err, interleaved),
        }
        round += 1;
        let more = match (rounds, tick) {
            (Some(rounds), _) => round <= rounds,
            (None, Some(_)) => started.elapsed() < HELPER_TTL,
            (None, None) => false,
        };
        if !more {
            break;
        }
        if let Some(tick) = tick {
            std::thread::sleep(tick);
        }
    }
    std::thread::sleep(Duration::from_millis(
        number(HELPER_PAUSE_MS_VAR).unwrap_or(0),
    ));
    if let Some(path) = setting(HELPER_REQUIRE_VAR)
        && !Path::new(&path).exists()
    {
        std::process::exit(1);
    }
    if setting(HELPER_ABORT_VAR).as_deref() == Some("1") {
        std::process::abort();
    }
    std::process::exit(number(HELPER_EXIT_VAR).unwrap_or(0));
}

/// Accept connections on `127.0.0.1:port` in the background, answering
/// each with an HTTP 200: enough for a TCP probe (it connects) and an HTTP
/// probe (it reads a status line) alike.
fn serve_ok(port: u16) {
    use std::io::{Read, Write};

    let listener = std::net::TcpListener::bind(("127.0.0.1", port)).expect("helper listen");
    std::thread::spawn(move || {
        for mut stream in listener.incoming().flatten() {
            let _ = stream.set_read_timeout(Some(Duration::from_millis(200)));
            let _ = stream.read(&mut [0u8; 1024]);
            let _ = stream
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok");
        }
    });
}

/// What a test process does, run by the re-executed test binary instead of
/// a shell or a system command, so it behaves the same on every platform.
///
/// In order: it ignores the graceful stop if asked, fails its first runs if
/// asked, creates a file, starts listening and a child if asked, writes the
/// environment variables and working directory asked for, then its stdout
/// lines and stderr lines; it repeats those writes every tick when ticking
/// or for each round when repeated, then pauses, checks for a required file
/// and exits.
#[derive(Debug, Clone, Default)]
pub struct Script {
    lines: Vec<String>,
    err_lines: Vec<String>,
    show_env: Vec<String>,
    show_cwd: bool,
    tick: Option<Duration>,
    rounds: Option<u64>,
    interleaved: bool,
    child_pid_file: Option<PathBuf>,
    listen: Option<u16>,
    create: Option<PathBuf>,
    require: Option<PathBuf>,
    fail_runs: Option<(u32, PathBuf)>,
    pause: Duration,
    exit: i32,
    ignore_stop: bool,
    abort: bool,
}

impl Script {
    /// Keeps running until it is killed (or [`HELPER_TTL`] passes).
    pub fn runs() -> Self {
        Self::default().then_pause(HELPER_TTL)
    }

    /// Exits at once with `code`.
    pub fn exits(code: i32) -> Self {
        Self::default().then_exit(code)
    }

    /// Writes `lines` to stdout and exits with 0.
    pub fn prints<S: Into<String>>(lines: impl IntoIterator<Item = S>) -> Self {
        Self {
            lines: lines.into_iter().map(Into::into).collect(),
            ..Self::default()
        }
    }

    /// Writes `NAME=value` for each variable that is set (`NAME=` when set
    /// but empty) and `NAME unset` for each that is not.
    pub fn shows_env<S: Into<String>>(names: impl IntoIterator<Item = S>) -> Self {
        Self {
            show_env: names.into_iter().map(Into::into).collect(),
            ..Self::default()
        }
    }

    /// Writes its working directory.
    pub fn shows_cwd() -> Self {
        Self {
            show_cwd: true,
            ..Self::default()
        }
    }

    /// Also writes `lines` to stderr, after the stdout lines.
    pub fn with_stderr<S: Into<String>>(mut self, lines: impl IntoIterator<Item = S>) -> Self {
        self.err_lines = lines.into_iter().map(Into::into).collect();
        self
    }

    /// Repeats its writes every `every` until it is killed (or
    /// [`HELPER_TTL`] passes), instead of pausing and exiting.
    pub fn ticking(mut self, every: Duration) -> Self {
        self.tick = Some(every);
        self
    }

    /// Writes its lines `rounds` times, with `{i}` in them replaced by the
    /// round (from 1), then pauses and exits. Rounds are a tick apart when
    /// ticking.
    pub fn repeated(mut self, rounds: u64) -> Self {
        self.rounds = Some(rounds);
        self
    }

    /// Writes each stdout and stderr line in two halves, the other
    /// stream's half in between, so a reader sees partial lines on both.
    pub fn interleaved(mut self) -> Self {
        self.interleaved = true;
        self
    }

    /// Creates an empty file at `path` when it starts.
    pub fn creating(mut self, path: impl Into<PathBuf>) -> Self {
        self.create = Some(path.into());
        self
    }

    /// Exits with 1 instead if `path` does not exist once it is done.
    pub fn requiring(mut self, path: impl Into<PathBuf>) -> Self {
        self.require = Some(path.into());
        self
    }

    /// Exits with 1 at once on each of its first `failures` runs, counting
    /// runs in `runs_file`.
    pub fn failing_first(mut self, failures: u32, runs_file: impl Into<PathBuf>) -> Self {
        self.fail_runs = Some((failures, runs_file.into()));
        self
    }

    /// Starts a sleeping child of its own first, writing its PID to
    /// `pid_file`.
    pub fn with_child(mut self, pid_file: impl Into<PathBuf>) -> Self {
        self.child_pid_file = Some(pid_file.into());
        self
    }

    /// Serves HTTP 200 on `127.0.0.1:port` while it runs.
    pub fn listening(mut self, port: u16) -> Self {
        self.listen = Some(port);
        self
    }

    /// Pauses for `pause` before exiting.
    pub fn then_pause(mut self, pause: Duration) -> Self {
        self.pause = pause;
        self
    }

    /// Exits with `code`.
    pub fn then_exit(mut self, code: i32) -> Self {
        self.exit = code;
        self
    }

    /// Ignores the graceful stop, so only a forced kill ends it early.
    pub fn ignoring_stop(mut self) -> Self {
        self.ignore_stop = true;
        self
    }

    /// Crashes instead of exiting, the way a native fault ends a process:
    /// a signal on Unix, an NTSTATUS exit code on Windows.
    pub fn aborts() -> Self {
        Self {
            abort: true,
            ..Self::default()
        }
    }

    /// The program to run: this test binary.
    pub fn program(&self) -> String {
        helper_command().remove(0)
    }

    /// The arguments that select the helper.
    pub fn args(&self) -> Vec<String> {
        helper_command().split_off(1)
    }

    /// The whole command line, as a container's `command`.
    pub fn command(&self) -> Vec<String> {
        helper_command()
    }

    /// The whole command line with [`Self::vars`] carried as `NAME=value`
    /// arguments, for a process that does not get the container's
    /// environment, such as an exec probe.
    pub fn standalone_command(&self) -> Vec<String> {
        let mut argv = helper_command();
        argv.extend(
            self.vars()
                .into_iter()
                .map(|(name, value)| format!("{name}={value}")),
        );
        argv
    }

    /// The environment that tells the helper what to do.
    pub fn vars(&self) -> Vec<(String, String)> {
        let mut vars = vec![
            (HELPER_VAR, HELPER_SCRIPT.to_string()),
            (HELPER_LINES_VAR, self.lines.join("\n")),
            (HELPER_ERR_LINES_VAR, self.err_lines.join("\n")),
            (HELPER_SHOW_ENV_VAR, self.show_env.join(",")),
            (HELPER_SHOW_CWD_VAR, u8::from(self.show_cwd).to_string()),
            (HELPER_PAUSE_MS_VAR, self.pause.as_millis().to_string()),
            (HELPER_EXIT_VAR, self.exit.to_string()),
            (HELPER_ABORT_VAR, u8::from(self.abort).to_string()),
            (
                HELPER_IGNORE_STOP_VAR,
                u8::from(self.ignore_stop).to_string(),
            ),
        ];
        if let Some(tick) = self.tick {
            vars.push((HELPER_TICK_MS_VAR, tick.as_millis().max(1).to_string()));
        }
        if let Some(path) = &self.child_pid_file {
            vars.push((HELPER_CHILD_PID_FILE_VAR, path.display().to_string()));
        }
        if let Some(port) = self.listen {
            vars.push((HELPER_LISTEN_VAR, port.to_string()));
        }
        if let Some(rounds) = self.rounds {
            vars.push((HELPER_REPEAT_VAR, rounds.to_string()));
        }
        if self.interleaved {
            vars.push((HELPER_INTERLEAVE_VAR, "1".to_string()));
        }
        if let Some(path) = &self.create {
            vars.push((HELPER_CREATE_VAR, path.display().to_string()));
        }
        if let Some(path) = &self.require {
            vars.push((HELPER_REQUIRE_VAR, path.display().to_string()));
        }
        if let Some((failures, path)) = &self.fail_runs {
            vars.push((HELPER_FAIL_RUNS_VAR, failures.to_string()));
            vars.push((HELPER_RUNS_FILE_VAR, path.display().to_string()));
        }
        vars.into_iter()
            .map(|(name, value)| (name.to_string(), value))
            .collect()
    }

    /// [`Self::vars`], as a container's `env`.
    pub fn env(&self) -> Vec<crate::spec::pod::EnvVar> {
        self.vars()
            .into_iter()
            .map(|(name, value)| crate::spec::pod::EnvVar {
                name,
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

#[cfg(test)]
mod script_tests {
    use std::process::{Command, Output, Stdio};
    use std::time::{Duration, Instant};

    use super::Script;

    fn command(script: &Script) -> Command {
        let mut command = Command::new(script.program());
        command
            .args(script.args())
            .envs(script.vars())
            .stdin(Stdio::null());
        command
    }

    fn run(script: &Script) -> Output {
        command(script).output().expect("run helper")
    }

    fn stdout(output: &Output) -> String {
        String::from_utf8_lossy(&output.stdout).into_owned()
    }

    #[test]
    fn prints_to_both_streams_and_exits_with_the_code() {
        let output = run(&Script::prints(["out-line"])
            .with_stderr(["err-line"])
            .then_exit(3));
        assert_eq!(output.status.code(), Some(3));
        assert!(stdout(&output).contains("out-line"));
        assert!(String::from_utf8_lossy(&output.stderr).contains("err-line"));
    }

    #[test]
    fn shows_set_empty_and_unset_variables_apart() {
        let script = Script::shows_env(["SET_VAR", "EMPTY_VAR", "NO_SUCH_VAR"]);
        let output = command(&script)
            .env("SET_VAR", "value")
            .env("EMPTY_VAR", "")
            .env_remove("NO_SUCH_VAR")
            .output()
            .expect("run helper");
        let out = stdout(&output);
        assert!(out.contains("SET_VAR=value"), "{out}");
        assert!(out.contains("EMPTY_VAR=\n"), "{out}");
        assert!(out.contains("NO_SUCH_VAR unset"), "{out}");
    }

    #[test]
    fn shows_its_working_directory() {
        let dir = test_dir!();
        let output = command(&Script::shows_cwd())
            .current_dir(&dir)
            .output()
            .expect("run helper");
        let shown = std::path::PathBuf::from(stdout(&output).lines().last().unwrap().trim());
        assert_eq!(shown.canonicalize().unwrap(), dir.canonicalize().unwrap());
    }

    #[test]
    fn ticking_repeats_until_killed() {
        let mut child = command(&Script::prints(["tick"]).ticking(Duration::from_millis(20)))
            .stdout(Stdio::piped())
            .spawn()
            .expect("spawn helper");
        std::thread::sleep(Duration::from_millis(300));
        assert!(child.try_wait().unwrap().is_none(), "still ticking");
        child.kill().unwrap();
        let output = child.wait_with_output().unwrap();
        assert!(stdout(&output).matches("tick").count() >= 3);
    }

    #[test]
    fn repeated_numbers_each_round() {
        let output = run(&Script::prints(["line-{i}"]).repeated(3));
        let out = stdout(&output);
        assert_eq!(output.status.code(), Some(0));
        assert!(out.ends_with("line-1\nline-2\nline-3\n"), "{out}");
    }

    #[test]
    fn interleaved_keeps_every_line_whole_on_its_stream() {
        let output = run(&Script::prints(["OUT_{i}_tail"])
            .with_stderr(["ERR_{i}_part1_tail"])
            .repeated(2)
            .interleaved());
        let out = stdout(&output);
        let err = String::from_utf8_lossy(&output.stderr);
        assert!(out.ends_with("OUT_1_tail\nOUT_2_tail\n"), "{out}");
        assert!(
            err.ends_with("ERR_1_part1_tail\nERR_2_part1_tail\n"),
            "{err}"
        );
    }

    #[test]
    fn creating_and_requiring_a_file() {
        let dir = test_dir!();
        let made = dir.join("made");
        let missing = dir.join("missing");
        assert_eq!(
            run(&Script::exits(0).creating(&made)).status.code(),
            Some(0)
        );
        assert!(made.exists());
        assert_eq!(
            run(&Script::exits(0).requiring(&made)).status.code(),
            Some(0)
        );
        assert_eq!(
            run(&Script::exits(0).requiring(&missing)).status.code(),
            Some(1)
        );
    }

    #[test]
    fn failing_first_runs_then_succeeds() {
        let dir = test_dir!();
        let script = Script::exits(0).failing_first(2, dir.join("runs"));
        let codes: Vec<_> = (0..3).map(|_| run(&script).status.code()).collect();
        assert_eq!(codes, [Some(1), Some(1), Some(0)]);
    }

    #[test]
    fn standalone_command_needs_no_environment() {
        let argv = Script::prints(["from-args"])
            .then_exit(4)
            .standalone_command();
        let output = Command::new(&argv[0])
            .args(&argv[1..])
            .env_remove(super::HELPER_VAR)
            .stdin(Stdio::null())
            .output()
            .expect("run helper");
        assert_eq!(output.status.code(), Some(4));
        assert!(stdout(&output).contains("from-args"));
    }

    #[test]
    fn records_a_live_child() {
        let dir = test_dir!();
        let pid_file = dir.join("child.pid");
        let mut parent = command(&Script::runs().with_child(&pid_file))
            .spawn()
            .expect("spawn helper");
        let deadline = Instant::now() + Duration::from_secs(10);
        let pid: u32 = loop {
            if let Some(pid) = std::fs::read_to_string(&pid_file)
                .ok()
                .and_then(|text| text.trim().parse().ok())
            {
                break pid;
            }
            assert!(Instant::now() < deadline, "no child pid recorded");
            std::thread::sleep(Duration::from_millis(20));
        };
        assert!(kcommon::signal::is_pid_alive(pid));
        kcommon::signal::force_kill(pid);
        parent.kill().unwrap();
        let _ = parent.wait();
    }

    #[test]
    fn listening_answers_http_200() {
        use std::io::{Read, Write};

        let port = std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        let mut child = command(&Script::runs().listening(port))
            .spawn()
            .expect("spawn helper");
        let deadline = Instant::now() + Duration::from_secs(10);
        let mut stream = loop {
            match std::net::TcpStream::connect(("127.0.0.1", port)) {
                Ok(stream) => break stream,
                Err(_) if Instant::now() < deadline => {
                    std::thread::sleep(Duration::from_millis(20))
                }
                Err(error) => panic!("helper never listened: {error}"),
            }
        };
        stream.write_all(b"GET / HTTP/1.1\r\n\r\n").unwrap();
        let mut reply = String::new();
        let _ = stream.read_to_string(&mut reply);
        assert!(reply.starts_with("HTTP/1.1 200"), "{reply}");
        child.kill().unwrap();
        let _ = child.wait();
    }
}
