// Copyright 2026 Tabsdata Inc.

//! Process executor with builder pattern and pluggable output redirection.
//!
//! # Example
//!
//! ```rust,no_run
//! use std::time::Duration;
//! use pod_runner::process::executor::*;
//!
//! # async fn example() -> std::io::Result<()> {
//! let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(());
//!
//! // Block until done
//! let mut handle = ProcessExecutor::new("my-app")
//!     .arg("--port=8080")
//!     .args(["--verbose", "--color=auto"])
//!     .env("APP_ENV", "production")
//!     .envs([("KEY1", "val1"), ("KEY2", "val2")])
//!     .propagate_env(true)
//!     .work_dir("/tmp/work")
//!     .die_with_parent(true)
//!     .output(MergedOutput::new(RollingFileStream::new("/tmp/logs/app.log", 100 * 1024 * 1024, 5)))
//!     .spawn(shutdown_rx.clone())
//!     .await?;
//!
//! let exit_code = handle.wait().await?;
//!
//! // Reactive with select!
//! let mut handle = ProcessExecutor::new("server")
//!     .output(DevNullOutput)
//!     .spawn(shutdown_rx)
//!     .await?;
//!
//! tokio::select! {
//!     result = handle.wait() => {
//!         let exit_code = result?;
//!     }
//!     _ = tokio::signal::ctrl_c() => {
//!         shutdown_tx.send(()).unwrap();
//!         handle.kill(Duration::from_secs(5)).await;
//!     }
//! }
//! # Ok(())
//! # }
//! ```

use std::collections::HashMap;
use std::io;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use kcommon::signal::{force_kill, force_kill_group, is_pid_alive, send_sigterm_group};
use tokio::process::Command;
use tokio::sync::watch;
use tokio::task::{JoinHandle, JoinSet};
use tracing::{Instrument, debug, debug_span, warn};

// ---------------------------------------------------------------------------
// LogStream — identifies the source stream
// ---------------------------------------------------------------------------

/// Identifies whether a log line came from stdout or stderr.
/// Used by CRI-format writers to tag each line.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogStream {
    Stdout,
    Stderr,
}

impl LogStream {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Stdout => "stdout",
            Self::Stderr => "stderr",
        }
    }
}

// ---------------------------------------------------------------------------
// StreamWriter — runtime trait for writing lines
// ---------------------------------------------------------------------------

/// Writes output lines from a process stream.
///
/// Implementations handle the actual I/O: writing to files, rotating,
/// teeing, or discarding. Created by [`StreamConfig::open`].
pub trait StreamWriter: Send + 'static {
    /// Write a complete line (may or may not include a trailing newline).
    /// `stream` identifies whether the line came from stdout or stderr.
    fn write_line(&mut self, line: &[u8], stream: LogStream) -> io::Result<()>;

    /// Flush any buffered data.
    fn flush(&mut self) -> io::Result<()>;
}

// ---------------------------------------------------------------------------
// StreamConfig — composable factory trait
// ---------------------------------------------------------------------------

/// Configuration for creating a [`StreamWriter`].
///
/// Implementations hold configuration and defer I/O to [`open`](StreamConfig::open).
/// This allows infallible construction and composition before spawning.
pub trait StreamConfig: Send + 'static {
    /// Create the [`StreamWriter`] for this configuration.
    ///
    /// Called at spawn time. Performs any necessary I/O (file creation,
    /// directory setup, rollover of existing files).
    fn open(self: Box<Self>) -> io::Result<Box<dyn StreamWriter>>;
}

// ---------------------------------------------------------------------------
// DevNullStream
// ---------------------------------------------------------------------------

/// Discards all output.
pub struct DevNullStream;

impl StreamConfig for DevNullStream {
    fn open(self: Box<Self>) -> io::Result<Box<dyn StreamWriter>> {
        Ok(Box::new(DevNullWriter))
    }
}

struct DevNullWriter;

impl StreamWriter for DevNullWriter {
    fn write_line(&mut self, _line: &[u8], _stream: LogStream) -> io::Result<()> {
        Ok(())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// FileStream
// ---------------------------------------------------------------------------

/// Writes output to a plain file (no rotation).
pub struct FileStream {
    path: PathBuf,
}

impl FileStream {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }
}

impl StreamConfig for FileStream {
    fn open(self: Box<Self>) -> io::Result<Box<dyn StreamWriter>> {
        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let file = std::fs::File::create(&self.path)?;
        Ok(Box::new(FileWriter { file }))
    }
}

struct FileWriter {
    file: std::fs::File,
}

impl StreamWriter for FileWriter {
    fn write_line(&mut self, line: &[u8], _stream: LogStream) -> io::Result<()> {
        use std::io::Write;
        self.file.write_all(line)?;
        if !line.ends_with(b"\n") {
            self.file.write_all(b"\n")?;
        }
        Ok(())
    }

    fn flush(&mut self) -> io::Result<()> {
        use std::io::Write;
        self.file.flush()
    }
}

// ---------------------------------------------------------------------------
// RollingFileStream
// ---------------------------------------------------------------------------

/// Writes output to a rolling log file using standard numbered rotation.
///
/// Files are named using the standard log rotation convention:
/// - `app.log`   — current (active) file
/// - `app.log.1` — most recent rolled file
/// - `app.log.2` — next oldest
/// - …up to `app.log.{max_files - 1}`
///
/// Rotation happens after a newline once the current file exceeds
/// `max_size_bytes`. On rotation, existing files are shifted:
/// `app.log.2` → `app.log.3`, `app.log.1` → `app.log.2`,
/// `app.log` → `app.log.1`, then a fresh `app.log` is created.
/// Files beyond `max_files` are deleted.
pub struct RollingFileStream {
    path: PathBuf,
    max_size_bytes: u64,
    max_files: usize,
}

impl RollingFileStream {
    pub fn new(path: impl Into<PathBuf>, max_size_bytes: u64, max_files: usize) -> Self {
        Self {
            path: path.into(),
            max_size_bytes,
            max_files,
        }
    }
}

impl StreamConfig for RollingFileStream {
    fn open(self: Box<Self>) -> io::Result<Box<dyn StreamWriter>> {
        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let file = std::fs::File::create(&self.path)?;
        Ok(Box::new(RollingStreamWriter {
            path: self.path,
            file,
            current_size: 0,
            max_size: self.max_size_bytes,
            max_files: self.max_files,
        }))
    }
}

struct RollingStreamWriter {
    path: PathBuf,
    file: std::fs::File,
    current_size: u64,
    max_size: u64,
    max_files: usize,
}

impl RollingStreamWriter {
    /// Build the path for rolled file number `n` (e.g. `app.log.1`).
    fn rolled_path(&self, n: usize) -> PathBuf {
        PathBuf::from(format!("{}.{}", self.path.display(), n))
    }

    /// Rotate files: shift numbered files up by one, move current to .1,
    /// delete anything beyond max_files, and open a fresh current file.
    fn rotate(&mut self) -> io::Result<()> {
        use std::io::Write;
        self.file.flush()?;

        // max_files includes the current file, so rolled files go 1..=max_rolled.
        let max_rolled = self.max_files.saturating_sub(1);

        if max_rolled > 0 {
            // Delete the oldest rolled file if at capacity.
            let oldest = self.rolled_path(max_rolled);
            if oldest.exists() {
                let _ = std::fs::remove_file(&oldest);
            }

            // Shift existing rolled files up: .N-1 → .N, … .1 → .2
            for i in (1..max_rolled).rev() {
                let from = self.rolled_path(i);
                let to = self.rolled_path(i + 1);
                if from.exists() {
                    let _ = std::fs::rename(&from, &to);
                }
            }

            // Move current → .1
            let _ = std::fs::rename(&self.path, self.rolled_path(1));
        }

        // Open fresh current file (truncates if max_rolled == 0)
        self.file = std::fs::File::create(&self.path)?;
        self.current_size = 0;
        Ok(())
    }
}

impl StreamWriter for RollingStreamWriter {
    fn write_line(&mut self, line: &[u8], _stream: LogStream) -> io::Result<()> {
        use std::io::Write;
        self.file.write_all(line)?;
        if !line.ends_with(b"\n") {
            self.file.write_all(b"\n")?;
            self.current_size += 1;
        }
        self.current_size += line.len() as u64;

        if self.max_size > 0 && self.current_size >= self.max_size {
            self.rotate()?;
        }
        Ok(())
    }

    fn flush(&mut self) -> io::Result<()> {
        use std::io::Write;
        self.file.flush()
    }
}

// ---------------------------------------------------------------------------
// TeeStream
// ---------------------------------------------------------------------------

/// Duplicates each line to two [`StreamConfig`] targets.
///
/// Composable: each target can itself be a `TeeStream`, `RollingFileStream`,
/// `FileStream`, etc.
pub struct TeeStream {
    a: Box<dyn StreamConfig>,
    b: Box<dyn StreamConfig>,
}

impl TeeStream {
    pub fn new(a: impl StreamConfig, b: impl StreamConfig) -> Self {
        Self {
            a: Box::new(a),
            b: Box::new(b),
        }
    }
}

impl StreamConfig for TeeStream {
    fn open(self: Box<Self>) -> io::Result<Box<dyn StreamWriter>> {
        let a = self.a.open()?;
        let b = self.b.open()?;
        Ok(Box::new(TeeWriter { a, b }))
    }
}

struct TeeWriter {
    a: Box<dyn StreamWriter>,
    b: Box<dyn StreamWriter>,
}

impl StreamWriter for TeeWriter {
    fn write_line(&mut self, line: &[u8], stream: LogStream) -> io::Result<()> {
        self.a.write_line(line, stream)?;
        self.b.write_line(line, stream)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.a.flush()?;
        self.b.flush()
    }
}

// ---------------------------------------------------------------------------
// OutputRedirector — orchestrates Stdio + async pumping
// ---------------------------------------------------------------------------

/// Configures how a spawned process's stdout and stderr are handled.
///
/// Implementations control two phases:
/// 1. [`setup`](OutputRedirector::setup) — called before spawn to configure
///    `Stdio` for stdout and stderr.
/// 2. [`start`](OutputRedirector::start) — called after spawn with the
///    child's piped streams (if any) to begin async I/O pumping. A shutdown
///    signal is provided so pump tasks can exit cleanly on shutdown.
pub trait OutputRedirector: Send + 'static {
    /// Validate that this redirector is compatible with the process
    /// configuration. Called before `setup`.
    fn validate(&self, die_with_parent: bool) -> io::Result<()>;

    /// Configure the `Stdio` for stdout and stderr before spawning.
    ///
    /// Returns `(stdout_stdio, stderr_stdio)`.
    fn setup(&mut self) -> io::Result<(Stdio, Stdio)>;

    /// Called after spawn. Receives piped stdout/stderr (if `setup` used
    /// `Stdio::piped()`) and a shutdown signal receiver.
    ///
    /// Implementations should spawn async tasks as needed to pump the
    /// streams. When the shutdown receiver fires, pump tasks should flush
    /// pending data and exit.
    ///
    /// Returns a `JoinHandle` that completes when all I/O pumping is done.
    /// For redirectors that don't need pumping, return a handle that
    /// resolves immediately.
    fn start(
        self: Box<Self>,
        stdout: Option<tokio::process::ChildStdout>,
        stderr: Option<tokio::process::ChildStderr>,
        shutdown: watch::Receiver<()>,
    ) -> JoinHandle<()>;
}

// ---------------------------------------------------------------------------
// DevNullOutput
// ---------------------------------------------------------------------------

/// Discards both stdout and stderr via `Stdio::null()`.
pub struct DevNullOutput;

impl OutputRedirector for DevNullOutput {
    fn validate(&self, _die_with_parent: bool) -> io::Result<()> {
        Ok(())
    }

    fn setup(&mut self) -> io::Result<(Stdio, Stdio)> {
        Ok((Stdio::null(), Stdio::null()))
    }

    fn start(
        self: Box<Self>,
        _stdout: Option<tokio::process::ChildStdout>,
        _stderr: Option<tokio::process::ChildStderr>,
        _shutdown: watch::Receiver<()>,
    ) -> JoinHandle<()> {
        tokio::spawn(async {})
    }
}

// ---------------------------------------------------------------------------
// DirectFileOutput
// ---------------------------------------------------------------------------

/// Redirects stdout and/or stderr directly to files via OS-level file
/// descriptors.
///
/// No async pumping — the OS writes directly to the files. This is the
/// only file-based redirector compatible with `die_with_parent(false)`,
/// since the file descriptors survive the parent process.
///
/// Incompatible with `die_with_parent(true)` — use [`MergedOutput`] or
/// [`SplitOutput`] instead.
pub struct DirectFileOutput {
    stdout_path: Option<PathBuf>,
    stderr_path: Option<PathBuf>,
    /// `true` when created via `merged()` — both streams share one fd.
    merged: bool,
    /// Lines written at the very beginning of the output file (the merged
    /// file, or the stderr file when split), before any process output —
    /// e.g. read-only mount warnings. Written raw (one line per entry),
    /// matching this redirector's unformatted output.
    header_lines: Vec<String>,
}

impl DirectFileOutput {
    /// Redirect stdout and stderr to separate files.
    ///
    /// At least one path must be `Some`. Both `Some` paths must differ.
    /// `None` means `/dev/null` for that stream.
    pub fn new(
        stdout_path: Option<impl Into<PathBuf>>,
        stderr_path: Option<impl Into<PathBuf>>,
    ) -> Self {
        Self {
            stdout_path: stdout_path.map(Into::into),
            stderr_path: stderr_path.map(Into::into),
            merged: false,
            header_lines: Vec::new(),
        }
    }

    /// Redirect both stdout and stderr to the same file.
    ///
    /// Uses `try_clone()` so both streams write to the same fd.
    pub fn merged(path: impl Into<PathBuf>) -> Self {
        let path = path.into();
        Self {
            stdout_path: Some(path),
            stderr_path: None,
            merged: true,
            header_lines: Vec::new(),
        }
    }

    /// Lines to write at the very beginning of the output file, before
    /// any process output.
    pub fn with_header_lines(mut self, lines: Vec<String>) -> Self {
        self.header_lines = lines;
        self
    }

    /// Write the header lines into a freshly-created output file.
    fn write_header(&self, file: &mut std::fs::File) -> io::Result<()> {
        use std::io::Write;
        for line in &self.header_lines {
            file.write_all(line.as_bytes())?;
            if !line.ends_with('\n') {
                file.write_all(b"\n")?;
            }
        }
        Ok(())
    }
}

impl OutputRedirector for DirectFileOutput {
    fn validate(&self, die_with_parent: bool) -> io::Result<()> {
        if die_with_parent {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "DirectFileOutput requires die_with_parent(false); \
                 use MergedOutput or SplitOutput for attached processes",
            ));
        }
        if !self.merged {
            if self.stdout_path.is_none() && self.stderr_path.is_none() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "DirectFileOutput: at least one path must be set; \
                     use DevNullOutput to discard both streams",
                ));
            }
            if let (Some(out), Some(err)) = (&self.stdout_path, &self.stderr_path)
                && out == err
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "DirectFileOutput: stdout and stderr paths must differ; \
                         use DirectFileOutput::merged() for a shared file",
                ));
            }
        }
        Ok(())
    }

    fn setup(&mut self) -> io::Result<(Stdio, Stdio)> {
        if self.merged {
            let path = self.stdout_path.as_ref().unwrap();
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            let mut file = std::fs::File::create(path)?;
            self.write_header(&mut file)?;
            let file_clone = file.try_clone()?;
            return Ok((Stdio::from(file), Stdio::from(file_clone)));
        }

        let mut stdout_file = match &self.stdout_path {
            Some(path) => {
                if let Some(parent) = path.parent() {
                    std::fs::create_dir_all(parent)?;
                }
                Some(std::fs::File::create(path)?)
            }
            None => None,
        };

        let mut stderr_file = match &self.stderr_path {
            Some(path) => {
                if let Some(parent) = path.parent() {
                    std::fs::create_dir_all(parent)?;
                }
                Some(std::fs::File::create(path)?)
            }
            None => None,
        };

        // Header lines go to the stderr file (they are warnings), falling
        // back to the stdout file when stderr is discarded.
        if let Some(file) = stderr_file.as_mut().or(stdout_file.as_mut()) {
            self.write_header(file)?;
        }

        let stdout_stdio = stdout_file.map(Stdio::from).unwrap_or_else(Stdio::null);
        let stderr_stdio = stderr_file.map(Stdio::from).unwrap_or_else(Stdio::null);
        Ok((stdout_stdio, stderr_stdio))
    }

    fn start(
        self: Box<Self>,
        _stdout: Option<tokio::process::ChildStdout>,
        _stderr: Option<tokio::process::ChildStderr>,
        _shutdown: watch::Receiver<()>,
    ) -> JoinHandle<()> {
        tokio::spawn(async {})
    }
}

// ---------------------------------------------------------------------------
// MergedOutput
// ---------------------------------------------------------------------------

/// Merges stdout and stderr into a single [`StreamWriter`], interleaving
/// at line boundaries.
///
/// Pipes both streams through an async pump task. Incompatible with
/// `die_with_parent(false)` — the pump dies with the parent.
pub struct MergedOutput {
    config: Box<dyn StreamConfig>,
}

impl MergedOutput {
    pub fn new(config: impl StreamConfig) -> Self {
        Self {
            config: Box::new(config),
        }
    }
}

impl OutputRedirector for MergedOutput {
    fn validate(&self, die_with_parent: bool) -> io::Result<()> {
        if !die_with_parent {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "MergedOutput requires die_with_parent(true); \
                 use DirectFileOutput for detached processes",
            ));
        }
        Ok(())
    }

    fn setup(&mut self) -> io::Result<(Stdio, Stdio)> {
        Ok((Stdio::piped(), Stdio::piped()))
    }

    fn start(
        self: Box<Self>,
        stdout: Option<tokio::process::ChildStdout>,
        stderr: Option<tokio::process::ChildStderr>,
        shutdown: watch::Receiver<()>,
    ) -> JoinHandle<()> {
        let config = self.config;

        tokio::spawn(async move {
            let mut writer = match config.open() {
                Ok(w) => w,
                Err(e) => {
                    warn!("Failed to create stream writer: {}", e);
                    return;
                }
            };

            // One pump task per child stream funnels tagged lines through
            // an mpsc channel to a single writer loop. `read_line` is
            // *not* cancel-safe, so it must never share a `select!` with
            // another `read_line` — racing two reads would silently drop
            // partial data on the losing branch.
            let (tx, mut rx) = tokio::sync::mpsc::channel::<(LogStream, String)>(256);

            let stdout_pump = spawn_merged_pump(
                "stdout",
                LogStream::Stdout,
                stdout,
                tx.clone(),
                shutdown.clone(),
            );
            let stderr_pump = spawn_merged_pump("stderr", LogStream::Stderr, stderr, tx, shutdown);

            while let Some((stream, line)) = rx.recv().await {
                if let Err(e) = writer.write_line(line.as_bytes(), stream) {
                    warn!("Error writing {}: {}", stream.as_str(), e);
                    break;
                }
            }

            // Dropping `rx` (loop exit) signals pumps to stop if they
            // haven't already; await them to keep this task's lifetime
            // covering the whole pipeline.
            drop(rx);
            let _ = stdout_pump.await;
            let _ = stderr_pump.await;

            let _ = writer.flush();
        })
    }
}

/// Pump one child stream's lines onto a shared mpsc channel.
///
/// `read_line` is only ever raced against `shutdown.changed()` (which
/// *is* cancel-safe), so partial reads can't be lost the way they would
/// be if two `read_line`s raced each other in a `select!`.
fn spawn_merged_pump(
    label: &'static str,
    log_stream: LogStream,
    stream: Option<impl tokio::io::AsyncRead + Unpin + Send + 'static>,
    tx: tokio::sync::mpsc::Sender<(LogStream, String)>,
    mut shutdown: watch::Receiver<()>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        use tokio::io::{AsyncBufReadExt, BufReader};

        let stream = match stream {
            Some(s) => s,
            None => return,
        };

        let mut reader = BufReader::new(stream);
        let mut buf = String::new();

        loop {
            tokio::select! {
                result = reader.read_line(&mut buf) => {
                    match result {
                        Ok(0) | Err(_) => break,
                        Ok(_) => {
                            let line = std::mem::take(&mut buf);
                            if tx.send((log_stream, line)).await.is_err() {
                                // Writer task is gone; nothing left to do.
                                break;
                            }
                        }
                    }
                }
                _ = shutdown.changed() => {
                    debug!("{} pump received shutdown signal", label);
                    break;
                }
            }
        }
    })
}

// ---------------------------------------------------------------------------
// SplitOutput
// ---------------------------------------------------------------------------

/// Routes stdout and stderr to separate [`StreamWriter`]s.
///
/// Each stream gets its own async pump task. Incompatible with
/// `die_with_parent(false)` — the pumps die with the parent.
pub struct SplitOutput {
    stdout_config: Box<dyn StreamConfig>,
    stderr_config: Box<dyn StreamConfig>,
}

impl SplitOutput {
    pub fn new(stdout: impl StreamConfig, stderr: impl StreamConfig) -> Self {
        Self {
            stdout_config: Box::new(stdout),
            stderr_config: Box::new(stderr),
        }
    }
}

impl OutputRedirector for SplitOutput {
    fn validate(&self, die_with_parent: bool) -> io::Result<()> {
        if !die_with_parent {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "SplitOutput requires die_with_parent(true); \
                 use DirectFileOutput for detached processes",
            ));
        }
        Ok(())
    }

    fn setup(&mut self) -> io::Result<(Stdio, Stdio)> {
        Ok((Stdio::piped(), Stdio::piped()))
    }

    fn start(
        self: Box<Self>,
        stdout: Option<tokio::process::ChildStdout>,
        stderr: Option<tokio::process::ChildStderr>,
        shutdown: watch::Receiver<()>,
    ) -> JoinHandle<()> {
        let stdout_config = self.stdout_config;
        let stderr_config = self.stderr_config;
        let shutdown_clone = shutdown.clone();

        tokio::spawn(async move {
            let stdout_handle =
                pump_stream("stdout", LogStream::Stdout, stdout, stdout_config, shutdown);
            let stderr_handle = pump_stream(
                "stderr",
                LogStream::Stderr,
                stderr,
                stderr_config,
                shutdown_clone,
            );
            let _ = stdout_handle.await;
            let _ = stderr_handle.await;
        })
    }
}

/// Spawn an async task that reads lines from a child stream and writes
/// them through a [`StreamWriter`]. Exits when the stream ends or the
/// shutdown signal fires.
fn pump_stream(
    label: &'static str,
    log_stream: LogStream,
    stream: Option<impl tokio::io::AsyncRead + Unpin + Send + 'static>,
    config: Box<dyn StreamConfig>,
    mut shutdown: watch::Receiver<()>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        use tokio::io::{AsyncBufReadExt, BufReader};

        let stream = match stream {
            Some(s) => s,
            None => return,
        };

        let mut writer = match config.open() {
            Ok(w) => w,
            Err(e) => {
                warn!("Failed to create {} stream writer: {}", label, e);
                return;
            }
        };

        let mut reader = BufReader::new(stream);
        let mut buf = String::new();

        loop {
            tokio::select! {
                result = reader.read_line(&mut buf) => {
                    match result {
                        Ok(0) | Err(_) => break,
                        Ok(_) => {
                            if let Err(e) = writer.write_line(buf.as_bytes(), log_stream) {
                                warn!("Error writing {}: {}", label, e);
                                break;
                            }
                            buf.clear();
                        }
                    }
                }
                _ = shutdown.changed() => {
                    debug!("{} pump received shutdown signal", label);
                    break;
                }
            }
        }
        let _ = writer.flush();
    })
}

// ---------------------------------------------------------------------------
// ExecutorHandle
// ---------------------------------------------------------------------------

/// Handle to a spawned process.
///
/// Provides the OS process ID, blocking and non-blocking wait, and kill.
/// The `wait` method is cancel-safe: if dropped mid-await (e.g. from a
/// `tokio::select!` branch), the handle remains valid for subsequent calls.
///
/// Owns a [`JoinSet`] containing the I/O pump task(s). When the handle
/// is dropped, the `JoinSet` is dropped and all pump tasks are aborted
/// automatically — no leaked tasks.
pub struct ExecutorHandle {
    pid: u32,
    wait_handle: JoinHandle<i32>,
    cached_exit_code: Option<i32>,
    /// I/O pump tasks. Joined by [`wait`] to ensure all output is
    /// flushed before returning.  Dropping the JoinSet aborts them.
    io_tasks: JoinSet<()>,
}

impl std::fmt::Debug for ExecutorHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExecutorHandle")
            .field("pid", &self.pid)
            .field("cached_exit_code", &self.cached_exit_code)
            .finish_non_exhaustive()
    }
}

impl ExecutorHandle {
    /// The OS process ID.
    pub fn pid(&self) -> u32 {
        self.pid
    }

    /// Wait for the process to exit, drain all I/O pump tasks (so
    /// output files are fully flushed), and return the exit code.
    ///
    /// Cancel-safe: if the future is dropped before completion (e.g.
    /// by `tokio::select!`), the handle remains valid and `wait` can
    /// be called again.
    ///
    /// On the first successful completion, the exit code is cached.
    /// Subsequent calls return the cached value immediately (and
    /// re-drain any I/O tasks that weren't joined yet).
    ///
    /// # Errors
    ///
    /// Returns an error if the internal monitoring task panicked or was
    /// cancelled (e.g. the tokio runtime shut down). This indicates a
    /// bug or an abnormal runtime condition, not a normal process exit.
    pub async fn wait(&mut self) -> io::Result<i32> {
        if let Some(code) = self.cached_exit_code {
            // I/O tasks may still be draining if a previous wait() was
            // cancelled between caching the code and finishing the drain.
            while self.io_tasks.join_next().await.is_some() {}
            return Ok(code);
        }
        let code = match (&mut self.wait_handle).await {
            Ok(code) => code,
            Err(e) => {
                // The handle is consumed either way; leaving nothing cached
                // means the next poll panics ("JoinHandle polled after
                // completion") — an abort under panic = "abort". Cache the
                // failure first, then report it.
                self.cached_exit_code = Some(-1);
                return Err(io::Error::other(format!(
                    "process monitor task failed: {}",
                    e
                )));
            }
        };
        self.cached_exit_code = Some(code);
        // Drain I/O pump tasks so all output is flushed before returning.
        while self.io_tasks.join_next().await.is_some() {}
        Ok(code)
    }

    /// Non-blocking check: returns the exit code if the process has
    /// finished, `None` if still running.
    pub fn try_wait(&mut self) -> Option<i32> {
        if let Some(code) = self.cached_exit_code {
            return Some(code);
        }
        if self.wait_handle.is_finished() {
            // The poll consumes the handle whatever it yields; a consumed
            // handle with nothing cached panics on the next poll.
            let code = match now_or_never(&mut self.wait_handle) {
                Some(Ok(code)) => code,
                Some(Err(_)) | None => -1,
            };
            self.cached_exit_code = Some(code);
            return Some(code);
        }
        None
    }

    /// Whether the child has been reaped — judged from the handle, never
    /// the process table. A reaped child's number is no longer ours to
    /// signal: the reservation (Unix zombie, Windows handle) ends exactly
    /// here, and a group kill aimed past it lands on a stranger's tree.
    /// Errs towards leaking (a panicked monitor reads as exited; the
    /// sweep reclaims the stray) rather than misfiring.
    pub fn has_exited(&self) -> bool {
        self.cached_exit_code.is_some() || self.wait_handle.is_finished()
    }

    /// Abort the internal monitor task so the next poll yields a
    /// `JoinError` — manufacturing the state that killed pod-runner in
    /// the field. Test-only.
    #[cfg(test)]
    pub(crate) fn abort_monitor(&self) {
        self.wait_handle.abort();
    }

    /// Send SIGTERM to the process and all its descendants without waiting.
    pub fn send_sigterm(&self) {
        if self.has_exited() {
            debug!(pid = self.pid, "SIGTERM skipped: already exited");
            return;
        }
        send_sigterm_group(self.pid);
        debug!(pid = self.pid, "SIGTERM sent to process group");
    }

    /// Send SIGTERM, wait for process exit or grace period, then SIGKILL
    /// if still alive.
    ///
    /// If `grace_period` is zero, sends SIGKILL immediately (no SIGTERM).
    ///
    /// Unlike a blind sleep, this races the grace period against the
    /// actual process exit — returns as soon as the process dies.
    pub async fn kill(&mut self, grace_period: Duration) {
        let kill_start = std::time::Instant::now();

        // Asked of the handle, not the process table: a reaped child's
        // number may already be somebody else's. See [`Self::has_exited`].
        if let Some(code) = self.try_wait() {
            debug!(
                pid = self.pid,
                exit_code = code,
                elapsed_ms = kill_start.elapsed().as_millis(),
                "Kill: already dead"
            );
            return;
        }

        if grace_period.is_zero() {
            force_kill_group(self.pid);
            debug!(
                pid = self.pid,
                elapsed_ms = kill_start.elapsed().as_millis(),
                "Kill: SIGKILL group (zero grace)"
            );
        } else {
            let pid = self.pid;
            send_sigterm_group(pid);
            let sigterm_elapsed = kill_start.elapsed();
            debug!(
                pid,
                sigterm_ms = sigterm_elapsed.as_millis(),
                "Kill: SIGTERM sent to group"
            );

            let exit_code = tokio::select! {
                result = &mut self.wait_handle => {
                    Some(result.unwrap_or(-1))
                }
                _ = tokio::time::sleep(grace_period) => None,
            };
            let wait_elapsed = kill_start.elapsed();

            if let Some(code) = exit_code {
                self.cached_exit_code = Some(code);
                debug!(
                    pid,
                    exit_code = code,
                    wait_ms = wait_elapsed.as_millis(),
                    "Kill: exited after SIGTERM",
                );
            } else if self.try_wait().is_none() {
                warn!(
                    pid,
                    grace_ms = wait_elapsed.as_millis(),
                    "Kill: grace period expired, sending SIGKILL to group",
                );
                force_kill_group(pid);
                debug!(
                    pid,
                    total_ms = kill_start.elapsed().as_millis(),
                    "Kill: SIGKILL sent after grace expiry",
                );
            } else {
                debug!(
                    pid,
                    wait_ms = wait_elapsed.as_millis(),
                    "Kill: died during grace period (race)",
                );
            }
        }

        debug!(
            pid = self.pid,
            total_ms = kill_start.elapsed().as_millis(),
            "Kill completed"
        );
    }
}

/// Poll a `JoinHandle` without blocking. Returns `Some` if ready, `None`
/// if pending. Avoids pulling in the `futures` crate for a single utility.
fn now_or_never(handle: &mut JoinHandle<i32>) -> Option<Result<i32, tokio::task::JoinError>> {
    use std::future::Future;
    use std::pin::Pin;
    use std::task::{Context, Poll, Wake, Waker};

    struct NoopWaker;
    impl Wake for NoopWaker {
        fn wake(self: std::sync::Arc<Self>) {}
    }

    let waker = Waker::from(std::sync::Arc::new(NoopWaker));
    let mut cx = Context::from_waker(&waker);
    match Pin::new(handle).poll(&mut cx) {
        Poll::Ready(val) => Some(val),
        Poll::Pending => None,
    }
}

// ---------------------------------------------------------------------------
// Env var handling
// ---------------------------------------------------------------------------

/// Set up a child's environment: the optional clear, the kill tokens that
/// must survive one, then the caller's explicit variables (which
/// therefore win — pod-runner's own token goes through `env_vars` and
/// needs no carve-out). A container that loses km's inherited token is
/// unreachable by the only mechanism that can still find it, so a clear
/// must not be what decides that. `inherited` is injected so tests never
/// mutate global state.
fn apply_env(
    cmd: &mut Command,
    propagate_env: bool,
    env_vars: &HashMap<String, String>,
    inherited: impl Fn(&str) -> Option<String>,
) {
    if !propagate_env {
        cmd.env_clear();
        kcommon::token::carry_kill_tokens(cmd.as_std_mut(), inherited);
    }
    if !env_vars.is_empty() {
        cmd.envs(env_vars);
    }
}

/// Validate environment variable names.
///
/// Rejects empty names, names containing `=`, and names containing NUL.
fn validate_env_vars(env_vars: &HashMap<String, String>) -> io::Result<()> {
    for key in env_vars.keys() {
        if key.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "environment variable name must not be empty",
            ));
        }
        if key.contains('=') {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("environment variable name must not contain '=': {:?}", key),
            ));
        }
        if key.contains('\0') {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("environment variable name must not contain NUL: {:?}", key),
            ));
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// ProcessExecutor — builder
// ---------------------------------------------------------------------------

/// Builder for configuring and spawning an OS process.
///
/// Defaults:
/// - `propagate_env`: `true` (inherit parent environment)
/// - `die_with_parent`: `true` (kill on drop, no session detach)
/// - `output`: [`DevNullOutput`] (discard stdout and stderr)
/// - `work_dir`: inherited from parent process
pub struct ProcessExecutor {
    program: String,
    args: Vec<String>,
    env_vars: HashMap<String, String>,
    propagate_env: bool,
    work_dir: Option<PathBuf>,
    pid_dir: Option<PathBuf>,
    die_with_parent: bool,
    output: Option<Box<dyn OutputRedirector>>,
}

impl ProcessExecutor {
    /// Create a new command builder for the given program.
    pub fn new(program: impl Into<String>) -> Self {
        Self {
            program: program.into(),
            args: Vec::new(),
            env_vars: HashMap::new(),
            propagate_env: true,
            work_dir: None,
            pid_dir: None,
            die_with_parent: true,
            output: None,
        }
    }

    /// Append a single argument.
    pub fn arg(mut self, arg: impl Into<String>) -> Self {
        self.args.push(arg.into());
        self
    }

    /// Append multiple arguments.
    pub fn args<I, S>(mut self, args: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.args.extend(args.into_iter().map(Into::into));
        self
    }

    /// Set a single environment variable.
    pub fn env(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.env_vars.insert(key.into(), value.into());
        self
    }

    /// Set multiple environment variables.
    pub fn envs<I, K, V>(mut self, vars: I) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        for (k, v) in vars {
            self.env_vars.insert(k.into(), v.into());
        }
        self
    }

    /// Whether to propagate the launcher's (parent's) environment variables
    /// to the child process.
    ///
    /// Default: `true`. When `false`, the child starts with a clean
    /// environment containing only the explicitly set variables.
    pub fn propagate_env(mut self, propagate: bool) -> Self {
        self.propagate_env = propagate;
        self
    }

    /// Set the working directory for the child process.
    ///
    /// If not set, the child inherits the parent's current working directory.
    pub fn work_dir(mut self, dir: impl Into<PathBuf>) -> Self {
        self.work_dir = Some(dir.into());
        self
    }

    /// Whether the child process should be killed when the parent exits.
    ///
    /// Default: `true`. When `false`, the child is detached (new session
    /// on Unix via `setsid`) and survives the parent.
    pub fn die_with_parent(mut self, die: bool) -> Self {
        self.die_with_parent = die;
        self
    }

    /// Set the PID tracking directory.
    ///
    /// When set, a `<pid>` file is written on spawn (containing the
    /// program name) and removed on process exit. Use
    /// [`cleanup_stale_pids`] at startup to kill orphaned processes.
    pub fn pid_dir(mut self, dir: impl Into<PathBuf>) -> Self {
        self.pid_dir = Some(dir.into());
        self
    }

    /// Set the output redirector for stdout and stderr.
    ///
    /// If not set, defaults to [`DevNullOutput`].
    pub fn output(mut self, redirector: impl OutputRedirector) -> Self {
        self.output = Some(Box::new(redirector));
        self
    }

    /// Set the output redirector from a pre-boxed trait object.
    pub fn output_boxed(mut self, redirector: Box<dyn OutputRedirector>) -> Self {
        self.output = Some(redirector);
        self
    }

    /// Spawn the process and return a [`ExecutorHandle`].
    ///
    /// The `shutdown` receiver is propagated to all internal async pump
    /// tasks. When the sender fires, pump tasks flush pending data and
    /// exit. The process itself is **not** killed automatically — use
    /// [`ExecutorHandle::kill`] for that.
    ///
    /// # Validation
    ///
    /// Returns an error if the configuration is invalid:
    /// - `die_with_parent(false)` with a piped redirector
    ///   ([`MergedOutput`], [`SplitOutput`])
    /// - `die_with_parent(true)` with [`DirectFileOutput`]
    /// - [`DirectFileOutput::new(None, None)`]
    /// - [`DirectFileOutput::new`] with identical stdout/stderr paths
    /// - Environment variable names that are empty, contain `=`, or
    ///   contain NUL
    pub async fn spawn(self, shutdown: watch::Receiver<()>) -> io::Result<ExecutorHandle> {
        let spawn_start = std::time::Instant::now();

        // Validate env vars
        validate_env_vars(&self.env_vars)?;

        // Validate redirector vs die_with_parent
        let mut redirector = self.output.unwrap_or_else(|| Box::new(DevNullOutput));
        redirector.validate(self.die_with_parent)?;

        let t_setup = std::time::Instant::now();
        let (stdout_stdio, stderr_stdio) = redirector.setup()?;
        let setup_us = t_setup.elapsed().as_micros();

        let program = normalize_spawn_path(Path::new(&self.program));
        let mut cmd = Command::new(&program);
        cmd.args(&self.args);

        apply_env(&mut cmd, self.propagate_env, &self.env_vars, |key| {
            std::env::var(key).ok()
        });

        if let Some(ref dir) = self.work_dir {
            cmd.current_dir(normalize_spawn_path(dir));
        }

        cmd.stdout(stdout_stdio);
        cmd.stderr(stderr_stdio);

        #[cfg(unix)]
        {
            let detach = !self.die_with_parent;
            unsafe {
                cmd.pre_exec(move || {
                    if detach {
                        // New session (detaches from terminal and creates a
                        // new process group).
                        let _ = nix::unistd::setsid();
                    } else {
                        // New process group only — allows us to signal all
                        // descendants with kill(-pgid, sig).
                        let _ = nix::unistd::setpgid(
                            nix::unistd::Pid::from_raw(0),
                            nix::unistd::Pid::from_raw(0),
                        );
                    }
                    Ok(())
                });
            }
        }

        cmd.kill_on_drop(self.die_with_parent);

        let t_fork = std::time::Instant::now();
        let mut child = cmd.spawn().map_err(|e| {
            warn!(
                program = %program.display(),
                args = ?self.args,
                work_dir = ?self.work_dir,
                error = %e,
                "Failed to spawn process",
            );
            e
        })?;
        let fork_us = t_fork.elapsed().as_micros();
        // Never defaulted. Every kill path signals the process *group*
        // led by this pid, and `kill(-0, ...)` is POSIX for "every process
        // in the sender's own group" — pod-runner included, since km
        // spawns it as its own group leader. A child we cannot address is
        // a child we cannot manage, so it is dropped (and killed with the
        // handle) rather than tracked under a number that would turn a
        // container kill into a self-kill.
        let pid = child.id().ok_or_else(|| {
            io::Error::other(format!(
                "spawned process has no pid, it was already reaped: {}",
                program.display()
            ))
        })?;

        // Create a span that tags all logs from this process (spawn, exit,
        // pump errors, kill) with the program and pid. Child tasks inherit
        // this context via .instrument().
        let proc_span = debug_span!("process", program = self.program.as_str(), pid);
        let _enter = proc_span.enter();

        debug!(
            work_dir = ?self.work_dir, die_with_parent = self.die_with_parent,
            setup_us, fork_us, total_us = spawn_start.elapsed().as_micros(),
            "Spawned",
        );

        // Write PID tracking file
        let pid_file = if let Some(ref pid_dir) = self.pid_dir {
            match write_pid_file(pid_dir, pid, &self.program) {
                Ok(path) => Some(path),
                Err(e) => {
                    warn!(error = %e, "Failed to write PID file");
                    None
                }
            }
        } else {
            None
        };

        let stdout = child.stdout.take();
        let stderr = child.stderr.take();
        let io_handle = redirector.start(stdout, stderr, shutdown);

        // Collect the I/O pump task into a JoinSet so it is aborted
        // when ExecutorHandle is dropped. Without this, pump tasks would
        // be orphaned if the handle is dropped before the process exits.
        let mut io_tasks = JoinSet::new();
        io_tasks.spawn(async move {
            let _ = io_handle.await;
        });

        let wait_span = proc_span.clone();
        let wait_handle = tokio::spawn(
            async move {
                let status = child.wait().await;
                let exit_code = match &status {
                    Ok(s) => s.code().unwrap_or(-1),
                    Err(_) => -1,
                };
                if exit_code == 0 {
                    debug!("Exited (exit_code=0)");
                } else {
                    debug!("Exited (exit_code={})", exit_code);
                }
                // Remove PID file on exit
                if let Some(ref path) = pid_file {
                    let _ = std::fs::remove_file(path);
                    debug!("PID file removed");
                }
                exit_code
            }
            .instrument(wait_span),
        );

        Ok(ExecutorHandle {
            pid,
            wait_handle,
            cached_exit_code: None,
            io_tasks,
        })
    }
}

// ---------------------------------------------------------------------------
// Spawn path normalization
// ---------------------------------------------------------------------------

/// Normalize a path for use as a `CreateProcess` program or working directory.
///
/// On Windows a path that mixes `\` (from a native container root) with `/`
/// (from a pod spec's forward-slash command/path) is rejected with os error
/// 123, and the `/` also prevents `dunce::simplified` from stripping a `\\?\`
/// verbatim prefix. Converting `/` to `\` first makes the path simplifiable and
/// acceptable to `CreateProcess`. No-op on other platforms.
#[cfg(windows)]
fn normalize_spawn_path(path: &Path) -> PathBuf {
    let native: PathBuf = path.to_string_lossy().replace('/', "\\").into();
    let resolved = dunce::simplified(&native).to_path_buf();
    debug!(input = %path.display(), output = %resolved.display(), "Normalized spawn path.");
    resolved
}

#[cfg(not(windows))]
fn normalize_spawn_path(path: &Path) -> PathBuf {
    let resolved = dunce::simplified(path).to_path_buf();
    debug!(input = %path.display(), output = %resolved.display(), "Normalized spawn path.");
    resolved
}

// ---------------------------------------------------------------------------
// PID file management
// ---------------------------------------------------------------------------

/// Write a PID tracking file to `<pid_dir>/<pid>` with the program name
/// as content. Creates `pid_dir` if it doesn't exist.
fn write_pid_file(pid_dir: &Path, pid: u32, program: &str) -> io::Result<PathBuf> {
    std::fs::create_dir_all(pid_dir)?;
    let path = pid_dir.join(pid.to_string());
    std::fs::write(&path, program)?;
    debug!(pid, path = ?path, "PID file written");
    Ok(path)
}

/// Get the command name for a running process by PID.
///
/// Returns `None` if the process doesn't exist or the command can't be
/// determined.
fn get_pid_command(pid: u32) -> Option<String> {
    #[cfg(unix)]
    {
        let output = std::process::Command::new("ps")
            .args(["-p", &pid.to_string(), "-o", "comm="])
            .output()
            .ok()?;
        if output.status.success() {
            let cmd = String::from_utf8_lossy(&output.stdout).trim().to_string();
            if cmd.is_empty() { None } else { Some(cmd) }
        } else {
            None
        }
    }
    #[cfg(windows)]
    {
        let output = std::process::Command::new("tasklist")
            .args(["/FI", &format!("PID eq {}", pid), "/FO", "CSV", "/NH"])
            .output()
            .ok()?;
        if output.status.success() {
            let text = String::from_utf8_lossy(&output.stdout);
            // CSV format: "image_name","pid",...
            text.split(',')
                .next()
                .map(|s| s.trim().trim_matches('"').to_string())
                .filter(|s| !s.is_empty() && s != "INFO:")
        } else {
            None
        }
    }
}

/// Scan the PID directory for stale PID files from a previous run.
///
/// For each `<pid>` file:
/// 1. Read the expected program name from file content
/// 2. Check if a process with that PID is still running
/// 3. If running the same command, kill it (SIGKILL)
/// 4. Remove the PID file
///
/// Call this once at startup before spawning new processes.
///
/// Returns the number of stale processes killed.
pub fn cleanup_stale_pids(pid_dir: &Path) -> u32 {
    let _span = debug_span!("cleanup_stale_pids", dir = ?pid_dir).entered();

    let entries = match std::fs::read_dir(pid_dir) {
        Ok(rd) => rd,
        Err(_) => return 0,
    };

    let mut killed: u32 = 0;

    for entry in entries.filter_map(|e| e.ok()) {
        let path = entry.path();
        if !path.is_file() {
            continue;
        }

        let filename = match path.file_name().and_then(|n| n.to_str()) {
            Some(n) => n,
            None => continue,
        };

        let pid: u32 = match filename.parse() {
            Ok(p) => p,
            Err(_) => continue,
        };

        let expected_program = match std::fs::read_to_string(&path) {
            Ok(s) => s,
            Err(_) => {
                let _ = std::fs::remove_file(&path);
                continue;
            }
        };

        if is_pid_alive(pid) {
            // Check if the running process matches the expected command
            let matches = get_pid_command(pid)
                .map(|cmd| {
                    // Compare basenames: the PID file may store a full path
                    // while ps returns just the command name, or vice versa.
                    let expected_base = Path::new(expected_program.trim())
                        .file_name()
                        .unwrap_or_default()
                        .to_string_lossy();
                    let running_base = Path::new(cmd.trim())
                        .file_name()
                        .unwrap_or_default()
                        .to_string_lossy();
                    expected_base == running_base
                })
                .unwrap_or(false);

            if matches {
                debug!(
                    pid,
                    program = expected_program.trim(),
                    "Killing stale process"
                );
                force_kill(pid);
                killed += 1;
            } else {
                debug!(pid, "PID reused by different process, skipping kill");
            }
        }

        let _ = std::fs::remove_file(&path);
        debug!(pid, "Stale PID file removed");
    }

    killed
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[cfg(unix)] // All process executor tests use Unix shell commands (sh, echo, sleep, etc.)
mod tests {
    use super::*;
    use std::fs;
    use std::time::Duration;

    use serial_test::serial;

    use crate::testutils::test_dir;

    /// Create a shutdown receiver for tests that don't need to trigger shutdown.
    /// The sender is leaked to prevent `changed()` from returning immediately.
    fn shutdown() -> watch::Receiver<()> {
        let (tx, rx) = watch::channel(());
        std::mem::forget(tx);
        rx
    }

    // -- basic spawn and wait ------------------------------------------------

    #[tokio::test]
    async fn test_execute_echo() {
        let mut handle = ProcessExecutor::new("echo")
            .arg("hello")
            .output(DevNullOutput)
            .spawn(shutdown())
            .await
            .unwrap();

        let code = handle.wait().await.unwrap();
        assert_eq!(code, 0);
    }

    #[tokio::test]
    async fn test_execute_exit_code() {
        let mut handle = ProcessExecutor::new("sh")
            .args(["-c", "exit 42"])
            .spawn(shutdown())
            .await
            .unwrap();

        let code = handle.wait().await.unwrap();
        assert_eq!(code, 42);
    }

    #[tokio::test]
    async fn test_pid_is_nonzero() {
        let mut handle = ProcessExecutor::new("echo")
            .arg("hi")
            .spawn(shutdown())
            .await
            .unwrap();

        assert!(handle.pid() > 0);
        let _ = handle.wait().await;
    }

    // -- wait caching --------------------------------------------------------

    #[tokio::test]
    async fn test_wait_returns_cached_value() {
        let mut handle = ProcessExecutor::new("sh")
            .args(["-c", "exit 7"])
            .spawn(shutdown())
            .await
            .unwrap();

        let first = handle.wait().await.unwrap();
        let second = handle.wait().await.unwrap();
        assert_eq!(first, 7);
        assert_eq!(second, 7);
    }

    // -- try_wait ------------------------------------------------------------

    #[tokio::test]
    async fn test_try_wait_none_while_running() {
        let mut handle = ProcessExecutor::new("sleep")
            .arg("10")
            .spawn(shutdown())
            .await
            .unwrap();

        assert!(handle.try_wait().is_none());
        handle.kill(Duration::ZERO).await;
    }

    #[tokio::test]
    async fn test_try_wait_some_after_exit() {
        let mut handle = ProcessExecutor::new("true")
            .spawn(shutdown())
            .await
            .unwrap();

        // Wait for process to finish
        let _ = handle.wait().await;
        assert_eq!(handle.try_wait(), Some(0));
    }

    // -- kill ----------------------------------------------------------------

    #[tokio::test]
    async fn test_kill_with_grace_period() {
        let mut handle = ProcessExecutor::new("sleep")
            .arg("60")
            .spawn(shutdown())
            .await
            .unwrap();

        tokio::time::sleep(Duration::from_millis(200)).await;
        handle.kill(Duration::from_secs(1)).await;
        let code = handle.wait().await.unwrap();
        assert_ne!(code, 0);
    }

    #[tokio::test]
    async fn test_kill_immediate() {
        let mut handle = ProcessExecutor::new("sleep")
            .arg("60")
            .spawn(shutdown())
            .await
            .unwrap();

        tokio::time::sleep(Duration::from_millis(200)).await;
        handle.kill(Duration::ZERO).await;
        let code = handle.wait().await.unwrap();
        assert_ne!(code, 0);
    }

    #[tokio::test]
    async fn test_kill_already_exited_is_noop() {
        let mut handle = ProcessExecutor::new("true")
            .spawn(shutdown())
            .await
            .unwrap();

        let _ = handle.wait().await;
        // Should not panic or error
        handle.kill(Duration::from_secs(1)).await;
    }

    #[tokio::test]
    async fn test_has_exited_follows_the_handle_not_the_pid() {
        let mut handle = ProcessExecutor::new("sh")
            .args(["-c", "sleep 0.3"])
            .spawn(shutdown())
            .await
            .unwrap();

        assert!(!handle.has_exited(), "a running child has not exited");
        assert_eq!(handle.wait().await.unwrap(), 0);
        // Reaped: from here the pid is the operating system's to reuse, so
        // no kill path may address it again.
        assert!(handle.has_exited(), "a reaped child has exited");
    }

    #[tokio::test]
    async fn test_kill_after_reaping_signals_nothing() {
        // The child leads a process group and leaves a member of it behind,
        // so a group signal is observable after the child itself is gone.
        // Once the handle is reaped the pid is no longer provably ours, and
        // a `kill(-pid, ...)` aimed at it is exactly the misfire that can
        // land on an unrelated process once the number is recycled. The
        // survivor stands in for that stranger.
        let dir = test_dir!("kill-after-reap");
        let pidfile = dir.join("survivor.pid");
        let mut handle = ProcessExecutor::new("sh")
            .args(["-c", &format!("sleep 30 & echo $! > {}", pidfile.display())])
            .spawn(shutdown())
            .await
            .unwrap();

        assert_eq!(handle.wait().await.unwrap(), 0);
        let survivor: u32 = fs::read_to_string(&pidfile)
            .unwrap()
            .trim()
            .parse()
            .unwrap();
        assert!(is_pid_alive(survivor), "the group member should be up");

        handle.kill(Duration::from_millis(200)).await;
        handle.send_sigterm();

        assert!(
            is_pid_alive(survivor),
            "a reaped handle must signal nothing, not even its own old group",
        );
        force_kill(survivor);
    }

    #[tokio::test]
    async fn test_kill_after_reaping_keeps_the_exit_code() {
        let mut handle = ProcessExecutor::new("sh")
            .args(["-c", "exit 7"])
            .spawn(shutdown())
            .await
            .unwrap();

        assert_eq!(handle.wait().await.unwrap(), 7);
        handle.kill(Duration::from_secs(1)).await;
        // The kill is a no-op, so what the child actually reported must
        // survive it — callers report this code as the container's status.
        assert_eq!(handle.try_wait(), Some(7));
        assert_eq!(handle.wait().await.unwrap(), 7);
    }

    #[tokio::test]
    async fn test_send_sigterm_reaches_a_live_process_group() {
        // The mirror of the test above: gating on the handle must not stop
        // a kill that is still owed.
        let dir = test_dir!("sigterm-live-group");
        let pidfile = dir.join("member.pid");
        let mut handle = ProcessExecutor::new("sh")
            .args([
                "-c",
                &format!("sleep 30 & echo $! > {}; sleep 30", pidfile.display()),
            ])
            .spawn(shutdown())
            .await
            .unwrap();

        // Wait for the group member to exist before signalling.
        let mut member = None;
        for _ in 0..100 {
            if let Ok(text) = fs::read_to_string(&pidfile)
                && let Ok(pid) = text.trim().parse::<u32>()
            {
                member = Some(pid);
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let member = member.expect("group member should have recorded its pid");

        handle.kill(Duration::ZERO).await;
        let _ = handle.wait().await;

        for _ in 0..100 {
            if !is_pid_alive(member) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(
            !is_pid_alive(member),
            "a live handle must still take its whole process group down",
        );
    }

    #[tokio::test]
    async fn test_kill_after_cancelled_wait_is_noop() {
        // `wait` caches the exit code before draining the I/O pumps, so a
        // cancellation in that window leaves the code cached and the join
        // handle consumed — the exact state behind the field crashes. Both
        // kill paths must read the cache instead of polling the handle.
        // (Ported from upstream TDKM-139; the gating it exercises is ours.)
        let mut handle = ProcessExecutor::new("true")
            .spawn(shutdown())
            .await
            .unwrap();

        loop {
            tokio::select! {
                biased;
                _ = handle.wait() => {}
                _ = std::future::ready(()) => {}
            }
            if handle.cached_exit_code.is_some() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        handle.kill(Duration::from_secs(1)).await;
        handle.send_sigterm();
        assert_eq!(handle.try_wait(), Some(0));
    }

    // -- monitor failure (JoinError) -------------------------------------

    #[tokio::test]
    async fn test_try_wait_survives_a_dead_monitor() {
        let mut handle = ProcessExecutor::new("sh")
            .args(["-c", "sleep 30"])
            .spawn(shutdown())
            .await
            .unwrap();

        handle.abort_monitor();

        // The first try_wait after the abort consumes the handle on the
        // JoinError arm. Without caching there, the *second* call polls a
        // consumed JoinHandle — the exact "JoinHandle polled after
        // completion" panic that aborted pod-runner in the field.
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        let code = loop {
            if let Some(code) = handle.try_wait() {
                break code;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "aborted monitor should read as finished"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        };
        assert_eq!(code, -1, "an unreportable exit reads as -1");
        assert_eq!(handle.try_wait(), Some(-1), "and stays settled");
        assert!(handle.has_exited());
    }

    #[tokio::test]
    async fn test_every_entry_point_settles_after_a_dead_monitor() {
        let mut handle = ProcessExecutor::new("sh")
            .args(["-c", "sleep 30"])
            .spawn(shutdown())
            .await
            .unwrap();

        handle.abort_monitor();

        // The failure is reported once...
        assert!(handle.wait().await.is_err());
        // ...and from then on every path must resolve from the cache
        // rather than touching the consumed handle: each of these lines
        // would abort the process without the caching.
        assert_eq!(handle.try_wait(), Some(-1));
        handle.kill(Duration::from_secs(1)).await;
        handle.send_sigterm();
        assert_eq!(handle.wait().await.unwrap(), -1);
    }

    // -- cancel-safe wait (select!) ------------------------------------------

    #[tokio::test]
    async fn test_wait_cancel_safe_in_select() {
        let mut handle = ProcessExecutor::new("sh")
            .args(["-c", "sleep 0.2 && exit 3"])
            .spawn(shutdown())
            .await
            .unwrap();

        // First select: timeout wins, wait is cancelled
        tokio::select! {
            _ = handle.wait() => { panic!("should not complete yet"); }
            _ = tokio::time::sleep(Duration::from_millis(50)) => {}
        }

        // Handle is still valid — wait again
        let code = handle.wait().await.unwrap();
        assert_eq!(code, 3);
    }

    // -- env setup (no global state touched) ---------------------------------

    /// A `TD_`-prefixed variable deliberately *not* on the allowlist, to
    /// pin that the carve-out is the list and not the prefix.
    const DECOY_VAR: &str = "TD_KM_NOT_A_TOKEN";

    /// The variables `apply_env` set on a command, as owned strings.
    fn envs_of(cmd: &Command) -> Vec<(String, Option<String>)> {
        cmd.as_std()
            .get_envs()
            .map(|(k, v)| {
                (
                    k.to_string_lossy().into_owned(),
                    v.map(|v| v.to_string_lossy().into_owned()),
                )
            })
            .collect()
    }

    /// A stand-in for the spawning process's environment.
    fn fake_env(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> + use<> {
        let owned: Vec<(String, String)> = pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        move |key| owned.iter().find(|(k, _)| k == key).map(|(_, v)| v.clone())
    }

    #[test]
    fn apply_env_carries_an_inherited_kill_token_through_a_clear() {
        let mut cmd = Command::new("true");
        apply_env(
            &mut cmd,
            false,
            &HashMap::new(),
            fake_env(&[(kcommon::token::KM_KILL_TOKEN_VAR, "s/4711@x/km")]),
        );
        assert_eq!(
            envs_of(&cmd),
            vec![(
                kcommon::token::KM_KILL_TOKEN_VAR.to_string(),
                Some("s/4711@x/km".to_string())
            )],
        );
    }

    #[test]
    fn apply_env_carries_nothing_when_no_token_is_inherited() {
        let mut cmd = Command::new("true");
        apply_env(&mut cmd, false, &HashMap::new(), fake_env(&[]));
        assert!(envs_of(&cmd).is_empty(), "{:?}", envs_of(&cmd));
    }

    #[test]
    fn apply_env_does_not_carry_a_variable_outside_the_allowlist() {
        let mut cmd = Command::new("true");
        apply_env(
            &mut cmd,
            false,
            &HashMap::new(),
            fake_env(&[(DECOY_VAR, "should stay behind")]),
        );
        // `propagate_env: false` would stop meaning what it says if a
        // neighbouring TD_ variable came along for the ride.
        assert!(envs_of(&cmd).is_empty(), "{:?}", envs_of(&cmd));
    }

    #[test]
    fn apply_env_lets_an_explicit_value_win_over_a_carried_token() {
        let mut explicit = HashMap::new();
        explicit.insert(
            kcommon::token::PR_KILL_TOKEN_VAR.to_string(),
            "mine/9999@y/pod/c".to_string(),
        );

        let mut cmd = Command::new("true");
        apply_env(
            &mut cmd,
            false,
            &explicit,
            fake_env(&[(kcommon::token::PR_KILL_TOKEN_VAR, "inherited/1@z/pod/c")]),
        );

        // The carry is applied first precisely so the token pod-runner
        // sets on a container is not overwritten by an inherited one.
        let envs = envs_of(&cmd);
        assert_eq!(
            envs.iter()
                .find(|(k, _)| k == kcommon::token::PR_KILL_TOKEN_VAR)
                .unwrap()
                .1,
            Some("mine/9999@y/pod/c".to_string()),
            "{:?}",
            envs
        );
    }

    #[test]
    fn apply_env_carries_nothing_when_the_environment_is_propagated() {
        let mut cmd = Command::new("true");
        apply_env(
            &mut cmd,
            true,
            &HashMap::new(),
            fake_env(&[(kcommon::token::KM_KILL_TOKEN_VAR, "s/4711@x/km")]),
        );
        // Nothing to carry: the token arrives with everything else.
        assert!(envs_of(&cmd).is_empty(), "{:?}", envs_of(&cmd));
    }

    // -- env vars ------------------------------------------------------------

    #[tokio::test]
    async fn test_env_vars_passed_to_child() {
        let dir = test_dir!();
        let out = dir.join("out.log");

        let mut handle = ProcessExecutor::new("sh")
            .args(["-c", "echo $MY_VAR"])
            .env("MY_VAR", "hello_from_test")
            .output(MergedOutput::new(FileStream::new(&out)))
            .spawn(shutdown())
            .await
            .unwrap();

        let code = handle.wait().await.unwrap();
        assert_eq!(code, 0);
        let content = fs::read_to_string(&out).unwrap();
        assert!(content.contains("hello_from_test"));
    }

    #[tokio::test]
    async fn test_envs_multiple() {
        let dir = test_dir!();
        let out = dir.join("out.log");

        let mut handle = ProcessExecutor::new("sh")
            .args(["-c", "echo $A $B"])
            .envs([("A", "aaa"), ("B", "bbb")])
            .output(MergedOutput::new(FileStream::new(&out)))
            .spawn(shutdown())
            .await
            .unwrap();

        let code = handle.wait().await.unwrap();
        assert_eq!(code, 0);
        let content = fs::read_to_string(&out).unwrap();
        assert!(content.contains("aaa"));
        assert!(content.contains("bbb"));
    }

    #[tokio::test]
    async fn test_propagate_env_false() {
        let dir = test_dir!();
        let out = dir.join("out.log");

        // HOME should be absent when propagate_env is false
        let mut handle = ProcessExecutor::new("sh")
            .args(["-c", "echo \"HOME=${HOME}END\""])
            .propagate_env(false)
            .output(MergedOutput::new(FileStream::new(&out)))
            .spawn(shutdown())
            .await
            .unwrap();

        let code = handle.wait().await.unwrap();
        assert_eq!(code, 0);
        let content = fs::read_to_string(&out).unwrap();
        assert!(
            content.contains("HOME=END"),
            "HOME should be empty, got: {}",
            content
        );
    }

    #[tokio::test]
    async fn test_empty_value_env_var_is_set_not_unset() {
        // Mirrors the EMPTY_SECRET_ENVFROM_BUG.md probe (set-vs-unset), but
        // via the POSIX `${VAR+SET}` builtin so no PATH lookup is needed.
        // An empty-valued env var must reach the child as *set-but-empty*.
        let dir = test_dir!();
        let out = dir.join("out.log");

        let mut handle = ProcessExecutor::new("sh")
            .args(["-c", "echo \"R=${EMPTY_VAR+SET}=${EMPTY_VAR}END\""])
            .env("EMPTY_VAR", "")
            .propagate_env(false)
            .output(MergedOutput::new(FileStream::new(&out)))
            .spawn(shutdown())
            .await
            .unwrap();

        let code = handle.wait().await.unwrap();
        assert_eq!(code, 0);
        let content = fs::read_to_string(&out).unwrap();
        // "R=SET=END" => the var is present (SET) and empty. A dropped var
        // would print "R==END".
        assert!(
            content.contains("R=SET=END"),
            "empty-valued env var must be delivered as set-but-empty, got: {}",
            content
        );
    }

    // -- env var validation --------------------------------------------------

    #[tokio::test]
    async fn test_env_var_empty_name_rejected() {
        let result = ProcessExecutor::new("true")
            .env("", "val")
            .spawn(shutdown())
            .await;

        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("empty"), "{}", msg);
    }

    #[tokio::test]
    async fn test_env_var_equals_in_name_rejected() {
        let result = ProcessExecutor::new("true")
            .env("FOO=BAR", "val")
            .spawn(shutdown())
            .await;

        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("'='"), "{}", msg);
    }

    #[tokio::test]
    async fn test_env_var_nul_in_name_rejected() {
        let result = ProcessExecutor::new("true")
            .env("FOO\0BAR", "val")
            .spawn(shutdown())
            .await;

        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("NUL"), "{}", msg);
    }

    // -- work_dir ------------------------------------------------------------

    #[tokio::test]
    async fn test_work_dir() {
        let dir = test_dir!();
        let out = dir.join("out.log");

        let mut handle = ProcessExecutor::new("sh")
            .args(["-c", "pwd"])
            .work_dir(&dir)
            .output(MergedOutput::new(FileStream::new(&out)))
            .spawn(shutdown())
            .await
            .unwrap();

        let code = handle.wait().await.unwrap();
        assert_eq!(code, 0);
        let content = fs::read_to_string(&out).unwrap().trim().to_string();
        // Resolve symlinks for macOS /private/var vs /var, etc.
        let expected = fs::canonicalize(&dir).unwrap();
        let actual = fs::canonicalize(&content).unwrap();
        assert_eq!(actual, expected);
    }

    // -- DevNullOutput -------------------------------------------------------

    #[tokio::test]
    async fn test_devnull_output() {
        let mut handle = ProcessExecutor::new("echo")
            .arg("should be discarded")
            .output(DevNullOutput)
            .spawn(shutdown())
            .await
            .unwrap();

        assert_eq!(handle.wait().await.unwrap(), 0);
    }

    // -- MergedOutput + FileStream -------------------------------------------

    #[tokio::test]
    async fn test_merged_output_captures_stdout_and_stderr() {
        let dir = test_dir!();
        let out = dir.join("merged.log");

        let mut handle = ProcessExecutor::new("sh")
            .args(["-c", "echo out_line && echo err_line >&2"])
            .output(MergedOutput::new(FileStream::new(&out)))
            .spawn(shutdown())
            .await
            .unwrap();

        assert_eq!(handle.wait().await.unwrap(), 0);
        let content = fs::read_to_string(&out).unwrap();
        assert!(content.contains("out_line"), "missing stdout: {}", content);
        assert!(content.contains("err_line"), "missing stderr: {}", content);
    }

    // Regression: with both streams producing heavy interleaved output,
    // earlier versions raced two `read_line` futures in a `tokio::select!`.
    // `read_line` is not cancel-safe, so the losing branch silently lost
    // bytes on every line.
    //
    // Each line is written as two halves with the *other* stream's write
    // in between (`printf` is unbuffered for fd != tty). That forces
    // multi-poll `read_line` calls and gives the select! a chance to
    // cancel a partially-completed read.
    #[tokio::test]
    async fn test_merged_output_no_data_loss_under_interleaved_load() {
        let dir = test_dir!();
        let out = dir.join("merged.log");

        const N: usize = 2000;
        let script = format!(
            "i=1; while [ $i -le {n} ]; do \
                printf 'OUT_%d_' $i; \
                printf 'ERR_%d_part1_' $i >&2; \
                printf 'tail\\n'; \
                printf 'tail\\n' >&2; \
                i=$((i+1)); \
             done",
            n = N
        );

        let mut handle = ProcessExecutor::new("sh")
            .args(["-c", &script])
            .output(MergedOutput::new(FileStream::new(&out)))
            .spawn(shutdown())
            .await
            .unwrap();

        assert_eq!(handle.wait().await.unwrap(), 0);
        let content = fs::read_to_string(&out).unwrap();
        for i in 1..=N {
            assert!(
                content.contains(&format!("OUT_{}_tail\n", i)),
                "missing OUT_{} (data loss?)",
                i
            );
            assert!(
                content.contains(&format!("ERR_{}_part1_tail\n", i)),
                "missing ERR_{} (data loss?)",
                i
            );
        }
    }

    // -- MergedOutput + RollingFileStream ------------------------------------

    #[tokio::test]
    async fn test_merged_rolling_output() {
        let dir = test_dir!("logs");
        let log = dir.join("app.log");

        let mut handle = ProcessExecutor::new("sh")
            .args(["-c", "for i in $(seq 1 10); do echo line_$i; done"])
            .output(MergedOutput::new(RollingFileStream::new(&log, 40, 10)))
            .spawn(shutdown())
            .await
            .unwrap();

        assert_eq!(handle.wait().await.unwrap(), 0);
        // Small delay to let the pump task flush after process exit
        tokio::time::sleep(Duration::from_millis(100)).await;

        // All lines present across current + rolled files (app.log, app.log.1, …)
        let mut all = String::new();
        all.push_str(&fs::read_to_string(&log).unwrap());
        for i in 1..10 {
            let rolled = PathBuf::from(format!("{}.{}", log.display(), i));
            if let Ok(content) = fs::read_to_string(&rolled) {
                all.push_str(&content);
            } else {
                break;
            }
        }
        for i in 1..=10 {
            assert!(all.contains(&format!("line_{}", i)), "missing line_{}", i);
        }
    }

    #[tokio::test]
    async fn test_merged_rolling_respects_max_files() {
        let dir = test_dir!("logs");
        let log = dir.join("app.log");

        let mut handle = ProcessExecutor::new("sh")
            .args(["-c", "for i in $(seq 1 30); do echo line_number_$i; done"])
            .output(MergedOutput::new(RollingFileStream::new(&log, 30, 3)))
            .spawn(shutdown())
            .await
            .unwrap();

        assert_eq!(handle.wait().await.unwrap(), 0);
        // max_files=3 means: app.log, app.log.1, app.log.2
        assert!(log.exists(), "current log should exist");
        let rolled_3 = PathBuf::from(format!("{}.3", log.display()));
        assert!(
            !rolled_3.exists(),
            "app.log.3 should not exist with max_files=3"
        );
    }

    #[tokio::test]
    async fn test_merged_rolling_numbered_names() {
        let dir = test_dir!("logs");
        let log = dir.join("app.log");

        let mut handle = ProcessExecutor::new("sh")
            .args(["-c", "for i in $(seq 1 20); do echo line_number_$i; done"])
            .output(MergedOutput::new(RollingFileStream::new(&log, 30, 10)))
            .spawn(shutdown())
            .await
            .unwrap();

        assert_eq!(handle.wait().await.unwrap(), 0);
        // Current file should exist
        assert!(log.exists());
        // At least one rolled file with .1 suffix
        let rolled_1 = PathBuf::from(format!("{}.1", log.display()));
        assert!(rolled_1.exists(), "app.log.1 should exist after rotation");
        // .1 should be more recent content than .2 (if it exists)
        let rolled_2 = PathBuf::from(format!("{}.2", log.display()));
        if rolled_2.exists() {
            let content_1 = fs::read_to_string(&rolled_1).unwrap();
            let content_2 = fs::read_to_string(&rolled_2).unwrap();
            assert_ne!(
                content_1, content_2,
                ".1 and .2 should have different content"
            );
        }
    }

    // -- SplitOutput ---------------------------------------------------------

    #[tokio::test]
    async fn test_split_output_separates_streams() {
        let dir = test_dir!();
        let stdout_log = dir.join("stdout.log");
        let stderr_log = dir.join("stderr.log");

        let mut handle = ProcessExecutor::new("sh")
            .args(["-c", "echo out_only && echo err_only >&2"])
            .output(SplitOutput::new(
                FileStream::new(&stdout_log),
                FileStream::new(&stderr_log),
            ))
            .spawn(shutdown())
            .await
            .unwrap();

        assert_eq!(handle.wait().await.unwrap(), 0);

        let stdout_content = fs::read_to_string(&stdout_log).unwrap();
        let stderr_content = fs::read_to_string(&stderr_log).unwrap();
        assert!(
            stdout_content.contains("out_only"),
            "stdout: {}",
            stdout_content
        );
        assert!(
            !stdout_content.contains("err_only"),
            "stdout should not have stderr: {}",
            stdout_content
        );
        assert!(
            stderr_content.contains("err_only"),
            "stderr: {}",
            stderr_content
        );
        assert!(
            !stderr_content.contains("out_only"),
            "stderr should not have stdout: {}",
            stderr_content
        );
    }

    #[tokio::test]
    async fn test_split_output_with_devnull_stream() {
        let dir = test_dir!();
        let stderr_log = dir.join("stderr.log");

        let mut handle = ProcessExecutor::new("sh")
            .args(["-c", "echo out_discarded && echo err_kept >&2"])
            .output(SplitOutput::new(
                DevNullStream,
                FileStream::new(&stderr_log),
            ))
            .spawn(shutdown())
            .await
            .unwrap();

        assert_eq!(handle.wait().await.unwrap(), 0);
        let content = fs::read_to_string(&stderr_log).unwrap();
        assert!(content.contains("err_kept"));
    }

    // -- TeeStream -----------------------------------------------------------

    #[tokio::test]
    async fn test_tee_stream_duplicates_output() {
        let dir = test_dir!();
        let a = dir.join("a.log");
        let b = dir.join("b.log");

        let mut handle = ProcessExecutor::new("sh")
            .args(["-c", "echo tee_test_line"])
            .output(MergedOutput::new(TeeStream::new(
                FileStream::new(&a),
                FileStream::new(&b),
            )))
            .spawn(shutdown())
            .await
            .unwrap();

        assert_eq!(handle.wait().await.unwrap(), 0);
        let a_content = fs::read_to_string(&a).unwrap();
        let b_content = fs::read_to_string(&b).unwrap();
        assert!(a_content.contains("tee_test_line"), "a: {}", a_content);
        assert!(b_content.contains("tee_test_line"), "b: {}", b_content);
    }

    #[tokio::test]
    async fn test_tee_with_rolling_and_file() {
        let dir = test_dir!();
        let rolling_dir = dir.join("rolling");
        fs::create_dir_all(&rolling_dir).unwrap();
        let rolling_log = rolling_dir.join("app.log");
        let copy_log = dir.join("copy.log");

        let mut handle = ProcessExecutor::new("sh")
            .args(["-c", "for i in $(seq 1 5); do echo line_$i; done"])
            .output(MergedOutput::new(TeeStream::new(
                RollingFileStream::new(&rolling_log, 50, 10),
                FileStream::new(&copy_log),
            )))
            .spawn(shutdown())
            .await
            .unwrap();

        assert_eq!(handle.wait().await.unwrap(), 0);
        let copy = fs::read_to_string(&copy_log).unwrap();
        for i in 1..=5 {
            assert!(
                copy.contains(&format!("line_{}", i)),
                "copy missing line_{}",
                i
            );
        }
    }

    // -- DirectFileOutput ----------------------------------------------------

    #[tokio::test]
    async fn test_direct_file_merged() {
        let dir = test_dir!();
        let log = dir.join("merged.log");

        let mut handle = ProcessExecutor::new("sh")
            .args(["-c", "echo direct_out && echo direct_err >&2"])
            .die_with_parent(false)
            .output(DirectFileOutput::merged(&log))
            .spawn(shutdown())
            .await
            .unwrap();

        let code = handle.wait().await.unwrap();
        assert_eq!(code, 0);
        // Give OS a moment to flush
        tokio::time::sleep(Duration::from_millis(200)).await;
        let content = fs::read_to_string(&log).unwrap();
        assert!(
            content.contains("direct_out"),
            "missing stdout: {}",
            content
        );
        assert!(
            content.contains("direct_err"),
            "missing stderr: {}",
            content
        );
    }

    #[tokio::test]
    async fn test_direct_file_split() {
        let dir = test_dir!();
        let out = dir.join("stdout.log");
        let err = dir.join("stderr.log");

        let mut handle = ProcessExecutor::new("sh")
            .args(["-c", "echo split_out && echo split_err >&2"])
            .die_with_parent(false)
            .output(DirectFileOutput::new(Some(&out), Some(&err)))
            .spawn(shutdown())
            .await
            .unwrap();

        let code = handle.wait().await.unwrap();
        assert_eq!(code, 0);
        tokio::time::sleep(Duration::from_millis(200)).await;
        let out_content = fs::read_to_string(&out).unwrap();
        let err_content = fs::read_to_string(&err).unwrap();
        assert!(out_content.contains("split_out"));
        assert!(err_content.contains("split_err"));
    }

    #[tokio::test]
    async fn test_direct_file_stdout_only() {
        let dir = test_dir!();
        let out = dir.join("stdout.log");

        let mut handle = ProcessExecutor::new("sh")
            .args(["-c", "echo only_out && echo discard_err >&2"])
            .die_with_parent(false)
            .output(DirectFileOutput::new(Some(&out), None::<PathBuf>))
            .spawn(shutdown())
            .await
            .unwrap();

        assert_eq!(handle.wait().await.unwrap(), 0);
        tokio::time::sleep(Duration::from_millis(200)).await;
        let content = fs::read_to_string(&out).unwrap();
        assert!(content.contains("only_out"));
        assert!(!content.contains("discard_err"));
    }

    #[tokio::test]
    async fn test_direct_file_merged_header_lines_written_first() {
        let dir = test_dir!();
        let log = dir.join("merged.log");

        let mut handle = ProcessExecutor::new("sh")
            .args(["-c", "echo direct_out"])
            .die_with_parent(false)
            .output(
                DirectFileOutput::merged(&log)
                    .with_header_lines(vec!["WARNING: header line".to_string()]),
            )
            .spawn(shutdown())
            .await
            .unwrap();

        assert_eq!(handle.wait().await.unwrap(), 0);
        tokio::time::sleep(Duration::from_millis(200)).await;
        let content = fs::read_to_string(&log).unwrap();
        let mut lines = content.lines();
        assert_eq!(lines.next(), Some("WARNING: header line"));
        assert!(lines.next().unwrap().contains("direct_out"));
    }

    // -- DirectFileOutput validation -----------------------------------------

    #[tokio::test]
    async fn test_direct_file_rejects_die_with_parent_true() {
        let result = ProcessExecutor::new("true")
            .die_with_parent(true)
            .output(DirectFileOutput::merged("/tmp/nope.log"))
            .spawn(shutdown())
            .await;

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("die_with_parent"));
    }

    #[tokio::test]
    async fn test_direct_file_rejects_none_none() {
        let result = ProcessExecutor::new("true")
            .die_with_parent(false)
            .output(DirectFileOutput::new(None::<PathBuf>, None::<PathBuf>))
            .spawn(shutdown())
            .await;

        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("at least one path")
        );
    }

    #[tokio::test]
    async fn test_direct_file_rejects_same_path() {
        let result = ProcessExecutor::new("true")
            .die_with_parent(false)
            .output(DirectFileOutput::new(
                Some("/tmp/same.log"),
                Some("/tmp/same.log"),
            ))
            .spawn(shutdown())
            .await;

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("must differ"));
    }

    // -- MergedOutput / SplitOutput validation -------------------------------

    #[tokio::test]
    async fn test_merged_rejects_die_with_parent_false() {
        let result = ProcessExecutor::new("true")
            .die_with_parent(false)
            .output(MergedOutput::new(DevNullStream))
            .spawn(shutdown())
            .await;

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("die_with_parent"));
    }

    #[tokio::test]
    async fn test_split_rejects_die_with_parent_false() {
        let result = ProcessExecutor::new("true")
            .die_with_parent(false)
            .output(SplitOutput::new(DevNullStream, DevNullStream))
            .spawn(shutdown())
            .await;

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("die_with_parent"));
    }

    // -- shutdown signal -----------------------------------------------------

    #[tokio::test]
    async fn test_shutdown_stops_merged_pump() {
        let dir = test_dir!();
        let log = dir.join("shutdown.log");
        let (tx, rx) = watch::channel(());

        let mut handle = ProcessExecutor::new("sh")
            .args(["-c", "while true; do echo tick; sleep 0.1; done"])
            .output(MergedOutput::new(FileStream::new(&log)))
            .spawn(rx)
            .await
            .unwrap();

        // Let it run for a bit
        tokio::time::sleep(Duration::from_millis(500)).await;
        tx.send(()).unwrap();

        // Pump should exit quickly after shutdown
        tokio::time::sleep(Duration::from_millis(300)).await;

        // File should have some content (was writing before shutdown)
        let content = fs::read_to_string(&log).unwrap();
        assert!(
            content.contains("tick"),
            "expected output before shutdown: {}",
            content
        );

        handle.kill(Duration::ZERO).await;
    }

    #[tokio::test]
    async fn test_shutdown_stops_split_pumps() {
        let dir = test_dir!();
        let out = dir.join("stdout.log");
        let err = dir.join("stderr.log");
        let (tx, rx) = watch::channel(());

        let mut handle = ProcessExecutor::new("sh")
            .args([
                "-c",
                "while true; do echo out_tick; echo err_tick >&2; sleep 0.1; done",
            ])
            .output(SplitOutput::new(
                FileStream::new(&out),
                FileStream::new(&err),
            ))
            .spawn(rx)
            .await
            .unwrap();

        tokio::time::sleep(Duration::from_millis(500)).await;
        tx.send(()).unwrap();
        tokio::time::sleep(Duration::from_millis(300)).await;

        let out_content = fs::read_to_string(&out).unwrap();
        let err_content = fs::read_to_string(&err).unwrap();
        assert!(out_content.contains("out_tick"));
        assert!(err_content.contains("err_tick"));

        handle.kill(Duration::ZERO).await;
    }

    // -- default output is DevNull -------------------------------------------

    #[tokio::test]
    async fn test_default_output_is_devnull() {
        let mut handle = ProcessExecutor::new("echo")
            .arg("no output config")
            .spawn(shutdown())
            .await
            .unwrap();

        assert_eq!(handle.wait().await.unwrap(), 0);
    }

    // -- die_with_parent(false) + DevNullOutput ------------------------------

    #[tokio::test]
    async fn test_detached_with_devnull() {
        let mut handle = ProcessExecutor::new("sh")
            .args(["-c", "exit 0"])
            .die_with_parent(false)
            .output(DevNullOutput)
            .spawn(shutdown())
            .await
            .unwrap();

        let code = handle.wait().await.unwrap();
        assert_eq!(code, 0);
    }

    // -- nested Tee composition ----------------------------------------------

    #[tokio::test]
    async fn test_nested_tee_three_targets() {
        let dir = test_dir!();
        let a = dir.join("a.log");
        let b = dir.join("b.log");
        let c = dir.join("c.log");

        let mut handle = ProcessExecutor::new("echo")
            .arg("triple")
            .output(MergedOutput::new(TeeStream::new(
                FileStream::new(&a),
                TeeStream::new(FileStream::new(&b), FileStream::new(&c)),
            )))
            .spawn(shutdown())
            .await
            .unwrap();

        assert_eq!(handle.wait().await.unwrap(), 0);
        for (label, path) in [("a", &a), ("b", &b), ("c", &c)] {
            let content = fs::read_to_string(path).unwrap();
            assert!(
                content.contains("triple"),
                "{} missing content: {}",
                label,
                content
            );
        }
    }

    // ========================================================================
    // RollingStreamWriter — direct unit tests
    // ========================================================================

    /// Helper: open a RollingFileStream as a Box<dyn StreamWriter>.
    fn open_rolling(
        path: &std::path::Path,
        max_size: u64,
        max_files: usize,
    ) -> Box<dyn StreamWriter> {
        Box::new(RollingFileStream::new(path, max_size, max_files))
            .open()
            .unwrap()
    }

    /// Helper: count files in a directory.
    fn count_files(dir: &std::path::Path) -> usize {
        fs::read_dir(dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().is_file())
            .count()
    }

    #[test]
    fn test_rolling_no_rotation_when_max_size_zero() {
        let dir = test_dir!("logs");
        let log = dir.join("app.log");
        let mut w = open_rolling(&log, 0, 5);
        for _ in 0..100 {
            w.write_line(
                b"this is a long line that would trigger rotation\n",
                LogStream::Stdout,
            )
            .unwrap();
        }
        w.flush().unwrap();
        // Only the current file should exist
        assert_eq!(count_files(&dir), 1);
        assert!(log.exists());
    }

    #[test]
    fn test_rolling_max_files_one() {
        // max_files=1 means only the current file, no rolled files kept
        let dir = test_dir!("logs");
        let log = dir.join("app.log");
        let mut w = open_rolling(&log, 20, 1);
        w.write_line(b"aaaaaaaaaaaaaaaaaaa\n", LogStream::Stdout)
            .unwrap(); // triggers rotation
        w.write_line(b"bbbbbbbbbbbbbbbbbbb\n", LogStream::Stdout)
            .unwrap(); // triggers again
        w.write_line(b"final\n", LogStream::Stdout).unwrap();
        w.flush().unwrap();
        // Only current file should exist (rolled files deleted)
        assert_eq!(count_files(&dir), 1);
        let content = fs::read_to_string(&log).unwrap();
        assert!(content.contains("final"));
    }

    #[test]
    fn test_rolling_basic_rotation() {
        let dir = test_dir!("logs");
        let log = dir.join("app.log");
        let mut w = open_rolling(&log, 10, 5);
        w.write_line(b"aaaaaaaaaa\n", LogStream::Stdout).unwrap(); // >=10 bytes, triggers rotation
        w.write_line(b"current\n", LogStream::Stdout).unwrap();
        w.flush().unwrap();
        // app.log has "current", app.log.1 has "aaaaaaaaaa"
        assert!(log.exists());
        let rolled_1 = dir.join("app.log.1");
        assert!(rolled_1.exists(), "app.log.1 should exist");
        let content = fs::read_to_string(&log).unwrap();
        assert!(content.contains("current"));
        let rolled_content = fs::read_to_string(&rolled_1).unwrap();
        assert!(rolled_content.contains("aaaaaaaaaa"));
    }

    #[test]
    fn test_rolling_shift_order() {
        // Verify .1 → .2, .2 → .3 shift during rotation
        let dir = test_dir!("logs");
        let log = dir.join("app.log");
        let mut w = open_rolling(&log, 10, 10);
        w.write_line(b"first_____\n", LogStream::Stdout).unwrap(); // rotation 1
        w.write_line(b"second____\n", LogStream::Stdout).unwrap(); // rotation 2
        w.write_line(b"third_____\n", LogStream::Stdout).unwrap(); // rotation 3
        w.write_line(b"current\n", LogStream::Stdout).unwrap();
        w.flush().unwrap();

        let current = fs::read_to_string(&log).unwrap();
        let r1 = fs::read_to_string(dir.join("app.log.1")).unwrap();
        let r2 = fs::read_to_string(dir.join("app.log.2")).unwrap();
        let r3 = fs::read_to_string(dir.join("app.log.3")).unwrap();
        assert!(current.contains("current"));
        assert!(
            r1.contains("third"),
            ".1 should have most recent rolled: {}",
            r1
        );
        assert!(
            r2.contains("second"),
            ".2 should have second most recent: {}",
            r2
        );
        assert!(r3.contains("first"), ".3 should have oldest: {}", r3);
    }

    #[test]
    fn test_rolling_deletes_oldest_at_capacity() {
        // max_files=3 means current + .1 + .2; .3 should be deleted
        let dir = test_dir!("logs");
        let log = dir.join("app.log");
        let mut w = open_rolling(&log, 10, 3);
        w.write_line(b"aaaaaaaaaa\n", LogStream::Stdout).unwrap(); // → .1
        w.write_line(b"bbbbbbbbbb\n", LogStream::Stdout).unwrap(); // .1→.2, → .1
        w.write_line(b"cccccccccc\n", LogStream::Stdout).unwrap(); // .2 deleted, .1→.2, → .1
        w.write_line(b"current\n", LogStream::Stdout).unwrap();
        w.flush().unwrap();

        assert!(log.exists());
        assert!(dir.join("app.log.1").exists());
        assert!(dir.join("app.log.2").exists());
        assert!(
            !dir.join("app.log.3").exists(),
            ".3 should have been deleted"
        );
        assert_eq!(count_files(&dir), 3);
    }

    #[test]
    fn test_rolling_no_data_loss_within_max_files() {
        // With enough max_files, all data should be preserved
        let dir = test_dir!("logs");
        let log = dir.join("app.log");
        let mut w = open_rolling(&log, 15, 50);
        for i in 1..=20 {
            w.write_line(format!("line_{}\n", i).as_bytes(), LogStream::Stdout)
                .unwrap();
        }
        w.flush().unwrap();

        let mut all = String::new();
        all.push_str(&fs::read_to_string(&log).unwrap());
        for i in 1..50 {
            let rolled = dir.join(format!("app.log.{}", i));
            if let Ok(content) = fs::read_to_string(&rolled) {
                all.push_str(&content);
            } else {
                break;
            }
        }
        for i in 1..=20 {
            assert!(all.contains(&format!("line_{}", i)), "missing line_{}", i);
        }
    }

    #[test]
    fn test_rolling_appends_newline_if_missing() {
        let dir = test_dir!("logs");
        let log = dir.join("app.log");
        let mut w = open_rolling(&log, 0, 5);
        w.write_line(b"no newline", LogStream::Stdout).unwrap();
        w.flush().unwrap();
        let content = fs::read_to_string(&log).unwrap();
        assert!(
            content.ends_with('\n'),
            "should append newline: {:?}",
            content
        );
    }

    #[test]
    fn test_rolling_preserves_existing_newline() {
        let dir = test_dir!("logs");
        let log = dir.join("app.log");
        let mut w = open_rolling(&log, 0, 5);
        w.write_line(b"has newline\n", LogStream::Stdout).unwrap();
        w.flush().unwrap();
        let content = fs::read_to_string(&log).unwrap();
        assert_eq!(content, "has newline\n", "should not double newline");
    }

    // ========================================================================
    // try_wait — polling without prior wait
    // ========================================================================

    #[tokio::test]
    async fn test_try_wait_poll_until_done() {
        let mut handle = ProcessExecutor::new("sh")
            .args(["-c", "sleep 0.2"])
            .spawn(shutdown())
            .await
            .unwrap();

        // Should be None initially
        assert!(handle.try_wait().is_none());

        // Poll until done
        loop {
            if let Some(code) = handle.try_wait() {
                assert_eq!(code, 0);
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    // ========================================================================
    // kill — SIGTERM ignored, escalates to SIGKILL
    // ========================================================================

    #[tokio::test]
    async fn test_kill_escalates_to_sigkill() {
        // Spawn a process that traps SIGTERM
        let mut handle = ProcessExecutor::new("sh")
            .args(["-c", "trap '' TERM; sleep 60"])
            .spawn(shutdown())
            .await
            .unwrap();

        tokio::time::sleep(Duration::from_millis(200)).await;
        // Grace period is short — SIGTERM will be ignored, then SIGKILL
        handle.kill(Duration::from_millis(500)).await;

        let code = handle.wait().await.unwrap();
        // SIGKILL produces -1 (no exit code, killed by signal)
        assert_ne!(code, 0);
    }

    // ========================================================================
    // FileStream — error on bad path
    // ========================================================================

    #[test]
    fn test_file_stream_creates_parent_dirs() {
        let dir = test_dir!();
        let nested = dir.join("a").join("b").join("c").join("file.log");
        let config: Box<dyn StreamConfig> = Box::new(FileStream::new(&nested));
        let mut writer = config.open().unwrap();
        writer.write_line(b"hello\n", LogStream::Stdout).unwrap();
        writer.flush().unwrap();
        assert!(nested.exists());
        let content = fs::read_to_string(&nested).unwrap();
        assert!(content.contains("hello"));
    }

    // ========================================================================
    // DevNullStream — unit test
    // ========================================================================

    #[test]
    fn test_devnull_stream_discards() {
        let config: Box<dyn StreamConfig> = Box::new(DevNullStream);
        let mut writer = config.open().unwrap();
        writer
            .write_line(b"discarded\n", LogStream::Stdout)
            .unwrap();
        writer.flush().unwrap();
        // No crash, no output — that's the test
    }

    // ========================================================================
    // TeeStream — unit test with error propagation
    // ========================================================================

    #[test]
    fn test_tee_stream_flush_both() {
        let dir = test_dir!();
        let a = dir.join("a.log");
        let b = dir.join("b.log");
        let config: Box<dyn StreamConfig> =
            Box::new(TeeStream::new(FileStream::new(&a), FileStream::new(&b)));
        let mut writer = config.open().unwrap();
        writer.write_line(b"line1\n", LogStream::Stdout).unwrap();
        writer.write_line(b"line2\n", LogStream::Stdout).unwrap();
        writer.flush().unwrap();
        let a_content = fs::read_to_string(&a).unwrap();
        let b_content = fs::read_to_string(&b).unwrap();
        assert_eq!(a_content, b_content);
        assert!(a_content.contains("line1"));
        assert!(a_content.contains("line2"));
    }

    // ========================================================================
    // RollingFileStream — open creates parent dirs
    // ========================================================================

    #[test]
    fn test_rolling_stream_creates_parent_dirs() {
        let dir = test_dir!();
        let nested = dir.join("x").join("y").join("app.log");
        let config: Box<dyn StreamConfig> = Box::new(RollingFileStream::new(&nested, 1024, 5));
        let mut writer = config.open().unwrap();
        writer.write_line(b"test\n", LogStream::Stdout).unwrap();
        writer.flush().unwrap();
        assert!(nested.exists());
    }

    // ========================================================================
    // Process killed by signal (not exit code)
    // ========================================================================

    #[tokio::test]
    async fn test_process_killed_by_signal_returns_minus_one() {
        // A process killed by SIGKILL has no exit code — code() returns None,
        // which we map to -1.
        let mut handle = ProcessExecutor::new("sleep")
            .arg("60")
            .spawn(shutdown())
            .await
            .unwrap();

        tokio::time::sleep(Duration::from_millis(100)).await;
        // Send SIGKILL directly
        handle.kill(Duration::ZERO).await;
        let code = handle.wait().await.unwrap();
        assert_eq!(code, -1, "signal death should return -1");
    }

    #[tokio::test]
    async fn test_process_segfault_returns_minus_one() {
        // Simulate a crash via SIGSEGV — process has no exit code
        let mut handle = ProcessExecutor::new("sh")
            .args(["-c", "kill -SEGV $$"])
            .spawn(shutdown())
            .await
            .unwrap();

        let code = handle.wait().await.unwrap();
        assert_eq!(code, -1, "SIGSEGV should return -1");
    }

    // ========================================================================
    // Spawn failure
    // ========================================================================

    #[tokio::test]
    async fn test_spawn_nonexistent_binary() {
        let result = ProcessExecutor::new("/nonexistent/binary/that/does/not/exist")
            .spawn(shutdown())
            .await;

        assert!(result.is_err());
    }

    // -- PID file management ------------------------------------------------

    #[tokio::test]
    async fn test_pid_file_created_on_spawn() {
        let dir = test_dir!("pid-create");
        let pid_dir = dir.join("pids");

        let mut handle = ProcessExecutor::new("sh")
            .args(["-c", "sleep 10"])
            .pid_dir(&pid_dir)
            .spawn(shutdown())
            .await
            .unwrap();

        let pid = handle.pid();
        let pid_file = pid_dir.join(pid.to_string());
        assert!(pid_file.exists(), "PID file should exist after spawn");

        let content = fs::read_to_string(&pid_file).unwrap();
        assert_eq!(content, "sh");

        handle.kill(Duration::ZERO).await;
        let _ = handle.wait().await;
    }

    #[tokio::test]
    async fn test_pid_file_removed_on_exit() {
        let dir = test_dir!("pid-exit");
        let pid_dir = dir.join("pids");

        let mut handle = ProcessExecutor::new("sh")
            .args(["-c", "exit 0"])
            .pid_dir(&pid_dir)
            .spawn(shutdown())
            .await
            .unwrap();

        let pid = handle.pid();
        let pid_file = pid_dir.join(pid.to_string());

        let _ = handle.wait().await;
        // Give the wait task a moment to clean up
        tokio::time::sleep(Duration::from_millis(50)).await;

        assert!(!pid_file.exists(), "PID file should be removed after exit");
    }

    #[tokio::test]
    async fn test_cleanup_stale_pids_kills_matching() {
        let dir = test_dir!("pid-stale");
        let pid_dir = dir.join("pids");

        // Spawn a long-running process.
        // Use "sleep" directly (not "sh -c sleep") so the PID file program
        // name matches what `ps` reports — "sh -c sleep 300" is racy because
        // sh may exec into sleep, changing the process command name.
        let mut handle = ProcessExecutor::new("sleep")
            .args(["300"])
            .pid_dir(&pid_dir)
            .die_with_parent(false)
            .output(DevNullOutput)
            .spawn(shutdown())
            .await
            .unwrap();

        let pid = handle.pid();
        assert!(is_pid_alive(pid));

        // cleanup_stale_pids should kill it (PID file says "sleep", process is "sleep")
        let killed = cleanup_stale_pids(&pid_dir);
        assert_eq!(killed, 1, "should report 1 killed");

        // Give kill a moment to take effect
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(!is_pid_alive(pid), "stale process should be killed");

        // PID file should be removed
        assert!(!pid_dir.join(pid.to_string()).exists());

        let _ = handle.wait().await;
    }

    #[test]
    fn test_cleanup_stale_pids_removes_dead_pid_file() {
        let dir = crate::testutils::make_test_dir(
            "process/executor/tests/test_cleanup_stale_pids_removes_dead",
        );
        let pid_dir = dir.join("pids");
        fs::create_dir_all(&pid_dir).unwrap();

        // Write a PID file for a PID that doesn't exist
        let pid_file = pid_dir.join("999999999");
        fs::write(&pid_file, "ghost-process").unwrap();

        cleanup_stale_pids(&pid_dir);

        // PID file should be cleaned up even though process doesn't exist
        assert!(!pid_file.exists(), "dead PID file should be removed");
    }

    #[test]
    fn test_cleanup_stale_pids_empty_dir() {
        let dir = crate::testutils::make_test_dir(
            "process/executor/tests/test_cleanup_stale_pids_empty_dir",
        );
        let pid_dir = dir.join("pids");
        fs::create_dir_all(&pid_dir).unwrap();

        // Should not panic on empty directory
        cleanup_stale_pids(&pid_dir);
    }

    #[test]
    fn test_cleanup_stale_pids_nonexistent_dir() {
        let dir = crate::testutils::make_test_dir(
            "process/executor/tests/test_cleanup_stale_pids_nonexistent",
        );
        let pid_dir = dir.join("nonexistent");

        // Should not panic on missing directory
        cleanup_stale_pids(&pid_dir);
    }

    // ========================================================================
    // Fork stress tests — spawn N processes, kill them, check for zombies
    // ========================================================================

    async fn fork_stress(
        n: usize,
        label: &str,
        output_fn: impl Fn(usize) -> Box<dyn OutputRedirector>,
    ) {
        let progress_every = if n >= 1000 {
            100
        } else if n >= 100 {
            50
        } else {
            10
        };
        let mut handles: Vec<ExecutorHandle> = Vec::with_capacity(n);

        // --- Phase 1: Spawn N ---
        eprintln!("[{label} n={n}] === Phase 1: Spawning ===");
        let t0 = std::time::Instant::now();
        let mut slow_spawns = 0u32;
        for i in 0..n {
            let t_spawn = std::time::Instant::now();
            let h = ProcessExecutor::new("sh")
                .args(["-c", "while true; do echo hello; sleep 1; done"])
                .output_boxed(output_fn(i))
                .spawn(shutdown())
                .await
                .unwrap();
            let spawn_elapsed = t_spawn.elapsed();
            if spawn_elapsed > Duration::from_millis(100) {
                eprintln!(
                    "[{label} n={n}]   spawn[{i}] SLOW: {}ms (pid={})",
                    spawn_elapsed.as_millis(),
                    h.pid()
                );
                slow_spawns += 1;
            }
            handles.push(h);
            if (i + 1) % progress_every == 0 {
                eprintln!(
                    "[{label} n={n}]   spawned {}/{n} (elapsed={}ms)",
                    i + 1,
                    t0.elapsed().as_millis(),
                );
            }
        }
        let spawn_ms = t0.elapsed().as_millis();
        eprintln!("[{label} n={n}]   spawn complete: {spawn_ms}ms, slow_spawns={slow_spawns}");

        // --- Phase 2: Let them run ---
        eprintln!("[{label} n={n}] === Phase 2: Running (3s settle) ===");
        tokio::time::sleep(Duration::from_secs(3)).await;

        let mut alive_before = 0;
        let mut dead_before = Vec::new();
        for (i, h) in handles.iter_mut().enumerate() {
            if h.try_wait().is_none() {
                alive_before += 1;
            } else {
                dead_before.push((i, h.pid()));
            }
        }
        if !dead_before.is_empty() {
            eprintln!(
                "[{label} n={n}]   WARNING: {} processes died before kill: {:?}",
                dead_before.len(),
                &dead_before[..dead_before.len().min(20)],
            );
        }
        eprintln!("[{label} n={n}]   alive_before={alive_before}/{n}");

        // A handful of processes may die early under heavy parallel
        // load — typically when the per-process output writer task
        // can't open its log file (FD pressure) and the pipe to the
        // child closes, killing it via SIGPIPE on the next echo.
        // We tolerate that: the *intent* of this test is to verify
        // the batch-kill mechanism works on the surviving swarm and
        // leaves no zombies. A floor of 80% catches real regressions
        // (e.g. fork() bugs) without being flaky under contention.
        let min_alive_before = (n * 8) / 10;

        // --- Phase 3: Batch kill ---
        // Two-phase approach: broadcast SIGTERM to all, wait once, then
        // SIGKILL any survivors.  This is O(grace_period) instead of
        // O(N * grace_period) that sequential per-process kill would be.
        let grace = Duration::from_secs(2);
        eprintln!("[{label} n={n}] === Phase 3: Batch kill (grace={grace:?}) ===");
        let t1 = std::time::Instant::now();

        // 3a: broadcast SIGTERM to all process groups
        let mut already_dead = 0u32;
        for h in handles.iter() {
            if is_pid_alive(h.pid()) {
                h.send_sigterm();
            } else {
                already_dead += 1;
            }
        }
        let sigterm_ms = t1.elapsed().as_millis();
        eprintln!(
            "[{label} n={n}]   SIGTERM broadcast: {sigterm_ms}ms (already_dead={already_dead})",
        );

        // 3b: wait for grace period
        tokio::time::sleep(grace).await;

        // 3c: SIGKILL any survivors
        let mut sigkill_count = 0u32;
        for h in handles.iter_mut() {
            if h.try_wait().is_none() && is_pid_alive(h.pid()) {
                h.kill(Duration::ZERO).await;
                sigkill_count += 1;
            }
        }
        let kill_ms = t1.elapsed().as_millis();
        eprintln!("[{label} n={n}]   kill complete: {kill_ms}ms, sigkills={sigkill_count}",);

        // --- Phase 4: Wait and reap ---
        eprintln!("[{label} n={n}] === Phase 4: Waiting/reaping ===");
        let t2 = std::time::Instant::now();
        let mut slow_waits = 0u32;
        for (i, h) in handles.iter_mut().enumerate() {
            let t_wait = std::time::Instant::now();
            let _ = h.wait().await;
            let wait_elapsed = t_wait.elapsed();
            if wait_elapsed > Duration::from_millis(100) {
                eprintln!(
                    "[{label} n={n}]   wait[{i}] SLOW: {}ms (pid={})",
                    wait_elapsed.as_millis(),
                    h.pid(),
                );
                slow_waits += 1;
            }
            if (i + 1) % progress_every == 0 {
                eprintln!(
                    "[{label} n={n}]   waited {}/{n} (elapsed={}ms)",
                    i + 1,
                    t2.elapsed().as_millis(),
                );
            }
        }
        let wait_ms = t2.elapsed().as_millis();
        eprintln!("[{label} n={n}]   wait complete: {wait_ms}ms, slow_waits={slow_waits}");

        tokio::time::sleep(Duration::from_millis(500)).await;

        // --- Phase 5: Final checks ---
        //
        // Phase 4 already `wait()`-ed every handle, which reaps the child
        // and frees its PID. `still_alive` checks each handle directly —
        // no PID-based scan, because once a PID is freed, the OS may
        // reassign it to a process spawned by a concurrent test, which
        // would look like a zombie to a naive `kill(pid, 0)` probe.
        eprintln!("[{label} n={n}] === Phase 5: Final checks ===");
        let mut still_alive = 0;
        for h in &mut handles {
            if h.try_wait().is_none() {
                still_alive += 1;
            }
        }

        eprintln!(
            "[{label} n={n}] === SUMMARY ===\n\
             [{label} n={n}]   spawn={spawn_ms}ms kill={kill_ms}ms wait={wait_ms}ms\n\
             [{label} n={n}]   slow_spawns={slow_spawns} sigkills={sigkill_count} slow_waits={slow_waits}\n\
             [{label} n={n}]   alive_before={alive_before} still_alive={still_alive}"
        );

        assert!(
            alive_before >= min_alive_before,
            "{label} n={n}: only {alive_before}/{n} alive before kill (floor {min_alive_before})",
        );
        assert_eq!(
            still_alive, 0,
            "{label} n={n}: {still_alive} processes still alive after kill"
        );

        drop(handles);
    }

    // The fork_stress tests fork up to 1000 child processes each, opening
    // hundreds of file descriptors. Running multiple instances concurrently
    // (default `cargo test` parallelism) exhausts OS resources on
    // resource-constrained machines, producing intermittent failures.
    // `#[serial(fork_stress)]` forces them through a shared mutex so only
    // one runs at a time — they still run; just sequentially.

    #[tokio::test]
    #[serial(fork_stress)]
    async fn test_fork_stress_100_devnull() {
        fork_stress(100, "devnull", |_| Box::new(DevNullOutput)).await;
    }

    #[tokio::test]
    #[serial(fork_stress)]
    async fn test_fork_stress_1000_devnull() {
        fork_stress(1000, "devnull", |_| Box::new(DevNullOutput)).await;
    }

    #[tokio::test]
    #[serial(fork_stress)]
    async fn test_fork_stress_100_file() {
        let dir = test_dir!();
        fork_stress(100, "file", move |i| {
            Box::new(MergedOutput::new(FileStream::new(
                dir.join(format!("{i}.log")),
            )))
        })
        .await;
    }

    #[tokio::test]
    #[serial(fork_stress)]
    async fn test_fork_stress_1000_file() {
        let dir = test_dir!();
        fork_stress(1000, "file", move |i| {
            Box::new(MergedOutput::new(FileStream::new(
                dir.join(format!("{i}.log")),
            )))
        })
        .await;
    }

    #[tokio::test]
    #[serial(fork_stress)]
    async fn test_fork_stress_100_rolling() {
        let dir = test_dir!();
        fork_stress(100, "rolling", move |i| {
            Box::new(MergedOutput::new(RollingFileStream::new(
                dir.join(format!("{i}.log")),
                1024 * 1024,
                3,
            )))
        })
        .await;
    }

    #[tokio::test]
    #[serial(fork_stress)]
    async fn test_fork_stress_1000_rolling() {
        let dir = test_dir!();
        fork_stress(1000, "rolling", move |i| {
            Box::new(MergedOutput::new(RollingFileStream::new(
                dir.join(format!("{i}.log")),
                1024 * 1024,
                3,
            )))
        })
        .await;
    }
}
