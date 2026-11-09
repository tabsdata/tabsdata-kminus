// Copyright 2026 Tabsdata Inc.

//! Container log streams — K8s-style log naming for pod containers.
//!
//! # File layout
//!
//! ```text
//! <log_dir>/
//!   <container_name>/
//!     0.log           ← current log for restart_count=0
//!     0.log.1         ← most recent rolled file
//!     0.log.2         ← next oldest
//!     1.log           ← current log for restart_count=1 (after restart)
//!     1.log.1
//!     probe-startup.log    ← probe log, appended across runs
//!     probe-liveness.log
//!     probe-readiness.log
//! ```
//!
//! Each container gets its own subdirectory under the pod's log directory.
//! Container logs are named `<restart_count>.log`. Size-based rolling
//! produces numbered suffixes (`.1`, `.2`, etc., standard log rotation).
//!
//! Probe logs use `probe-<type>.log` and **append** across multiple probe
//! executions — they are not truncated on each run.

use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use tokio::sync::{Mutex, mpsc};

use crate::pod::executor::PodState;
use crate::process::executor::{LogStream, RotationSchedule, StreamConfig, StreamWriter};

// ---------------------------------------------------------------------------
// Read-only mount warning
// ---------------------------------------------------------------------------

/// Warning line written at the very beginning of a container's log for
/// each read-only volume mount.
///
/// Pod-runner mounts volumes as plain symlinks and cannot enforce
/// read-only access, so the container is warned that not writing into
/// the volume is its own responsibility.
pub fn read_only_mount_warning(volume: &str) -> String {
    format!(
        "WARNING: volume '{volume}' has been mounted as read-only; the \
         pod-runner cannot enforce read-only mounts — it is the \
         container's responsibility not to write into this volume"
    )
}

// ---------------------------------------------------------------------------
// Log path resolution — used by log readers
// ---------------------------------------------------------------------------

/// Resolve the log file path for a container.
///
/// - `pod_log_dir`: the pod-level log directory (e.g. from `ExecutorProvider::log_dir`)
/// - `container`: container name
/// - `restart_count`: current restart count (from container status)
/// - `previous`: if true, returns the previous restart's log
///
/// Returns `<pod_log_dir>/<container>/<restart_count>.log`.
pub fn resolve_log_path(
    pod_log_dir: &Path,
    container: &str,
    restart_count: u32,
    previous: bool,
) -> PathBuf {
    let rc = if previous && restart_count > 0 {
        restart_count - 1
    } else {
        restart_count
    };
    pod_log_dir.join(container).join(format!("{}.log", rc))
}

// ---------------------------------------------------------------------------
// Log reading options and streaming
// ---------------------------------------------------------------------------

/// Options for streaming container logs.
#[derive(Default)]
pub struct ContainerLogOptions {
    /// Keep streaming new output until the pod terminates.
    pub follow: bool,
    /// Only return the last N lines (applied before follow starts).
    pub tail_lines: Option<u64>,
    /// Return logs from the previous container restart.
    pub previous: bool,
    /// Prefix each line with its CRI timestamp (kubectl --timestamps).
    pub timestamps: bool,
}

/// Read buffer size for log streaming.
const LOG_CHUNK_SIZE: usize = 8192;

/// Poll interval when following logs and waiting for new data.
const LOG_POLL_INTERVAL: Duration = Duration::from_millis(100);

/// Maximum time to wait for a log file to appear before giving up.
const LOG_WAIT_TIMEOUT: Duration = Duration::from_secs(5);

/// Stream a log file through the sender, reformatting CRI lines into
/// kubectl-style output (message only, or `<ts> <message>` with
/// `timestamps`).
///
/// Handles `tail_lines` (seek to last N lines before reading) and
/// `follow` (poll for new data until the pod reaches a terminal phase
/// or the receiver is dropped). In follow mode the file is reopened
/// when the writer rotates it (current → `.1`, fresh current file), so
/// the stream keeps following across rotations; bytes appended to the
/// old file between the last read and the rotation are drained first.
pub(crate) async fn stream_log_file(
    tx: &mpsc::Sender<Result<Bytes, io::Error>>,
    path: &Path,
    tail_lines: Option<u64>,
    follow: bool,
    timestamps: bool,
    state: &Arc<Mutex<PodState>>,
    kill_watch: &mut tokio::sync::watch::Receiver<bool>,
) -> Result<(), io::Error> {
    use tokio::io::{AsyncReadExt, AsyncSeekExt};

    // Wait for the log file to appear (container may not have started yet)
    let deadline = tokio::time::Instant::now() + LOG_WAIT_TIMEOUT;
    loop {
        if path.exists() {
            break;
        }
        if *kill_watch.borrow() {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            return Ok(());
        }
        tokio::time::sleep(LOG_POLL_INTERVAL).await;
    }

    let mut file = tokio::fs::File::open(path).await?;

    // Handle tail_lines: seek near end and find the start of the last N lines
    if let Some(n) = tail_lines {
        let metadata = file.metadata().await?;
        let file_size = metadata.len();
        if file_size > 0 && n > 0 {
            let seek_back = std::cmp::min(file_size, n * 200);
            let seek_pos = file_size - seek_back;
            file.seek(io::SeekFrom::Start(seek_pos)).await?;

            let mut buf = vec![0u8; seek_back as usize];
            let bytes_read = file.read(&mut buf).await?;
            buf.truncate(bytes_read);

            let mut line_count = 0u64;
            let mut start = 0;
            for i in (0..buf.len()).rev() {
                if buf[i] == b'\n' {
                    line_count += 1;
                    if line_count == n + 1 {
                        start = i + 1;
                        break;
                    }
                }
            }

            if line_count <= n && seek_pos > 0 {
                file.seek(io::SeekFrom::Start(0)).await?;
            } else {
                file.seek(io::SeekFrom::Start(seek_pos + start as u64))
                    .await?;
            }
        }
    }

    // Bytes consumed from the current file — compared against the
    // on-disk size to detect rotation (fresh file is smaller).
    let mut offset = file.stream_position().await?;
    let mut formatter = CriFormatter::new(timestamps);

    // Send the reformatted lines completed by one chunk. Returns false
    // when the receiver is gone.
    async fn send_chunk(
        tx: &mpsc::Sender<Result<Bytes, io::Error>>,
        formatter: &mut CriFormatter,
        chunk: &[u8],
    ) -> bool {
        let out = formatter.push(chunk);
        out.is_empty() || tx.send(Ok(Bytes::from(out))).await.is_ok()
    }

    // Send any trailing line that never got its newline (stream end).
    async fn send_rest(tx: &mpsc::Sender<Result<Bytes, io::Error>>, formatter: &mut CriFormatter) {
        let out = formatter.finish();
        if !out.is_empty() {
            let _ = tx.send(Ok(Bytes::from(out))).await;
        }
    }

    // Stream the file in chunks
    let mut buf = vec![0u8; LOG_CHUNK_SIZE];
    loop {
        let n = file.read(&mut buf).await?;
        if n > 0 {
            offset += n as u64;
            if !send_chunk(tx, &mut formatter, &buf[..n]).await {
                return Ok(());
            }
        } else if follow {
            // EOF — check if killed, rotated, or pod is terminal
            if *kill_watch.borrow() {
                drain_to_end(&mut file, &mut offset, path, &mut buf, tx, &mut formatter).await;
                send_rest(tx, &mut formatter).await;
                return Ok(());
            }

            // Rotation: the writer moved the current file aside and
            // started a fresh one when the on-disk size is smaller than
            // what we've already consumed. Drain anything appended to
            // the old file after our last read, then switch to the
            // fresh file. A missing path / failed open is transient
            // (mid-rotation) — retry on the next poll.
            if let Ok(meta) = tokio::fs::metadata(path).await
                && meta.len() < offset
                && let Ok(fresh) = tokio::fs::File::open(path).await
            {
                loop {
                    let n = file.read(&mut buf).await?;
                    if n == 0 {
                        break;
                    }
                    if !send_chunk(tx, &mut formatter, &buf[..n]).await {
                        return Ok(());
                    }
                }
                file = fresh;
                offset = 0;
                continue;
            }

            let is_terminal = {
                let st = state.lock().await;
                st.lifecycle.is_terminal()
            };
            if is_terminal {
                drain_to_end(&mut file, &mut offset, path, &mut buf, tx, &mut formatter).await;
                send_rest(tx, &mut formatter).await;
                return Ok(());
            }
            tokio::time::sleep(LOG_POLL_INTERVAL).await;
        } else {
            send_rest(tx, &mut formatter).await;
            return Ok(());
        }
    }
}

/// Final catch-up when a followed stream ends (pod killed or terminal):
/// drain the current fd, then follow any rotation that raced with the
/// shutdown so the freshest lines (e.g. the container's last output)
/// aren't lost. Best-effort — an I/O error or a dropped receiver just
/// stops the drain. The rotation hops are bounded as a safety valve.
async fn drain_to_end(
    file: &mut tokio::fs::File,
    offset: &mut u64,
    path: &Path,
    buf: &mut [u8],
    tx: &mpsc::Sender<Result<Bytes, io::Error>>,
    formatter: &mut CriFormatter,
) {
    use tokio::io::AsyncReadExt;

    const MAX_ROTATION_HOPS: usize = 8;
    for _ in 0..MAX_ROTATION_HOPS {
        loop {
            match file.read(buf).await {
                Ok(0) => break,
                Ok(n) => {
                    *offset += n as u64;
                    let out = formatter.push(&buf[..n]);
                    if !out.is_empty() && tx.send(Ok(Bytes::from(out))).await.is_err() {
                        return;
                    }
                }
                Err(_) => return,
            }
        }
        match tokio::fs::metadata(path).await {
            Ok(meta) if meta.len() < *offset => match tokio::fs::File::open(path).await {
                Ok(fresh) => {
                    *file = fresh;
                    *offset = 0;
                }
                Err(_) => return,
            },
            _ => return,
        }
    }
}

// ---------------------------------------------------------------------------
// CRI line reformatting — kubectl-style output
// ---------------------------------------------------------------------------

/// Reformat CRI log lines (`<ts> <stdout|stderr> <P|F> <message>`) into
/// kubectl-style output: the message alone, or `<ts> <message>` when
/// `timestamps` is set — matching what a real kubelet serves.
///
/// Input arrives in arbitrary chunks; a line straddling a chunk boundary
/// is buffered until its newline arrives. Lines that don't parse as CRI
/// pass through unchanged.
pub(crate) struct CriFormatter {
    timestamps: bool,
    partial: Vec<u8>,
}

impl CriFormatter {
    pub(crate) fn new(timestamps: bool) -> Self {
        Self {
            timestamps,
            partial: Vec::new(),
        }
    }

    /// Feed a chunk, returning the reformatted bytes of every line the
    /// chunk completes.
    pub(crate) fn push(&mut self, chunk: &[u8]) -> Vec<u8> {
        self.partial.extend_from_slice(chunk);
        let mut out = Vec::with_capacity(self.partial.len());
        let mut start = 0;
        while let Some(rel) = self.partial[start..].iter().position(|&b| b == b'\n') {
            let end = start + rel;
            reformat_cri_line(&self.partial[start..end], self.timestamps, &mut out);
            out.push(b'\n');
            start = end + 1;
        }
        self.partial.drain(..start);
        out
    }

    /// Flush a trailing line that never got its newline (stream end).
    pub(crate) fn finish(&mut self) -> Vec<u8> {
        if self.partial.is_empty() {
            return Vec::new();
        }
        let line = std::mem::take(&mut self.partial);
        let mut out = Vec::with_capacity(line.len());
        reformat_cri_line(&line, self.timestamps, &mut out);
        out
    }
}

/// Rewrite one CRI line (without its trailing newline) into `out`.
fn reformat_cri_line(line: &[u8], timestamps: bool, out: &mut Vec<u8>) {
    // CRI: "<ts> <stdout|stderr> <P|F> <message>"
    let parsed = (|| {
        let sp1 = line.iter().position(|&b| b == b' ')?;
        let rest = &line[sp1 + 1..];
        let sp2 = rest.iter().position(|&b| b == b' ')?;
        let stream = &rest[..sp2];
        if stream != b"stdout" && stream != b"stderr" {
            return None;
        }
        let rest = &rest[sp2 + 1..];
        let sp3 = rest.iter().position(|&b| b == b' ')?;
        let tag = &rest[..sp3];
        if tag != b"F" && tag != b"P" {
            return None;
        }
        Some((&line[..sp1], &rest[sp3 + 1..]))
    })();

    match parsed {
        Some((ts, msg)) => {
            if timestamps {
                out.extend_from_slice(ts);
                out.push(b' ');
            }
            out.extend_from_slice(msg);
        }
        None => out.extend_from_slice(line),
    }
}

// ---------------------------------------------------------------------------
// ContainerLogStream — StreamConfig for container stdout/stderr
// ---------------------------------------------------------------------------

/// Log stream for a container's stdout/stderr output.
///
/// Creates a log file at `<log_dir>/<container_name>/<restart_count>.log`
/// with standard numbered rotation (`.1`, `.2`, etc.) when the file
/// exceeds `max_size_bytes`.
///
/// If a file for the same `restart_count` already exists (e.g. from a
/// previous execution that was interrupted), it is shifted into the
/// rotation before a fresh file is created.
pub struct ContainerLogStream {
    log_dir: PathBuf,
    container_name: String,
    restart_count: u32,
    max_size_bytes: u64,
    max_files: usize,
    /// Lines written (as stderr) at the very beginning of the log file,
    /// before any container output — e.g. read-only mount warnings.
    header_lines: Vec<String>,
}

impl ContainerLogStream {
    pub fn new(
        log_dir: impl Into<PathBuf>,
        container_name: impl Into<String>,
        restart_count: u32,
        max_size_bytes: u64,
        max_files: usize,
    ) -> Self {
        Self {
            log_dir: log_dir.into(),
            container_name: container_name.into(),
            restart_count,
            max_size_bytes,
            max_files,
            header_lines: Vec::new(),
        }
    }

    /// Lines to write (as stderr) at the very beginning of the log file,
    /// before any container output.
    pub fn with_header_lines(mut self, lines: Vec<String>) -> Self {
        self.header_lines = lines;
        self
    }

    /// The directory for this container's logs.
    fn container_dir(&self) -> PathBuf {
        self.log_dir.join(&self.container_name)
    }

    /// The path of the current log file.
    fn log_path(&self) -> PathBuf {
        self.container_dir()
            .join(format!("{}.log", self.restart_count))
    }
}

impl StreamConfig for ContainerLogStream {
    fn open(self: Box<Self>) -> io::Result<Box<dyn StreamWriter>> {
        let dir = self.container_dir();
        std::fs::create_dir_all(&dir)?;

        let path = self.log_path();

        // If a file for this restart_count already exists (e.g. from
        // an interrupted previous run), shift it into the rotation
        // so we start fresh — or, when it cannot be moved aside, keep it
        // and append rather than truncate it.
        let mut kept = false;
        if path.exists()
            && std::fs::metadata(&path)
                .map(|m| m.len() > 0)
                .unwrap_or(false)
            && let Err(error) = shift_files(&path, self.max_files)
        {
            tracing::warn!(
                path = %path.display(),
                %error,
                "Cannot rotate the previous container log; appending to it",
            );
            kept = true;
        }

        let file = if kept {
            std::fs::OpenOptions::new().append(true).open(&path)?
        } else {
            std::fs::File::create(&path)?
        };
        let current_size = file.metadata()?.len();
        let mut writer = ContainerLogWriter {
            path,
            file,
            current_size,
            schedule: RotationSchedule::new(self.max_size_bytes),
            max_files: self.max_files,
        };
        for line in &self.header_lines {
            writer.write_line(line.as_bytes(), LogStream::Stderr)?;
        }
        Ok(Box::new(writer))
    }
}

// ---------------------------------------------------------------------------
// ProbeLogStream — StreamConfig for probe execution output
// ---------------------------------------------------------------------------

/// Log stream for probe execution output.
///
/// Creates (or appends to) a file at
/// `<log_dir>/<container_name>/probe-<probe_type>.log`.
///
/// Unlike container logs, probe logs **append** across multiple probe
/// executions — they are not truncated on each run. Size-based rolling
/// still applies.
pub struct ProbeLogStream {
    log_dir: PathBuf,
    container_name: String,
    probe_type: String,
    max_size_bytes: u64,
    max_files: usize,
}

impl ProbeLogStream {
    pub fn new(
        log_dir: impl Into<PathBuf>,
        container_name: impl Into<String>,
        probe_type: impl Into<String>,
        max_size_bytes: u64,
        max_files: usize,
    ) -> Self {
        Self {
            log_dir: log_dir.into(),
            container_name: container_name.into(),
            probe_type: probe_type.into(),
            max_size_bytes,
            max_files,
        }
    }

    fn log_path(&self) -> PathBuf {
        self.log_dir
            .join(&self.container_name)
            .join(format!("probe-{}.log", self.probe_type))
    }
}

impl StreamConfig for ProbeLogStream {
    fn open(self: Box<Self>) -> io::Result<Box<dyn StreamWriter>> {
        let dir = self.log_dir.join(&self.container_name);
        std::fs::create_dir_all(&dir)?;

        let path = self.log_path();

        // Open in append mode — probe logs accumulate across runs
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)?;

        // Get current file size for rotation tracking
        let current_size = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);

        Ok(Box::new(ContainerLogWriter {
            path,
            file,
            current_size,
            schedule: RotationSchedule::new(self.max_size_bytes),
            max_files: self.max_files,
        }))
    }
}

// ---------------------------------------------------------------------------
// ContainerLogWriter — shared rolling writer for both container and probe logs
// ---------------------------------------------------------------------------

/// Rolling log writer with standard numbered rotation.
///
/// Writes lines to the current file. When the file exceeds `max_size`,
/// it is rotated: existing `.N` files shift up, the current file becomes
/// `.1`, and a fresh current file is created.
///
/// This is the same rotation logic as `RollingStreamWriter` in the process
/// executor, factored here for container/probe log naming.
struct ContainerLogWriter {
    path: PathBuf,
    file: std::fs::File,
    current_size: u64,
    schedule: RotationSchedule,
    max_files: usize,
}

impl ContainerLogWriter {
    /// Rotate: delete oldest, shift `.N-1` → `.N`, current → `.1`, fresh current.
    ///
    /// When the current file cannot be moved aside (on Windows, while
    /// another process holds it), it is kept and appended to rather than
    /// truncated, and rotation is tried again as [`RotationSchedule`] says.
    fn rotate(&mut self) -> io::Result<()> {
        self.file.flush()?;
        self.current_size = 0;
        if let Err(error) = shift_files(&self.path, self.max_files) {
            tracing::warn!(
                path = %self.path.display(),
                %error,
                "Cannot rotate the container log; appending to it",
            );
            self.schedule.failed();
            return Ok(());
        }
        self.file = std::fs::File::create(&self.path)?;
        self.schedule.succeeded();
        Ok(())
    }
}

impl StreamWriter for ContainerLogWriter {
    /// Write a line in CRI log format:
    /// `<RFC3339Nano timestamp> <stdout|stderr> F <message>\n`
    ///
    /// Example: `2026-04-14T15:30:00.000000000Z stdout F hello world\n`
    fn write_line(
        &mut self,
        line: &[u8],
        stream: crate::process::executor::LogStream,
    ) -> io::Result<()> {
        // CRI prefix: "<timestamp> <stream> F "
        let ts = chrono::Utc::now().format("%Y-%m-%dT%H:%M:%S%.9fZ");
        let prefix = format!("{} {} F ", ts, stream.as_str());
        self.file.write_all(prefix.as_bytes())?;
        self.current_size += prefix.len() as u64;

        // Strip the line ending, `\r\n` included (we add our own)
        let msg = line.strip_suffix(b"\n").unwrap_or(line);
        let msg = msg.strip_suffix(b"\r").unwrap_or(msg);
        self.file.write_all(msg)?;
        self.file.write_all(b"\n")?;
        self.current_size += msg.len() as u64 + 1;

        if self.schedule.due(self.current_size) {
            self.rotate()?;
        }
        Ok(())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }
}

// ---------------------------------------------------------------------------
// File rotation helper
// ---------------------------------------------------------------------------

/// Shift numbered files: delete `.{max_files-1}`, rename `.{N-1}` → `.N`,
/// rename current → `.1`.
///
/// Used both during rotation (within a run) and when opening a log file
/// that already exists (from a previous interrupted run).
///
/// Fails only when the current file cannot be moved to `.1`; the caller
/// must then keep it rather than recreate it, which would truncate it.
fn shift_files(path: &PathBuf, max_files: usize) -> io::Result<()> {
    let max_rolled = max_files.saturating_sub(1);

    if max_rolled == 0 {
        // max_files=1: only current file, no rolled files kept.
        // Just leave the current file to be overwritten by the caller.
        return Ok(());
    }

    // Delete the oldest rolled file if at capacity
    let oldest = PathBuf::from(format!("{}.{}", path.display(), max_rolled));
    if oldest.exists() {
        let _ = std::fs::remove_file(&oldest);
    }

    // Shift existing rolled files up: .N-1 → .N, … .1 → .2
    for i in (1..max_rolled).rev() {
        let from = PathBuf::from(format!("{}.{}", path.display(), i));
        let to = PathBuf::from(format!("{}.{}", path.display(), i + 1));
        if from.exists() {
            let _ = std::fs::rename(&from, &to);
        }
    }

    // Move current → .1
    std::fs::rename(path, PathBuf::from(format!("{}.1", path.display())))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::process::executor::LogStream;
    use crate::testutils::test_dir;

    // -- ContainerLogStream --------------------------------------------------

    #[test]
    fn test_container_log_creates_directory_and_file() {
        let dir = test_dir!();
        let stream = Box::new(ContainerLogStream::new(&dir, "main", 0, 1024, 5));
        let mut writer = stream.open().unwrap();
        writer.write_line(b"hello\n", LogStream::Stdout).unwrap();
        writer.flush().unwrap();

        let log = dir.join("main").join("0.log");
        assert!(log.exists());
        let content = std::fs::read_to_string(&log).unwrap();
        assert!(content.contains("hello"));
    }

    #[test]
    fn test_container_log_restart_count_naming() {
        let dir = test_dir!();

        // restart_count=0
        let s0 = Box::new(ContainerLogStream::new(&dir, "app", 0, 1024, 5));
        let mut w0 = s0.open().unwrap();
        w0.write_line(b"attempt-0\n", LogStream::Stdout).unwrap();
        w0.flush().unwrap();

        // restart_count=1
        let s1 = Box::new(ContainerLogStream::new(&dir, "app", 1, 1024, 5));
        let mut w1 = s1.open().unwrap();
        w1.write_line(b"attempt-1\n", LogStream::Stdout).unwrap();
        w1.flush().unwrap();

        let c0 = std::fs::read_to_string(dir.join("app/0.log")).unwrap();
        let c1 = std::fs::read_to_string(dir.join("app/1.log")).unwrap();
        assert!(c0.contains("attempt-0"));
        assert!(c1.contains("attempt-1"));
    }

    #[test]
    fn test_container_log_shifts_existing_on_reopen() {
        let dir = test_dir!();
        let container_dir = dir.join("main");

        // First run
        let s1 = Box::new(ContainerLogStream::new(&dir, "main", 0, 1024, 5));
        let mut w1 = s1.open().unwrap();
        w1.write_line(b"first-run\n", LogStream::Stdout).unwrap();
        w1.flush().unwrap();
        drop(w1);

        // Second run with same restart_count — should shift first to .1
        let s2 = Box::new(ContainerLogStream::new(&dir, "main", 0, 1024, 5));
        let mut w2 = s2.open().unwrap();
        w2.write_line(b"second-run\n", LogStream::Stdout).unwrap();
        w2.flush().unwrap();

        let current = std::fs::read_to_string(container_dir.join("0.log")).unwrap();
        let rolled = std::fs::read_to_string(container_dir.join("0.log.1")).unwrap();
        assert!(current.contains("second-run"));
        assert!(rolled.contains("first-run"));
    }

    #[test]
    fn test_container_log_rotation_on_size() {
        let dir = test_dir!();

        // max_size must exceed the CRI-formatted "current" line (~48 bytes)
        // but be smaller than the "aaa..." line (~60 bytes) so only the
        // first write triggers rotation.
        let stream = Box::new(ContainerLogStream::new(&dir, "main", 0, 55, 5));
        let mut writer = stream.open().unwrap();
        writer
            .write_line(b"aaaaaaaaaaaaaaaaaaa\n", LogStream::Stdout)
            .unwrap(); // triggers rotation
        writer.write_line(b"current\n", LogStream::Stdout).unwrap();
        writer.flush().unwrap();

        let current = std::fs::read_to_string(dir.join("main/0.log")).unwrap();
        assert!(current.contains("current"));

        let rolled = dir.join("main/0.log.1");
        assert!(rolled.exists());
        let rolled_content = std::fs::read_to_string(&rolled).unwrap();
        assert!(rolled_content.contains("aaa"));
    }

    #[test]
    fn test_container_log_max_files_respected() {
        let dir = test_dir!();

        let stream = Box::new(ContainerLogStream::new(&dir, "main", 0, 10, 3));
        let mut writer = stream.open().unwrap();
        // Write enough to trigger multiple rotations
        writer
            .write_line(b"aaaaaaaaaa\n", LogStream::Stdout)
            .unwrap();
        writer
            .write_line(b"bbbbbbbbbb\n", LogStream::Stdout)
            .unwrap();
        writer
            .write_line(b"cccccccccc\n", LogStream::Stdout)
            .unwrap();
        writer.write_line(b"current\n", LogStream::Stdout).unwrap();
        writer.flush().unwrap();

        // max_files=3: current + .1 + .2; .3 should not exist
        assert!(dir.join("main/0.log").exists());
        assert!(dir.join("main/0.log.1").exists());
        assert!(dir.join("main/0.log.2").exists());
        assert!(!dir.join("main/0.log.3").exists());
    }

    #[test]
    fn test_container_log_max_files_one() {
        let dir = test_dir!();

        let stream = Box::new(ContainerLogStream::new(&dir, "main", 0, 10, 1));
        let mut writer = stream.open().unwrap();
        writer
            .write_line(b"aaaaaaaaaa\n", LogStream::Stdout)
            .unwrap();
        writer.write_line(b"current\n", LogStream::Stdout).unwrap();
        writer.flush().unwrap();

        // Only current file, no rolled
        let container_dir = dir.join("main");
        let file_count = std::fs::read_dir(&container_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().is_file())
            .count();
        assert_eq!(file_count, 1);
    }

    #[test]
    fn test_container_log_header_lines_written_first_as_stderr() {
        let dir = test_dir!();
        let stream = Box::new(
            ContainerLogStream::new(&dir, "main", 0, 1024, 5).with_header_lines(vec![
                read_only_mount_warning("vol-a"),
                read_only_mount_warning("vol-b"),
            ]),
        );
        let mut writer = stream.open().unwrap();
        writer
            .write_line(b"container output\n", LogStream::Stdout)
            .unwrap();
        writer.flush().unwrap();

        let content = std::fs::read_to_string(dir.join("main").join("0.log")).unwrap();
        let lines: Vec<&str> = content.lines().collect();
        assert_eq!(lines.len(), 3);
        assert!(
            lines[0].contains(&read_only_mount_warning("vol-a")),
            "got: {}",
            lines[0]
        );
        assert!(lines[0].contains(" stderr F "));
        assert!(lines[1].contains(&read_only_mount_warning("vol-b")));
        assert!(lines[2].contains("container output"));
    }

    #[test]
    fn test_read_only_mount_warning_names_volume() {
        let w = read_only_mount_warning("my-vol");
        assert!(w.contains("'my-vol'"));
        assert!(w.contains("read-only"));
    }

    // -- ProbeLogStream ------------------------------------------------------

    #[test]
    fn test_probe_log_creates_file() {
        let dir = test_dir!();
        let stream = Box::new(ProbeLogStream::new(&dir, "main", "startup", 1024, 5));
        let mut writer = stream.open().unwrap();
        writer
            .write_line(b"probe check 1\n", LogStream::Stdout)
            .unwrap();
        writer.flush().unwrap();

        let log = dir.join("main/probe-startup.log");
        assert!(log.exists());
        let content = std::fs::read_to_string(&log).unwrap();
        assert!(content.contains("probe check 1"));
    }

    #[test]
    fn test_probe_log_appends_across_runs() {
        let dir = test_dir!();

        // First probe run
        let s1 = Box::new(ProbeLogStream::new(&dir, "main", "liveness", 1024, 5));
        let mut w1 = s1.open().unwrap();
        w1.write_line(b"run-1\n", LogStream::Stdout).unwrap();
        w1.flush().unwrap();
        drop(w1);

        // Second probe run — should append, not truncate
        let s2 = Box::new(ProbeLogStream::new(&dir, "main", "liveness", 1024, 5));
        let mut w2 = s2.open().unwrap();
        w2.write_line(b"run-2\n", LogStream::Stdout).unwrap();
        w2.flush().unwrap();

        let content = std::fs::read_to_string(dir.join("main/probe-liveness.log")).unwrap();
        assert!(
            content.contains("run-1"),
            "first run should be present: {}",
            content
        );
        assert!(
            content.contains("run-2"),
            "second run should be present: {}",
            content
        );
    }

    #[test]
    fn test_probe_log_rotation_on_size() {
        let dir = test_dir!();

        // Same as container rotation test: CRI prefix adds ~40 bytes per line.
        let stream = Box::new(ProbeLogStream::new(&dir, "main", "readiness", 55, 5));
        let mut writer = stream.open().unwrap();
        writer
            .write_line(b"aaaaaaaaaaaaaaaaaaa\n", LogStream::Stdout)
            .unwrap(); // triggers rotation
        writer.write_line(b"current\n", LogStream::Stdout).unwrap();
        writer.flush().unwrap();

        let current = std::fs::read_to_string(dir.join("main/probe-readiness.log")).unwrap();
        assert!(current.contains("current"));
        assert!(dir.join("main/probe-readiness.log.1").exists());
    }

    #[test]
    fn test_probe_log_all_three_types() {
        let dir = test_dir!();

        for probe_type in &["startup", "liveness", "readiness"] {
            let s = Box::new(ProbeLogStream::new(&dir, "app", *probe_type, 1024, 5));
            let mut w = s.open().unwrap();
            w.write_line(
                format!("{}-check\n", probe_type).as_bytes(),
                LogStream::Stdout,
            )
            .unwrap();
            w.flush().unwrap();
        }

        for probe_type in &["startup", "liveness", "readiness"] {
            let path = dir.join(format!("app/probe-{}.log", probe_type));
            assert!(path.exists(), "missing {}", path.display());
            let content = std::fs::read_to_string(&path).unwrap();
            assert!(content.contains(&format!("{}-check", probe_type)));
        }
    }

    // -- CriFormatter ----------------------------------------------------------

    fn fmt_all(timestamps: bool, chunks: &[&str]) -> String {
        let mut f = CriFormatter::new(timestamps);
        let mut out = Vec::new();
        for c in chunks {
            out.extend(f.push(c.as_bytes()));
        }
        out.extend(f.finish());
        String::from_utf8(out).unwrap()
    }

    #[test]
    fn test_cri_formatter_strips_prefix() {
        let s = fmt_all(
            false,
            &["2026-04-14T15:30:00.000000000Z stdout F hello world\n\
             2026-04-14T15:30:01.000000000Z stderr F warn line\n"],
        );
        assert_eq!(s, "hello world\nwarn line\n");
    }

    #[test]
    fn test_cri_formatter_timestamps() {
        let s = fmt_all(true, &["2026-04-14T15:30:00.000000000Z stdout F hello\n"]);
        assert_eq!(s, "2026-04-14T15:30:00.000000000Z hello\n");
    }

    #[test]
    fn test_cri_formatter_line_across_chunks() {
        let s = fmt_all(
            false,
            &[
                "2026-04-14T15:30:00.000000000Z std",
                "out F spl",
                "it line\n2026-04-14T15:30:01.000000000Z stdout F next\n",
            ],
        );
        assert_eq!(s, "split line\nnext\n");
    }

    #[test]
    fn test_cri_formatter_non_cri_passthrough() {
        let s = fmt_all(false, &["plain line no prefix\nanother\n"]);
        assert_eq!(s, "plain line no prefix\nanother\n");
        // Wrong stream token is not CRI either.
        let s = fmt_all(false, &["2026-04-14T15:30:00Z bogus F x\n"]);
        assert_eq!(s, "2026-04-14T15:30:00Z bogus F x\n");
    }

    #[test]
    fn test_cri_formatter_empty_message_and_trailing_partial() {
        // Empty message keeps its line; trailing partial (no newline)
        // is flushed by finish() without adding a newline.
        let s = fmt_all(
            false,
            &["2026-04-14T15:30:00.000000000Z stdout F \n\
             2026-04-14T15:30:01.000000000Z stdout F no newline"],
        );
        assert_eq!(s, "\nno newline");
    }

    #[test]
    fn a_crlf_line_is_logged_without_its_carriage_return() {
        let dir = test_dir!();
        let stream = Box::new(ContainerLogStream::new(&dir, "main", 0, 1024, 5));
        let mut writer = stream.open().unwrap();
        writer
            .write_line(b"from windows\r\n", LogStream::Stdout)
            .unwrap();
        writer.flush().unwrap();

        let content = std::fs::read(dir.join("main").join("0.log")).unwrap();
        assert!(content.ends_with(b" F from windows\n"), "{content:?}");
    }

    /// A log whose `.1` slot is a non-empty folder: moving the current file
    /// there fails on every platform, as a held file does on Windows.
    fn blocked_rotation(dir: &Path, name: &str) -> PathBuf {
        let path = dir.join(name);
        let slot = dir.join(format!("{name}.1"));
        std::fs::create_dir_all(&slot).unwrap();
        std::fs::write(slot.join("occupied"), "x").unwrap();
        path
    }

    #[test]
    fn a_failed_rotation_keeps_the_current_log() {
        let dir = test_dir!();
        let path = blocked_rotation(&dir, "test.log");
        std::fs::write(&path, "original\n").unwrap();

        assert!(shift_files(&path, 2).is_err());
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "original\n");
    }

    #[test]
    fn a_writer_that_cannot_rotate_keeps_appending() {
        let dir = test_dir!();
        blocked_rotation(&dir.join("main"), "0.log");
        let stream = Box::new(ContainerLogStream::new(&dir, "main", 0, 64, 2));
        let mut writer = stream.open().unwrap();
        for line in ["first line of output\n", "second line of output\n"] {
            writer
                .write_line(line.as_bytes(), LogStream::Stdout)
                .unwrap();
        }
        writer.flush().unwrap();

        let content = std::fs::read_to_string(dir.join("main").join("0.log")).unwrap();
        assert!(content.contains("first line"), "{content}");
        assert!(content.contains("second line"), "{content}");
    }

    // -- shift_files (unit tests) --------------------------------------------

    #[test]
    fn test_shift_files_basic() {
        let dir = test_dir!();
        let path = dir.join("test.log");
        std::fs::write(&path, "original\n").unwrap();

        shift_files(&path, 5).unwrap();

        assert!(!path.exists(), "original should be renamed");
        let rolled = dir.join("test.log.1");
        assert!(rolled.exists());
        assert!(
            std::fs::read_to_string(&rolled)
                .unwrap()
                .contains("original")
        );
    }

    #[test]
    fn test_shift_files_cascades() {
        let dir = test_dir!();
        let path = dir.join("test.log");

        // Create existing rolled files
        std::fs::write(dir.join("test.log.1"), "rolled-1\n").unwrap();
        std::fs::write(dir.join("test.log.2"), "rolled-2\n").unwrap();
        std::fs::write(&path, "current\n").unwrap();

        shift_files(&path, 5).unwrap();

        assert!(!path.exists());
        assert!(
            std::fs::read_to_string(dir.join("test.log.1"))
                .unwrap()
                .contains("current")
        );
        assert!(
            std::fs::read_to_string(dir.join("test.log.2"))
                .unwrap()
                .contains("rolled-1")
        );
        assert!(
            std::fs::read_to_string(dir.join("test.log.3"))
                .unwrap()
                .contains("rolled-2")
        );
    }

    #[test]
    fn test_shift_files_deletes_oldest_at_capacity() {
        let dir = test_dir!();
        let path = dir.join("test.log");

        std::fs::write(dir.join("test.log.1"), "r1\n").unwrap();
        std::fs::write(dir.join("test.log.2"), "r2\n").unwrap();
        std::fs::write(&path, "current\n").unwrap();

        shift_files(&path, 3).unwrap(); // max=3: current + .1 + .2

        // .2 (oldest) should have been deleted, then rebuilt from shift
        assert!(
            std::fs::read_to_string(dir.join("test.log.1"))
                .unwrap()
                .contains("current")
        );
        assert!(
            std::fs::read_to_string(dir.join("test.log.2"))
                .unwrap()
                .contains("r1")
        );
        assert!(!dir.join("test.log.3").exists());
    }

    #[test]
    fn test_shift_files_max_one_is_noop() {
        let dir = test_dir!();
        let path = dir.join("test.log");
        std::fs::write(&path, "content\n").unwrap();

        shift_files(&path, 1).unwrap();

        // max_files=1: no rolled files, current should still exist
        assert!(path.exists());
        assert!(!dir.join("test.log.1").exists());
    }
}
