// Copyright 2026 Tabsdata Inc.

//! Messages reported to km for display at startup and shutdown.
//!
//! The pod-runner's own log records everything, which is another way of
//! saying nobody reads it until something has already gone wrong. A handful
//! of facts are worth putting in front of whoever started the stack —
//! `capacity.pods` above all, since a run that quietly exceeds it shows up
//! only as a wave of `ProviderFailed` pods with nothing pointing at the cap.
//!
//! km empties `<workdir>/messages/` when it starts, collects the files once
//! the stack is up, orders them by time and prints them. This is reporting
//! only: readiness stays with the heartbeat file, and every failure here is
//! swallowed, because a message that cannot be written must never be able to
//! stop the pod-runner from serving.

use std::io::Write;
use std::path::Path;

use serde::Serialize;
use tracing::debug;

/// Directory under the workdir that km empties on startup and reads from.
pub const MESSAGES_DIR: &str = "messages";

/// Our name in the `binary` field and in the message filename.
pub const BINARY: &str = "pod-runner";

/// Filename prefixes that keep the startup and shutdown sets apart, so km
/// does not reprint the startup messages as the stack comes down.
pub const START_PHASE: &str = "start-";
pub const STOP_PHASE: &str = "stop-";

/// Levels km understands. Anything else is shown as written.
pub const LEVEL_INFO: &str = "INFO";
pub const LEVEL_WARN: &str = "WARN";
pub const LEVEL_ERROR: &str = "ERROR";

/// RFC 3339 in UTC at fixed millisecond precision. km orders these as plain
/// strings, so neither the precision nor the zone may vary between writers.
const TIME_FORMAT: &str = "%Y-%m-%dT%H:%M:%S%.3fZ";

/// One newline-delimited JSON record in a message file.
#[derive(Debug, Serialize)]
struct Message<'a> {
    time: String,
    binary: &'a str,
    level: &'a str,
    message: &'a str,
}

/// Append one message for km to print.
///
/// `phase` is [`START_PHASE`] or [`STOP_PHASE`]. Appending rather than
/// rewriting lets a component add to what it has already said without
/// holding the earlier lines.
pub fn write(workdir: &Path, phase: &str, level: &str, message: &str) {
    if let Err(e) = try_write(workdir, phase, level, message) {
        // Logged at debug: a missing messages directory is the normal state
        // when the pod-runner is run outside km, and is not worth a warning
        // on every start.
        debug!(error = %e, "Could not write a km message");
    }
}

fn try_write(workdir: &Path, phase: &str, level: &str, message: &str) -> std::io::Result<()> {
    let record = Message {
        time: chrono::Utc::now().format(TIME_FORMAT).to_string(),
        binary: BINARY,
        level,
        message,
    };
    let mut line = serde_json::to_string(&record)
        .map_err(|e| std::io::Error::other(format!("encoding a km message: {e}")))?;
    line.push('\n');

    let path = workdir
        .join(MESSAGES_DIR)
        .join(format!("{phase}{BINARY}.json"));
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)?;
    file.write_all(line.as_bytes())
}

/// Report the node sizing the pod-runner will enforce.
///
/// `capacity.pods` is the one that bites: the virtual-kubelet keeps
/// scheduling past it and the overflow fails as `ProviderFailed` /
/// `BackoffLimitExceeded`, which reads as a pod problem rather than a
/// configured ceiling. Saying the number out loud at startup is what makes
/// that diagnosable, so it is reported even when nothing is wrong.
pub fn report_sizing(workdir: &Path, cpu: u32, memory_gb: u32, pods: u32) {
    write(
        workdir,
        START_PHASE,
        LEVEL_INFO,
        &format!(
            "node capacity: {pods} pods, {cpu} cpu, {memory_gb}GB memory \
             (prsysconfig.yaml 'capacity'); pods scheduled beyond the pod \
             ceiling fail as ProviderFailed"
        ),
    );
}

#[cfg(test)]
mod tests;
