// Copyright 2026 Tabsdata Inc.

//! Component messages, surfaced by km at startup and shutdown.
//!
//! A component with something the operator should see — a sizing figure, a
//! setting that will bite later, a warning not worth failing the start over —
//! writes it into `<workdir>/messages/` rather than only into its own log,
//! which nobody opens on a good day. km clears the directory when it starts,
//! collects the files once the stack is up (or has failed to come up), sorts
//! everything by time and prints it to the terminal and its own log. The
//! shutdown set is handled the same way as the stack comes down.
//!
//! This channel reports; it does not decide anything. Readiness and liveness
//! stay with the heartbeat files: a component with nothing to say is not
//! unhealthy, and a message that never arrives must never hold up a start.
//! Every failure here is swallowed for the same reason.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};

use serde::Deserialize;
use tracing::{error, info, warn};

/// Directory under the workdir where components leave their messages.
pub const MESSAGES_DIR: &str = "messages";

/// Levels a component may report at. Anything else is passed through and
/// shown as written, so an unknown level is visible rather than swallowed.
pub const LEVEL_INFO: &str = "INFO";
pub const LEVEL_WARN: &str = "WARN";
pub const LEVEL_ERROR: &str = "ERROR";

/// Which set of messages: those written while the stack starts, or while it
/// stops. A component writes `<prefix><binary>.json` for the phase it is
/// reporting on, so the two sets never mix and the startup set is not
/// reprinted at shutdown.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Phase {
    Start,
    Stop,
}

impl Phase {
    /// Filename prefix for this phase.
    pub fn prefix(self) -> &'static str {
        match self {
            Phase::Start => "start-",
            Phase::Stop => "stop-",
        }
    }

    /// How the phase is named in the printed heading.
    fn label(self) -> &'static str {
        match self {
            Phase::Start => "startup",
            Phase::Stop => "shutdown",
        }
    }

    /// Whether this phase has already been printed.
    fn reported(self) -> &'static AtomicBool {
        static START: AtomicBool = AtomicBool::new(false);
        static STOP: AtomicBool = AtomicBool::new(false);
        match self {
            Phase::Start => &START,
            Phase::Stop => &STOP,
        }
    }
}

/// One line of a component's message file.
///
/// `time` is RFC 3339 in UTC at fixed millisecond precision, e.g.
/// `2026-08-22T16:24:24.204Z`. km orders on that string: at a fixed precision
/// in a fixed zone, lexicographic order is chronological, which spares km a
/// date library for what is only a display ordering. A component that writes
/// some other shape still gets printed — it just sorts by its text.
///
/// Every field defaults, so a malformed or partial line degrades to a blank
/// rather than discarding the whole file.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct Message {
    pub time: String,
    pub binary: String,
    pub level: String,
    pub message: String,
}

/// The directory components write their messages into.
pub fn messages_dir(workdir: &Path) -> PathBuf {
    workdir.join(MESSAGES_DIR)
}

/// Empty the messages directory and recreate it, so a previous run's
/// messages cannot be read as this one's. Called once, before anything is
/// spawned.
pub fn clear(workdir: &Path) {
    let dir = messages_dir(workdir);
    let _ = fs::remove_dir_all(&dir);
    if let Err(e) = fs::create_dir_all(&dir) {
        // Not fatal: components find the directory missing and skip writing,
        // which costs a report, not a start.
        warn!(dir = %dir.display(), error = %e, "Could not create the messages directory");
    }
}

/// Read every message written for `phase`, oldest first.
///
/// Files are newline-delimited JSON: one object per line, so a component can
/// append as it goes without rewriting. Unreadable files and unparseable
/// lines are skipped — a component that writes rubbish loses its own message
/// and nobody else's.
pub fn read(workdir: &Path, phase: Phase) -> Vec<Message> {
    let dir = messages_dir(workdir);
    let Ok(entries) = fs::read_dir(&dir) else {
        return Vec::new();
    };

    let mut found = Vec::new();
    for entry in entries.filter_map(|e| e.ok()) {
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if !name.starts_with(phase.prefix()) || !name.ends_with(".json") {
            continue;
        }
        let Ok(text) = fs::read_to_string(entry.path()) else {
            continue;
        };
        for line in text.lines() {
            if line.trim().is_empty() {
                continue;
            }
            // JSON is a subset of YAML, so km's existing parser reads a line
            // without pulling in a JSON crate — as the heartbeat reader does.
            if let Ok(message) = serde_yaml::from_str::<Message>(line) {
                found.push(message);
            }
        }
    }

    // Time first, then binary, so two components reporting in the same
    // millisecond still come out in a stable order rather than the order the
    // filesystem happened to list them in.
    found.sort_by(|a, b| a.time.cmp(&b.time).then_with(|| a.binary.cmp(&b.binary)));
    found
}

/// The level to show for a message. A component that omitted the field gets
/// INFO rather than a blank column.
fn level_of(m: &Message) -> &str {
    if m.level.is_empty() {
        LEVEL_INFO
    } else {
        &m.level
    }
}

/// One printed line: level, the component that reported it, then the text.
/// The component's own timestamp is deliberately left out — it only exists
/// to order the lines, and every one of them lands within a second or two of
/// the others. The exact time stays in the file and on the logged record.
fn format_line(m: &Message) -> String {
    format!("    {:<6} {:<16} {}", level_of(m), m.binary, m.message)
}

/// Print the messages for `phase` to the terminal and the log, once.
///
/// Idempotent by phase: the startup set is printed when the stack comes up,
/// and again from the teardown path only if the stack never got that far.
pub fn report(workdir: &Path, phase: Phase) {
    if phase.reported().swap(true, Ordering::SeqCst) {
        return;
    }
    let messages = read(workdir, phase);
    if messages.is_empty() {
        return;
    }

    println!();
    println!("  Component messages ({}):", phase.label());
    for m in &messages {
        let level = level_of(m);
        println!("{}", format_line(m));
        // Logged individually so each keeps its own level and the component
        // that reported it stays attached to the line.
        match level {
            LEVEL_ERROR => {
                error!(binary = %m.binary, time = %m.time, phase = phase.label(), "{}", m.message)
            }
            LEVEL_WARN => {
                warn!(binary = %m.binary, time = %m.time, phase = phase.label(), "{}", m.message)
            }
            _ => info!(binary = %m.binary, time = %m.time, phase = phase.label(), "{}", m.message),
        }
    }
}

#[cfg(test)]
mod tests;
