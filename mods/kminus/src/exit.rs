// Copyright 2026 Tabsdata Inc.

//! Why km stopped, reported where the operator is looking.
//!
//! Every exit path — a signal aimed at km, a component that never became
//! ready, a component lost while running — prints one block on stdout and
//! logs the same thing. The log is for afterwards; stdout is for the person
//! watching the terminal, who would otherwise see the startup banner, then
//! silence, and be left to guess which component went and why.

use std::sync::atomic::{AtomicBool, Ordering};

use tracing::{error, info};

/// Product name as printed in the console banners.
const PRODUCT: &str = "Tabsdata K- (K minus)";

/// Raised by whichever path reaches the exit first.
static EXITING: AtomicBool = AtomicBool::new(false);

/// Whether an exit is already under way. Loops read this to stand down
/// instead of acting on a stack that is being torn down.
pub fn in_progress() -> bool {
    EXITING.load(Ordering::SeqCst)
}

/// Claim the exit. The first caller owns the reason, the teardown and the
/// exit code; later callers get false and must stand down.
///
/// Four paths can reach an exit — the readiness gate, the process watcher,
/// the heartbeat monitor and the signal thread — and a second reason printed
/// over the first would mislead rather than inform: a readiness gate killing
/// the child it gave up on is not that child dying of its own accord.
pub fn claim() -> bool {
    !EXITING.swap(true, Ordering::SeqCst)
}

/// Why km is stopping.
#[derive(Debug, PartialEq, Eq)]
pub enum ExitReason {
    /// A termination signal arrived for km itself.
    Signal(String),
    /// A component never reported ready within its startup budget, so the
    /// stack never came up.
    Readiness {
        component: &'static str,
        detail: String,
    },
    /// A component was lost while running: its process went away, or its
    /// heartbeats did.
    Liveness {
        component: &'static str,
        detail: String,
    },
}

impl ExitReason {
    /// The one-line summary: what stopped km, and for a component failure
    /// which check it failed — readiness or liveness.
    pub fn headline(&self) -> String {
        match self {
            Self::Signal(signal) => format!("{signal} received"),
            Self::Readiness { component, .. } => format!("{component} failed readiness"),
            Self::Liveness { component, .. } => format!("{component} failed liveness"),
        }
    }

    /// The supporting line: what exactly was observed.
    pub fn detail(&self) -> &str {
        match self {
            Self::Signal(_) => "",
            Self::Readiness { detail, .. } | Self::Liveness { detail, .. } => detail,
        }
    }

    /// Whether this is a failure rather than an orderly stop.
    fn is_failure(&self) -> bool {
        !matches!(self, Self::Signal(_))
    }
}

/// Announce the reason on stdout and in the log. Callers tear down and exit;
/// this only reports.
pub fn report(reason: &ExitReason) {
    let headline = reason.headline();
    let detail = reason.detail();

    println!();
    println!("{PRODUCT} stopping: {headline}");
    if !detail.is_empty() {
        println!("  {detail}");
    }
    println!();

    if reason.is_failure() {
        error!(reason = %headline, detail, "Stopping");
    } else {
        info!(reason = %headline, "Stopping");
    }
}

#[cfg(test)]
mod tests;
