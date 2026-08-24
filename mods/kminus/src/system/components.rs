// Copyright 2026 Tabsdata Inc.

//! The components km has started, held as [`Tracked`] views.
//!
//! km spawns three components and has to answer two questions about them
//! from two places: is this one still alive (the process watcher, on its
//! own thread, while startup may still be gating on a later component),
//! and tear down whatever is up so far (a readiness failure from the
//! startup path, a signal from the km-signal thread). Both reach the same
//! registry; [`Tracked`] views being cheap clones, nothing juggles
//! handles.
//!
//! Liveness answered here comes from the monitor that owns each child —
//! never from the process table, where a recycled number can impersonate
//! a dead component — and carries the exit outcome, which for a component
//! that died without logging is often the only account of what happened:
//! on Windows an abnormal exit's code is the NTSTATUS that ended it, on
//! Unix the signal that did.

use std::path::Path;
use std::sync::{Arc, Mutex, OnceLock};

use kcommon::tracked::{Exit, Tracked};

use crate::system::pid::remove_pid;

/// The registry of the km run in this process, if there is one.
///
/// Set once at startup so the teardown paths can reach the components
/// rather than re-reading PID files. Absent in `km stop`, which runs in a
/// different process and owns nothing.
static CURRENT: OnceLock<Components> = OnceLock::new();

/// The components of one km run, in start order.
///
/// Cloning shares the same set: the startup path and the process watcher
/// hold their own clones of one registry.
#[derive(Clone, Default)]
pub struct Components(Arc<Mutex<Vec<(&'static str, Tracked)>>>);

impl Components {
    pub fn new() -> Self {
        Self::default()
    }

    /// Publish this registry as the run's, so teardown can find it.
    /// Idempotent; the first call wins.
    pub fn install(&self) {
        let _ = CURRENT.set(self.clone());
    }

    /// The registry this run installed, if any.
    pub fn current() -> Option<&'static Components> {
        CURRENT.get()
    }

    /// Take note of a freshly started component. Registered as soon as it
    /// is spawned, so the watcher covers it while startup is still gating
    /// on a later one.
    pub fn register(&self, name: &'static str, tracked: Tracked) {
        if let Ok(mut entries) = self.0.lock() {
            entries.push((name, tracked));
        }
    }

    /// The PID of a registered component, for display.
    pub fn pid(&self, name: &str) -> Option<u32> {
        let entries = self.0.lock().ok()?;
        entries
            .iter()
            .find(|(n, _)| *n == name)
            .map(|(_, t)| t.pid())
    }

    /// The first registered component that is no longer running, with how
    /// it went.
    pub fn first_failed(&self) -> Option<(&'static str, Exit)> {
        let entries = self.0.lock().ok()?;
        entries
            .iter()
            .find_map(|(name, tracked)| match tracked.state() {
                kcommon::tracked::State::Gone(exit) => Some((*name, exit)),
                kcommon::tracked::State::Running => None,
            })
    }

    /// Stop every registered component, most recently started first, with
    /// its own grace, and drop the PID files that named them. Returns the
    /// names handled, so a caller that also knows about components from
    /// PID files can skip the ones already dealt with here.
    ///
    /// Reverse order so a component is never left running against a
    /// dependency that has already gone. Callers must have claimed the
    /// exit first, so the watcher does not report these stops as a
    /// liveness failure.
    pub fn teardown(&self, pids: &Path) -> Vec<&'static str> {
        // Cloned out so no lock is held across a stop, which can last a
        // component's whole grace period.
        let entries: Vec<(&'static str, Tracked)> = match self.0.lock() {
            Ok(mut entries) => entries.drain(..).collect(),
            Err(_) => return Vec::new(),
        };
        let mut handled = Vec::with_capacity(entries.len());
        for (name, tracked) in entries.iter().rev() {
            tracked.stop(crate::shutdown_grace_for(name));
            remove_pid(pids, name);
            handled.push(*name);
        }
        handled
    }
}

#[cfg(test)]
mod tests;
