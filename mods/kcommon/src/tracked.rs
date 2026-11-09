// Copyright 2026 Tabsdata Inc.

//! A spawned child, owned by its monitor.
//!
//! # Ownership is the design
//!
//! Every crash and near-miss in the history behind this module came from
//! one shape: two paths sharing mutable access to a child's exit state —
//! a `wait` cancelled mid-flight, a `kill` re-entering, a handle polled
//! after something else had consumed it. Under `panic = "abort"` the last
//! of those took the whole pod-runner down in production.
//!
//! So here nothing shares. A monitor thread owns the `Child` exclusively:
//! it alone reaps, it alone signals on escalation, and it publishes the
//! outcome exactly once into a cell that everything else only reads.
//! [`Tracked`] is a cheap clonable view — a pid, a token, the cell, and a
//! command channel. Cancelling a caller cancels a read; the child's state
//! cannot be torn. There is no handle for a second path to poll.
//!
//! # What dropping is not
//!
//! Dropping every [`Tracked`] view does not kill the child; the monitor
//! keeps waiting so the child is reaped whenever it exits. That is the
//! right default for a supervisor whose children must outlive a closed
//! terminal. A child that must die with its parent is the spawner's
//! decision to arrange, and the kill token is what reclaims anything that
//! survives anyway.

use std::process::{Child, Command, ExitStatus};
use std::sync::mpsc::{Receiver, Sender};
use std::sync::{Arc, Condvar, Mutex, mpsc};
use std::time::{Duration, Instant};
use std::{io, thread};

use tracing::{debug, warn};

use crate::signal;
use crate::token::Run;

/// How often the monitor looks at its child and its mailbox. Supervision
/// latency, not correctness, rides on this.
const MONITOR_TICK: Duration = Duration::from_millis(20);

/// How a tracked child ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Exit {
    /// The exit code. On Windows an abnormal end arrives here too, as the
    /// NTSTATUS that ended the process — often the only account of a
    /// death that reached no log.
    Code(i32),
    /// Terminated by this signal. Unix only.
    Signal(i32),
    /// The child's state became unreadable, or the monitor itself failed.
    /// Treated as exited: erring the other way would mean signalling a
    /// number nothing vouches for.
    Unknown,
}

/// What a view sees right now.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum State {
    Running,
    Gone(Exit),
}

/// A view onto a spawned child. Clone freely; all views observe the same
/// outcome and may all issue commands.
#[derive(Clone)]
pub struct Tracked {
    pid: u32,
    token: String,
    cell: Arc<OutcomeCell>,
    commands: Sender<Cmd>,
}

impl std::fmt::Debug for Tracked {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Tracked")
            .field("pid", &self.pid)
            .field("token", &self.token)
            .field("state", &self.state())
            .finish()
    }
}

impl Tracked {
    /// Spawn `cmd` with its kill token in `var`, owned by a new monitor.
    /// Everything else about the command stays the caller's configuration.
    pub fn spawn(mut cmd: Command, var: &str, token: &Run) -> io::Result<Tracked> {
        cmd.env(var, token.as_str());
        Self::watch(Process::Std(cmd.spawn()?), token)
    }

    /// [`Self::spawn`], in a hidden console of its own that leads a new
    /// process group: what a long-lived Windows child needs for a graceful
    /// stop to reach it (see [`crate::console`]). Standard error goes to
    /// `stderr`; standard input and output to `NUL`.
    #[cfg(windows)]
    pub fn spawn_in_own_console(
        mut cmd: Command,
        stderr: Option<&std::fs::File>,
        var: &str,
        token: &Run,
    ) -> io::Result<Tracked> {
        cmd.env(var, token.as_str());
        let child = crate::console::OwnConsoleChild::spawn(&cmd, stderr)?;
        Self::watch(Process::OwnConsole(child), token)
    }

    fn watch(process: Process, token: &Run) -> io::Result<Tracked> {
        let pid = process.id();
        debug!(pid, token = token.as_str(), "Tracked child spawned");

        let (commands, mailbox) = mpsc::channel();
        let cell = Arc::new(OutcomeCell::default());
        let monitor_cell = Arc::clone(&cell);
        thread::Builder::new()
            .name(format!("tracked-{pid}"))
            .spawn(move || monitor(process, pid, mailbox, monitor_cell))?;

        Ok(Tracked {
            pid,
            token: token.as_str().to_string(),
            cell,
            commands,
        })
    }

    /// The OS process id — valid to display always, valid to signal only
    /// while the monitor says `Running`, which is why signalling is its job.
    pub fn pid(&self) -> u32 {
        self.pid
    }

    /// The kill token the child carries (and its descendants inherit).
    pub fn token(&self) -> &str {
        &self.token
    }

    pub fn state(&self) -> State {
        match self.cell.get() {
            Some(exit) => State::Gone(exit),
            None => State::Running,
        }
    }

    pub fn has_exited(&self) -> bool {
        self.cell.get().is_some()
    }

    /// Block until the child is gone; settled after the first return.
    /// Async callers wrap this in `spawn_blocking` — cancelling that
    /// cancels a read, never the monitoring itself.
    pub fn wait(&self) -> Exit {
        self.cell.wait()
    }

    /// [`Self::wait`], giving up after `timeout`.
    pub fn wait_timeout(&self, timeout: Duration) -> Option<Exit> {
        self.cell.wait_timeout(timeout)
    }

    /// Ask the child to stop, give it `grace`, then force-kill it; block
    /// until it is gone and return how it went.
    ///
    /// A settled child returns its outcome at once with nothing signalled
    /// — its number is no longer ours. Zero grace skips the asking;
    /// repeated stops from any view share one clock. This reaches the
    /// child and the group it leads; escapees beyond that are the kill
    /// token's job ([`crate::scan`]).
    pub fn stop(&self, grace: Duration) -> Exit {
        // A send can only fail because the monitor has finished, and it
        // publishes before finishing — so falling through to wait() is
        // returning the settled outcome.
        let _ = self.commands.send(Cmd::Stop { grace });
        self.wait()
    }
}

/// The child a monitor owns.
enum Process {
    Std(Child),
    #[cfg(windows)]
    OwnConsole(crate::console::OwnConsoleChild),
}

impl Process {
    fn id(&self) -> u32 {
        match self {
            Process::Std(child) => child.id(),
            #[cfg(windows)]
            Process::OwnConsole(child) => child.id(),
        }
    }

    fn try_wait(&mut self) -> io::Result<Option<ExitStatus>> {
        match self {
            Process::Std(child) => child.try_wait(),
            #[cfg(windows)]
            Process::OwnConsole(child) => child.try_wait(),
        }
    }

    fn kill(&mut self) -> io::Result<()> {
        match self {
            Process::Std(child) => child.kill(),
            #[cfg(windows)]
            Process::OwnConsole(child) => child.kill(),
        }
    }
}

/// Commands a view can send its monitor.
enum Cmd {
    Stop { grace: Duration },
}

/// The outcome, published once, read forever.
#[derive(Default)]
struct OutcomeCell {
    outcome: Mutex<Option<Exit>>,
    settled: Condvar,
}

impl OutcomeCell {
    fn get(&self) -> Option<Exit> {
        self.outcome.lock().expect("outcome lock").clone()
    }

    /// First writer wins; a settled outcome is never torn.
    fn set(&self, exit: Exit) {
        let mut outcome = self.outcome.lock().expect("outcome lock");
        if outcome.is_none() {
            *outcome = Some(exit);
            self.settled.notify_all();
        }
    }

    fn wait(&self) -> Exit {
        let mut outcome = self.outcome.lock().expect("outcome lock");
        loop {
            if let Some(exit) = outcome.clone() {
                return exit;
            }
            outcome = self.settled.wait(outcome).expect("outcome lock");
        }
    }

    fn wait_timeout(&self, timeout: Duration) -> Option<Exit> {
        let deadline = Instant::now() + timeout;
        let mut outcome = self.outcome.lock().expect("outcome lock");
        loop {
            if let Some(exit) = outcome.clone() {
                return Some(exit);
            }
            let now = Instant::now();
            if now >= deadline {
                return None;
            }
            let (guard, _) = self
                .settled
                .wait_timeout(outcome, deadline - now)
                .expect("outcome lock");
            outcome = guard;
        }
    }
}

/// Publishes `Unknown` if the monitor unwinds without having published,
/// so even a panic releases waiters and leaves the child unsignable.
struct PublishOnDrop(Arc<OutcomeCell>);

impl Drop for PublishOnDrop {
    fn drop(&mut self) {
        self.0.set(Exit::Unknown);
    }
}

/// The monitor: sole owner of the child.
///
/// A poll loop rather than a blocking `wait`, so one thread both reaps
/// and signals. That ordering is what makes the escalation safe with no
/// further checks: a signal is only ever sent between `try_wait` returning
/// `None` and the next reap attempt, and an unreaped child — running,
/// or zombie — still pins its number on both platforms.
fn monitor(mut child: Process, pid: u32, mailbox: Receiver<Cmd>, cell: Arc<OutcomeCell>) {
    let publish_guard = PublishOnDrop(Arc::clone(&cell));
    let mut deadline: Option<Instant> = None;
    let mut forced = false;

    loop {
        match child.try_wait() {
            Ok(Some(status)) => {
                let exit = exit_of(status);
                debug!(pid, ?exit, "Tracked child exited");
                cell.set(exit);
                drop(publish_guard);
                return;
            }
            Err(e) => {
                // Unreadable is unrecoverable; the kill token still names
                // whatever is left.
                warn!(pid, error = %e, "Cannot read tracked child's state");
                cell.set(Exit::Unknown);
                drop(publish_guard);
                return;
            }
            Ok(None) => {}
        }

        // Drained and disconnected read the same: monitoring continues
        // even with every view gone, so the child is always reaped.
        while let Ok(Cmd::Stop { grace }) = mailbox.try_recv() {
            if deadline.is_none() {
                if grace.is_zero() {
                    deadline = Some(Instant::now());
                } else {
                    debug!(pid, grace_secs = grace.as_secs(), "Asking child to stop");
                    signal::request_graceful(pid);
                    deadline = Some(Instant::now() + grace);
                }
            }
        }

        if let Some(d) = deadline
            && !forced
            && Instant::now() >= d
        {
            warn!(pid, "Grace expired, killing tracked child");
            // The tree before the handle on Windows: `taskkill /T` finds the
            // descendants through a live parent. Safe either way, because the
            // reap attempt is this loop's next iteration, so the child's
            // number is still pinned.
            #[cfg(windows)]
            signal::force_kill_group(pid);
            // Handle first on Unix (cannot misfire), then the group it leads.
            let _ = child.kill();
            #[cfg(unix)]
            if signal::leads_own_group(pid) {
                signal::force_kill_group(pid);
            }
            forced = true;
        }

        thread::sleep(MONITOR_TICK);
    }
}

fn exit_of(status: ExitStatus) -> Exit {
    if let Some(code) = status.code() {
        return Exit::Code(code);
    }
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;
        if let Some(sig) = status.signal() {
            return Exit::Signal(sig);
        }
    }
    Exit::Unknown
}

#[cfg(test)]
mod tests;
