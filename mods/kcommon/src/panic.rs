// Copyright 2026 Tabsdata Inc.

use std::backtrace::Backtrace;
use std::io::Write;
use std::panic;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

const TEARDOWN_BUDGET: Duration = Duration::from_secs(30);

static TEARDOWN_STARTED: AtomicBool = AtomicBool::new(false);

pub fn install(name: &'static str) {
    let previous = panic::take_hook();
    panic::set_hook(Box::new(move |info| {
        let _ = Backtrace::force_capture();
        report(name, info);
        previous(info);
    }));
}

/// [`install`], plus `teardown` before the panic proceeds.
///
/// `teardown` must be safe to call from any thread, must not panic, and
/// must not assume the process is consistent — whatever invariant the
/// panic broke may be one it depends on. Stopping children by recorded PID
/// qualifies; walking in-memory state built by the panicking path does not.
///
/// It runs at most once per process. Two panicking threads would otherwise
/// re-enter the same teardown, and two of those racing over the same PID
/// files ends worse than the panic did.
pub fn install_with_teardown<F>(name: &'static str, teardown: F)
where
    F: Fn() + Send + Sync + 'static,
{
    let teardown = Arc::new(teardown);
    let previous = panic::take_hook();
    panic::set_hook(Box::new(move |info| {
        let _ = Backtrace::force_capture();
        report(name, info);
        if !TEARDOWN_STARTED.swap(true, Ordering::SeqCst) {
            bounded(Arc::clone(&teardown));
        }
        previous(info);
    }));
}

/// Write the panic to stderr.
///
/// Deliberately not through `tracing`: a panicking process cannot be
/// relied upon to still have a working subscriber, and every km binary
/// already has its stderr captured into its log by whoever spawned it.
fn report(name: &'static str, info: &panic::PanicHookInfo<'_>) {
    let mut stderr = std::io::stderr().lock();
    let _ = writeln!(stderr, "{name} panicked: {info}");
    let _ = stderr.flush();
}

/// Run `teardown` on a helper thread, abandoning it after
/// [`TEARDOWN_BUDGET`].
///
/// Detached rather than joined: a teardown that outruns the budget is
/// waiting on a child that will not answer, and the abort that follows
/// takes it down regardless.
fn bounded<F>(teardown: Arc<F>)
where
    F: Fn() + Send + Sync + 'static,
{
    let (sender, receiver) = mpsc::channel();
    thread::spawn(move || {
        teardown();
        let _ = sender.send(());
    });
    if receiver.recv_timeout(TEARDOWN_BUDGET).is_err() {
        let mut stderr = std::io::stderr().lock();
        let _ = writeln!(
            stderr,
            "Teardown did not finish within {}s; abandoning it.",
            TEARDOWN_BUDGET.as_secs()
        );
        let _ = stderr.flush();
    }
}

#[cfg(test)]
mod tests;
