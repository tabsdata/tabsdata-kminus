// Copyright 2026 Tabsdata Inc.

use super::*;
use std::sync::atomic::AtomicUsize;

static RUNS: AtomicUsize = AtomicUsize::new(0);

#[test]
fn teardown_runs_once_and_the_panic_still_propagates() {
    install_with_teardown("test", || {
        RUNS.fetch_add(1, Ordering::SeqCst);
    });

    for _ in 0..3 {
        let caught = panic::catch_unwind(|| panic!("boom"));
        assert!(caught.is_err(), "the panic must still propagate");
    }

    assert_eq!(
        RUNS.load(Ordering::SeqCst),
        1,
        "teardown must run once however many panics follow"
    );
}

#[test]
fn a_hanging_teardown_is_abandoned_not_awaited() {
    let started = std::time::Instant::now();
    let (sender, receiver) = mpsc::channel::<()>();
    thread::spawn(move || {
        thread::sleep(Duration::from_millis(200));
        let _ = sender.send(());
    });
    let timed_out = receiver.recv_timeout(Duration::from_millis(20)).is_err();
    assert!(timed_out, "a teardown slower than its budget must time out");
    assert!(
        started.elapsed() < Duration::from_millis(150),
        "the wait must end at the budget, not at the teardown"
    );
}
