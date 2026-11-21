//
// Copyright 2026 Tabsdata Inc.
//

//! The log subscriber of the km crates' tests.
//!
//! Each crate's test binary installs it before any test runs, from a
//! constructor in its crate root, so no test has to ask for it. It sends the
//! km crates' debug traces to the test's captured output, which nextest
//! shows only for a failing test: a failure carries the timeline of what
//! led to it, with no rerun.
//!
//! The filter is fixed rather than read from `RUST_LOG`, which the build
//! sets to `error` for every process it starts.

const FILTER: &str = "warn,kcommon=debug,km=debug,pod_runner=debug";

/// Install the subscriber; a no-op when this process already has one.
pub fn install() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::new(FILTER))
        .with_timer(tracing_subscriber::fmt::time::uptime())
        .with_thread_ids(true)
        .with_ansi(false)
        .with_test_writer()
        .try_init();
}
