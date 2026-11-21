// Copyright 2026 Tabsdata Inc.

pub mod client;
pub mod config;
pub mod logic;
pub mod metrics;
pub mod pod;
pub mod process;
pub mod server;
pub mod spec;
pub mod store;
pub mod tls;
pub(crate) mod utils;

#[cfg(test)]
#[ctor::ctor(unsafe)]
fn install_test_tracing() {
    kcommon::test_tracing::install();
}

/// Package version from Cargo.toml.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Copyright notice.
pub const COPYRIGHT: &str = "Copyright 2026 Tabsdata Inc.";

/// License.
pub const LICENSE: &str = "License: Proprietary (all rights reserved)";

#[cfg(test)]
pub(crate) mod testutils;
