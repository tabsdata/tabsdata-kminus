// Copyright 2026 Tabsdata Inc.

pub mod pod;
pub mod process;
pub mod spec;

/// Package version from Cargo.toml.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Copyright notice.
pub const COPYRIGHT: &str = "Copyright 2026 Tabsdata Inc.";

/// License.
pub const LICENSE: &str = "License: Proprietary (all rights reserved)";

#[cfg(test)]
pub(crate) mod testutils;
