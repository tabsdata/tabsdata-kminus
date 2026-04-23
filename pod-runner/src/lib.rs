// Copyright 2026 Tabsdata Inc.

pub mod process;

/// Package version from Cargo.toml.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Copyright notice.
pub const COPYRIGHT: &str = "Copyright 2026 Tabsdata Inc.";

/// License.
pub const LICENSE: &str = "License: Proprietary (all rights reserved)";

#[cfg(test)]
pub(crate) mod testutils;
