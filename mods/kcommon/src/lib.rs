//
// Copyright 2026 Tabsdata Inc.
//

//! Components shared between the km binaries.

pub mod console;
pub mod identity;
pub mod panic;
pub mod scan;
pub mod signal;
pub mod stopevent;
#[cfg(any(test, feature = "test-tracing"))]
pub mod test_tracing;
pub mod token;
pub mod tracked;

#[cfg(test)]
#[ctor::ctor(unsafe)]
fn install_test_tracing() {
    test_tracing::install();
}
