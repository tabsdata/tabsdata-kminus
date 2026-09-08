// Copyright 2026 Tabsdata Inc.

use std::fmt;
use std::io::Write;
use std::sync::atomic::{AtomicBool, Ordering};

pub const MARKER_PREFIX: &str = "K- startup complete:";

static ANNOUNCED: AtomicBool = AtomicBool::new(false);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    Ready,
    Failed,
}

impl Outcome {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Ready => "ready",
            Self::Failed => "failed",
        }
    }
}

impl fmt::Display for Outcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

pub fn announce(outcome: Outcome) {
    if ANNOUNCED.swap(true, Ordering::SeqCst) {
        return;
    }
    println!("{MARKER_PREFIX} {outcome}");
    let _ = std::io::stdout().flush();
}

#[cfg(test)]
mod tests;
