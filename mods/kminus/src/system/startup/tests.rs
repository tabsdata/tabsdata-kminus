// Copyright 2026 Tabsdata Inc.

use super::*;

#[test]
fn the_outcome_words_are_stable() {
    assert_eq!(Outcome::Ready.as_str(), "ready");
    assert_eq!(Outcome::Failed.as_str(), "failed");
}

#[test]
fn the_marker_prefix_is_stable() {
    assert_eq!(MARKER_PREFIX, "K- startup complete:");
}

#[test]
fn a_marker_line_starts_with_the_prefix_and_names_the_outcome() {
    for outcome in [Outcome::Ready, Outcome::Failed] {
        let line = format!("{MARKER_PREFIX} {outcome}");
        assert!(line.starts_with(MARKER_PREFIX));
        assert_eq!(
            line.strip_prefix(MARKER_PREFIX).unwrap().trim(),
            outcome.as_str()
        );
    }
}
