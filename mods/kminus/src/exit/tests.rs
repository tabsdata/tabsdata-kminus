// Copyright 2026 Tabsdata Inc.

use super::*;

#[test]
fn signal_headline_names_the_signal_and_has_no_detail() {
    let reason = ExitReason::Signal("SIGTERM".into());
    assert_eq!(reason.headline(), "SIGTERM received");
    assert_eq!(reason.detail(), "");
    assert!(!reason.is_failure());
}

#[test]
fn readiness_headline_names_the_component_and_the_check() {
    let reason = ExitReason::Readiness {
        component: "virtual-kubelet",
        detail: "did not report ready within 60s".into(),
    };
    assert_eq!(reason.headline(), "virtual-kubelet failed readiness");
    assert_eq!(reason.detail(), "did not report ready within 60s");
    assert!(reason.is_failure());
}

#[test]
fn liveness_headline_distinguishes_itself_from_readiness() {
    let reason = ExitReason::Liveness {
        component: "pod-runner",
        detail: "missed 4 consecutive heartbeats".into(),
    };
    assert_eq!(reason.headline(), "pod-runner failed liveness");
    assert_eq!(reason.detail(), "missed 4 consecutive heartbeats");
    assert!(reason.is_failure());
}

// Order-independent: whichever test claims first, a second claim must fail —
// that is the invariant the single-owner exit depends on.
#[test]
fn the_exit_can_only_be_claimed_once() {
    let _ = claim();
    assert!(!claim(), "a second claim must not take the exit");
    assert!(in_progress());
}
