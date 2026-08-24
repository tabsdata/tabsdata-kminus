// Copyright 2026 Tabsdata Inc.

use std::process::Command;

use super::*;
use crate::identity;

fn tempfile(name: &str) -> std::path::PathBuf {
    static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    std::env::temp_dir().join(format!("km-token-{}-{}-{}", std::process::id(), n, name))
}

#[test]
fn current_run_root_carries_this_identity_plus_millis() {
    let run = Run::current();
    let (pid, stamp) = identity::parse_owner(run.as_str()).expect("pid@stamp");
    assert_eq!(pid, std::process::id());
    assert!(
        !identity::is_unknown_stamp(stamp),
        "own start time must be readable"
    );
    // The uniqueness suffix: fixed-width millis after the stamp, so no
    // run root can be a string-prefix of a different run's root.
    let (_, tail) = run.as_str().split_once('.').expect("millis suffix");
    assert_eq!(tail.len(), 3, "fixed width: {tail:?}");
    assert!(tail.chars().all(|c| c.is_ascii_digit()));
}

#[test]
fn run_parse_rejects_what_would_corrupt_a_sweep() {
    for bad in ["", "  ", "no-owner-sep", "has ws@x", "a@b/scope"] {
        assert!(Run::parse(bad).is_err(), "{bad:?} must be rejected");
    }
    // Trimmed, so a trailing newline from a hand-edited file is fine.
    assert!(Run::parse("4711@20260823T173004Z.718\n").is_ok());
}

#[test]
fn run_round_trips_through_its_file() {
    let path = tempfile("roundtrip");
    let run = Run::current();
    run.save(&path).unwrap();
    assert_eq!(Run::load(&path).unwrap(), Some(run));
    std::fs::remove_file(&path).ok();
}

#[test]
fn run_load_distinguishes_missing_from_invalid() {
    // Missing: the first run ever. Not an error.
    assert_eq!(Run::load(&tempfile("never-written")).unwrap(), None);

    // Invalid: something wrote garbage where reclamation state lives.
    // Skipping cleanup silently would be the wrong reading of that.
    let path = tempfile("garbage");
    std::fs::write(&path, "not a run@token/with-scope").unwrap();
    assert!(Run::load(&path).is_err());
    std::fs::remove_file(&path).ok();
}

#[test]
fn run_save_creates_missing_parents() {
    let dir = tempfile("nested");
    let path = dir.join("deeper").join("run");
    Run::current().save(&path).unwrap();
    assert!(path.exists());
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn scopes_extend_and_prefix_select() {
    let run = Run::parse("4711@20260823T173004Z.718").unwrap();
    let pod = run.scope("db-pod_uid-1");
    let container = pod.scope("postgres");

    // The fields nest, so a prefix at any level selects everything below.
    assert!(container.as_str().starts_with(pod.as_prefix()));
    assert!(container.as_str().starts_with(run.as_prefix()));

    // And a *different* pod under the same run is not selected by this
    // pod's prefix — the whole point of scoping.
    let other = run.scope("db-pod_uid-2");
    assert!(!other.as_str().starts_with(container.as_prefix()));

    // Nor is a same-second sibling run's tree selected by this run's
    // prefix: the fixed-width millis keep the roots from nesting.
    let sibling = Run::parse("4711@20260823T173004Z.719").unwrap();
    assert!(!sibling.scope("x").as_str().starts_with(run.as_prefix()));
}

#[test]
fn owner_of_reads_the_root_field() {
    assert_eq!(
        owner_of("4711@20260823T173004Z.718/pod/container"),
        "4711@20260823T173004Z.718"
    );
    assert_eq!(owner_of("4711@20260823T173004Z"), "4711@20260823T173004Z");
}

#[test]
fn carry_kill_tokens_reapplies_the_allowlist_and_nothing_else() {
    let mut cmd = Command::new("true");
    cmd.env_clear();
    carry_kill_tokens(&mut cmd, |key| match key {
        KM_KILL_TOKEN_VAR => Some("s/1@x".to_string()),
        // A neighbouring TD_ variable stays behind: allowlist, not prefix.
        _ => None,
    });

    let envs: Vec<(String, Option<String>)> = cmd
        .get_envs()
        .map(|(k, v)| {
            (
                k.to_string_lossy().into_owned(),
                v.map(|v| v.to_string_lossy().into_owned()),
            )
        })
        .collect();
    assert_eq!(
        envs,
        vec![(KM_KILL_TOKEN_VAR.to_string(), Some("s/1@x".to_string()))]
    );
}
