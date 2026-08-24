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
fn generated_seeds_are_valid_and_distinct_across_time() {
    let seed = Seed::generate();
    assert_eq!(Seed::parse(seed.as_str()).unwrap(), seed);
    // Same process, so uniqueness rides on the stamp; two generations a
    // few ms apart must differ.
    std::thread::sleep(std::time::Duration::from_millis(5));
    assert_ne!(Seed::generate(), seed);
}

#[test]
fn seed_parse_rejects_what_would_corrupt_a_token() {
    for bad in ["", "  ", "has/slash", "has@at", "has space", "tab\there"] {
        assert!(Seed::parse(bad).is_err(), "{bad:?} must be rejected");
    }
    // Trimmed, so a trailing newline from a hand-edited file is fine.
    assert!(Seed::parse("20260824T031500.123Z-42\n").is_ok());
}

#[test]
fn seed_round_trips_through_its_file() {
    let path = tempfile("roundtrip");
    let seed = Seed::generate();
    seed.save(&path).unwrap();
    assert_eq!(Seed::load(&path).unwrap(), Some(seed));
    std::fs::remove_file(&path).ok();
}

#[test]
fn seed_load_distinguishes_missing_from_invalid() {
    // Missing: the first run ever. Not an error.
    assert_eq!(Seed::load(&tempfile("never-written")).unwrap(), None);

    // Invalid: something wrote garbage where reclamation state lives.
    // Skipping cleanup silently would be the wrong reading of that.
    let path = tempfile("garbage");
    std::fs::write(&path, "not/a/seed").unwrap();
    assert!(Seed::load(&path).is_err());
    std::fs::remove_file(&path).ok();
}

#[test]
fn seed_save_creates_missing_parents() {
    let dir = tempfile("nested");
    let path = dir.join("deeper").join("seed");
    Seed::generate().save(&path).unwrap();
    assert!(path.exists());
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn run_tokens_nest_under_the_seed_and_carry_this_identity() {
    let seed = Seed::parse("20260824T031500.123Z-42").unwrap();
    let run = Run::of(&seed);

    assert!(run.as_str().starts_with(&seed.as_prefix()));
    let owner = owner_of(run.as_str()).expect("owner field");
    let (pid, stamp) = identity::parse_owner(owner).expect("pid@stamp");
    assert_eq!(pid, std::process::id());
    assert!(
        !identity::is_unknown_stamp(stamp),
        "own start time must be readable"
    );
}

#[test]
fn scopes_extend_and_prefix_select() {
    let seed = Seed::parse("s1").unwrap();
    let run = Run::of(&seed);
    let pod = run.scope("db-pod_uid-1");
    let container = pod.scope("postgres");

    // The fields nest, so a prefix at any level selects everything below.
    assert!(container.as_str().starts_with(pod.as_prefix()));
    assert!(container.as_str().starts_with(run.as_prefix()));
    assert!(container.as_str().starts_with(&seed.as_prefix()));

    // And a *different* pod under the same run is not selected by this
    // pod's prefix — the whole point of scoping.
    let other = run.scope("db-pod_uid-2");
    assert!(!other.as_str().starts_with(container.as_prefix()));
}

#[test]
fn owner_of_reads_the_second_field_only() {
    assert_eq!(
        owner_of("seed/4711@20260823T173004Z"),
        Some("4711@20260823T173004Z")
    );
    assert_eq!(
        owner_of("seed/4711@20260823T173004Z/pod/container"),
        Some("4711@20260823T173004Z")
    );
    assert_eq!(owner_of("just-a-seed"), None);
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
