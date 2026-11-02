// Copyright 2026 Tabsdata Inc.

use super::*;

#[test]
fn owner_stamp_is_fixed_width_utc() {
    assert_eq!(owner_stamp(0), "19700101T000000Z");
    // 2026-08-23T17:30:04Z, the shape an identity actually carries.
    assert_eq!(owner_stamp(1_787_506_204), "20260823T173004Z");
}

#[test]
fn owner_stamp_orders_chronologically_as_a_string() {
    // The format exists so a lexicographic compare is a chronological
    // one, which is what makes identities readable in a process list.
    assert!(owner_stamp(1_000_000) < owner_stamp(2_000_000));
}

#[test]
fn owner_stamp_falls_back_when_unrepresentable() {
    // Too large for the signed seconds chrono takes, so the conversion
    // refuses it rather than wrapping into a plausible pre-epoch stamp.
    assert_eq!(owner_stamp(u64::MAX), UNKNOWN_STAMP);
    // In range for i64, out of range for a calendar.
    assert_eq!(owner_stamp(i64::MAX as u64), UNKNOWN_STAMP);
    assert_ne!(owner_stamp(u64::MAX), owner_stamp(0));
}

#[test]
fn owner_identity_joins_pid_and_stamp() {
    assert_eq!(owner_identity(4711, 0), "4711@19700101T000000Z");
}

#[test]
fn parse_owner_accepts_a_well_formed_identity() {
    assert_eq!(
        parse_owner("4711@20260823T173004Z"),
        Some((4711, "20260823T173004Z"))
    );
}

#[test]
fn parse_owner_rejects_what_it_cannot_read() {
    // Each of these must read as "cannot verify" rather than as a valid
    // identity, or a caller would act on a stranger.
    assert_eq!(parse_owner("4711"), None);
    assert_eq!(parse_owner("notapid@20260823T173004Z"), None);
    assert_eq!(parse_owner(""), None);
    assert_eq!(parse_owner("@20260823T173004Z"), None);
}

#[test]
fn parse_owner_round_trips_what_owner_identity_builds() {
    let identity = owner_identity(4711, 1_787_506_204);
    let (pid, stamp) = parse_owner(&identity).expect("its own output must parse");
    assert_eq!(pid, 4711);
    assert_eq!(stamp, owner_stamp(1_787_506_204));
}

#[test]
fn is_unknown_stamp_recognises_only_the_fallback() {
    assert!(is_unknown_stamp(&owner_stamp(u64::MAX)));
    assert!(!is_unknown_stamp(&owner_stamp(0)));
}

#[test]
fn running_since_confirms_this_process_and_rejects_impostors() {
    let own = std::process::id();
    let start = process_start_time(own).unwrap().expect("own start time");

    assert!(running_since(own, &owner_stamp(start)).unwrap());
    // Right pid, wrong birth: what a recycled number looks like.
    assert!(!running_since(own, UNKNOWN_STAMP).unwrap());
    // A pid nothing holds.
    assert!(!running_since(u32::MAX - 1, &owner_stamp(start)).unwrap());
}

#[cfg(unix)]
#[test]
fn running_since_rejects_an_exited_child() {
    // Exited but deliberately not reaped. Linux keeps it in the process
    // table as a zombie, carrying its original start time; macOS drops it
    // outright. Either way it is not running — nothing can be asked of it
    // or waited for — and must not read as though it were.
    //
    // The child outlives the stamp read on purpose: with a command that
    // exits at once there is no window in which to read a start time, and
    // nothing to compare against afterwards.
    let mut child = std::process::Command::new("sleep")
        .arg("30")
        .stdout(std::process::Stdio::null())
        .spawn()
        .expect("spawn");
    let pid = child.id();
    let stamp = owner_stamp(
        process_start_time(pid)
            .unwrap()
            .expect("start time of a running child"),
    );
    assert!(
        running_since(pid, &stamp).unwrap(),
        "a live child must read as running",
    );

    child.kill().expect("kill");

    // Poll until the exit has landed in the process table.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while running_since(pid, &stamp).unwrap() {
        assert!(
            std::time::Instant::now() < deadline,
            "an unreaped exited child must stop reading as running",
        );
        std::thread::sleep(std::time::Duration::from_millis(10));
    }

    let _ = child.wait();
}

#[test]
fn parse_owner_ignores_a_uniqueness_suffix() {
    // The suffix is mint-time millis, not identity: verification must see
    // the bare stamp or it would never match the OS-reported start time.
    assert_eq!(
        parse_owner("4711@20260823T173004Z.718"),
        Some((4711, "20260823T173004Z"))
    );
    assert_eq!(
        parse_owner("4711@20260823T173004Z"),
        Some((4711, "20260823T173004Z"))
    );
}
