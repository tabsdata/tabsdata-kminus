// Copyright 2026 Tabsdata Inc.

use super::*;

#[test]
fn a_folder_always_yields_the_same_name() {
    assert_eq!(
        stop_event_name("C:\\instances\\tabsdata\\cluster"),
        stop_event_name("C:\\instances\\tabsdata\\cluster"),
    );
}

#[test]
fn separators_case_and_trailing_slashes_do_not_change_the_name() {
    let canonical = stop_event_name("C:\\Instances\\Tabsdata\\cluster");
    for variant in [
        "C:/Instances/Tabsdata/cluster",
        "c:\\instances\\tabsdata\\cluster",
        "C:\\Instances\\Tabsdata\\cluster\\",
        "C:/Instances/Tabsdata/cluster/",
    ] {
        assert_eq!(canonical, stop_event_name(variant), "variant: {variant}");
    }
}

#[test]
fn different_instances_yield_different_names() {
    assert_ne!(
        stop_event_name("C:\\instances\\one\\cluster"),
        stop_event_name("C:\\instances\\two\\cluster"),
    );
}

#[test]
fn the_name_is_a_valid_local_object_name() {
    let name = stop_event_name("C:\\instances\\tabsdata\\cluster");
    assert!(name.starts_with("Local\\"), "name: {name}");
    assert!(
        !name["Local\\".len()..].contains('\\'),
        "an object name may hold no further separator: {name}"
    );
    assert!(name.len() < 260, "name: {name}");
}

#[test]
fn the_fingerprint_is_pinned() {
    assert_eq!(
        stop_event_name("C:\\instances\\tabsdata\\cluster"),
        "Local\\tabsdata-km-stop-a2b0536564787962",
    );
}
