// Copyright 2026 Tabsdata Inc.

use std::path::PathBuf;

use super::*;

#[test]
fn find_in_path_returns_none_for_nonexistent_name() {
    let missing = "definitely-not-a-real-binary-xyzqwerty-12345";
    assert!(find_in_path(missing).is_none());
}

#[test]
fn resolve_binary_returns_bare_name_when_unresolvable() {
    let missing = "definitely-not-a-real-binary-xyzqwerty-12345";
    let resolved = resolve_binary(missing);
    let expected = PathBuf::from(with_exe_extension(missing));
    assert_eq!(resolved, expected);
    assert!(!resolved.is_absolute());
}

#[cfg(windows)]
#[test]
fn with_exe_extension_appends_exe_on_windows() {
    assert_eq!(with_exe_extension("foo"), "foo.exe");
    assert_eq!(with_exe_extension("foo.exe"), "foo.exe");
    assert_eq!(with_exe_extension("foo.EXE"), "foo.EXE");
}

#[cfg(not(windows))]
#[test]
fn with_exe_extension_is_identity_on_unix() {
    assert_eq!(with_exe_extension("foo"), "foo");
    assert_eq!(with_exe_extension("foo.exe"), "foo.exe");
}
