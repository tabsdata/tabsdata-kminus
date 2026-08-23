// Copyright 2026 Tabsdata Inc.

use super::*;

#[test]
fn resolve_bin_dir_returns_a_path() {
    let bin = resolve_bin_dir();
    assert!(bin.is_absolute());
}
