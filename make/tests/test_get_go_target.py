#
# Copyright 2026 Tabsdata Inc.
#

"""Unit tests for get_go_target.resolve / select_triple.

The `rustc --print cfg` call is stubbed so the tests do not depend on which
Rust targets happen to be installed on the machine running them.
"""

import importlib.util
import os

import pytest

MODULE_PATH = os.path.join(
    os.path.dirname(os.path.dirname(__file__)), "scripts", "get_go_target.py"
)

spec = importlib.util.spec_from_file_location("get_go_target", MODULE_PATH)
go_target = importlib.util.module_from_spec(spec)
spec.loader.exec_module(go_target)


# Minimal stand-in for `rustc --print cfg`, keyed by target triple. The empty
# string key represents the host build (rustc invoked without --target).
RUST_CFG_BY_TRIPLE = {
    "x86_64-unknown-linux-musl": {"target_os": "linux", "target_arch": "x86_64"},
    "x86_64-unknown-linux-gnu": {"target_os": "linux", "target_arch": "x86_64"},
    "aarch64-unknown-linux-musl": {"target_os": "linux", "target_arch": "aarch64"},
    "aarch64-apple-darwin": {"target_os": "macos", "target_arch": "aarch64"},
    "x86_64-apple-darwin": {"target_os": "macos", "target_arch": "x86_64"},
    "x86_64-pc-windows-msvc": {"target_os": "windows", "target_arch": "x86_64"},
    "": {"target_os": "macos", "target_arch": "aarch64"},  # host
}


@pytest.fixture(autouse=True)
def stub_rustc(monkeypatch):
    def fake_rust_cfg(triple):
        if triple not in RUST_CFG_BY_TRIPLE:
            raise AssertionError(f"unexpected triple queried: {triple!r}")
        return dict(RUST_CFG_BY_TRIPLE[triple])

    monkeypatch.setattr(go_target, "rust_cfg", fake_rust_cfg)


class TestSelectTriple:
    """Precedence: TD_TARGET > RUST_TARGET > host (empty)."""

    def test_prefers_td_target(self):
        environment = {
            "TD_TARGET": "x86_64-unknown-linux-musl",
            "RUST_TARGET": "x86_64-unknown-linux-gnu",
        }
        assert go_target.select_triple(environment) == "x86_64-unknown-linux-musl"

    def test_falls_back_to_rust_target(self):
        environment = {"RUST_TARGET": "aarch64-apple-darwin"}
        assert go_target.select_triple(environment) == "aarch64-apple-darwin"

    def test_host_when_neither_set(self):
        assert go_target.select_triple({}) == ""

    def test_blank_values_are_ignored(self):
        environment = {"TD_TARGET": "  ", "RUST_TARGET": "x86_64-apple-darwin"}
        assert go_target.select_triple(environment) == "x86_64-apple-darwin"


class TestResolve:
    """Rust triple -> (GOOS, GOARCH, extension)."""

    def test_linux_musl_cross(self):
        environment = {"TD_TARGET": "x86_64-unknown-linux-musl"}
        assert go_target.resolve(environment) == ("linux", "amd64", "")

    def test_linux_arm(self):
        environment = {"RUST_TARGET": "aarch64-unknown-linux-musl"}
        assert go_target.resolve(environment) == ("linux", "arm64", "")

    def test_macos_arm(self):
        environment = {"TD_TARGET": "aarch64-apple-darwin"}
        assert go_target.resolve(environment) == ("darwin", "arm64", "")

    def test_macos_intel(self):
        environment = {"RUST_TARGET": "x86_64-apple-darwin"}
        assert go_target.resolve(environment) == ("darwin", "amd64", "")

    def test_windows_gets_exe_extension(self):
        environment = {"TD_TARGET": "x86_64-pc-windows-msvc"}
        assert go_target.resolve(environment) == ("windows", "amd64", ".exe")

    def test_td_target_wins_over_rust_target(self):
        environment = {
            "TD_TARGET": "x86_64-pc-windows-msvc",
            "RUST_TARGET": "aarch64-apple-darwin",
        }
        assert go_target.resolve(environment) == ("windows", "amd64", ".exe")

    def test_host_leaves_goos_goarch_empty(self):
        # Host cfg is macos/aarch64 here, so extension is empty but GOOS/GOARCH
        # are blank to let the Go toolchain pick its own host defaults.
        assert go_target.resolve({}) == ("", "", "")


class TestUnsupportedTargets:
    """Targets rustc reports but Go mapping does not cover must fail loudly."""

    def test_unsupported_operating_system(self, monkeypatch):
        monkeypatch.setattr(
            go_target,
            "rust_cfg",
            lambda triple: {"target_os": "freebsd", "target_arch": "x86_64"},
        )
        with pytest.raises(ValueError, match="operating system"):
            go_target.resolve({"TD_TARGET": "x86_64-unknown-freebsd"})

    def test_unsupported_architecture(self, monkeypatch):
        monkeypatch.setattr(
            go_target,
            "rust_cfg",
            lambda triple: {"target_os": "linux", "target_arch": "riscv64"},
        )
        with pytest.raises(ValueError, match="architecture"):
            go_target.resolve({"TD_TARGET": "riscv64gc-unknown-linux-gnu"})
