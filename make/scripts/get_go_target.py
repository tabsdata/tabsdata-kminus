#!/usr/bin/env python3

#
# Copyright 2026 Tabsdata Inc.
#

"""Resolve a Go build target (GOOS, GOARCH, binary extension) for the build.

The target is decided from the same environment variables the rest of the
build uses to pin a specific target, in this order:

  1. TD_TARGET   - the artifact target (e.g. the linux musl cross target);
                   authoritative when present.
  2. RUST_TARGET - the native target, always set in the CI matrix.
  3. host        - neither set (local development): let the toolchain decide.

The chosen Rust target triple is canonicalized with `rustc --print cfg`
(rustc is always available in this build), so there is no hand-maintained
triple table. Only the Rust -> Go vocabulary differs and is translated here.

Resolution traces are written to stderr; stdout carries ONLY the result so it
can be captured by the calling duckscript:
    <GOOS>
    <GOARCH>
    <extension>
An empty GOOS/GOARCH line means "let Go decide (host build)".

Usage:
    python get_go_target.py
"""

import os
import subprocess
import sys

OPERATING_SYSTEM_BY_RUST = {
    "macos": "darwin",
    "linux": "linux",
    "windows": "windows",
}

ARCHITECTURE_BY_RUST = {
    "x86_64": "amd64",
    "aarch64": "arm64",
}


def trace(message: str) -> None:
    """Emit a progress trace to stderr (stdout is reserved for the result)."""
    print(f"🔧 go_target: {message}", file=sys.stderr)


def select_triple(environment: dict[str, str]) -> str:
    """Pick the target triple from the build environment, by precedence."""
    td_target = (environment.get("TD_TARGET") or "").strip()
    if td_target:
        trace(f"Using target from TD_TARGET: '{td_target}'")
        return td_target

    rust_target = (environment.get("RUST_TARGET") or "").strip()
    if rust_target:
        trace(f"Using target from RUST_TARGET: '{rust_target}'")
        return rust_target

    trace("No TD_TARGET or RUST_TARGET set; resolving host target")
    return ""


def rust_cfg(triple: str) -> dict[str, str]:
    """Return the `rustc --print cfg` key/value pairs for a triple (host if empty)."""
    command = ["rustc", "--print", "cfg"]
    if triple:
        command += ["--target", triple]
    trace(f"Running: {' '.join(command)}")
    output = subprocess.check_output(command, text=True)
    cfg: dict[str, str] = {}
    for line in output.splitlines():
        if "=" in line:
            key, _, value = line.partition("=")
            cfg[key.strip()] = value.strip().strip('"')
    return cfg


def resolve(environment: dict[str, str]) -> tuple[str, str, str]:
    """Resolve (GOOS, GOARCH, extension) from the build environment."""
    triple = select_triple(environment)

    cfg = rust_cfg(triple)
    rust_operating_system = cfg.get("target_os", "")
    rust_architecture = cfg.get("target_arch", "")
    trace(
        f"rustc reports target_os='{rust_operating_system}',"
        f" target_arch='{rust_architecture}'"
    )

    if rust_operating_system not in OPERATING_SYSTEM_BY_RUST:
        raise ValueError(
            f"Unsupported target operating system '{rust_operating_system}'"
            f" (triple: '{triple or '<host>'}')"
        )
    if rust_architecture not in ARCHITECTURE_BY_RUST:
        raise ValueError(
            f"Unsupported target architecture '{rust_architecture}'"
            f" (triple: '{triple or '<host>'}')"
        )

    operating_system = OPERATING_SYSTEM_BY_RUST[rust_operating_system]
    architecture = ARCHITECTURE_BY_RUST[rust_architecture]
    extension = ".exe" if operating_system == "windows" else ""

    # When no target was requested, emit empty GOOS/GOARCH so the Go toolchain
    # uses its own host defaults; the extension still reflects the host.
    if not triple:
        trace(
            f"Host build: leaving GOOS/GOARCH empty for Go to decide"
            f" (extension '{extension or '<none>'}')"
        )
        return "", "", extension

    trace(
        f"Resolved GOOS='{operating_system}', GOARCH='{architecture}',"
        f" extension='{extension or '<none>'}'"
    )
    return operating_system, architecture, extension


def main() -> int:
    operating_system, architecture, extension = resolve(dict(os.environ))
    print(operating_system)
    print(architecture)
    print(extension)
    return 0


if __name__ == "__main__":
    sys.exit(main())
