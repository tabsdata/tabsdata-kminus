#!/usr/bin/env python3

#
# Copyright 2026 Tabsdata Inc.
#

"""Fetch pre-built K8s binaries for the current platform and cache them.

Idempotent — skips if binaries are already cached. Platform is detected
from the host OS / arch, so the build does not have to manage it.

Binaries fetched:
  - kube-apiserver, etcd, kubectl        (via setup-envtest)
  - kube-controller-manager              (built from K8s source)

Usage:
    python fetch_k8s_deps.py <cache-folder> <k8s-version>
    e.g.: python fetch_k8s_deps.py ../cache 1.29
"""

import os
import platform as host_platform
import re
import shutil
import subprocess
import sys


def detect_platform():
    """Return (os_name, arch) for the build target, normalized to the naming
    the rest of the script (and setup-envtest) expects.

    Honors GOOS / GOARCH when set (the build exports them for cross builds);
    otherwise falls back to the running host. The values are already in the
    Go vocabulary the script uses (e.g. darwin, amd64, arm64)."""
    goos = os.environ.get("GOOS", "").strip()
    goarch = os.environ.get("GOARCH", "").strip()
    if goos and goarch:
        return goos, goarch

    os_name = host_platform.system().lower()
    machine = host_platform.machine().lower()
    arch = {
        "x86_64": "amd64",
        "aarch64": "arm64",
    }.get(machine, machine)
    return os_name, arch


def run(cmd, **kwargs):
    """Run a command, printing it and raising on failure."""
    print(f"  > {' '.join(cmd)}")
    return subprocess.run(cmd, check=True, **kwargs)


def ensure_setup_envtest():
    """Ensure setup-envtest is installed, return its path."""
    gobin = subprocess.check_output(["go", "env", "GOBIN"], text=True).strip()
    if not gobin:
        gopath = subprocess.check_output(["go", "env", "GOPATH"], text=True).strip()
        gobin = os.path.join(gopath.split(os.pathsep)[0], "bin")
    ext = ".exe" if sys.platform == "win32" else ""
    envtest_bin = os.path.join(gobin, f"setup-envtest{ext}")

    if not os.path.isfile(envtest_bin):
        print("Installing setup-envtest...")
        run([
            "go",
            "install",
            "sigs.k8s.io/controller-runtime/tools/setup-envtest@release-0.19",
        ])

    # Ensure gobin is on PATH for subprocess calls
    os.environ["PATH"] = gobin + os.pathsep + os.environ.get("PATH", "")
    return envtest_bin


def fetch_envtest_binaries(envtest_bin, k8s_version, os_name, arch, cache_dir):
    """Fetch kube-apiserver, etcd, kubectl via setup-envtest.

    Returns the full K8s version string (e.g., 'v1.29.5').
    """
    result = subprocess.check_output(
        [
            envtest_bin,
            "use",
            k8s_version,
            "--os",
            os_name,
            "--arch",
            arch,
            "-p",
            "path",
        ],
        text=True,
    ).strip()
    envtest_path = result

    for binary in ("kube-apiserver", "etcd", "kubectl"):
        for name in (binary, f"{binary}.exe"):
            src = os.path.join(envtest_path, name)
            if os.path.isfile(src):
                shutil.copy2(src, cache_dir)

    # Extract full version from path (e.g., .../1.29.5-darwin-arm64/ -> v1.29.5)
    dirname = os.path.basename(envtest_path)
    version = re.sub(r"-.*", "", dirname)
    return f"v{version}"


def clone_k8s_source(source_dir, full_version):
    """Clone K8s source if not already present (shared across platforms)."""
    k8s_dir = os.path.join(source_dir, "kubernetes")
    if os.path.isdir(k8s_dir):
        return k8s_dir

    print(f"Cloning kubernetes {full_version} source (shallow)...")
    os.makedirs(source_dir, exist_ok=True)
    run([
        "git",
        "clone",
        "--config",
        "core.longpaths=true",
        "--depth",
        "1",
        "--branch",
        full_version,
        "https://github.com/kubernetes/kubernetes.git",
        k8s_dir,
    ])
    return k8s_dir


def build_kube_controller_manager(k8s_source, full_version, os_name, arch, cache_dir):
    """Build kube-controller-manager from K8s source."""
    ext = ".exe" if os_name == "windows" else ""
    output = os.path.join(os.path.abspath(cache_dir), f"kube-controller-manager{ext}")

    print(f"Building kube-controller-manager {full_version} for {os_name}-{arch}...")
    env = os.environ.copy()
    env["CGO_ENABLED"] = "0"
    run(
        [
            "go",
            "build",
            "-C",
            k8s_source,
            "-trimpath",
            "-ldflags=-s -w",
            "-o",
            output,
            "./cmd/kube-controller-manager",
        ],
        env=env,
    )


def main():
    if len(sys.argv) < 3:
        print(f"Usage: {sys.argv[0]} <cache-folder> <k8s-version>")
        sys.exit(1)

    cache_dir = sys.argv[1]
    k8s_version = sys.argv[2]

    os_name, arch = detect_platform()
    platform = f"{os_name}-{arch}"
    platform_cache = os.path.join(cache_dir, platform)
    k8s_source_dir = os.path.join(cache_dir, "k8s-source")

    # Skip if already cached
    apiserver = os.path.join(platform_cache, "kube-apiserver")
    if os.path.isfile(apiserver) or os.path.isfile(apiserver + ".exe"):
        return

    print(f"Fetching K8s {k8s_version} binaries for {platform}...")
    os.makedirs(platform_cache, exist_ok=True)

    envtest_bin = ensure_setup_envtest()
    full_version = fetch_envtest_binaries(
        envtest_bin, k8s_version, os_name, arch, platform_cache
    )

    k8s_source = clone_k8s_source(k8s_source_dir, full_version)
    build_kube_controller_manager(
        k8s_source, full_version, os_name, arch, platform_cache
    )

    # Ensure all binaries are executable
    for entry in os.listdir(platform_cache):
        path = os.path.join(platform_cache, entry)
        if os.path.isfile(path):
            os.chmod(path, 0o755)

    print(f"Cached K8s binaries at {platform_cache}/")


if __name__ == "__main__":
    main()
