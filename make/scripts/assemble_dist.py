#
# Copyright 2026 Tabsdata Inc.
#

import os
import shutil
import subprocess
import sys
import time

COPY_TIMEOUT_SECONDS = 60
COPY_RETRY_INTERVAL_SECONDS = 2

GO_MODULES = ["control-plane", "virtual-kubelet"]
RUST_MODULES = [("pod-runner", "pod-runner"), ("kminus", "km")]
KUBERNETES_DEPENDENCIES = ["kube-apiserver", "kube-controller-manager", "etcd", "kubectl"]
KUBERNETES_RENAMES = {"kubectl": "kmc"}


def copy_verified(source, destination):
    deadline = time.monotonic() + COPY_TIMEOUT_SECONDS
    last_error = None
    while True:
        try:
            shutil.copy2(source, destination)
        except OSError as error:
            last_error = error
        if (
            os.path.isfile(destination)
            and os.path.getsize(destination) == os.path.getsize(source)
        ):
            return
        if time.monotonic() >= deadline:
            if os.path.isfile(destination) and os.path.getsize(destination) > 0:
                print(
                    f"Warning: Copied '{source}' -> '{destination}' but sizes "
                    f"differ ({os.path.getsize(destination)} != "
                    f"{os.path.getsize(source)}) after {COPY_TIMEOUT_SECONDS}s; "
                    f"keeping the copy."
                )
                return
            raise RuntimeError(
                f"Failed to copy '{source}' -> '{destination}' after "
                f"{COPY_TIMEOUT_SECONDS}s"
                + (f": {last_error}" if last_error else "")
            )
        print(
            f"   retrying copy '{source}' -> '{destination}' in "
            f"{COPY_RETRY_INTERVAL_SECONDS}s"
        )
        time.sleep(COPY_RETRY_INTERVAL_SECONDS)


def main():
    if len(sys.argv) < 5:
        print(
            "Usage: assemble_dist.py <km_root> <build_folder> <os_family> "
            "<check_script>"
        )
        sys.exit(1)

    km_root = sys.argv[1]
    build_folder = sys.argv[2]
    os_family = sys.argv[3]
    check_script = sys.argv[4]

    exe_suffix = ".exe" if os_family == "windows" else ""

    distribution_folder = os.path.join(km_root, "target", "dist")
    if os.path.isdir(distribution_folder):
        shutil.rmtree(distribution_folder)
    os.makedirs(distribution_folder, exist_ok=True)

    native_binaries = []

    for module in GO_MODULES:
        source = os.path.join(
            km_root, "mods", module, "target", f"{module}{exe_suffix}"
        )
        if os.path.isfile(source):
            destination = os.path.join(
                distribution_folder, f"{module}{exe_suffix}"
            )
            copy_verified(source, destination)
            print(f"Added Go binary '{destination}'")
            native_binaries.append(destination)
        else:
            print(f"Go binary not found (skipped): '{source}'")

    for module, binary in RUST_MODULES:
        source = os.path.join(
            km_root, "mods", module, "target", build_folder, f"{binary}{exe_suffix}"
        )
        if os.path.isfile(source):
            destination = os.path.join(
                distribution_folder, f"{binary}{exe_suffix}"
            )
            copy_verified(source, destination)
            print(f"Added Rust binary '{destination}'")
            native_binaries.append(destination)
        else:
            print(f"Rust binary not found (skipped): '{source}'")

    cache_folder = os.path.join(km_root, "mods", "control-plane", "cache")
    if os.path.isdir(cache_folder):
        for platform_folder in sorted(os.listdir(cache_folder)):
            platform_path = os.path.join(cache_folder, platform_folder)
            if not os.path.isdir(platform_path):
                continue
            for dependency in KUBERNETES_DEPENDENCIES:
                source = os.path.join(platform_path, f"{dependency}{exe_suffix}")
                if os.path.isfile(source):
                    name = KUBERNETES_RENAMES.get(dependency, dependency)
                    destination = os.path.join(
                        distribution_folder, f"{name}{exe_suffix}"
                    )
                    copy_verified(source, destination)

    if native_binaries:
        print(f"Native check: running check on {len(native_binaries)} binaries")
        subprocess.run(
            [sys.executable, check_script, "files", *native_binaries],
            check=True,
        )
    else:
        print("Native check: no km binaries collected; skipping check")

    print(f"Distribution folder: {distribution_folder}")


if __name__ == "__main__":
    main()
