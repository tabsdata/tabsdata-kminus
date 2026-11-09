#
# Copyright 2026 Tabsdata Inc.
#
"""Shared integration test utilities for all subprojects.

Provides:
- binary / start_process / request_stop: find, start and stop km binaries
  the same way on every platform
- ControlPlaneProcess: manages a control-plane binary for tests
- find_free_port: find an available TCP port on localhost
- start_on_free_ports: start a server on free ports, again if one was taken
- make_test_dir / get_session_id: test directory helpers
"""

import os
import signal
import socket
import subprocess
import sys
import warnings
import time
from datetime import datetime, timezone

import pytest

def pytest_configure(config):
    # The control-plane writes kubeconfigs with insecure-skip-tls-verify: true. The
    # filter must go through pytest: it drops filters set while collecting tests.
    config.addinivalue_line(
        "filterwarnings", "ignore::urllib3.exceptions.InsecureRequestWarning"
    )


# ---------------------------------------------------------------------------
# Binaries and processes
# ---------------------------------------------------------------------------


def binary(bin_dir: str, name: str) -> str:
    """Path of the km binary `name` in `bin_dir`; fails when it is missing."""
    path = os.path.join(bin_dir, name + (".exe" if sys.platform == "win32" else ""))
    if not os.path.isfile(path):
        pytest.fail(f"{name} binary not found at {path}")
    return path


def start_process(args, **kwargs) -> subprocess.Popen:
    """Start a binary that request_stop can stop gracefully.

    On Windows that takes a process group of its own: a CTRL_BREAK is aimed
    at a group, and the child shares this process's console for it to travel.
    """
    if sys.platform == "win32":
        kwargs["creationflags"] = (
            kwargs.get("creationflags", 0) | subprocess.CREATE_NEW_PROCESS_GROUP
        )
    return subprocess.Popen(args, **kwargs)


def request_stop(process: subprocess.Popen) -> None:
    """Ask a process started by start_process to shut down: SIGTERM, or
    CTRL_BREAK on Windows, which every km binary handles the same way."""
    if sys.platform == "win32":
        process.send_signal(signal.CTRL_BREAK_EVENT)
    else:
        process.send_signal(signal.SIGTERM)


# ---------------------------------------------------------------------------
# Session id
# ---------------------------------------------------------------------------

_session_id: str | None = None


def get_session_id() -> str:
    global _session_id
    if _session_id is None:
        _session_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S.%f")[:-3]
        # xdist workers start within the same millisecond; without their id
        # they would share session folders and each other's servers.
        worker = os.environ.get("PYTEST_XDIST_WORKER")
        if worker:
            _session_id = f"{_session_id}.{worker}"
    return _session_id


# ---------------------------------------------------------------------------
# Test directory helpers
# ---------------------------------------------------------------------------


def make_test_dir(node_id: str, target_dir: str) -> str:
    """Create a unique test directory under target_dir/testrun.<ts>/..."""
    ts = get_session_id()
    rel = node_id
    for prefix in ("bin/", "test_"):
        idx = rel.find(prefix)
        if idx >= 0:
            rel = rel[idx + len(prefix):]
            break
    rel = rel.replace(".py::", "/").replace("::", "/")
    test_path = os.path.join(target_dir, f"testrun.{ts}", rel)
    os.makedirs(test_path, exist_ok=True)
    return test_path


# ---------------------------------------------------------------------------
# Port helpers
# ---------------------------------------------------------------------------


def find_free_port():
    """Find a free TCP port on localhost."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


_PORT_TAKEN = (
    "already in use",
    "only one usage of each socket address",
)


def start_on_free_ports(start, count, attempts=4):
    """Return start(*ports) run on `count` freshly found free ports.

    A port is only free when it is found: another process can take it before
    the server binds it, more so with parallel workers. When the server
    reports exactly that, it is started again on new ports.
    """
    for attempt in range(1, attempts + 1):
        try:
            return start(*(find_free_port() for _ in range(count)))
        except (RuntimeError, TimeoutError) as error:
            taken = any(marker in str(error).lower() for marker in _PORT_TAKEN)
            if not taken or attempt == attempts:
                raise


# ---------------------------------------------------------------------------
# ControlPlaneProcess
# ---------------------------------------------------------------------------


class ControlPlaneProcess:
    """Manages a control-plane process for testing."""

    def __init__(self, binary, work_dir, port, kcm_port, extra_args=None):
        self.binary = binary
        self.work_dir = work_dir
        self.port = port
        self.kcm_port = kcm_port
        self.extra_args = extra_args or []
        self.process = None

    def start(self, timeout=30):
        args = [
            self.binary,
            "-port", str(self.port),
            "-kcm-port", str(self.kcm_port),
            "-workdir", self.work_dir,
        ] + self.extra_args

        self.process = start_process(
            args,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )

        deadline = time.time() + timeout
        output_lines = []
        while time.time() < deadline:
            line = self.process.stdout.readline()
            if not line:
                if self.process.poll() is not None:
                    raise RuntimeError(
                        f"control-plane exited with code {self.process.returncode}\n"
                        f"output: {''.join(l.decode() for l in output_lines)}"
                    )
                time.sleep(0.1)
                continue
            output_lines.append(line)
            if b"Cluster is ready!" in line:
                return
        raise TimeoutError(
            f"control-plane did not become ready within {timeout}s\n"
            f"output: {''.join(l.decode() for l in output_lines)}"
        )

    def stop(self, timeout=15):
        if self.process is None:
            return
        request_stop(self.process)
        try:
            self.process.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            self.process.kill()
            self.process.wait(timeout=5)

    @property
    def pid(self):
        return self.process.pid if self.process else None

    @property
    def config_dir(self):
        return os.path.join(self.work_dir, "config")

    @property
    def log_dir(self):
        return os.path.join(self.work_dir, "log")

    @property
    def data_dir(self):
        return os.path.join(self.work_dir, "data", "control-plane")

    @property
    def pids_dir(self):
        return os.path.join(self.data_dir, "pids")

    @property
    def kubeconfig(self):
        return os.path.join(self.config_dir, "kubeconfig.yaml")

    @property
    def vk_kubeconfig(self):
        return os.path.join(self.config_dir, "vk-kubeconfig.yaml")
