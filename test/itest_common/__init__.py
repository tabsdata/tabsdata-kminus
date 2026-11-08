#
# Copyright 2026 Tabsdata Inc.
#
"""Shared integration test utilities for all subprojects.

Provides:
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


def pytest_collection_modifyitems(config, items):
    if sys.platform != "win32":
        return
    # Tracked gap: the suites start the binaries by their POSIX names and stop
    # them with POSIX signals; skipping says so instead of "binary not found".
    skip = pytest.mark.skip(
        reason="km integration tests are not ported to Windows: POSIX binary names and signals"
    )
    for item in items:
        item.add_marker(skip)


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

        self.process = subprocess.Popen(
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
        self.process.send_signal(signal.SIGTERM)
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
