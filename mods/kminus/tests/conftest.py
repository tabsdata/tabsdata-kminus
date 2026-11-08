#
# Copyright 2026 Tabsdata Inc.
#

import os
import signal
import subprocess
import time

import pytest

from itest_common import find_free_port, get_session_id, pytest_configure  # noqa: F401
from kminus_testlib import TARGET_DIR, make_test_dir


@pytest.fixture
def test_dir(request) -> str:
    return make_test_dir(request.node.nodeid)


@pytest.fixture(scope="session")
def bin_dir():
    env_val = os.environ.get("TD_BIN_FOLDER")
    if env_val:
        return env_val
    raise RuntimeError("TD_BIN_FOLDER environment variable must be set")


@pytest.fixture(scope="session")
def km_bin(bin_dir):
    path = os.path.join(bin_dir, "km")
    if not os.path.isfile(path):
        pytest.skip(f"km binary not found at {path}")
    return path


class KminusProcess:
    """Manages a km process for testing."""

    def __init__(self, binary, work_dir):
        self.binary = binary
        # km create requires the workdir not to exist, but pytest's
        # test_dir fixture has already created `work_dir`. Use a
        # subdirectory as the actual kminus instance folder.
        self.work_dir = os.path.join(work_dir, "km")
        self.process = None

    def _ensure_created(self):
        """Run `km create` if the instance has not been initialized yet."""
        sysconfig = os.path.join(
            self.work_dir, "config", "kmsysconfig.yaml"
        )
        if os.path.isfile(sysconfig):
            return
        # Use --portstart 0 so km picks random free ports at every start.
        subprocess.run(
            [
                self.binary, "create",
                "--workdir", self.work_dir,
                "--portstart", "0",
            ],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

    def start(self, timeout=90):
        self._ensure_created()
        args = [
            self.binary, "start",
            "--workdir", self.work_dir,
        ]

        # Note the log file's pre-existing size so we only scan lines
        # written by THIS km process. Restart tests reuse the same
        # workdir; without this, start() would see a stale
        # "K- ready" line from the previous km and return
        # before the new km actually finishes booting.
        log_path = os.path.join(self.work_dir, "log", "km.log")
        last_pos = os.path.getsize(log_path) if os.path.isfile(log_path) else 0

        # km writes its own log to <workdir>/log/km.log via the rotating
        # file appender (no longer to stdout).
        # Force info-level logs so the "K- ready" startup line we poll
        # for below is emitted. cargo-make sets RUST_LOG=error in
        # output.env, which would otherwise suppress it.
        env = {**os.environ, "RUST_LOG": "info"}
        self.process = subprocess.Popen(
            args,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            env=env,
        )

        deadline = time.time() + timeout
        while time.time() < deadline:
            if self.process.poll() is not None:
                tail = ""
                if os.path.isfile(log_path):
                    try:
                        with open(log_path) as f:
                            tail = f.read()[-2000:]
                    except OSError:
                        pass
                raise RuntimeError(
                    f"km exited with code {self.process.returncode}\n"
                    f"log tail: {tail}"
                )
            if os.path.isfile(log_path):
                try:
                    with open(log_path) as f:
                        f.seek(last_pos)
                        chunk = f.read()
                        last_pos = f.tell()
                    if "K- ready" in chunk:
                        return
                except OSError:
                    pass
            time.sleep(0.1)
        raise TimeoutError(
            f"km did not become ready within {timeout}s\n"
            f"log: {log_path}"
        )

    def stop(self, timeout=20):
        if self.process is None:
            return
        self.process.send_signal(signal.SIGINT)
        try:
            self.process.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            self.process.kill()
            self.process.wait(timeout=5)

    @property
    def kubeconfig(self):
        return os.path.join(self.work_dir, "config", "kubeconfig.yaml")


# ---------------------------------------------------------------------------
# Session-scoped km fixture for tests that don't need their own restart cycle.
# Tests that exercise restart behavior should still construct their own
# KminusProcess via the `km_bin` and `test_dir` fixtures.
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def _km_session_dir(tmp_path_factory):
    return str(tmp_path_factory.mktemp("kminus-session"))


@pytest.fixture(scope="session")
def km(km_bin, _km_session_dir):
    proc = KminusProcess(km_bin, _km_session_dir)
    proc.start()
    yield proc
    proc.stop()


@pytest.fixture(scope="session")
def k8s_client(km):
    from kubernetes import client, config as k8s_config
    k8s_config.load_kube_config(config_file=km.kubeconfig)
    return client.CoreV1Api()


@pytest.fixture(scope="session")
def batch_client(km):
    from kubernetes import client, config as k8s_config
    k8s_config.load_kube_config(config_file=km.kubeconfig)
    return client.BatchV1Api()


@pytest.fixture(scope="session")
def apps_client(km):
    from kubernetes import client, config as k8s_config
    k8s_config.load_kube_config(config_file=km.kubeconfig)
    return client.AppsV1Api()
