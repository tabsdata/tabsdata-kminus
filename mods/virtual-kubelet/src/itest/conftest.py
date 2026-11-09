#
# Copyright 2026 Tabsdata Inc.
#

import os
import subprocess
import time

import pytest
import yaml

from itest_common import (  # noqa: F401
    ControlPlaneProcess,
    binary,
    get_session_id,
    pytest_configure,
    request_stop,
    start_on_free_ports,
    start_process,
)
from mock_pod_runner import MockPodRunner
from virtual_kubelet_testlib import TARGET_DIR, make_test_dir


# ---------------------------------------------------------------------------
# test_dir fixture
# ---------------------------------------------------------------------------


@pytest.fixture
def test_dir(request) -> str:
    return make_test_dir(request.node.nodeid)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def bin_dir():
    env_val = os.environ.get("TD_BIN_FOLDER")
    if env_val:
        return env_val
    raise RuntimeError("TD_BIN_FOLDER environment variable must be set")


@pytest.fixture(scope="session")
def control_plane_bin(bin_dir):
    return binary(bin_dir, "control-plane")


@pytest.fixture(scope="session")
def virtual_kubelet_bin(bin_dir):
    return binary(bin_dir, "virtual-kubelet")


# ---------------------------------------------------------------------------
# Control-plane fixture (session-scoped)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def control_plane(control_plane_bin):
    ts = get_session_id()
    work_dir = os.path.join(TARGET_DIR, f"testrun.{ts}", "_control_plane")
    os.makedirs(work_dir, exist_ok=True)
    def start(api_port, kcm_port):
        cp = ControlPlaneProcess(control_plane_bin, work_dir, api_port, kcm_port)
        cp.start()
        return cp

    cp = start_on_free_ports(start, 2)
    yield cp
    cp.stop()


# ---------------------------------------------------------------------------
# Mock pod-runner fixture
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def mock_pod_runner():
    runner = MockPodRunner()
    runner.start()
    yield runner
    runner.stop()


# ---------------------------------------------------------------------------
# Virtual-kubelet process
# ---------------------------------------------------------------------------


class VirtualKubeletProcess:
    """Manages a virtual-kubelet process for testing."""

    def __init__(self, binary, kubeconfig, prconfig_path, work_dir, kubelet_port):
        self.binary = binary
        self.kubeconfig = kubeconfig
        self.prconfig_path = prconfig_path
        self.work_dir = work_dir
        self.kubelet_port = kubelet_port
        self.process = None

    def start(self, timeout=30):
        self.process = start_process(
            [
                self.binary,
                "--kubeconfig", self.kubeconfig,
                "--prconfig", self.prconfig_path,
                "--workdir", self.work_dir,
                "--kubelet-port", str(self.kubelet_port),
            ],
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
                        f"virtual-kubelet exited with code {self.process.returncode}\n"
                        f"output: {''.join(l.decode() for l in output_lines)}"
                    )
                time.sleep(0.1)
                continue
            output_lines.append(line)
            if b"VK PR Config" in line:
                break

        log_path = os.path.join(self.work_dir, "log", "virtual-kubelet.log")
        while time.time() < deadline:
            if self.process.poll() is not None:
                log_content = ""
                if os.path.isfile(log_path):
                    with open(log_path, encoding="utf-8", errors="replace") as f:
                        log_content = f.read()
                raise RuntimeError(
                    f"virtual-kubelet exited with code {self.process.returncode}\n"
                    f"log: {log_content}"
                )
            if os.path.isfile(log_path):
                with open(log_path, encoding="utf-8", errors="replace") as f:
                    if "Starting pod controller" in f.read():
                        time.sleep(3)
                        return
            time.sleep(0.5)

        raise TimeoutError("virtual-kubelet did not fully start within timeout")

    def stop(self, timeout=10):
        if self.process is None:
            return
        request_stop(self.process)
        try:
            self.process.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            self.process.kill()
            self.process.wait(timeout=5)


def write_prconfig(path, pod_runner_url, node_name="test-vk-node", namespace="default"):
    config = {
        "url": pod_runner_url,
        "nodeName": node_name,
        "namespace": namespace,
        "server": {"tls": {"insecure-skip-tls-verify": True}},
    }
    with open(path, "w", encoding="utf-8") as f:
        yaml.dump(config, f)
    return path


@pytest.fixture(scope="session")
def virtual_kubelet(virtual_kubelet_bin, control_plane, mock_pod_runner):
    ts = get_session_id()
    work_dir = os.path.join(TARGET_DIR, f"testrun.{ts}", "_virtual_kubelet")
    os.makedirs(work_dir, exist_ok=True)

    prconfig_path = write_prconfig(
        os.path.join(work_dir, "prconfig.yaml"),
        mock_pod_runner.url,
        node_name="test-vk-node",
    )

    def start(kubelet_port):
        vk = VirtualKubeletProcess(
            virtual_kubelet_bin,
            control_plane.vk_kubeconfig,
            prconfig_path,
            work_dir,
            kubelet_port=kubelet_port,
        )
        vk.start()
        return vk

    vk = start_on_free_ports(start, 1)
    yield vk
    vk.stop()


# ---------------------------------------------------------------------------
# Kubernetes client fixture
# ---------------------------------------------------------------------------


@pytest.fixture
def k8s_client(control_plane):
    from kubernetes import client, config as k8s_config
    k8s_config.load_kube_config(config_file=control_plane.kubeconfig)
    return client.CoreV1Api()
