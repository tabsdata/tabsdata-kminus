#
# Copyright 2026 Tabsdata Inc.
#

"""Integration tests for pod-runner server endpoints."""

import os
import signal
import sys
import time

import pytest
from conftest import MtlsClient, PodRunnerProcess, _write_file_resolver_prsysconfig
from itest_common import start_on_free_ports


def _wait_for_log_line(path, line, timeout=15):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if os.path.isfile(path):
            with open(path, "rb") as f:
                if line in f.read():
                    return
        time.sleep(0.1)
    raise TimeoutError(f"{line!r} never appeared in {path}")


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="SIGHUP is a POSIX signal; Windows reports a closed console differently",
)
class TestHangup:
    """A hangup, as from a closed terminal, leaves pod-runner serving."""

    def test_a_hangup_leaves_the_runner_serving(self, pod_runner_bin, test_dir):
        _write_file_resolver_prsysconfig(os.path.join(test_dir, "config"))

        def start(port):
            runner = PodRunnerProcess(pod_runner_bin, test_dir, port)
            runner.start()
            return runner

        runner = start_on_free_ports(start, 1)
        try:
            runner.process.send_signal(signal.SIGHUP)
            _wait_for_log_line(
                os.path.join(test_dir, "log", "pod-runner.log"), b"Ignoring SIGHUP"
            )
            assert runner.process.poll() is None, "pod-runner ended on SIGHUP"
            client = MtlsClient(runner.vk_prconfig_path, runner.work_dir)
            status, _ = client.get(f"{runner.url}/capacity")
            assert status == 200
        finally:
            runner.stop()
        assert runner.process.returncode == 0, "pod-runner did not stop gracefully"


class TestServerStartup:
    """Pod-runner auto-generates config and starts."""

    def test_prconfig_generated(self, pod_runner):
        assert os.path.isfile(os.path.join(pod_runner.config_dir, "prconfig.yaml"))

    def test_vk_prconfig_generated(self, pod_runner):
        assert os.path.isfile(pod_runner.vk_prconfig_path)


class TestNodeEndpoints:
    """Node info endpoints return valid data."""

    def test_capacity(self, pod_runner, mtls_client):
        status, data = mtls_client.get(f"{pod_runner.url}/capacity")
        assert status == 200
        assert "cpu" in data
        assert "memory" in data
        assert "pods" in data

    def test_node_conditions(self, pod_runner, mtls_client):
        status, data = mtls_client.get(f"{pod_runner.url}/nodeConditions")
        assert status == 200
        assert isinstance(data, list)
        assert any(c["type"] == "Ready" for c in data)

    def test_node_addresses(self, pod_runner, mtls_client):
        status, data = mtls_client.get(f"{pod_runner.url}/nodeAddresses")
        assert status == 200
        assert isinstance(data, list)
        assert any(a["type"] == "InternalIP" for a in data)


class TestPodEndpoints:
    """Pod CRUD endpoints."""

    def test_get_pods_returns_list(self, pod_runner, mtls_client):
        status, data = mtls_client.get(f"{pod_runner.url}/getPods")
        assert status == 200, f"Expected 200, got {status}: {data}"
        assert isinstance(data, list)

    def test_create_and_get_pod(self, pod_runner, mtls_client):
        pod_spec = {
            "metadata": {
                "namespace": "default",
                "name": "test-echo",
                "uid": "uid-test-echo",
            },
            "spec": {
                "containers": [{
                    "name": "main",
                    "command": [
                        sys.executable, "-c",
                        "import time; print('hello', flush=True); time.sleep(30)",
                    ],
                }],
            },
        }
        status, data = mtls_client.post(f"{pod_runner.url}/createPod", pod_spec)
        assert status in (200, 201), f"Expected 200/201, got {status}: {data}"

        status, data = mtls_client.get(
            f"{pod_runner.url}/getPod?namespace=default&name=test-echo"
        )
        assert status == 200, f"Expected 200, got {status}: {data}"
        assert data["metadata"]["name"] == "test-echo"

        # Cleanup
        mtls_client.delete(
            f"{pod_runner.url}/deletePod?namespace=default&name=test-echo"
        )
