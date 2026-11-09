#
# Copyright 2026 Tabsdata Inc.
#

"""Integration tests for pod-runner server endpoints."""

import os
import sys


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
