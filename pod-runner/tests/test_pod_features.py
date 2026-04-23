#
# Copyright 2026 Tabsdata Inc.
#

"""Integration tests for pod-runner pod features.

Each test class exercises a specific K8s pod feature via the pod-runner
HTTP API. All pod definitions are loaded from resources/pod-def/.
"""

import os
import tempfile
import time

from conftest import (
    load_pod_def, submit_pod, wait_for_pod_phase, unique_name,
)


class TestSingleContainer:
    """Basic single-container pod."""

    def test_pod_succeeds(self, pod_runner, mtls_client):
        pod = load_pod_def("single-container")
        name, ns = submit_pod(mtls_client, pod_runner, pod)
        data = wait_for_pod_phase(mtls_client, pod_runner, ns, name,
                                  {"Succeeded"})
        assert data["phase"] == "Succeeded"

    def test_pod_in_list(self, pod_runner, mtls_client):
        pod = load_pod_def("single-container")
        name, ns = submit_pod(mtls_client, pod_runner, pod)
        wait_for_pod_phase(mtls_client, pod_runner, ns, name, {"Succeeded"})
        st, pods = mtls_client.get(f"{pod_runner.url}/getPods")
        assert st == 200
        names = [p["metadata"]["name"] for p in pods]
        assert name in names


class TestMultiContainer:
    """Multiple main containers running concurrently."""

    def test_all_succeed(self, pod_runner, mtls_client):
        pod = load_pod_def("multi-container")
        name, ns = submit_pod(mtls_client, pod_runner, pod)
        data = wait_for_pod_phase(mtls_client, pod_runner, ns, name,
                                  {"Succeeded"})
        assert data["phase"] == "Succeeded"


class TestInitContainers:
    """Init containers run sequentially before main containers."""

    def test_pod_succeeds(self, pod_runner, mtls_client):
        pod = load_pod_def("init-containers")
        name, ns = submit_pod(mtls_client, pod_runner, pod)
        data = wait_for_pod_phase(mtls_client, pod_runner, ns, name,
                                  {"Succeeded"})
        assert data["phase"] == "Succeeded"


class TestSidecar:
    """Init container with restartPolicy: Always runs as sidecar."""

    def test_sidecar_terminates_after_main(self, pod_runner, mtls_client):
        pod = load_pod_def("sidecar")
        name, ns = submit_pod(mtls_client, pod_runner, pod)
        data = wait_for_pod_phase(mtls_client, pod_runner, ns, name,
                                  {"Succeeded"}, timeout=30)
        assert data["phase"] == "Succeeded"


class TestProbes:
    """Exec-based liveness and readiness probes."""

    def test_pod_with_probes_succeeds(self, pod_runner, mtls_client):
        pod = load_pod_def("probes")
        name, ns = submit_pod(mtls_client, pod_runner, pod)
        data = wait_for_pod_phase(mtls_client, pod_runner, ns, name,
                                  {"Succeeded"}, timeout=30)
        assert data["phase"] == "Succeeded"


class TestEnvLiteral:
    """Literal environment variables."""

    def test_env_vars_set(self, pod_runner, mtls_client):
        pod = load_pod_def("env-literal")
        name, ns = submit_pod(mtls_client, pod_runner, pod)
        wait_for_pod_phase(mtls_client, pod_runner, ns, name, {"Succeeded"})

        st, data = mtls_client.get(f"{pod_runner.url}/getPod?namespace={ns}&name={name}")
        assert st == 200


class TestMounts:
    """Volume mounts: hostPath and emptyDir."""

    def test_host_path_and_empty_dir(self, pod_runner, mtls_client, test_dir):
        # Create a hostPath directory with test data
        host_dir = os.path.join(test_dir, "host_input")
        os.makedirs(host_dir, exist_ok=True)
        with open(os.path.join(host_dir, "data.txt"), "w") as f:
            f.write("test-data-from-host")

        pod = load_pod_def("mounts", HOST_PATH=host_dir)
        name, ns = submit_pod(mtls_client, pod_runner, pod)
        data = wait_for_pod_phase(mtls_client, pod_runner, ns, name,
                                  {"Succeeded"}, timeout=15)
        assert data["phase"] == "Succeeded"


class TestOnFailureRestart:
    """Pod with restartPolicy: OnFailure."""

    def test_on_failure_succeeds(self, pod_runner, mtls_client):
        pod = load_pod_def("on-failure-restart")
        name, ns = submit_pod(mtls_client, pod_runner, pod)
        data = wait_for_pod_phase(mtls_client, pod_runner, ns, name,
                                  {"Succeeded"}, timeout=30)
        assert data["phase"] == "Succeeded"


class TestKitchenSink:
    """Complex pod combining init containers, sidecar, multi-container,
    volumes, env vars, and probes."""

    def test_full_lifecycle(self, pod_runner, mtls_client, test_dir):
        pod = load_pod_def("kitchen-sink")
        name, ns = submit_pod(mtls_client, pod_runner, pod)
        data = wait_for_pod_phase(mtls_client, pod_runner, ns, name,
                                  {"Succeeded"}, timeout=30)
        assert data["phase"] == "Succeeded"

    def test_get_pod_has_sections(self, pod_runner, mtls_client):
        pod = load_pod_def("kitchen-sink")
        name, ns = submit_pod(mtls_client, pod_runner, pod)
        wait_for_pod_phase(mtls_client, pod_runner, ns, name, {"Succeeded"}, timeout=30)

        st, data = mtls_client.get(f"{pod_runner.url}/getPod?namespace={ns}&name={name}")
        assert st == 200
        assert "metadata" in data
        assert "spec" in data
        assert "status" in data


class TestDelete:
    """Pod deletion."""

    def test_delete_completed_pod(self, pod_runner, mtls_client):
        pod = load_pod_def("single-container")
        name, ns = submit_pod(mtls_client, pod_runner, pod)
        wait_for_pod_phase(mtls_client, pod_runner, ns, name, {"Succeeded"})

        st, _ = mtls_client.delete(
            f"{pod_runner.url}/deletePod?namespace={ns}&name={name}"
        )
        assert st == 200

    def test_delete_running_pod(self, pod_runner, mtls_client):
        pod = load_pod_def("single-container")
        # Make it long-running
        pod["spec"]["containers"][0]["command"] = ["sh", "-c"]
        pod["spec"]["containers"][0]["args"] = ["sleep 60"]
        name, ns = submit_pod(mtls_client, pod_runner, pod)
        time.sleep(2)

        st, _ = mtls_client.delete(
            f"{pod_runner.url}/deletePod?namespace={ns}&name={name}"
        )
        assert st == 200
