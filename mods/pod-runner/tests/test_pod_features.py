#
# Copyright 2026 Tabsdata Inc.
#

"""Integration tests for pod-runner pod features.

Each test class exercises a specific K8s pod feature via the pod-runner
HTTP API. All pod definitions are loaded from resources/pod-def/.
"""

import json
import os
import tempfile
import time

from conftest import (
    RESOURCES_DIR, load_pod_def, submit_pod, wait_for_pod_phase, unique_name,
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


class TestSecrets:
    """Pod that resolves secret values via env valueFrom and envFrom."""

    def test_secrets_resolve(self, pod_runner, mtls_client):
        pod = load_pod_def("secrets")
        name, ns = submit_pod(mtls_client, pod_runner, pod)
        data = wait_for_pod_phase(
            mtls_client, pod_runner, ns, name, {"Succeeded"}, timeout=15,
        )
        assert data["phase"] == "Succeeded"


class TestConfigMaps:
    """Pod that resolves configmap values via env valueFrom and envFrom."""

    def test_configmaps_resolve(self, pod_runner, mtls_client):
        pod = load_pod_def("configmaps")
        name, ns = submit_pod(mtls_client, pod_runner, pod)
        data = wait_for_pod_phase(
            mtls_client, pod_runner, ns, name, {"Succeeded"}, timeout=15,
        )
        assert data["phase"] == "Succeeded"


class TestConfigMapVolume:
    """Pod that mounts a configmap as a volume (one file per key)."""

    def test_configmap_volume_files_present(self, pod_runner, mtls_client):
        pod = load_pod_def("configmap-volume")
        name, ns = submit_pod(mtls_client, pod_runner, pod)
        data = wait_for_pod_phase(
            mtls_client, pod_runner, ns, name, {"Succeeded"}, timeout=15,
        )
        # The pod's command `cat`s two files written from the configmap; if
        # they aren't created the container exits non-zero and Failed.
        assert data["phase"] == "Succeeded"


class TestContainerRoot:
    """Pod that exercises the auto-injected KM_CONTAINER_ROOT env var,
    including a chained env entry referencing it (resolved via shell eval)."""

    def test_km_container_root_resolves(self, pod_runner, mtls_client):
        pod = load_pod_def("container-root")
        name, ns = submit_pod(mtls_client, pod_runner, pod)
        data = wait_for_pod_phase(
            mtls_client, pod_runner, ns, name, {"Succeeded"}, timeout=15,
        )
        # The pod's command asserts $KM_CONTAINER_ROOT is set, that the
        # chained $ROOT (=$KM_CONTAINER_ROOT) resolves to the same path,
        # and writes a marker file under it. Non-zero exit on any of
        # those checks would surface as Failed rather than Succeeded.
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


class TestResolverWait:
    """Preparer waits for missing configmaps/secrets to appear, bounded by
    `activeDeadlineSeconds`. Covers the behavior introduced by the
    pod::admission + pod::resolver::backoff modules.
    """

    def _pod_with_missing_secret(self, secret_name, *, deadline=None):
        """Build a minimal pod that references a not-yet-existing secret.

        The reference is via `valueFrom.secretKeyRef` so the preparer
        must wait for `<secret_name>.yaml` to appear under the session's
        secret-resolver directory before any container can start.
        """
        spec = {
            "metadata": {
                "name": unique_name("wait"),
                "namespace": "default",
                "uid": unique_name("uid"),
            },
            "spec": {
                "nodeName": "virtual-node",
                "restartPolicy": "Never",
                "containers": [{
                    "name": "app",
                    "image": "/dev/null",
                    "command": ["sh", "-c"],
                    "args": ["echo $X"],
                    "env": [{
                        "name": "X",
                        "valueFrom": {
                            "secretKeyRef": {
                                "name": secret_name,
                                "key": "k",
                            }
                        }
                    }],
                }],
            }
        }
        if deadline is not None:
            spec["spec"]["activeDeadlineSeconds"] = deadline
        return spec

    def test_missing_secret_with_deadline_fails(self, pod_runner, mtls_client):
        """A referenced secret that never appears makes the pod transition
        to Failed once `activeDeadlineSeconds` elapses, with a status
        message that surfaces the timeout."""
        secret_name = f"never-{unique_name('s')}"
        pod = self._pod_with_missing_secret(secret_name, deadline=2)
        name, ns = submit_pod(mtls_client, pod_runner, pod)
        data = wait_for_pod_phase(
            mtls_client, pod_runner, ns, name, {"Failed"}, timeout=10,
        )
        assert data["phase"] == "Failed"
        msg = (data.get("message") or "").lower()
        assert "timed out" in msg, f"status missing timeout marker: {data}"

    def test_missing_secret_appearing_succeeds(self, pod_runner, mtls_client):
        """A secret that materializes mid-wait unblocks the pod to
        Succeeded. Validates the resolver's backoff-then-retry loop."""
        secret_name = f"late-{unique_name('s')}"
        secret_path = os.path.join(RESOURCES_DIR, "secrets", f"{secret_name}.yaml")
        # Defensive cleanup in case a previous run left the file behind.
        if os.path.exists(secret_path):
            os.remove(secret_path)
        pod = self._pod_with_missing_secret(secret_name, deadline=30)
        try:
            name, ns = submit_pod(mtls_client, pod_runner, pod)
            # Let the preparer fail at least once and enter its first
            # backoff (500ms) before we materialize the secret file.
            time.sleep(1.0)
            with open(secret_path, "w") as f:
                f.write("stringData:\n  k: top-secret\n")
            data = wait_for_pod_phase(
                mtls_client, pod_runner, ns, name, {"Succeeded"}, timeout=20,
            )
            assert data["phase"] == "Succeeded"
        finally:
            if os.path.exists(secret_path):
                os.remove(secret_path)

    def test_optional_true_rejected_at_admission(self, pod_runner, mtls_client):
        """`optional: true` on a secret reference is rejected by the
        admission module before the pod is ever stored."""
        pod = self._pod_with_missing_secret(f"opt-{unique_name('s')}")
        pod["spec"]["containers"][0]["env"][0]["valueFrom"]["secretKeyRef"]["optional"] = True
        spec = {"metadata": pod["metadata"], "spec": pod["spec"]}
        status, data = mtls_client.post(f"{pod_runner.url}/createPod", spec)
        assert status >= 400, f"expected rejection, got {status}: {data}"
        body = json.dumps(data) if data is not None else ""
        assert "optional" in body.lower(), (
            f"expected admission error mentioning 'optional', got: {body}"
        )

    def test_delete_during_preparation_removes_pod(self, pod_runner, mtls_client):
        """`deletePod` while the preparer is still waiting cancels the
        wait and removes the pod cleanly."""
        secret_name = f"absent-{unique_name('s')}"
        # No deadline → preparer would wait indefinitely without the delete.
        pod = self._pod_with_missing_secret(secret_name)
        name, ns = submit_pod(mtls_client, pod_runner, pod)

        # Give the preparer a moment to enter its first wait.
        time.sleep(0.5)
        st, status = mtls_client.get(
            f"{pod_runner.url}/getPodStatus?namespace={ns}&name={name}"
        )
        assert st == 200, f"unexpected pre-delete status: {st} {status}"
        assert status["phase"] == "Pending", (
            f"expected Pending while preparer waits, got {status}"
        )

        st, _ = mtls_client.delete(
            f"{pod_runner.url}/deletePod?namespace={ns}&name={name}"
        )
        assert st == 200

        # After delete, getPodStatus must report 404.
        deadline = time.time() + 5
        while time.time() < deadline:
            st, _ = mtls_client.get(
                f"{pod_runner.url}/getPodStatus?namespace={ns}&name={name}"
            )
            if st == 404:
                return
            time.sleep(0.2)
        raise AssertionError(
            "pod was still present after delete during preparation"
        )
