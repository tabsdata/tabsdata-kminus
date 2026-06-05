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

    def _submit_mounts_pod(self, pod_runner, mtls_client, test_dir):
        # Create a hostPath input directory with test data and an empty
        # hostPath output directory.
        host_dir = os.path.join(test_dir, "host_input")
        os.makedirs(host_dir, exist_ok=True)
        with open(os.path.join(host_dir, "data.txt"), "w") as f:
            f.write("test-data-from-host")
        host_out = os.path.join(test_dir, "host_output")
        os.makedirs(host_out, exist_ok=True)

        pod = load_pod_def("mounts", HOST_PATH=host_dir, HOST_OUT_PATH=host_out)
        name, ns = submit_pod(mtls_client, pod_runner, pod)
        return name, ns, host_out

    def test_host_path_and_empty_dir(self, pod_runner, mtls_client, test_dir):
        name, ns, host_out = self._submit_mounts_pod(
            pod_runner, mtls_client, test_dir)
        data = wait_for_pod_phase(mtls_client, pod_runner, ns, name,
                                  {"Succeeded"}, timeout=15)
        assert data["phase"] == "Succeeded"

        # hostPath volumes are symlinks to the host directory, so a write
        # through the container's RW mount must land in the host directory.
        out_file = os.path.join(host_out, "from-pod.txt")
        assert os.path.exists(out_file), "write through hostPath mount missing"
        with open(out_file) as f:
            assert "wrote-to-host" in f.read()

    def test_read_only_mount_warning_at_log_start(self, pod_runner,
                                                  mtls_client, test_dir):
        name, ns, _ = self._submit_mounts_pod(pod_runner, mtls_client, test_dir)
        data = wait_for_pod_phase(mtls_client, pod_runner, ns, name,
                                  {"Succeeded"}, timeout=15)
        assert data["phase"] == "Succeeded"

        st, text = mtls_client.get_text(
            f"{pod_runner.url}/getContainerLogs"
            f"?podName={name}&namespace={ns}&containerName=app"
        )
        assert st == 200, f"getContainerLogs failed ({st}): {text}"
        lines = text.splitlines()
        assert lines, "container log is empty"
        # The read-only warning for input-vol is the very first log line,
        # before any container output. Output is kubectl-style: the CRI
        # prefix ("<ts> stderr F ") is stripped.
        assert lines[0].startswith(
            "WARNING: volume 'input-vol' has been mounted as read-only"), \
            f"first log line is not the read-only warning: {lines[0]}"
        # No warning for the read-write mounts.
        assert "'scratch-vol'" not in text
        assert "'output-vol'" not in text

    def test_empty_dir_shared_between_containers(self, pod_runner,
                                                 mtls_client):
        # The reader container only exits 0 if it sees the file written by
        # the writer container through its own mount of the same emptyDir.
        pod = load_pod_def("shared-empty-dir")
        name, ns = submit_pod(mtls_client, pod_runner, pod)
        data = wait_for_pod_phase(mtls_client, pod_runner, ns, name,
                                  {"Succeeded"}, timeout=20)
        assert data["phase"] == "Succeeded"


class TestContainerLogs:
    """Container log streaming: kubectl-style output, tailLines,
    timestamps, and follow."""

    def _logs_url(self, pod_runner, name, ns, extra=""):
        return (f"{pod_runner.url}/getContainerLogs"
                f"?podName={name}&namespace={ns}&containerName=app{extra}")

    def _run_log_lines_pod(self, pod_runner, mtls_client):
        pod = load_pod_def("log-lines")
        name, ns = submit_pod(mtls_client, pod_runner, pod)
        data = wait_for_pod_phase(mtls_client, pod_runner, ns, name,
                                  {"Succeeded"}, timeout=15)
        assert data["phase"] == "Succeeded"
        return name, ns

    def test_logs_are_plain_messages(self, pod_runner, mtls_client):
        name, ns = self._run_log_lines_pod(pod_runner, mtls_client)
        st, text = mtls_client.get_text(self._logs_url(pod_runner, name, ns))
        assert st == 200
        # kubectl-style output: messages only, CRI prefix stripped.
        assert text.splitlines() == [f"line-{i}" for i in range(10)]

    def test_tail_lines(self, pod_runner, mtls_client):
        name, ns = self._run_log_lines_pod(pod_runner, mtls_client)
        st, text = mtls_client.get_text(
            self._logs_url(pod_runner, name, ns, "&tailLines=3"))
        assert st == 200
        assert text.splitlines() == ["line-7", "line-8", "line-9"]

    def test_timestamps(self, pod_runner, mtls_client):
        import re
        name, ns = self._run_log_lines_pod(pod_runner, mtls_client)
        st, text = mtls_client.get_text(
            self._logs_url(pod_runner, name, ns, "&timestamps=true"))
        assert st == 200
        lines = text.splitlines()
        assert len(lines) == 10
        # Each line: "<RFC3339Nano ts> <message>"
        pat = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+Z line-\d$")
        for line in lines:
            assert pat.match(line), f"bad timestamped line: {line}"

    def test_follow_streams_until_pod_terminates(self, pod_runner,
                                                 mtls_client):
        # Ticker prints one line per second for 5s then exits. follow=true
        # must keep the stream open while the pod runs and deliver every
        # line, closing only once the pod is terminal.
        pod = load_pod_def("log-lines")
        pod["spec"]["containers"][0]["args"] = [
            "i=0; while [ $i -lt 5 ]; do echo tick-$i; i=$((i+1)); sleep 1; done"
        ]
        name, ns = submit_pod(mtls_client, pod_runner, pod)
        wait_for_pod_phase(mtls_client, pod_runner, ns, name,
                           {"Running"}, timeout=10)

        import time
        start = time.time()
        st, text = mtls_client.get_text(
            self._logs_url(pod_runner, name, ns, "&follow=true"))
        elapsed = time.time() - start
        assert st == 200
        lines = text.splitlines()
        assert [l for l in lines if l.startswith("tick-")] == \
            [f"tick-{i}" for i in range(5)], f"missing ticks: {lines}"
        # The stream stayed open until the pod finished (~5s), rather
        # than returning the current snapshot immediately.
        assert elapsed > 2, f"follow returned too quickly ({elapsed:.1f}s)"


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

    def test_projected_volume_files_present(self, pod_runner, mtls_client):
        pod = load_pod_def("projected-configmap-volume")
        name, ns = submit_pod(mtls_client, pod_runner, pod)
        data = wait_for_pod_phase(
            mtls_client, pod_runner, ns, name, {"Succeeded"}, timeout=15,
        )
        # The pod `cat`s both files projected from two configMap sources into
        # nested paths under one mount; a missing file would exit non-zero
        # and surface as Failed.
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

    def test_missing_projected_configmap_appearing_succeeds(self, pod_runner, mtls_client):
        """A configMap referenced by a `projected` volume source goes through
        the same wait/backoff path as any configMap: a source that is missing
        at create time blocks preparation until it materializes (or the
        deadline elapses). Here it appears mid-wait and the pod Succeeds."""
        cm_name = f"late-proj-{unique_name('cm')}"
        cm_path = os.path.join(RESOURCES_DIR, "configmaps", f"{cm_name}.yaml")
        if os.path.exists(cm_path):
            os.remove(cm_path)
        spec = {
            "metadata": {
                "name": unique_name("wait-proj"),
                "namespace": "default",
                "uid": unique_name("uid"),
            },
            "spec": {
                "nodeName": "virtual-node",
                "restartPolicy": "Never",
                "activeDeadlineSeconds": 30,
                "containers": [{
                    "name": "app",
                    "image": "/dev/null",
                    "command": ["sh", "-c"],
                    "args": ["cat $KM_CONTAINER_ROOT/configs/app.yaml"],
                    "volumeMounts": [{
                        "name": "cfg", "mountPath": "/configs", "readOnly": True,
                    }],
                }],
                "volumes": [{
                    "name": "cfg",
                    "projected": {"sources": [{"configMap": {"name": cm_name}}]},
                }],
            },
        }
        try:
            name, ns = submit_pod(mtls_client, pod_runner, spec)
            # Let the preparer fail at least once and enter backoff before the
            # configMap materializes.
            time.sleep(1.0)
            with open(cm_path, "w") as f:
                f.write("data:\n  app.yaml: \"ready\"\n")
            data = wait_for_pod_phase(
                mtls_client, pod_runner, ns, name, {"Succeeded"}, timeout=20,
            )
            assert data["phase"] == "Succeeded"
        finally:
            if os.path.exists(cm_path):
                os.remove(cm_path)

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
