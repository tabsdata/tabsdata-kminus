#
# Copyright 2026 Tabsdata Inc.
#

"""Fixtures for the pod-runner integration tests.

These tests exercise the pod-runner HTTP API in isolation, with no
control-plane or virtual-kubelet around it. The pod_runner fixture
pre-writes a prsysconfig.yaml that points the SecretResolver and
ConfigMapResolver at file-based directories under tests/resources/, so
no Kubernetes API server is needed.

Tests that need the full kminus stack (control-plane + pod-runner +
virtual-kubelet) live under mods/kminus/tests/.
"""

import base64
import http.client
import json
import os
import signal
import ssl
import subprocess
import time
import uuid

import pytest
import yaml

from itest_common import (  # noqa: F401
    get_session_id,
    pytest_collection_modifyitems,
    start_on_free_ports,
)
from pod_runner_testlib import TARGET_DIR, make_test_dir


# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

RESOURCES_DIR = os.path.join(os.path.dirname(__file__), "resources")


# ---------------------------------------------------------------------------
# test_dir fixture
# ---------------------------------------------------------------------------


@pytest.fixture
def test_dir(request) -> str:
    return make_test_dir(request.node.nodeid)


# ---------------------------------------------------------------------------
# Binary fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def bin_dir():
    env_val = os.environ.get("TD_BIN_FOLDER")
    if env_val:
        return env_val
    raise RuntimeError("TD_BIN_FOLDER environment variable must be set")


@pytest.fixture(scope="session")
def pod_runner_bin(bin_dir):
    path = os.path.join(bin_dir, "pod-runner")
    if not os.path.isfile(path):
        pytest.skip(f"pod-runner binary not found at {path}")
    return path


# ---------------------------------------------------------------------------
# PodRunnerProcess
# ---------------------------------------------------------------------------


class PodRunnerProcess:
    """Manages a pod-runner process for testing."""

    def __init__(self, binary, work_dir, port, metrics_port=0, extra_args=None):
        self.binary = binary
        self.work_dir = work_dir
        self.port = port
        self.metrics_port = metrics_port
        self.extra_args = extra_args or []
        self.process = None

    def start(self, timeout=30):
        configdir = os.path.join(self.work_dir, "config")
        workdir = os.path.join(self.work_dir, "work_pr")
        logdir = os.path.join(self.work_dir, "log")
        os.makedirs(logdir, exist_ok=True)

        args = [
            self.binary,
            "--port", str(self.port),
            "--metrics-port", str(self.metrics_port),
            "--configdir", configdir,
            "--workdir", workdir,
            "--logdir", logdir,
            # File resolvers don't need a kubeconfig but pod-runner CLI
            # still accepts the flag; pass an empty path.
            "--kubeconfig", "",
        ] + self.extra_args

        # Note the log file's pre-existing size so we only scan lines
        # written by THIS pod-runner process. Tests that reuse the
        # same workdir would otherwise pick up a stale "Server
        # listening" line from a previous run.
        log_path = os.path.join(logdir, "pod-runner.log")
        last_pos = os.path.getsize(log_path) if os.path.isfile(log_path) else 0

        # Force info-level logs so the "Server listening" startup line
        # we poll for below is emitted. cargo-make sets RUST_LOG=error
        # in output.env, which would otherwise suppress it.
        env = {**os.environ, "RUST_LOG": "info"}
        self.process = subprocess.Popen(
            args, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, env=env,
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
                    f"pod-runner exited with code {self.process.returncode}\n"
                    f"log tail: {tail}"
                )
            if os.path.isfile(log_path):
                try:
                    with open(log_path) as f:
                        f.seek(last_pos)
                        chunk = f.read()
                        last_pos = f.tell()
                    if "Server listening" in chunk:
                        return
                except OSError:
                    pass
            time.sleep(0.1)
        raise TimeoutError(
            f"pod-runner did not become ready within {timeout}s\n"
            f"log: {log_path}"
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
    def config_dir(self):
        return os.path.join(self.work_dir, "config")

    @property
    def vk_prconfig_path(self):
        return os.path.join(self.config_dir, "vk-prconfig.yaml")

    @property
    def url(self):
        return f"https://127.0.0.1:{self.port}"


# ---------------------------------------------------------------------------
# mTLS HTTP client
# ---------------------------------------------------------------------------


class MtlsClient:
    """Simple mTLS HTTP client using vk-prconfig.yaml credentials."""

    def __init__(self, vk_prconfig_path, work_dir):
        with open(vk_prconfig_path) as f:
            cfg = yaml.safe_load(f)

        certs_dir = os.path.join(work_dir, "_certs")
        os.makedirs(certs_dir, exist_ok=True)

        ca_pem = base64.b64decode(cfg["server"]["tls"]["certificate-authority-data"])
        client_cert_pem = base64.b64decode(cfg["client"]["certificate-data"])
        client_key_pem = base64.b64decode(cfg["client"]["certificate-key-data"])

        self._ca_path = os.path.join(certs_dir, "ca.pem")
        self._cert_path = os.path.join(certs_dir, "client.pem")
        self._key_path = os.path.join(certs_dir, "client-key.pem")

        with open(self._ca_path, "wb") as f:
            f.write(ca_pem)
        with open(self._cert_path, "wb") as f:
            f.write(client_cert_pem)
        with open(self._key_path, "wb") as f:
            f.write(client_key_pem)

        self._ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        self._ctx.load_cert_chain(self._cert_path, self._key_path)
        self._ctx.load_verify_locations(self._ca_path)

    def request_raw(self, method, url, body=None):
        """Issue a request and return (status, raw bytes body)."""
        from urllib.parse import urlparse
        parsed = urlparse(url)
        path = parsed.path or "/"
        if parsed.query:
            path = f"{path}?{parsed.query}"
        conn = http.client.HTTPSConnection(
            parsed.hostname, parsed.port, context=self._ctx,
        )
        headers = {"Content-Type": "application/json"} if body else {}
        payload = json.dumps(body).encode() if body else None
        conn.request(method, path, body=payload, headers=headers)
        resp = conn.getresponse()
        data = resp.read()
        conn.close()
        return resp.status, data

    def request(self, method, url, body=None):
        status, data = self.request_raw(method, url, body)
        return status, json.loads(data) if data else None

    def get(self, url):
        return self.request("GET", url)

    def get_text(self, url):
        """GET returning the raw response body as text (no JSON decode)."""
        status, data = self.request_raw("GET", url)
        return status, data.decode() if data else ""

    def post(self, url, body):
        return self.request("POST", url, body)

    def delete(self, url):
        return self.request("DELETE", url)


# ---------------------------------------------------------------------------
# Pod-runner fixture (session-scoped)
# ---------------------------------------------------------------------------
#
# The fixture pre-writes a prsysconfig.yaml that points the secret and
# configmap resolvers at file-based directories under tests/resources/.
# This lets the pod-runner start without a Kubernetes control-plane.


def _write_file_resolver_prsysconfig(configdir):
    os.makedirs(configdir, exist_ok=True)
    prsysconfig = {
        "secretResolver": {
            "type": "file",
            "configs": {
                "dir": os.path.join(RESOURCES_DIR, "secrets"),
            },
        },
        "configMapResolver": {
            "type": "file",
            "configs": {
                "dir": os.path.join(RESOURCES_DIR, "configmaps"),
            },
        },
        "nodeEnvsResolver": {"type": "none"},
    }
    with open(os.path.join(configdir, "prsysconfig.yaml"), "w") as f:
        yaml.safe_dump(prsysconfig, f)


@pytest.fixture(scope="session")
def pod_runner(pod_runner_bin):
    ts = get_session_id()
    work_dir = os.path.join(TARGET_DIR, f"testrun.{ts}", "_pod_runner")
    os.makedirs(work_dir, exist_ok=True)

    _write_file_resolver_prsysconfig(os.path.join(work_dir, "config"))

    def start(port):
        pr = PodRunnerProcess(pod_runner_bin, work_dir, port, metrics_port=0)
        pr.start()
        return pr

    pr = start_on_free_ports(start, 1)
    yield pr
    pr.stop()


@pytest.fixture(scope="session")
def mtls_client(pod_runner):
    return MtlsClient(pod_runner.vk_prconfig_path, pod_runner.work_dir)


# ---------------------------------------------------------------------------
# Pod definition helpers
# ---------------------------------------------------------------------------


def unique_name(prefix="test"):
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


def load_pod_def(name, **replacements):
    """Load a pod definition YAML, apply template replacements, assign unique name."""
    path = os.path.join(RESOURCES_DIR, "pod-def", f"{name}.yaml")
    with open(path) as f:
        text = f.read()
    for key, value in replacements.items():
        text = text.replace(f"{{{{{key}}}}}", str(value))
    pod = yaml.safe_load(text)
    pod_name = unique_name(name)
    pod["metadata"]["name"] = pod_name
    pod["metadata"]["uid"] = unique_name("uid")
    return pod


def submit_pod(mtls_client, pod_runner, pod_def):
    """Submit a pod to pod-runner, return (name, namespace)."""
    spec = {"metadata": pod_def["metadata"], "spec": pod_def["spec"]}
    if "uid" not in spec["metadata"]:
        spec["metadata"]["uid"] = unique_name("uid")
    status, data = mtls_client.post(f"{pod_runner.url}/createPod", spec)
    assert status in (200, 201), f"createPod failed ({status}): {data}"
    return spec["metadata"]["name"], spec["metadata"].get("namespace", "default")


def wait_for_pod_phase(mtls_client, pod_runner, ns, name, phases, timeout=30):
    """Poll getPodStatus until phase is in the given set."""
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        st, data = mtls_client.get(
            f"{pod_runner.url}/getPodStatus?namespace={ns}&name={name}"
        )
        if st == 200 and data and "phase" in data:
            last = data["phase"]
            if last in phases:
                return data
        time.sleep(0.5)
    raise TimeoutError(f"{ns}/{name} did not reach {phases} (last: {last})")
