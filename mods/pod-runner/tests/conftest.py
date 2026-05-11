#
# Copyright 2026 Tabsdata Inc.
#

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

from itest_common import ControlPlaneProcess, find_free_port, get_session_timestamp
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
def control_plane_bin(bin_dir):
    path = os.path.join(bin_dir, "control-plane")
    if not os.path.isfile(path):
        pytest.skip(f"control-plane binary not found at {path}")
    return path


@pytest.fixture(scope="session")
def virtual_kubelet_bin(bin_dir):
    path = os.path.join(bin_dir, "virtual-kubelet")
    if not os.path.isfile(path):
        pytest.skip(f"virtual-kubelet binary not found at {path}")
    return path


@pytest.fixture(scope="session")
def pod_runner_bin(bin_dir):
    path = os.path.join(bin_dir, "pod-runner")
    if not os.path.isfile(path):
        pytest.skip(f"pod-runner binary not found at {path}")
    return path


# ---------------------------------------------------------------------------
# Control-plane fixture (session-scoped)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def control_plane(control_plane_bin):
    ts = get_session_timestamp()
    work_dir = os.path.join(TARGET_DIR, f"testrun.{ts}", "_control_plane")
    os.makedirs(work_dir, exist_ok=True)
    api_port = find_free_port()
    kcm_port = find_free_port()
    cp = ControlPlaneProcess(control_plane_bin, work_dir, api_port, kcm_port)
    cp.start()
    yield cp
    cp.stop()


# ---------------------------------------------------------------------------
# PodRunnerProcess
# ---------------------------------------------------------------------------


class PodRunnerProcess:
    """Manages a pod-runner process for testing."""

    def __init__(self, binary, work_dir, port, kubeconfig,
                 metrics_port=0, extra_args=None):
        self.binary = binary
        self.work_dir = work_dir
        self.port = port
        self.kubeconfig = kubeconfig
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
            "--kubeconfig", self.kubeconfig,
        ] + self.extra_args

        # Note the log file's pre-existing size so we only scan lines
        # written by THIS pod-runner process. Tests that reuse the
        # same workdir would otherwise pick up a stale "Server
        # listening" line from a previous run.
        log_path = os.path.join(logdir, "pod-runner.log")
        last_pos = os.path.getsize(log_path) if os.path.isfile(log_path) else 0

        # pod-runner writes its own log to <logdir>/pod-runner.log via
        # the rotating file appender (no longer to stdout).
        debug_path = os.path.join(logdir, "pr-stderr.log")
        self.debug_path = debug_path
        debug_fd = open(debug_path, "wb")
        # Force info-level logs so the "Server listening" startup line
        # we poll for below is emitted. cargo-make sets RUST_LOG=error
        # in output.env, which would otherwise suppress it.
        env = {**os.environ, "RUST_LOG": "info"}
        self.process = subprocess.Popen(
            args, stdout=debug_fd, stderr=subprocess.STDOUT, env=env,
        )
        debug_fd.close()

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
        debug_tail = ""
        if os.path.isfile(self.debug_path):
            try:
                with open(self.debug_path) as f:
                    debug_tail = f.read()[-2000:]
            except OSError:
                pass
        raise TimeoutError(
            f"pod-runner did not become ready within {timeout}s\n"
            f"log: {log_path}\n"
            f"stderr:\n{debug_tail}"
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

    def request(self, method, url, body=None):
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
        return resp.status, json.loads(data) if data else None

    def get(self, url):
        return self.request("GET", url)

    def post(self, url, body):
        return self.request("POST", url, body)

    def delete(self, url):
        return self.request("DELETE", url)


# ---------------------------------------------------------------------------
# Pod-runner fixture (session-scoped)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def pod_runner(pod_runner_bin, control_plane):
    ts = get_session_timestamp()
    work_dir = os.path.join(TARGET_DIR, f"testrun.{ts}", "_pod_runner")
    os.makedirs(work_dir, exist_ok=True)

    port = find_free_port()
    pr = PodRunnerProcess(
        pod_runner_bin, work_dir, port,
        kubeconfig=control_plane.kubeconfig,
        metrics_port=0,
    )
    pr.start()
    yield pr
    pr.stop()


@pytest.fixture(scope="session")
def mtls_client(pod_runner):
    return MtlsClient(pod_runner.vk_prconfig_path, pod_runner.work_dir)


# ---------------------------------------------------------------------------
# Pod-runner fixture with file-based secret/configmap resolvers
# ---------------------------------------------------------------------------
#
# A separate pod-runner instance (different work_dir, different port) that
# starts with a pre-written prsysconfig.yaml pointing the file resolvers at
# tests/resources/secrets/ and tests/resources/configmaps/. Used by tests
# that submit pod-defs which reference secret/configmap data.


@pytest.fixture(scope="session")
def pod_runner_with_resolvers(pod_runner_bin, control_plane):
    ts = get_session_timestamp()
    work_dir = os.path.join(
        TARGET_DIR, f"testrun.{ts}", "_pod_runner_resolvers"
    )
    os.makedirs(work_dir, exist_ok=True)

    # Pre-write prsysconfig.yaml with file resolvers before pod-runner starts.
    # Once present, pod-runner will read it instead of generating defaults.
    configdir = os.path.join(work_dir, "config")
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

    port = find_free_port()
    pr = PodRunnerProcess(
        pod_runner_bin, work_dir, port,
        kubeconfig=control_plane.kubeconfig,
        metrics_port=0,
    )
    pr.start()
    yield pr
    pr.stop()


@pytest.fixture(scope="session")
def mtls_client_with_resolvers(pod_runner_with_resolvers):
    return MtlsClient(
        pod_runner_with_resolvers.vk_prconfig_path,
        pod_runner_with_resolvers.work_dir,
    )


# ---------------------------------------------------------------------------
# Virtual-kubelet (session-scoped, for job/cronjob tests)
# ---------------------------------------------------------------------------


class VirtualKubeletProcess:
    def __init__(self, binary, kubeconfig, prconfig_path, work_dir, kubelet_port):
        self.binary = binary
        self.kubeconfig = kubeconfig
        self.prconfig_path = prconfig_path
        self.work_dir = work_dir
        self.kubelet_port = kubelet_port
        self.process = None

    def start(self, timeout=30):
        self.process = subprocess.Popen(
            [self.binary, "--kubeconfig", self.kubeconfig,
             "--prconfig", self.prconfig_path,
             "--workdir", self.work_dir,
             "--kubelet-port", str(self.kubelet_port)],
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        )
        deadline = time.time() + timeout
        output_lines = []
        while time.time() < deadline:
            line = self.process.stdout.readline()
            if not line:
                if self.process.poll() is not None:
                    raise RuntimeError(
                        f"virtual-kubelet exited: {self.process.returncode}\n"
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
                lc = ""
                if os.path.isfile(log_path):
                    with open(log_path) as f:
                        lc = f.read()
                raise RuntimeError(f"virtual-kubelet exited: {self.process.returncode}\nlog: {lc}")
            if os.path.isfile(log_path):
                with open(log_path) as f:
                    if "Starting pod controller" in f.read():
                        time.sleep(3)
                        return
            time.sleep(0.5)
        raise TimeoutError("virtual-kubelet did not start")

    def stop(self, timeout=10):
        if self.process is None:
            return
        self.process.send_signal(signal.SIGTERM)
        try:
            self.process.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            self.process.kill()
            self.process.wait(timeout=5)


@pytest.fixture(scope="session")
def virtual_kubelet(virtual_kubelet_bin, control_plane, pod_runner):
    ts = get_session_timestamp()
    work_dir = os.path.join(TARGET_DIR, f"testrun.{ts}", "_virtual_kubelet")
    os.makedirs(work_dir, exist_ok=True)

    vk = VirtualKubeletProcess(
        virtual_kubelet_bin,
        control_plane.vk_kubeconfig,
        pod_runner.vk_prconfig_path,
        work_dir,
        kubelet_port=find_free_port(),
    )
    vk.start()
    yield vk
    vk.stop()


# ---------------------------------------------------------------------------
# Kubernetes client fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def k8s_client(control_plane):
    from kubernetes import client, config as k8s_config
    k8s_config.load_kube_config(config_file=control_plane.kubeconfig)
    return client.CoreV1Api()


@pytest.fixture(scope="session")
def batch_client(control_plane):
    from kubernetes import client, config as k8s_config
    k8s_config.load_kube_config(config_file=control_plane.kubeconfig)
    return client.BatchV1Api()


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


def load_job_def(name):
    """Load a job definition YAML with unique name."""
    path = os.path.join(RESOURCES_DIR, "job-def", f"{name}.yaml")
    with open(path) as f:
        pod = yaml.safe_load(f)
    pod["metadata"]["name"] = unique_name(name)
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
