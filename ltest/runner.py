#!/usr/bin/env python3
#
# Copyright 2026 Tabsdata Inc.
#
"""Load test runner — simplified metrics.

For each pod we capture three numbers:

  - nominal_time_secs:  expected execution time from the generated plan
                        (inits sequential, mains parallel; respects
                        init failures that abort the pod early).
  - actual_time_secs:   first container startedAt -> last container
                        finishedAt, from pod status.
  - start_latency_secs: client submission -> first container startedAt.

Aggregates (mean/p50/p95/max) are reported over these three metrics.

Usage:
    python runner.py --test test-input.yaml --mode k8s \\
        --kubeconfig /path/to/kubeconfig --workdir run1/

    python runner.py --replay run1/ --mode k8s --kubeconfig ...

    python runner.py --gen-report run1/report-*.json
"""

import argparse
import base64
import http.client
import json
import os
import signal
import socket
import ssl
import subprocess
import sys
import time
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone

import urllib3
import yaml

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

from pod_generator import generate_pods, PodPlan, ContainerPlan

VERSION = "0.3.0"


# ---------------------------------------------------------------------------
# Time helpers
# ---------------------------------------------------------------------------


def parse_k8s_time(s) -> float:
    """Parse a K8s/RFC3339 timestamp to epoch seconds. Accepts string or
    datetime. Returns 0.0 on failure."""
    if not s:
        return 0.0
    if isinstance(s, datetime):
        if s.tzinfo is None:
            s = s.replace(tzinfo=timezone.utc)
        return s.timestamp()
    if isinstance(s, (int, float)):
        return float(s)
    try:
        t = datetime.fromisoformat(str(s).replace("Z", "+00:00"))
        if t.tzinfo is None:
            t = t.replace(tzinfo=timezone.utc)
        return t.timestamp()
    except (ValueError, TypeError):
        return 0.0


def percentile(values, p):
    if not values:
        return 0.0
    s = sorted(values)
    k = (len(s) - 1) * (p / 100.0)
    f = int(k)
    c = min(f + 1, len(s) - 1)
    if f == c:
        return s[f]
    return s[f] + (s[c] - s[f]) * (k - f)


def stats(values) -> dict:
    """Compute p50/p95/max/mean over a list of floats."""
    if not values:
        return {"count": 0, "mean": 0.0, "p50": 0.0, "p95": 0.0, "max": 0.0}
    return {
        "count": len(values),
        "mean": round(sum(values) / len(values), 3),
        "p50": round(percentile(values, 50), 3),
        "p95": round(percentile(values, 95), 3),
        "max": round(max(values), 3),
    }


def _find_free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


# ---------------------------------------------------------------------------
# Pod timing data model
# ---------------------------------------------------------------------------


@dataclass
class PodTiming:
    """Timing for a single pod — the three numbers we care about,
    plus minimal diagnostics for unexpected failures."""
    plan: PodPlan

    # Client submit time (epoch).
    submitted_at: float = 0.0

    # From pod status: first container startedAt, last finishedAt
    # (across init + main; sidecars are excluded — they're long-running
    # and don't cleanly bound "work time").
    first_started_at: float = 0.0
    last_finished_at: float = 0.0

    # Terminal phase as observed at the end of the test.
    actual_phase: str = ""
    phase_correct: bool = False

    # Failure diagnostics (populated for Failed pods after status fetch).
    # fail_container is the first container (in iteration order: inits
    # then mains) observed with a non-zero exit code, or "" if none
    # (in which case the failure is at the pod/job level).
    fail_container: str = ""
    fail_exit_code: int = 0
    # Containers that were present in the plan but never reached a
    # terminated state (mains that never started, etc.).
    unterminated: list = field(default_factory=list)
    # Pod-level phase/reason from status.
    pod_phase: str = ""
    pod_reason: str = ""
    # Job-level failure condition (K8s mode only).
    job_failure_reason: str = ""
    job_failure_message: str = ""

    @property
    def nominal_time_secs(self) -> float:
        return self.plan.nominal_time_secs

    @property
    def actual_time_secs(self) -> float:
        if self.first_started_at > 0 and self.last_finished_at > 0:
            return max(0.0, self.last_finished_at - self.first_started_at)
        return 0.0

    @property
    def start_latency_secs(self) -> float:
        if self.submitted_at > 0 and self.first_started_at > 0:
            return max(0.0, self.first_started_at - self.submitted_at)
        return 0.0

    def to_dict(self) -> dict:
        return {
            "name": self.plan.name,
            "expected_phase": self.plan.expected_phase,
            "actual_phase": self.actual_phase,
            "phase_correct": self.phase_correct,
            "nominal_time_secs": round(self.nominal_time_secs, 3),
            "actual_time_secs": round(self.actual_time_secs, 3),
            "start_latency_secs": round(self.start_latency_secs, 3),
            "fail_container": self.fail_container,
            "fail_exit_code": self.fail_exit_code,
            "unterminated": list(self.unterminated),
            "pod_phase": self.pod_phase,
            "pod_reason": self.pod_reason,
            "job_failure_reason": self.job_failure_reason,
            "job_failure_message": self.job_failure_message,
        }


# ---------------------------------------------------------------------------
# K8s client (with detailed pod inspection)
# ---------------------------------------------------------------------------


class K8sClient:
    """Submits pods as Jobs and extracts detailed K8s timing."""

    def __init__(self, kubeconfig: str, node_name: str, backoff_limit: int = 0):
        from kubernetes import client, config as k8s_config
        k8s_config.load_kube_config(config_file=kubeconfig)
        self._batch = client.BatchV1Api()
        self._core = client.CoreV1Api()
        self._node_name = node_name
        self._backoff_limit = backoff_limit

    def create_pod(self, spec: dict) -> tuple[int, dict]:
        from kubernetes.client import (
            V1Job, V1JobSpec, V1PodTemplateSpec, V1PodSpec,
            V1Container, V1ObjectMeta, V1Toleration,
        )
        pod_meta = spec["metadata"]
        pod_spec = spec["spec"]
        name = pod_meta["name"]
        ns = pod_meta.get("namespace", "default")

        containers = [V1Container(
            name=c["name"], image=c.get("image", "/dev/null"),
            command=c.get("command"), args=c.get("args"),
        ) for c in pod_spec.get("containers", [])]

        init_containers = []
        for c in pod_spec.get("initContainers", []):
            ic = V1Container(
                name=c["name"], image=c.get("image", "/dev/null"),
                command=c.get("command"), args=c.get("args"),
            )
            if c.get("restartPolicy") == "Always":
                ic.restart_policy = "Always"
            init_containers.append(ic)

        job = V1Job(
            metadata=V1ObjectMeta(name=name, namespace=ns),
            spec=V1JobSpec(
                backoff_limit=self._backoff_limit,
                ttl_seconds_after_finished=300,
                template=V1PodTemplateSpec(
                    spec=V1PodSpec(
                        node_name=self._node_name,
                        restart_policy=pod_spec.get("restartPolicy", "Never"),
                        containers=containers,
                        init_containers=init_containers or None,
                        tolerations=[V1Toleration(
                            key="virtual-kubelet.io/provider",
                            operator="Exists", effect="NoSchedule",
                        )],
                    ),
                ),
            ),
        )
        try:
            self._batch.create_namespaced_job(namespace=ns, body=job)
            return 201, {"name": name}
        except Exception as e:
            return 500, {"error": str(e)}

    def get_job_phase(self, ns: str, name: str) -> str:
        """Return 'Succeeded', 'Failed', 'Running', 'Pending', or 'NotFound'."""
        try:
            job = self._batch.read_namespaced_job(name=name, namespace=ns)
            if job.status.succeeded and job.status.succeeded >= 1:
                return "Succeeded"
            if job.status.conditions:
                for c in job.status.conditions:
                    if c.type == "Failed" and c.status == "True":
                        return "Failed"
            if job.status.active and job.status.active >= 1:
                return "Running"
            return "Pending"
        except Exception:
            return "NotFound"

    def get_pod_for_job(self, ns: str, job_name: str):
        """Return the most recently created Pod for a Job, or None."""
        try:
            pods = self._core.list_namespaced_pod(
                namespace=ns, label_selector=f"job-name={job_name}",
            )
        except Exception:
            return None
        if not pods.items:
            return None
        # Use the most recently created pod (retry attempts have the same
        # job-name label; we want the latest).
        return sorted(
            pods.items,
            key=lambda p: p.metadata.creation_timestamp or datetime.min.replace(tzinfo=timezone.utc),
        )[-1]

    def extract_pod_timing(self, pod, plan: PodPlan) -> dict:
        """Return {first_started_at, last_finished_at, terminated: [(name, exit_code)],
        pod_phase, pod_reason}. Excludes sidecars."""
        result = {
            "first_started_at": 0.0,
            "last_finished_at": 0.0,
            "terminated": [],
            "pod_phase": "",
            "pod_reason": "",
        }
        if not pod or not pod.status:
            return result
        result["pod_phase"] = pod.status.phase or ""
        result["pod_reason"] = pod.status.reason or ""
        sidecars = {c.name for c in plan.containers if c.is_sidecar}
        starts: list[float] = []
        finishes: list[float] = []
        for cs_list in (
            pod.status.init_container_statuses or [],
            pod.status.container_statuses or [],
        ):
            for cs in cs_list:
                if cs.name in sidecars:
                    continue
                if cs.state and cs.state.terminated:
                    term = cs.state.terminated
                    if term.started_at:
                        starts.append(parse_k8s_time(term.started_at))
                    if term.finished_at:
                        finishes.append(parse_k8s_time(term.finished_at))
                    result["terminated"].append((cs.name, term.exit_code or 0))
        result["first_started_at"] = min(starts) if starts else 0.0
        result["last_finished_at"] = max(finishes) if finishes else 0.0
        return result

    def get_job_failure(self, ns: str, name: str) -> tuple[str, str]:
        """Return (reason, message) from the Job's Failed condition,
        or ('', '') if the Job has no Failed condition."""
        try:
            job = self._batch.read_namespaced_job(name=name, namespace=ns)
        except Exception:
            return ("", "")
        for c in job.status.conditions or []:
            if c.type == "Failed" and c.status == "True":
                return (c.reason or "", c.message or "")
        return ("", "")

    def delete_pod(self, ns: str, name: str):
        try:
            self._batch.delete_namespaced_job(
                name=name, namespace=ns, propagation_policy="Background",
            )
        except Exception:
            pass

    def delete_all_pods(self, ns: str = "default"):
        try:
            jobs = self._batch.list_namespaced_job(namespace=ns)
            for job in jobs.items:
                self.delete_pod(ns, job.metadata.name)
            return len(jobs.items)
        except Exception:
            return 0

    def stop(self):
        pass


# ---------------------------------------------------------------------------
# Pod-runner client (simpler; extracts from PodStatus)
# ---------------------------------------------------------------------------


class PodRunnerClient:
    """Direct mTLS client for pod-runner."""

    def __init__(self, binary: str, work_dir: str, port: int = 0, max_pods: int = 200):
        self._binary = binary
        self._work_dir = work_dir
        self._port = port or _find_free_port()
        self._process = None
        self._ssl_ctx = None
        self._configdir = os.path.join(work_dir, "config")
        self._prworkdir = os.path.join(work_dir, "work_pr")
        self._logdir = os.path.join(work_dir, "log")
        self._max_pods = max_pods

    @property
    def url(self):
        return f"https://127.0.0.1:{self._port}"

    def start(self, timeout=30):
        os.makedirs(self._configdir, exist_ok=True)
        syscfg_path = os.path.join(self._configdir, "prsysconfig.yaml")
        with open(syscfg_path, "w") as f:
            f.write(
                'nodename: "tdknode"\nnamespace: "default"\n'
                'defaultGracePeriodSecs: 30\nmaxRestartCount: 0\n'
                'restartBackoffBaseSecs: 2\ncleanUpForceKillWaitSecs: 10\n'
                'propagateEnv: true\ndieWithParent: true\n'
                'writeSecretsToContainerShellScript: false\n'
                'keepDeletedPodDir: false\nkeepFailedPodDir: false\n'
                'deletedPodDirTtlHours: 1\npodArchiveTtlDays: 0\n'
                f'capacity:\n  cpu: 100\n  memoryGB: 100\n  pods: {self._max_pods}\n'
                'metricsUrl: ""\n'
                'secretResolver:\n  type: "none"\n  configs: {}\n'
                'configMapResolver:\n  type: "none"\n  configs: {}\n'
                'nodeEnvsResolver:\n  type: "none"\n  configs: {}\n'
            )
        args = [
            self._binary, "--port", str(self._port), "--metrics-port", "0",
            "--configdir", self._configdir, "--workdir", self._prworkdir,
            "--logdir", self._logdir,
        ]
        os.makedirs(self._logdir, exist_ok=True)
        self._pr_log_file = open(os.path.join(self._logdir, "pod-runner.log"), "wb")
        env = os.environ.copy()
        env.setdefault("RUST_LOG", "info,pod_runner=debug")
        self._process = subprocess.Popen(
            args, stdout=self._pr_log_file, stderr=subprocess.STDOUT, env=env,
        )
        pr_log_path = os.path.join(self._logdir, "pod-runner.log")
        deadline = time.time() + timeout
        with open(pr_log_path, "rb") as tail:
            while time.time() < deadline:
                line = tail.readline()
                if not line:
                    if self._process.poll() is not None:
                        raise RuntimeError(f"pod-runner exited: {self._process.returncode}")
                    time.sleep(0.1)
                    continue
                if b"Server listening" in line:
                    break
            else:
                raise TimeoutError("pod-runner not ready")
        self._setup_mtls()
        for _ in range(10):
            try:
                self._request("GET", "/capacity")
                return
            except Exception:
                time.sleep(0.3)
        raise TimeoutError("pod-runner not accepting connections")

    def _setup_mtls(self):
        vk_cfg_path = os.path.join(self._configdir, "vk-prconfig.yaml")
        with open(vk_cfg_path) as f:
            cfg = yaml.safe_load(f)
        certs_dir = os.path.join(self._work_dir, "_certs")
        os.makedirs(certs_dir, exist_ok=True)
        ca_pem = base64.b64decode(cfg["server"]["tls"]["certificate-authority-data"])
        client_cert = base64.b64decode(cfg["client"]["certificate-data"])
        client_key = base64.b64decode(cfg["client"]["certificate-key-data"])
        ca_path = os.path.join(certs_dir, "ca.pem")
        cert_path = os.path.join(certs_dir, "client.pem")
        key_path = os.path.join(certs_dir, "client-key.pem")
        with open(ca_path, "wb") as f: f.write(ca_pem)
        with open(cert_path, "wb") as f: f.write(client_cert)
        with open(key_path, "wb") as f: f.write(client_key)
        self._ssl_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        self._ssl_ctx.load_cert_chain(cert_path, key_path)
        self._ssl_ctx.load_verify_locations(ca_path)

    def _request(self, method, path, body=None):
        headers = {"Content-Type": "application/json"} if body else {}
        payload = json.dumps(body).encode() if body else None
        last_err = None
        for attempt in range(5):
            try:
                conn = http.client.HTTPSConnection(
                    "127.0.0.1", self._port, context=self._ssl_ctx, timeout=15,
                )
                conn.request(method, path, body=payload, headers=headers)
                resp = conn.getresponse()
                data = resp.read()
                conn.close()
                return resp.status, json.loads(data) if data else None
            except Exception as e:
                last_err = e
                if attempt < 4:
                    time.sleep(0.5)
        raise last_err

    def create_pod(self, spec: dict):
        return self._request("POST", "/createPod", spec)

    def get_job_phase(self, ns: str, name: str) -> str:
        status, data = self._request("GET", f"/getPodStatus?namespace={ns}&name={name}")
        if status != 200 or not data:
            return "NotFound"
        phase = data.get("phase", "Pending")
        if phase in ("Succeeded", "Failed", "Running", "Pending"):
            return phase
        return "Pending"

    def get_pod_for_job(self, ns: str, name: str):
        """Return the raw status dict (mimics K8s pod object for extraction)."""
        status, data = self._request("GET", f"/getPodStatus?namespace={ns}&name={name}")
        if status != 200 or not data:
            return None
        return data

    def extract_pod_timing(self, status_dict, plan: PodPlan) -> dict:
        """Return {first_started_at, last_finished_at, terminated,
        pod_phase, pod_reason}. Excludes sidecars."""
        result = {
            "first_started_at": 0.0,
            "last_finished_at": 0.0,
            "terminated": [],
            "pod_phase": "",
            "pod_reason": "",
        }
        if not status_dict:
            return result
        result["pod_phase"] = status_dict.get("phase", "") or ""
        result["pod_reason"] = status_dict.get("reason", "") or ""
        sidecars = {c.name for c in plan.containers if c.is_sidecar}
        starts: list[float] = []
        finishes: list[float] = []
        for cs_list_key in ("initContainerStatuses", "containerStatuses"):
            for cs in status_dict.get(cs_list_key, []):
                name = cs.get("name", "")
                if name in sidecars:
                    continue
                term = (cs.get("state") or {}).get("terminated") or {}
                if term:
                    if term.get("startedAt"):
                        starts.append(parse_k8s_time(term["startedAt"]))
                    if term.get("finishedAt"):
                        finishes.append(parse_k8s_time(term["finishedAt"]))
                    result["terminated"].append((name, term.get("exitCode", 0) or 0))
        result["first_started_at"] = min(starts) if starts else 0.0
        result["last_finished_at"] = max(finishes) if finishes else 0.0
        return result

    def get_job_failure(self, ns: str, name: str) -> tuple[str, str]:
        """Pod-runner has no Job concept; always returns ('', '')."""
        return ("", "")

    def delete_pod(self, ns: str, name: str):
        try:
            self._request("DELETE", f"/deletePod?namespace={ns}&name={name}")
        except Exception:
            pass

    def delete_all_pods(self, ns: str = "default"):
        try:
            status, data = self._request("GET", f"/getPods?namespace={ns}")
            if status == 200 and data:
                pods = data if isinstance(data, list) else []
                for p in pods:
                    name = p.get("metadata", {}).get("name", "")
                    if name:
                        self.delete_pod(ns, name)
                return len(pods)
        except Exception:
            pass
        return 0

    def stop(self):
        if self._process is None:
            return
        self._process.send_signal(signal.SIGTERM)
        try:
            self._process.wait(timeout=15)
        except subprocess.TimeoutExpired:
            self._process.kill()
            self._process.wait(timeout=5)
        if getattr(self, "_pr_log_file", None):
            try:
                self._pr_log_file.close()
            except Exception:
                pass


# ---------------------------------------------------------------------------
# Plan save/load (compatible with runner.py)
# ---------------------------------------------------------------------------


def save_test_plan(workdir: str, cfg: dict, plans: list[PodPlan], dispatch_rate: float):
    pods_dir = os.path.join(workdir, "pods")
    os.makedirs(pods_dir, exist_ok=True)
    for plan in plans:
        with open(os.path.join(pods_dir, f"{plan.name}.yaml"), "w") as f:
            yaml.dump(plan.spec, f, default_flow_style=False)
    dispatch_plan = {
        "config": cfg, "dispatch_rate": dispatch_rate,
        "test_timeout": cfg.get("test_timeout", 300),
        "monitor_interval": cfg.get("monitor_interval", 2),
        "pods": [],
    }
    for i, plan in enumerate(plans):
        dispatch_plan["pods"].append({
            "name": plan.name, "uid": plan.uid,
            "file": f"pods/{plan.name}.yaml",
            "expected_phase": plan.expected_phase,
            "nominal_time_secs": plan.nominal_time_secs,
            "containers": [asdict(c) for c in plan.containers],
            "dispatch_order": i,
        })
    with open(os.path.join(workdir, "plan.yaml"), "w") as f:
        yaml.dump(dispatch_plan, f, default_flow_style=False)


def load_test_plan(workdir: str):
    with open(os.path.join(workdir, "plan.yaml")) as f:
        dispatch_plan = yaml.safe_load(f)
    cfg = dispatch_plan["config"]
    dispatch_rate = dispatch_plan["dispatch_rate"]
    plans = []
    for entry in sorted(dispatch_plan["pods"], key=lambda e: e["dispatch_order"]):
        with open(os.path.join(workdir, entry["file"])) as f:
            spec = yaml.safe_load(f)
        containers = [ContainerPlan(**c) for c in entry["containers"]]
        plans.append(PodPlan(
            name=entry["name"], uid=entry["uid"], spec=spec,
            containers=containers, expected_phase=entry["expected_phase"],
            nominal_time_secs=entry["nominal_time_secs"],
        ))
    return cfg, plans, dispatch_rate


# ---------------------------------------------------------------------------
# Dispatch and tight-loop completion detection
# ---------------------------------------------------------------------------


def run_test(client, plans, rate, max_nominal):
    """Dispatch all pods, tight-poll for terminal, extract detailed timings.

    Returns (results, dispatch_time_secs, wall_time_secs).
    """
    total = len(plans)
    interval = 1.0 / rate if rate > 0 else 0
    terminal = {"Succeeded", "Failed"}
    t0 = time.time()

    # Phase 1: dispatch
    results: list[PodTiming] = []
    last_report = t0
    for i, plan in enumerate(plans):
        status, data = client.create_pod(plan.spec)
        now = time.time()
        if status not in (200, 201):
            print(f"  WARN: {plan.name} dispatch failed ({status}): {data}")
        results.append(PodTiming(plan=plan, submitted_at=now))
        if now - last_report >= 5:
            print(f"  Dispatched {i + 1}/{total}")
            last_report = now
        if interval > 0 and i < total - 1:
            time.sleep(interval)

    dispatch_time = time.time() - t0
    # Generous timeout: dispatch + 4x max nominal (accounts for queueing).
    timeout = dispatch_time + max(max_nominal * 10, 60)
    deadline = t0 + timeout
    print(f"  Dispatched {total}/{total} in {dispatch_time:.1f}s, "
          f"timeout={timeout:.1f}s")

    # Phase 2: tight poll loop — check every 0.25s for terminal transitions.
    # Timestamp terminal observation as close as possible to actual completion.
    pending = {r.plan.name: r for r in results}
    last_report = time.time()
    poll_interval = 0.25

    while pending and time.time() < deadline:
        for name in list(pending.keys()):
            r = pending[name]
            try:
                phase = client.get_job_phase("default", name)
                if phase in terminal:
                    r.actual_phase = phase
                    r.phase_correct = phase == r.plan.expected_phase
                    del pending[name]
            except Exception:
                pass

        now = time.time()
        if now - last_report >= 5:
            completed = total - len(pending)
            print(f"  [{now - t0:5.1f}s] running={len(pending)} "
                  f"completed={completed}/{total} "
                  f"timeout_left={deadline - now:.1f}s")
            last_report = now

        if pending:
            time.sleep(poll_interval)

    for r in pending.values():
        r.actual_phase = "Timeout"
        r.phase_correct = False

    wall_time = time.time() - t0
    return results, dispatch_time, wall_time


def fetch_pod_details(client, results: list[PodTiming]):
    """After completion, fetch each pod's status. Extract timing
    (first startedAt / last finishedAt across non-sidecar containers)
    and failure diagnostics (first non-zero exit code, unterminated
    containers)."""
    completed = [r for r in results if r.actual_phase != "Timeout"]
    if not completed:
        return
    print(f"  Fetching pod details for {len(completed)} pods...")
    for r in completed:
        try:
            pod = client.get_pod_for_job("default", r.plan.name)
            if pod is not None:
                data = client.extract_pod_timing(pod, r.plan)
                r.first_started_at = data["first_started_at"]
                r.last_finished_at = data["last_finished_at"]
                r.pod_phase = data["pod_phase"]
                r.pod_reason = data["pod_reason"]
                # First container with non-zero exit (inits iterate first,
                # so init failures are naturally prioritized).
                for name, code in data["terminated"]:
                    if code != 0:
                        r.fail_container = name
                        r.fail_exit_code = code
                        break
                expected_non_sidecar = {
                    c.name for c in r.plan.containers if not c.is_sidecar
                }
                terminated_names = {name for name, _ in data["terminated"]}
                r.unterminated = sorted(expected_non_sidecar - terminated_names)
            if r.actual_phase == "Failed":
                r.job_failure_reason, r.job_failure_message = client.get_job_failure(
                    "default", r.plan.name,
                )
        except Exception as e:
            print(f"    WARN: {r.plan.name}: {e}")


# ---------------------------------------------------------------------------
# JSON report
# ---------------------------------------------------------------------------


def generate_json_report(
    cfg, results, dispatch_time, wall_time, mode, workdir, is_replay,
    max_nominal=0, test_timeout=0,
    kubeconfig=None, node_name=None, pr_binary=None,
    backoff_limit=0,
):
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    pod_results = [r.to_dict() for r in results]
    completed = [r for r in results if r.actual_phase != "Timeout"]

    def _metric(attr):
        return [getattr(r, attr) for r in completed
                if getattr(r, attr) > 0]

    aggregates = {
        "nominal_time":   stats(_metric("nominal_time_secs")),
        "actual_time":    stats(_metric("actual_time_secs")),
        "start_latency":  stats(_metric("start_latency_secs")),
    }

    return {
        "version": VERSION,
        "generated": ts,
        "config": {
            "mode": mode, "is_replay": is_replay,
            "kubeconfig": kubeconfig, "node_name": node_name,
            "pr_binary": pr_binary, "workdir": workdir,
            "test_timeout_secs": round(test_timeout, 1),
            "backoff_limit": backoff_limit,
            "input": cfg,
        },
        "summary": {
            "total": len(results),
            "succeeded": sum(1 for r in results if r.actual_phase == "Succeeded"),
            "failed": sum(1 for r in results if r.actual_phase == "Failed"),
            "timed_out": sum(1 for r in results if r.actual_phase == "Timeout"),
            "phase_correct": sum(1 for r in results if r.phase_correct),
            "dispatch_time_secs": round(dispatch_time, 1),
            "wall_time_secs": round(wall_time, 1),
            "max_pod_nominal_secs": round(max_nominal, 1),
        },
        "aggregates": aggregates,
        "pods": pod_results,
    }


# ---------------------------------------------------------------------------
# Markdown report
# ---------------------------------------------------------------------------


def _stats_row(name, s):
    return (f"| {name} | {s['count']} | {s['mean']:.2f}s | {s['p50']:.2f}s | "
            f"{s['p95']:.2f}s | {s['max']:.2f}s |")


def json_to_markdown(rpt: dict) -> str:
    cfg = rpt["config"]["input"]
    s = rpt["summary"]
    mode = rpt["config"]["mode"]
    run_mode = "Replay" if rpt["config"]["is_replay"] else "Generate"
    total = s["total"]
    phase_correct = s["phase_correct"]
    ag = rpt["aggregates"]

    lines = [
        f"# kminus Load Test Report",
        f"",
        f"Generated: {rpt['generated']}  ",
        f"Runner version: {rpt['version']}",
        f"",
        f"## Configuration",
        f"",
        f"| Parameter | Value |",
        f"|-----------|-------|",
        f"| Mode | {mode} ({run_mode}) |",
    ]
    if rpt["config"].get("kubeconfig"):
        lines.append(f"| Kubeconfig | `{rpt['config']['kubeconfig']}` |")
    if rpt["config"].get("workdir"):
        lines.append(f"| Work dir | `{rpt['config']['workdir']}` |")
    lines += [
        f"| Pod count | {cfg['pod_count']} |",
        f"| Dispatch rate | {cfg['dispatch_rate']} pods/sec |",
        f"| Duration range | {cfg['duration']['min']}-{cfg['duration']['max']}s |",
        f"| Failure probability | {cfg.get('failure_probability', 0):.0%} |",
        f"",
        f"## Summary",
        f"",
        f"| Metric | Value |",
        f"|--------|-------|",
        f"| Total pods | {total} |",
        f"| Succeeded | {s['succeeded']} |",
        f"| Failed | {s['failed']} |",
        f"| Timed out | {s['timed_out']} |",
        f"| Phase accuracy | {phase_correct}/{total} ({100*phase_correct/total:.1f}%) |",
        f"| Dispatch time | {s['dispatch_time_secs']:.1f}s |",
        f"| Wall time | {s['wall_time_secs']:.1f}s |",
        f"",
        f"## Timings",
        f"",
        f"- **nominal_time**: expected execution time from the plan",
        f"  (inits sequential + max main; truncated on init failure)",
        f"- **actual_time**: first container startedAt → last finishedAt",
        f"- **start_latency**: client submission → first container startedAt",
        f"",
        f"| Metric | n | mean | p50 | p95 | max |",
        f"|--------|---|------|-----|-----|-----|",
        _stats_row("nominal_time",  ag["nominal_time"]),
        _stats_row("actual_time",   ag["actual_time"]),
        _stats_row("start_latency", ag["start_latency"]),
        f"",
    ]

    # Slowest 10 by actual_time
    pods = rpt["pods"]
    by_actual = sorted(
        pods, key=lambda p: p.get("actual_time_secs") or 0, reverse=True,
    )[:10]
    lines += [
        f"## Slowest 10 Pods (by actual_time)",
        f"",
        f"| Pod | Nominal | Actual | Start lat | Phase (expected → actual) |",
        f"|-----|---------|--------|-----------|---------------------------|",
    ]
    for p in by_actual:
        lines.append(
            f"| {p['name']} | "
            f"{p['nominal_time_secs']:.2f}s | "
            f"{p['actual_time_secs']:.2f}s | "
            f"{p['start_latency_secs']:.2f}s | "
            f"{p['expected_phase']} → {p['actual_phase']} |"
        )
    lines.append("")

    # Mismatched pods (expected phase != actual phase).
    mismatched = [p for p in pods if not p["phase_correct"]]
    if mismatched:
        lines += [
            f"## Phase Mismatches ({len(mismatched)})",
            f"",
            f"Pods whose observed phase disagrees with the plan. Columns:",
            f"",
            f"- **fail / exit**: first container observed with a non-zero",
            f"  exit code (empty → failure is pod/job-level, not container-level)",
            f"- **unterminated**: non-sidecar containers that never reached",
            f"  a terminated state (never ran, or killed before termination)",
            f"- **pod_phase / pod_reason**: from pod.status (K8s mode) or",
            f"  pod-runner status",
            f"- **job_reason / job_msg**: from the Job's Failed condition",
            f"  (K8s mode only). Common values: `BackoffLimitExceeded`,",
            f"  `DeadlineExceeded`, `PodFailurePolicy`.",
            f"",
            f"| Pod | Exp | Act | Nom | Actual | fail:exit | unterminated | pod_phase | pod_reason | job_reason | job_msg |",
            f"|-----|-----|-----|-----|--------|-----------|--------------|-----------|------------|------------|---------|",
        ]
        for p in mismatched:
            fail_c = p.get("fail_container") or "—"
            fail_ec = p.get("fail_exit_code") or 0
            fail_col = f"{fail_c}:{fail_ec}" if fail_c != "—" else "—"
            unterm = ",".join(p.get("unterminated") or []) or "—"
            pod_phase = p.get("pod_phase") or "—"
            pod_reason = p.get("pod_reason") or "—"
            job_reason = p.get("job_failure_reason") or "—"
            job_msg = (p.get("job_failure_message") or "—").replace("|", "\\|")
            lines.append(
                f"| {p['name']} | {p['expected_phase']} | {p['actual_phase']} | "
                f"{p['nominal_time_secs']:.0f}s | {p['actual_time_secs']:.0f}s | "
                f"{fail_col} | {unterm} | {pod_phase} | {pod_reason} | "
                f"{job_reason} | {job_msg} |"
            )
        lines.append("")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(
        description="kminus load test runner (detailed K8s timing)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--test", default=None)
    parser.add_argument("--replay", default=None)
    parser.add_argument("--gen-report", default=None, metavar="REPORT_JSON")
    parser.add_argument("--mode", choices=["k8s", "pr"], default="k8s")
    parser.add_argument("--kubeconfig", default=None)
    parser.add_argument("--node-name", default="tdknode")
    parser.add_argument("--pod-runner", default=None)
    parser.add_argument("--backoff-limit", type=int, default=0)
    parser.add_argument("--workdir", default=None)
    args = parser.parse_args()

    if args.gen_report:
        with open(args.gen_report) as f:
            rpt = json.load(f)
        md_path = args.gen_report.rsplit(".", 1)[0] + ".md"
        with open(md_path, "w") as f:
            f.write(json_to_markdown(rpt))
        print(f"Markdown report written to: {md_path}")
        return

    if not args.test and not args.replay:
        parser.error("Either --test, --replay, or --gen-report is required")
    if args.test and args.replay:
        parser.error("Cannot use both --test and --replay")
    if args.mode == "k8s" and not args.kubeconfig:
        parser.error("--kubeconfig is required for K8s mode")

    print(f"kminus load test runner (detailed) v{VERSION}")
    is_replay = args.replay is not None

    if is_replay:
        workdir = args.replay
        if not os.path.isfile(os.path.join(workdir, "plan.yaml")):
            parser.error(f"No plan.yaml found in {workdir}")
        print(f"Replaying from: {workdir}")
        cfg, plans, dispatch_rate = load_test_plan(workdir)
    else:
        with open(args.test) as f:
            cfg = yaml.safe_load(f)
        dispatch_rate = cfg["dispatch_rate"]
        workdir = args.workdir
        if not workdir:
            ts = datetime.now().strftime("%Y%m%d-%H%M%S")
            workdir = f"ltest-run-{ts}"
        os.makedirs(workdir, exist_ok=True)
        print(f"Generating from: {args.test}")
        print(f"Work dir:        {workdir}")
        plans = generate_pods(cfg)
        save_test_plan(workdir, cfg, plans, dispatch_rate)
        print(f"Saved {len(plans)} pod specs and plan to {workdir}/")

    backoff_limit = args.backoff_limit if args.mode == "k8s" else 0
    n_fail = sum(1 for p in plans if p.expected_phase == "Failed")
    max_nominal = max(p.nominal_time_secs for p in plans) if plans else 0
    est_dispatch = len(plans) / dispatch_rate if dispatch_rate > 0 else 0

    print(f"\nMode:           {args.mode}")
    print(f"Pod count:      {len(plans)}")
    print(f"Dispatch rate:  {dispatch_rate} pods/sec")
    print(f"Expected fail:  {n_fail}")
    print(f"Max nominal:    {max_nominal:.1f}s")
    print(f"Est dispatch:   {est_dispatch:.1f}s")

    pr_binary = None
    if args.mode == "pr":
        pr_binary = args.pod_runner
        if not pr_binary:
            script_dir = os.path.dirname(os.path.abspath(__file__))
            project_dir = os.path.dirname(script_dir)
            candidate = os.path.join(project_dir, "target", "darwin-arm64", "pod-runner")
            if os.path.isfile(candidate):
                pr_binary = candidate
            else:
                parser.error("Cannot find pod-runner. Use --pod-runner.")
        print(f"Pod-runner:     {pr_binary}")
        pr_workdir = os.path.join(workdir, "_pod_runner")
        os.makedirs(pr_workdir, exist_ok=True)
        max_pods = max(200, int(len(plans) * 1.2))
        client = PodRunnerClient(pr_binary, pr_workdir, max_pods=max_pods)
        print("Starting pod-runner...")
        client.start()
        print(f"Pod-runner ready at {client.url}")
    else:
        print(f"Kubeconfig:     {args.kubeconfig}")
        print(f"Node name:      {args.node_name}")
        client = K8sClient(args.kubeconfig, args.node_name, backoff_limit=backoff_limit)

    test_start = datetime.now()

    print("\nCleaning up existing pods...")
    deleted = client.delete_all_pods("default")
    if deleted:
        print(f"  Deleted {deleted} existing pods/jobs, waiting for cleanup...")
        time.sleep(3)
    else:
        print("  None found.")

    print(f"\nRunning test...")
    results, dispatch_time, wall_time = run_test(
        client, plans, dispatch_rate, max_nominal,
    )
    test_timeout = dispatch_time + max(max_nominal * 4, 60)
    completed_count = sum(1 for r in results if r.actual_phase != "Timeout")
    print(f"\nCompleted: {completed_count}/{len(results)} in {wall_time:.1f}s")

    fetch_pod_details(client, results)

    print("\nDeleting pods...")
    for r in results:
        client.delete_pod("default", r.plan.name)
    client.stop()

    report_kwargs = dict(
        cfg=cfg, results=results,
        dispatch_time=dispatch_time, wall_time=wall_time, mode=args.mode,
        workdir=workdir, is_replay=is_replay,
        max_nominal=max_nominal, test_timeout=test_timeout,
        kubeconfig=args.kubeconfig,
        node_name=args.node_name if args.mode == "k8s" else None,
        pr_binary=pr_binary, backoff_limit=backoff_limit,
    )
    test_start_ts = test_start.strftime("%Y%m%dT%H%M%S")
    json_report = generate_json_report(**report_kwargs)
    json_path = os.path.join(workdir, f"report-{test_start_ts}.json")
    with open(json_path, "w") as f:
        json.dump(json_report, f, indent=2)
    md_report = json_to_markdown(json_report)
    md_path = os.path.join(workdir, f"report-{test_start_ts}.md")
    with open(md_path, "w") as f:
        f.write(md_report)

    print(f"\nReports written to:")
    print(f"  {md_path}")
    print(f"  {json_path}")

    total = len(results)
    succeeded = sum(1 for r in results if r.actual_phase == "Succeeded")
    failed = sum(1 for r in results if r.actual_phase == "Failed")
    timed_out = sum(1 for r in results if r.actual_phase == "Timeout")
    phase_correct = sum(1 for r in results if r.phase_correct)
    print(f"\nSummary: {succeeded} succeeded, {failed} failed, "
          f"{timed_out} timed out, accuracy={phase_correct}/{total}")
    ag = json_report["aggregates"]
    print(f"  nominal_time   mean={ag['nominal_time']['mean']:.2f}s "
          f"p50={ag['nominal_time']['p50']:.2f}s "
          f"max={ag['nominal_time']['max']:.2f}s")
    print(f"  actual_time    mean={ag['actual_time']['mean']:.2f}s "
          f"p50={ag['actual_time']['p50']:.2f}s "
          f"max={ag['actual_time']['max']:.2f}s")
    print(f"  start_latency  mean={ag['start_latency']['mean']:.2f}s "
          f"p50={ag['start_latency']['p50']:.2f}s "
          f"max={ag['start_latency']['max']:.2f}s")

    # Surface unexpected mismatches prominently.
    mismatched = [p for p in json_report["pods"] if not p["phase_correct"]]
    if mismatched:
        print(f"\n{len(mismatched)} phase mismatch(es) — see report for details. Sample:")
        for p in mismatched[:5]:
            fail_c = p.get("fail_container") or "-"
            fail_ec = p.get("fail_exit_code") or 0
            unterm = ",".join(p.get("unterminated") or []) or "-"
            pod_ph = p.get("pod_phase") or "-"
            pod_rs = p.get("pod_reason") or "-"
            job_rs = p.get("job_failure_reason") or "-"
            print(f"  {p['name']} expected={p['expected_phase']} "
                  f"actual={p['actual_phase']} pod_phase={pod_ph} "
                  f"pod_reason={pod_rs} job_reason={job_rs}")
            print(f"    fail={fail_c}(exit {fail_ec}) unterminated={unterm}")
            job_msg = p.get("job_failure_message") or ""
            if job_msg:
                print(f"    job_msg={job_msg}")


if __name__ == "__main__":
    main()
