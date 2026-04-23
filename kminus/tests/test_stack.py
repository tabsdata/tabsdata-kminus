#
# Copyright 2026 Tabsdata Inc.
#

"""Integration tests for the full kminus stack (km orchestrator).

Tests start the complete stack via km, submit workloads through the
K8s API, and verify execution through restart cycles.
"""

import base64
import time

from conftest import KminusProcess
from itest_common import find_free_port


def _k8s_client(kubeconfig):
    from kubernetes import client, config as k8s_config
    k8s_config.load_kube_config(config_file=kubeconfig)
    return client.CoreV1Api()


def _batch_client(kubeconfig):
    from kubernetes import client, config as k8s_config
    k8s_config.load_kube_config(config_file=kubeconfig)
    return client.BatchV1Api()


class TestStackStartStop:
    """The kminus stack starts and stops cleanly."""

    def test_start_and_stop(self, km_bin, test_dir):
        km = KminusProcess(km_bin, test_dir)
        km.start()
        try:
            assert km.process.poll() is None, "km should still be running"

            # Verify K8s API is reachable
            k8s = _k8s_client(km.kubeconfig)
            ns = k8s.list_namespace(limit=1)
            assert len(ns.items) > 0
        finally:
            km.stop()

    def test_node_registered(self, km_bin, test_dir):
        km = KminusProcess(km_bin, test_dir)
        km.start()
        try:
            k8s = _k8s_client(km.kubeconfig)
            nodes = k8s.list_node()
            assert len(nodes.items) >= 1
            node_names = [n.metadata.name for n in nodes.items]
            assert any("tdknode" in name for name in node_names)
        finally:
            km.stop()


class TestPodSurvivesRestart:
    """A long-running pod started before restart completes after restart."""

    def test_resources_survive_restart(self, km_bin, test_dir):
        """Create a Secret before shutdown; verify it persists after restart."""
        from kubernetes.client import V1Secret, V1ObjectMeta
        import base64

        # --- First run: create a secret ---
        km1 = KminusProcess(km_bin, test_dir)
        km1.start()

        k8s = _k8s_client(km1.kubeconfig)
        secret_name = "survive-restart"
        k8s.create_namespaced_secret(
            namespace="default",
            body=V1Secret(
                metadata=V1ObjectMeta(name=secret_name),
                string_data={"key": "restart-value"},
            ),
        )

        # Verify it exists
        result = k8s.read_namespaced_secret(name=secret_name, namespace="default")
        assert result.data["key"] == base64.b64encode(b"restart-value").decode()

        km1.stop()

        # --- Second run: same workdir, new ports ---
        km2 = KminusProcess(km_bin, test_dir)
        km2.start()

        try:
            k8s2 = _k8s_client(km2.kubeconfig)
            result = k8s2.read_namespaced_secret(
                name=secret_name, namespace="default"
            )
            assert result.data["key"] == base64.b64encode(b"restart-value").decode()
        finally:
            km2.stop()

    def test_job_completes_after_restart(self, km_bin, test_dir):
        """Start stack, stop, restart, create a Job, verify it completes."""
        from kubernetes.client import (
            V1Job, V1JobSpec, V1PodTemplateSpec, V1PodSpec,
            V1Container, V1ObjectMeta, V1Toleration,
        )

        km1 = KminusProcess(km_bin, test_dir)
        km1.start()
        km1.stop()

        km2 = KminusProcess(km_bin, test_dir)
        km2.start()

        try:
            batch = _batch_client(km2.kubeconfig)
            job_name = "post-restart-job"
            batch.create_namespaced_job(
                namespace="default",
                body=V1Job(
                    metadata=V1ObjectMeta(name=job_name),
                    spec=V1JobSpec(
                        template=V1PodTemplateSpec(
                            spec=V1PodSpec(
                                node_name="tdknode",
                                restart_policy="Never",
                                containers=[V1Container(
                                    name="worker",
                                    image="/dev/null",
                                    command=["sh", "-c"],
                                    args=["echo 'post-restart done'"],
                                )],
                                tolerations=[V1Toleration(
                                    key="virtual-kubelet.io/provider",
                                    operator="Exists",
                                    effect="NoSchedule",
                                )],
                            ),
                        ),
                    ),
                ),
            )

            deadline = time.time() + 60
            succeeded = False
            while time.time() < deadline:
                j = batch.read_namespaced_job(name=job_name, namespace="default")
                if j.status.succeeded and j.status.succeeded >= 1:
                    succeeded = True
                    break
                time.sleep(2)

            assert succeeded, f"Job {job_name} did not complete after restart"
        finally:
            km2.stop()

    def test_pod_in_k8s_persists_across_restart(self, km_bin, test_dir):
        """A pod created via K8s API is still visible after stack restart.

        The pod spec is stored in etcd. After restart, the control-plane
        loads it from etcd and the K8s API still shows it.
        """
        from kubernetes.client import (
            V1Pod, V1PodSpec, V1Container, V1ObjectMeta, V1Toleration,
        )

        # --- First run: create a pod via K8s API ---
        km1 = KminusProcess(km_bin, test_dir)
        km1.start()

        k8s = _k8s_client(km1.kubeconfig)
        pod_name = "persist-across-restart"
        k8s.create_namespaced_pod(
            namespace="default",
            body=V1Pod(
                metadata=V1ObjectMeta(name=pod_name),
                spec=V1PodSpec(
                    node_name="tdknode",
                    restart_policy="Never",
                    containers=[V1Container(
                        name="main",
                        image="/dev/null",
                        command=["sh", "-c"],
                        args=["sleep 120"],
                    )],
                    tolerations=[V1Toleration(
                        key="virtual-kubelet.io/provider",
                        operator="Exists",
                        effect="NoSchedule",
                    )],
                ),
            ),
        )

        # Verify it was created
        pod = k8s.read_namespaced_pod(name=pod_name, namespace="default")
        assert pod.metadata.name == pod_name

        km1.stop()

        # --- Second run: same workdir, verify pod still in K8s ---
        km2 = KminusProcess(km_bin, test_dir)
        km2.start()

        try:
            k8s2 = _k8s_client(km2.kubeconfig)
            pod = k8s2.read_namespaced_pod(name=pod_name, namespace="default")
            assert pod.metadata.name == pod_name, (
                f"Pod {pod_name} not found in K8s after restart"
            )
        finally:
            km2.stop()

