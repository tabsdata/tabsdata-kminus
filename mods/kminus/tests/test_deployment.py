#
# Copyright 2026 Tabsdata Inc.
#

"""Integration tests for K8s Deployments through the full kminus stack.

The kube-controller-manager expands a Deployment
(`Deployment` -> `ReplicaSet` -> `Pod`); each pod is pinned to the
virtual-kubelet node via `spec.nodeName` (kminus has no scheduler) and
tolerates the node's `virtual-kubelet.io/provider:NoSchedule` taint, so the
pod-runner executes it as a native process and the Deployment reports the
replicas available.
"""

import time
import uuid


NODE_NAME = "tdknode"


def _unique_name(prefix):
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


def _deployment(name, replicas):
    """Build a Deployment whose pod template pins to the kminus node and
    tolerates its taint — the convention required on a scheduler-less,
    tainted single node. `restartPolicy` is left unset (defaults to
    `Always`, the only value Deployments allow)."""
    from kubernetes.client import (
        V1Container,
        V1Deployment,
        V1DeploymentSpec,
        V1LabelSelector,
        V1ObjectMeta,
        V1PodSpec,
        V1PodTemplateSpec,
        V1Toleration,
    )

    labels = {"app": name}
    return V1Deployment(
        metadata=V1ObjectMeta(name=name, namespace="default"),
        spec=V1DeploymentSpec(
            replicas=replicas,
            selector=V1LabelSelector(match_labels=labels),
            template=V1PodTemplateSpec(
                metadata=V1ObjectMeta(labels=labels),
                spec=V1PodSpec(
                    node_name=NODE_NAME,
                    containers=[
                        V1Container(
                            name="app",
                            image="/dev/null",
                            command=["sh", "-c"],
                            args=["while true; do sleep 1; done"],
                        ),
                    ],
                    tolerations=[
                        V1Toleration(
                            key="virtual-kubelet.io/provider",
                            operator="Exists",
                            effect="NoSchedule",
                        ),
                    ],
                ),
            ),
        ),
    )


def _wait(predicate, timeout, message, interval=2):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return
        time.sleep(interval)
    raise TimeoutError(f"timed out after {timeout}s waiting for {message}")


def _available(apps_client, name):
    """Replicas the Deployment reports as available (running and Ready)."""
    d = apps_client.read_namespaced_deployment_status(name=name, namespace="default")
    return d.status.available_replicas or 0


def _running_pod_names(k8s_client, name):
    """Names of the deployment's pods currently in the Running phase."""
    pods = k8s_client.list_namespaced_pod(
        namespace="default", label_selector=f"app={name}"
    ).items
    return {p.metadata.name for p in pods if p.status.phase == "Running"}


def _delete_deployment(apps_client, name):
    apps_client.delete_namespaced_deployment(
        name=name, namespace="default", propagation_policy="Background"
    )


class TestDeployment:
    """K8s apps/v1 Deployment executed through virtual-kubelet -> pod-runner."""

    def test_deployment_runs_two_pods(self, apps_client, k8s_client):
        """A `replicas: 2` Deployment brings up exactly two running pods and
        reports both available."""
        name = _unique_name("deploy2")
        apps_client.create_namespaced_deployment(
            namespace="default", body=_deployment(name, replicas=2)
        )
        try:
            _wait(
                lambda: _available(apps_client, name) == 2,
                timeout=90,
                message=f"deployment {name} to report 2 available replicas",
            )
            assert len(_running_pod_names(k8s_client, name)) == 2
        finally:
            _delete_deployment(apps_client, name)

    def test_deployment_restarts_crashed_pod(self, apps_client, k8s_client):
        """When one of the pods crashes (is lost from the node), the
        ReplicaSet controller restarts it: the deployment recreates a
        replacement and returns to two running pods."""
        name = _unique_name("crash")
        apps_client.create_namespaced_deployment(
            namespace="default", body=_deployment(name, replicas=2)
        )
        try:
            _wait(
                lambda: len(_running_pod_names(k8s_client, name)) == 2,
                timeout=90,
                message=f"deployment {name} to reach 2 running pods",
            )
            before = _running_pod_names(k8s_client, name)
            victim = sorted(before)[0]

            # Simulate a pod crash: force-delete it so it disappears from the
            # node abruptly, as if its process died.
            k8s_client.delete_namespaced_pod(
                name=victim, namespace="default", grace_period_seconds=0
            )

            # The ReplicaSet controller notices the missing replica and
            # restarts it: a fresh pod (different name) comes up so the
            # deployment is back to two running pods, none of them the victim.
            def restarted():
                names = _running_pod_names(k8s_client, name)
                return len(names) == 2 and victim not in names

            _wait(
                restarted,
                timeout=120,
                message=f"deployment {name} to restart the crashed pod",
            )
            assert _available(apps_client, name) == 2
        finally:
            _delete_deployment(apps_client, name)
