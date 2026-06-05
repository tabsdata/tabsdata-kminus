#
# Copyright 2026 Tabsdata Inc.
#

"""Integration tests for pod lifecycle through virtual-kubelet."""

import time

from kubernetes.client import V1Pod, V1PodSpec, V1Container, V1ObjectMeta, V1Toleration


def _create_test_pod(k8s_client, name, namespace="default",
                     node_name="test-vk-node"):
    """Create a pod scheduled to the given node (the virtual node by default)."""
    pod = V1Pod(
        metadata=V1ObjectMeta(name=name, namespace=namespace),
        spec=V1PodSpec(
            node_name=node_name,
            containers=[
                V1Container(name="test", image="busybox", command=["sleep", "3600"]),
            ],
            tolerations=[
                V1Toleration(
                    key="virtual-kubelet.io/provider",
                    operator="Exists",
                    effect="NoSchedule",
                ),
            ],
        ),
    )
    return k8s_client.create_namespaced_pod(namespace=namespace, body=pod)


class TestPodCreate:
    """Pods created on the virtual node reach the mock pod-runner."""

    def test_pod_reaches_mock_pod_runner(
        self, virtual_kubelet, k8s_client, mock_pod_runner
    ):
        """A pod assigned to the VK node should appear in the mock pod-runner."""
        _create_test_pod(k8s_client, "test-pod-create")

        # Wait for the pod to propagate to the mock
        found = False
        for _ in range(30):
            if "default/test-pod-create" in mock_pod_runner.pods:
                found = True
                break
            time.sleep(1)

        assert found, "Pod did not reach the mock pod-runner"

        # Cleanup
        k8s_client.delete_namespaced_pod(name="test-pod-create", namespace="default")


class TestNodeFiltering:
    """The VK only runs pods assigned to its node — like a real kubelet,
    pods pinned to another (or nonexistent) node stay Pending. Running
    them anyway would break apiserver routing (e.g. `kubectl logs`
    resolves the kubelet through the pod's spec.nodeName)."""

    def test_pod_for_other_node_never_reaches_pod_runner(
        self, virtual_kubelet, k8s_client, mock_pod_runner
    ):
        _create_test_pod(k8s_client, "test-pod-other-node",
                         node_name="some-other-node")

        # Give the VK ample time to (wrongly) pick it up.
        deadline = time.time() + 10
        while time.time() < deadline:
            assert "default/test-pod-other-node" not in mock_pod_runner.pods, \
                "pod pinned to another node must not reach the pod-runner"
            time.sleep(1)

        # The pod was never run: it stays Pending with no status updates.
        pod = k8s_client.read_namespaced_pod(
            name="test-pod-other-node", namespace="default")
        assert pod.status.phase in (None, "Pending"), \
            f"pod pinned to another node should stay Pending, got {pod.status.phase}"

        # Cleanup
        k8s_client.delete_namespaced_pod(
            name="test-pod-other-node", namespace="default")


class TestPodDelete:
    """Deleting a pod removes it from the mock pod-runner."""

    def test_pod_removed_from_mock_on_delete(
        self, virtual_kubelet, k8s_client, mock_pod_runner
    ):
        """After deleting a pod, it should be removed from the mock pod-runner."""
        _create_test_pod(k8s_client, "test-pod-delete")

        # Wait for pod to appear in mock
        for _ in range(30):
            if "default/test-pod-delete" in mock_pod_runner.pods:
                break
            time.sleep(1)

        assert "default/test-pod-delete" in mock_pod_runner.pods

        # Delete the pod
        k8s_client.delete_namespaced_pod(name="test-pod-delete", namespace="default")

        # Wait for deletion to propagate
        removed = False
        for _ in range(30):
            if "default/test-pod-delete" not in mock_pod_runner.pods:
                removed = True
                break
            time.sleep(1)

        assert removed, "Pod was not removed from mock pod-runner after deletion"
