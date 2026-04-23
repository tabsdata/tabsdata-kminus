#
# Copyright 2026 Tabsdata Inc.
#

"""Integration tests for virtual-kubelet node registration."""

import time


class TestNodeRegistration:
    """The virtual-kubelet registers a node in the cluster."""

    def test_node_appears_in_cluster(self, virtual_kubelet, k8s_client):
        """The virtual node should be visible via the K8s API."""
        # Give node time to register
        node = None
        for _ in range(10):
            try:
                node = k8s_client.read_node(name="test-vk-node")
                break
            except Exception:
                time.sleep(1)

        assert node is not None, "Virtual node 'test-vk-node' not found"
        assert node.metadata.name == "test-vk-node"

    def test_node_has_expected_labels(self, virtual_kubelet, k8s_client):
        """The virtual node should have virtual-kubelet labels."""
        node = None
        for _ in range(10):
            try:
                node = k8s_client.read_node(name="test-vk-node")
                break
            except Exception:
                time.sleep(1)

        assert node is not None
        labels = node.metadata.labels
        assert labels.get("type") == "virtual-kubelet"

    def test_node_has_taint(self, virtual_kubelet, k8s_client):
        """The virtual node should have the NoSchedule taint."""
        node = None
        for _ in range(10):
            try:
                node = k8s_client.read_node(name="test-vk-node")
                break
            except Exception:
                time.sleep(1)

        assert node is not None
        taints = node.spec.taints or []
        vk_taints = [t for t in taints if t.key == "virtual-kubelet.io/provider"]
        assert len(vk_taints) == 1
        assert vk_taints[0].effect == "NoSchedule"

    def test_node_reports_capacity(self, virtual_kubelet, k8s_client):
        """The virtual node should report capacity from the mock pod-runner."""
        node = None
        for _ in range(10):
            try:
                node = k8s_client.read_node(name="test-vk-node")
                if node.status and node.status.capacity:
                    break
            except Exception:
                pass
            time.sleep(1)

        assert node is not None
        assert node.status.capacity is not None
        assert "cpu" in node.status.capacity
