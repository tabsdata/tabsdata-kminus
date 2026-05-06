#
# Copyright 2026 Tabsdata Inc.
#

"""Integration tests for K8s secret and configmap resolution via pod-runner.

These tests create K8s resources via the control-plane, then create pods
through the pod-runner that reference those resources. The pod-runner
resolves them via its kube resolvers.

Note: the auto-generated prconfig has empty resolver types. These tests
verify that when resolvers are not configured, pods can still be created
(resolution is skipped). Full kube-resolver tests require a custom
prconfig with secretResolver.type and configMapResolver.type set to "kube".
"""

import base64
import time
import uuid


def _unique_name(prefix):
    return f"{prefix}-{uuid.uuid4().hex[:12]}"


class TestPodCreateWithoutResolvers:
    """Pods can be created even without resolver configuration."""

    def test_create_simple_pod(self, pod_runner, mtls_client):
        name = _unique_name("simple")
        pod_spec = {
            "metadata": {
                "namespace": "default",
                "name": name,
                "uid": f"uid-{name}",
            },
            "spec": {
                "containers": [{
                    "name": "main",
                    "command": ["sh", "-c", "echo running && sleep 5"],
                }],
            },
        }
        status, _ = mtls_client.post(f"{pod_runner.url}/createPod", pod_spec)
        assert status in (200, 201), f"Expected 200/201, got {status}"

        # Verify pod exists
        status, data = mtls_client.get(
            f"{pod_runner.url}/getPod?namespace=default&name={name}"
        )
        assert status == 200
        assert data["metadata"]["name"] == name

        # Cleanup
        mtls_client.delete(
            f"{pod_runner.url}/deletePod?namespace=default&name={name}"
        )

    def test_pod_status_after_create(self, pod_runner, mtls_client):
        name = _unique_name("status")
        pod_spec = {
            "metadata": {
                "namespace": "default",
                "name": name,
                "uid": f"uid-{name}",
            },
            "spec": {
                "containers": [{
                    "name": "main",
                    "command": ["sh", "-c", "sleep 10"],
                }],
            },
        }
        mtls_client.post(f"{pod_runner.url}/createPod", pod_spec)

        # Wait briefly for the pod to start
        time.sleep(2)

        status, data = mtls_client.get(
            f"{pod_runner.url}/getPodStatus?namespace=default&name={name}"
        )
        assert status == 200
        assert data is not None

        # Cleanup
        mtls_client.delete(
            f"{pod_runner.url}/deletePod?namespace=default&name={name}"
        )


class TestKubernetesResourcesExist:
    """Verify K8s resources can be created (control-plane works)."""

    def test_create_secret(self, k8s_client):
        from kubernetes.client import V1Secret, V1ObjectMeta

        name = _unique_name("pr-secret")
        secret = V1Secret(
            metadata=V1ObjectMeta(name=name, namespace="default"),
            string_data={"KEY": "value"},
        )
        k8s_client.create_namespaced_secret(namespace="default", body=secret)

        result = k8s_client.read_namespaced_secret(name=name, namespace="default")
        assert result.data["KEY"] == base64.b64encode(b"value").decode()

        # Cleanup
        k8s_client.delete_namespaced_secret(name=name, namespace="default")

    def test_create_configmap(self, k8s_client):
        from kubernetes.client import V1ConfigMap, V1ObjectMeta

        name = _unique_name("pr-cm")
        cm = V1ConfigMap(
            metadata=V1ObjectMeta(name=name, namespace="default"),
            data={"host": "localhost"},
        )
        k8s_client.create_namespaced_config_map(namespace="default", body=cm)

        result = k8s_client.read_namespaced_config_map(name=name, namespace="default")
        assert result.data["host"] == "localhost"

        # Cleanup
        k8s_client.delete_namespaced_config_map(name=name, namespace="default")
