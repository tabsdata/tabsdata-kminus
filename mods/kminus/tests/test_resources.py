#
# Copyright 2026 Tabsdata Inc.
#

"""Integration tests verifying that the kminus stack accepts K8s resources.

These tests create Secrets and ConfigMaps via the K8s API exposed by the
control-plane that km brought up.
"""

import base64
import uuid


def _unique_name(prefix):
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


class TestKubernetesResourcesExist:
    """Verify K8s Secrets and ConfigMaps can be created and read back."""

    def test_create_secret(self, km, k8s_client):
        from kubernetes.client import V1Secret, V1ObjectMeta

        name = _unique_name("k-secret")
        secret = V1Secret(
            metadata=V1ObjectMeta(name=name, namespace="default"),
            string_data={"KEY": "value"},
        )
        k8s_client.create_namespaced_secret(namespace="default", body=secret)

        result = k8s_client.read_namespaced_secret(name=name, namespace="default")
        assert result.data["KEY"] == base64.b64encode(b"value").decode()

        k8s_client.delete_namespaced_secret(name=name, namespace="default")

    def test_create_configmap(self, km, k8s_client):
        from kubernetes.client import V1ConfigMap, V1ObjectMeta

        name = _unique_name("k-cm")
        cm = V1ConfigMap(
            metadata=V1ObjectMeta(name=name, namespace="default"),
            data={"host": "localhost"},
        )
        k8s_client.create_namespaced_config_map(namespace="default", body=cm)

        result = k8s_client.read_namespaced_config_map(
            name=name, namespace="default",
        )
        assert result.data["host"] == "localhost"

        k8s_client.delete_namespaced_config_map(name=name, namespace="default")
