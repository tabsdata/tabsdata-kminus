#
# Copyright 2026 Tabsdata Inc.
#

"""Integration tests for control-plane restart behavior.

Verifies that Kubernetes resources (secrets, configmaps) stored in etcd
are preserved across control-plane restarts, since the etcd data directory
is persistent.
"""

import base64
import os
import time

import yaml
from itest_common import ControlPlaneProcess, find_free_port


def _load_kubeconfig(kubeconfig_path):
    """Load kubeconfig and return a kubernetes client."""
    from kubernetes import client, config as k8s_config

    k8s_config.load_kube_config(config_file=kubeconfig_path)
    return client.CoreV1Api()


class TestRestartPreservesResources:
    """Resources created before restart survive across restarts."""

    def test_secret_survives_restart(self, control_plane_bin, test_dir):
        """A secret created before shutdown is still present after restart."""
        api_port = find_free_port()
        kcm_port = find_free_port()

        # --- First run: create a secret ---
        cp1 = ControlPlaneProcess(control_plane_bin, test_dir, api_port, kcm_port)
        cp1.start()

        k8s = _load_kubeconfig(cp1.kubeconfig)

        from kubernetes.client import V1Secret, V1ObjectMeta

        secret = V1Secret(
            metadata=V1ObjectMeta(name="test-secret", namespace="default"),
            data={"password": base64.b64encode(b"s3cret").decode()},
        )
        k8s.create_namespaced_secret(namespace="default", body=secret)

        # Verify it exists
        result = k8s.read_namespaced_secret(name="test-secret", namespace="default")
        assert result.data["password"] == base64.b64encode(b"s3cret").decode()

        cp1.stop()

        # --- Second run: same workdir, new ports, verify secret persists ---
        api_port2 = find_free_port()
        kcm_port2 = find_free_port()
        cp2 = ControlPlaneProcess(control_plane_bin, test_dir, api_port2, kcm_port2)
        cp2.start()

        k8s2 = _load_kubeconfig(cp2.kubeconfig)

        result = k8s2.read_namespaced_secret(name="test-secret", namespace="default")
        assert result.data["password"] == base64.b64encode(b"s3cret").decode()

        cp2.stop()

    def test_configmap_survives_restart(self, control_plane_bin, test_dir):
        """A configmap created before shutdown is still present after restart."""
        api_port = find_free_port()
        kcm_port = find_free_port()

        # --- First run: create a configmap ---
        cp1 = ControlPlaneProcess(control_plane_bin, test_dir, api_port, kcm_port)
        cp1.start()

        k8s = _load_kubeconfig(cp1.kubeconfig)

        from kubernetes.client import V1ConfigMap, V1ObjectMeta

        cm = V1ConfigMap(
            metadata=V1ObjectMeta(name="test-config", namespace="default"),
            data={"app.conf": "debug=true\nport=8080"},
        )
        k8s.create_namespaced_config_map(namespace="default", body=cm)

        # Verify it exists
        result = k8s.read_namespaced_config_map(
            name="test-config", namespace="default"
        )
        assert result.data["app.conf"] == "debug=true\nport=8080"

        cp1.stop()

        # --- Second run: same workdir, new ports, verify configmap persists ---
        api_port2 = find_free_port()
        kcm_port2 = find_free_port()
        cp2 = ControlPlaneProcess(control_plane_bin, test_dir, api_port2, kcm_port2)
        cp2.start()

        k8s2 = _load_kubeconfig(cp2.kubeconfig)

        result = k8s2.read_namespaced_config_map(
            name="test-config", namespace="default"
        )
        assert result.data["app.conf"] == "debug=true\nport=8080"

        cp2.stop()

    def test_multiple_resources_survive_restart(self, control_plane_bin, test_dir):
        """Multiple resources of different types all persist across restart."""
        api_port = find_free_port()
        kcm_port = find_free_port()

        # --- First run: create several resources ---
        cp1 = ControlPlaneProcess(control_plane_bin, test_dir, api_port, kcm_port)
        cp1.start()

        k8s = _load_kubeconfig(cp1.kubeconfig)

        from kubernetes.client import V1Secret, V1ConfigMap, V1ObjectMeta

        for i in range(3):
            k8s.create_namespaced_secret(
                namespace="default",
                body=V1Secret(
                    metadata=V1ObjectMeta(name=f"secret-{i}"),
                    data={"val": base64.b64encode(f"value-{i}".encode()).decode()},
                ),
            )
            k8s.create_namespaced_config_map(
                namespace="default",
                body=V1ConfigMap(
                    metadata=V1ObjectMeta(name=f"cm-{i}"),
                    data={"key": f"data-{i}"},
                ),
            )

        cp1.stop()

        # --- Second run: verify all resources ---
        api_port2 = find_free_port()
        kcm_port2 = find_free_port()
        cp2 = ControlPlaneProcess(control_plane_bin, test_dir, api_port2, kcm_port2)
        cp2.start()

        k8s2 = _load_kubeconfig(cp2.kubeconfig)

        for i in range(3):
            secret = k8s2.read_namespaced_secret(name=f"secret-{i}", namespace="default")
            assert secret.data["val"] == base64.b64encode(f"value-{i}".encode()).decode()

            cm = k8s2.read_namespaced_config_map(name=f"cm-{i}", namespace="default")
            assert cm.data["key"] == f"data-{i}"

        cp2.stop()
