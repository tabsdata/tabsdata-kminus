#
# Copyright 2026 Tabsdata Inc.
#

"""End-to-end tests for the kube SecretResolver / ConfigMapResolver path.

km configures pod-runner with `secretResolver.type: kube` and
`configMapResolver.type: kube` by default, so when a pod references a
Secret or ConfigMap, pod-runner fetches it from the apiserver started
by control-plane.

Each test creates the K8s resource via the K8s API, submits a Job whose
pod references it (env or volume mount), and asserts the Job completes —
which means the container could read the resolved value at the expected
location.
"""

import time
import uuid


def _unique_name(prefix):
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


def _wait_for_job(batch_client, name, namespace="default", timeout=60):
    deadline = time.time() + timeout
    while time.time() < deadline:
        j = batch_client.read_namespaced_job(name=name, namespace=namespace)
        if j.status.succeeded and j.status.succeeded >= 1:
            return j
        if j.status.failed and j.status.failed >= 1:
            raise AssertionError(f"Job {name} failed: {j.status}")
        time.sleep(2)
    raise TimeoutError(f"Job {name} did not complete within {timeout}s")


def _job_body(name, container_args, *, env=None, env_from=None,
              volumes=None, volume_mounts=None):
    """Build a minimal V1Job that runs a `sh -c` command on tdknode."""
    from kubernetes.client import (
        V1Job, V1JobSpec, V1PodTemplateSpec, V1PodSpec,
        V1Container, V1ObjectMeta, V1Toleration,
    )

    container = V1Container(
        name="probe",
        image="/dev/null",
        command=["sh", "-c"],
        args=[container_args],
        env=env,
        env_from=env_from,
        volume_mounts=volume_mounts,
    )

    return V1Job(
        metadata=V1ObjectMeta(name=name, namespace="default"),
        spec=V1JobSpec(
            template=V1PodTemplateSpec(
                spec=V1PodSpec(
                    node_name="tdknode",
                    restart_policy="Never",
                    containers=[container],
                    volumes=volumes,
                    tolerations=[V1Toleration(
                        key="virtual-kubelet.io/provider",
                        operator="Exists",
                        effect="NoSchedule",
                    )],
                ),
            ),
        ),
    )


# ---------------------------------------------------------------------------
# Secrets
# ---------------------------------------------------------------------------


class TestSecretEnvResolution:
    """Pod consumes a Secret via env valueFrom and envFrom."""

    def test_secret_via_value_from(self, km, k8s_client, batch_client):
        from kubernetes.client import (
            V1Secret, V1ObjectMeta, V1EnvVar, V1EnvVarSource, V1SecretKeySelector,
        )

        secret_name = _unique_name("sec")
        k8s_client.create_namespaced_secret(
            namespace="default",
            body=V1Secret(
                metadata=V1ObjectMeta(name=secret_name),
                string_data={"password": "s3cret-pw"},
            ),
        )

        try:
            job_name = _unique_name("sec-env")
            job = _job_body(
                job_name,
                # exits 0 only if DB_PASS resolved correctly
                "[ \"$DB_PASS\" = \"s3cret-pw\" ]",
                env=[V1EnvVar(
                    name="DB_PASS",
                    value_from=V1EnvVarSource(
                        secret_key_ref=V1SecretKeySelector(
                            name=secret_name, key="password",
                        ),
                    ),
                )],
            )
            batch_client.create_namespaced_job(namespace="default", body=job)
            try:
                _wait_for_job(batch_client, job_name)
            finally:
                batch_client.delete_namespaced_job(
                    name=job_name, namespace="default",
                    propagation_policy="Background",
                )
        finally:
            k8s_client.delete_namespaced_secret(
                name=secret_name, namespace="default",
            )

    def test_secret_via_env_from(self, km, k8s_client, batch_client):
        from kubernetes.client import (
            V1Secret, V1ObjectMeta, V1EnvFromSource, V1SecretEnvSource,
        )

        secret_name = _unique_name("sec-bulk")
        k8s_client.create_namespaced_secret(
            namespace="default",
            body=V1Secret(
                metadata=V1ObjectMeta(name=secret_name),
                string_data={"USER": "admin", "TOKEN": "abc123"},
            ),
        )

        try:
            job_name = _unique_name("sec-envfrom")
            job = _job_body(
                job_name,
                "[ \"$USER\" = \"admin\" ] && [ \"$TOKEN\" = \"abc123\" ]",
                env_from=[V1EnvFromSource(
                    secret_ref=V1SecretEnvSource(name=secret_name),
                )],
            )
            batch_client.create_namespaced_job(namespace="default", body=job)
            try:
                _wait_for_job(batch_client, job_name)
            finally:
                batch_client.delete_namespaced_job(
                    name=job_name, namespace="default",
                    propagation_policy="Background",
                )
        finally:
            k8s_client.delete_namespaced_secret(
                name=secret_name, namespace="default",
            )


# ---------------------------------------------------------------------------
# ConfigMaps via env
# ---------------------------------------------------------------------------


class TestConfigMapEnvResolution:
    """Pod consumes a ConfigMap via env valueFrom and envFrom."""

    def test_configmap_via_value_from(self, km, k8s_client, batch_client):
        from kubernetes.client import (
            V1ConfigMap, V1ObjectMeta, V1EnvVar, V1EnvVarSource,
            V1ConfigMapKeySelector,
        )

        cm_name = _unique_name("cm")
        k8s_client.create_namespaced_config_map(
            namespace="default",
            body=V1ConfigMap(
                metadata=V1ObjectMeta(name=cm_name),
                data={"host": "db.local"},
            ),
        )

        try:
            job_name = _unique_name("cm-env")
            job = _job_body(
                job_name,
                "[ \"$DB_HOST\" = \"db.local\" ]",
                env=[V1EnvVar(
                    name="DB_HOST",
                    value_from=V1EnvVarSource(
                        config_map_key_ref=V1ConfigMapKeySelector(
                            name=cm_name, key="host",
                        ),
                    ),
                )],
            )
            batch_client.create_namespaced_job(namespace="default", body=job)
            try:
                _wait_for_job(batch_client, job_name)
            finally:
                batch_client.delete_namespaced_job(
                    name=job_name, namespace="default",
                    propagation_policy="Background",
                )
        finally:
            k8s_client.delete_namespaced_config_map(
                name=cm_name, namespace="default",
            )

    def test_configmap_via_env_from(self, km, k8s_client, batch_client):
        from kubernetes.client import (
            V1ConfigMap, V1ObjectMeta, V1EnvFromSource, V1ConfigMapEnvSource,
        )

        cm_name = _unique_name("cm-bulk")
        k8s_client.create_namespaced_config_map(
            namespace="default",
            body=V1ConfigMap(
                metadata=V1ObjectMeta(name=cm_name),
                data={"FEATURE_X": "enabled", "MAX_RETRIES": "3"},
            ),
        )

        try:
            job_name = _unique_name("cm-envfrom")
            job = _job_body(
                job_name,
                "[ \"$FEATURE_X\" = \"enabled\" ] && [ \"$MAX_RETRIES\" = \"3\" ]",
                env_from=[V1EnvFromSource(
                    config_map_ref=V1ConfigMapEnvSource(name=cm_name),
                )],
            )
            batch_client.create_namespaced_job(namespace="default", body=job)
            try:
                _wait_for_job(batch_client, job_name)
            finally:
                batch_client.delete_namespaced_job(
                    name=job_name, namespace="default",
                    propagation_policy="Background",
                )
        finally:
            k8s_client.delete_namespaced_config_map(
                name=cm_name, namespace="default",
            )


# ---------------------------------------------------------------------------
# ConfigMaps via volume mount
# ---------------------------------------------------------------------------


class TestConfigMapVolumeMount:
    """Pod mounts a ConfigMap as a directory; one file per key."""

    def test_configmap_files_match_keys(self, km, k8s_client, batch_client):
        from kubernetes.client import (
            V1ConfigMap, V1ObjectMeta, V1Volume, V1ConfigMapVolumeSource,
            V1VolumeMount,
        )

        cm_name = _unique_name("cm-vol")
        k8s_client.create_namespaced_config_map(
            namespace="default",
            body=V1ConfigMap(
                metadata=V1ObjectMeta(name=cm_name),
                data={"app.conf": "setting=value", "log.yaml": "level: info"},
            ),
        )

        try:
            mount_path = f"/configmaps/{cm_name}"
            job_name = _unique_name("cm-vol-job")
            # pod-runner rewrites absolute mountPath values under the
            # container root, so the container reads the files via the
            # injected $KM_CONTAINER_ROOT env var rather than the K8s-style
            # absolute path.
            check = (
                f"[ \"$(cat $KM_CONTAINER_ROOT{mount_path}/app.conf)\" = \"setting=value\" ] && "
                f"[ \"$(cat $KM_CONTAINER_ROOT{mount_path}/log.yaml)\" = \"level: info\" ]"
            )
            job = _job_body(
                job_name,
                check,
                volumes=[V1Volume(
                    name="cfg",
                    config_map=V1ConfigMapVolumeSource(name=cm_name),
                )],
                volume_mounts=[V1VolumeMount(
                    name="cfg",
                    mount_path=mount_path,
                    read_only=True,
                )],
            )
            batch_client.create_namespaced_job(namespace="default", body=job)
            try:
                _wait_for_job(batch_client, job_name)
            finally:
                batch_client.delete_namespaced_job(
                    name=job_name, namespace="default",
                    propagation_policy="Background",
                )
        finally:
            k8s_client.delete_namespaced_config_map(
                name=cm_name, namespace="default",
            )
