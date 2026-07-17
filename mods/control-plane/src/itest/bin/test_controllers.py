#
# Copyright 2026 Tabsdata Inc.
#

"""Integration tests for the kube-controller-manager controllers.

Verifies that the ttl-after-finished controller deletes finished Jobs
once `ttlSecondsAfterFinished` expires, and that the garbage-collector
controller cascade-deletes dependents via `ownerReferences`.

Since there is no kubelet in the control-plane integration environment,
job pods never actually run; tests drive job completion by patching the
pod status to `Succeeded` and letting the job controller react.
"""

import time

import pytest


def _load_clients(kubeconfig_path):
    """Load kubeconfig and return (CoreV1Api, BatchV1Api) clients."""
    from kubernetes import client, config as k8s_config

    k8s_config.load_kube_config(config_file=kubeconfig_path)
    return client.CoreV1Api(), client.BatchV1Api()


def _wait_for(predicate, timeout=60, interval=0.5, message="condition"):
    """Poll predicate until it returns truthy or timeout expires."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        result = predicate()
        if result:
            return result
        time.sleep(interval)
    pytest.fail(f"Timed out after {timeout}s waiting for {message}")


def _is_not_found(func):
    """Return True if func raises a 404 ApiException."""
    from kubernetes.client.rest import ApiException

    try:
        func()
        return False
    except ApiException as e:
        if e.status == 404:
            return True
        raise


def _make_job(name, ttl_seconds_after_finished=None):
    """Build a minimal job manifest."""
    from kubernetes.client import (
        V1Container,
        V1Job,
        V1JobSpec,
        V1ObjectMeta,
        V1PodSpec,
        V1PodTemplateSpec,
    )

    return V1Job(
        metadata=V1ObjectMeta(name=name, namespace="default"),
        spec=V1JobSpec(
            ttl_seconds_after_finished=ttl_seconds_after_finished,
            backoff_limit=0,
            template=V1PodTemplateSpec(
                spec=V1PodSpec(
                    restart_policy="Never",
                    containers=[
                        V1Container(name="main", image="busybox", command=["true"])
                    ],
                )
            ),
        ),
    )


def _wait_for_job_pod(core, job_name):
    """Wait until the job controller creates a pod for the job."""

    def pod_created():
        pods = core.list_namespaced_pod(
            namespace="default", label_selector=f"job-name={job_name}"
        )
        return pods.items[0] if pods.items else None

    return _wait_for(pod_created, message=f"pod of job {job_name}")


def _mark_pod_succeeded(core, pod_name):
    """Patch the pod status to Succeeded, simulating a kubelet run."""
    core.patch_namespaced_pod_status(
        name=pod_name,
        namespace="default",
        body={"status": {"phase": "Succeeded"}},
    )


class TestTTLAfterFinished:
    """The ttl-after-finished controller deletes finished jobs."""

    def test_finished_job_deleted_after_ttl(self, control_plane):
        """A job that completes is deleted once ttlSecondsAfterFinished expires."""
        core, batch = _load_clients(control_plane.kubeconfig)

        batch.create_namespaced_job(
            namespace="default",
            body=_make_job("ttl-job", ttl_seconds_after_finished=1),
        )

        pod = _wait_for_job_pod(core, "ttl-job")
        _mark_pod_succeeded(core, pod.metadata.name)

        # The job controller marks the job Complete, then the
        # ttl-after-finished controller deletes it 1s later.
        _wait_for(
            lambda: _is_not_found(
                lambda: batch.read_namespaced_job(name="ttl-job", namespace="default")
            ),
            message="job ttl-job to be deleted by ttl-after-finished controller",
        )

    def test_job_without_ttl_is_not_deleted(self, control_plane):
        """A finished job without ttlSecondsAfterFinished stays around."""
        core, batch = _load_clients(control_plane.kubeconfig)

        batch.create_namespaced_job(namespace="default", body=_make_job("no-ttl-job"))

        pod = _wait_for_job_pod(core, "no-ttl-job")
        _mark_pod_succeeded(core, pod.metadata.name)

        # Wait for the job controller to mark the job complete.
        def job_complete():
            job = batch.read_namespaced_job(name="no-ttl-job", namespace="default")
            conditions = (job.status and job.status.conditions) or []
            return any(c.type == "Complete" and c.status == "True" for c in conditions)

        _wait_for(job_complete, message="job no-ttl-job to be marked Complete")

        # Give the ttl controller a chance to misbehave, then verify the
        # job is still present.
        time.sleep(3)
        job = batch.read_namespaced_job(name="no-ttl-job", namespace="default")
        assert job.metadata.deletion_timestamp is None


class TestDeploymentController:
    """The deployment + replicaset controllers expand a Deployment.

    As with Jobs, there is no kubelet in the control-plane-only
    environment, so the created pods never run (they stay Pending); this
    test only verifies that the controllers are registered and perform the
    `Deployment` -> `ReplicaSet` -> `Pod` expansion.
    """

    def _make_deployment(self, name, replicas=2):
        from kubernetes.client import (
            V1Container,
            V1Deployment,
            V1DeploymentSpec,
            V1LabelSelector,
            V1ObjectMeta,
            V1PodSpec,
            V1PodTemplateSpec,
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
                        restart_policy="Always",
                        containers=[
                            V1Container(name="main", image="busybox", command=["true"])
                        ],
                    ),
                ),
            ),
        )

    def test_deployment_expands_to_replicaset_and_pods(self, control_plane):
        """Creating a Deployment yields a ReplicaSet (owned by the
        Deployment) and Pods (owned by the ReplicaSet)."""
        from kubernetes import client

        core, _ = _load_clients(control_plane.kubeconfig)
        apps = client.AppsV1Api()

        name = "expand-deploy"
        apps.create_namespaced_deployment(
            namespace="default", body=self._make_deployment(name, replicas=2)
        )

        # deployment controller -> a ReplicaSet owned by the Deployment.
        def replicaset_created():
            rs = apps.list_namespaced_replica_set(
                namespace="default", label_selector=f"app={name}"
            )
            return rs.items[0] if rs.items else None

        rs = _wait_for(
            replicaset_created, message=f"replicaset of deployment {name}"
        )
        rs_owners = [o.kind for o in (rs.metadata.owner_references or [])]
        assert "Deployment" in rs_owners

        # replicaset controller -> Pods owned by the ReplicaSet.
        def pods_created():
            pods = core.list_namespaced_pod(
                namespace="default", label_selector=f"app={name}"
            )
            return pods.items if len(pods.items) >= 2 else None

        pods = _wait_for(pods_created, message=f"pods of deployment {name}")
        assert len(pods) == 2
        for pod in pods:
            pod_owners = [o.kind for o in (pod.metadata.owner_references or [])]
            assert "ReplicaSet" in pod_owners

        apps.delete_namespaced_deployment(
            name=name, namespace="default", propagation_policy="Background"
        )


class TestGarbageCollector:
    """The garbage-collector controller cascade-deletes dependents."""

    def test_dependent_deleted_with_owner(self, control_plane):
        """Deleting an owner garbage-collects dependents via ownerReferences."""
        from kubernetes.client import V1ConfigMap, V1ObjectMeta, V1OwnerReference

        core, _ = _load_clients(control_plane.kubeconfig)

        owner = core.create_namespaced_config_map(
            namespace="default",
            body=V1ConfigMap(
                metadata=V1ObjectMeta(name="gc-owner"), data={"role": "owner"}
            ),
        )

        core.create_namespaced_config_map(
            namespace="default",
            body=V1ConfigMap(
                metadata=V1ObjectMeta(
                    name="gc-dependent",
                    owner_references=[
                        V1OwnerReference(
                            api_version="v1",
                            kind="ConfigMap",
                            name="gc-owner",
                            uid=owner.metadata.uid,
                        )
                    ],
                ),
                data={"role": "dependent"},
            ),
        )

        core.delete_namespaced_config_map(name="gc-owner", namespace="default")

        _wait_for(
            lambda: _is_not_found(
                lambda: core.read_namespaced_config_map(
                    name="gc-dependent", namespace="default"
                )
            ),
            message="dependent configmap to be garbage-collected",
        )

    def test_job_deletion_cascades_to_pods(self, control_plane):
        """Deleting a job garbage-collects the pods it created."""
        core, batch = _load_clients(control_plane.kubeconfig)

        batch.create_namespaced_job(namespace="default", body=_make_job("gc-job"))

        _wait_for_job_pod(core, "gc-job")

        batch.delete_namespaced_job(
            name="gc-job", namespace="default", propagation_policy="Background"
        )

        def pods_gone():
            pods = core.list_namespaced_pod(
                namespace="default", label_selector="job-name=gc-job"
            )
            return not pods.items

        _wait_for(pods_gone, message="pods of job gc-job to be garbage-collected")
