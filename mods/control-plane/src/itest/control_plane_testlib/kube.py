#
# Copyright 2026 Tabsdata Inc.
#

"""Kubernetes client helpers shared by the control-plane integration tests."""

import time

import pytest


def load_clients(kubeconfig_path):
    """Load kubeconfig and return (CoreV1Api, BatchV1Api) clients."""
    from kubernetes import client, config as k8s_config

    k8s_config.load_kube_config(config_file=kubeconfig_path)
    return client.CoreV1Api(), client.BatchV1Api()


def wait_for(predicate, timeout=60, interval=0.5, message="condition"):
    """Poll predicate until it returns truthy or timeout expires."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        result = predicate()
        if result:
            return result
        time.sleep(interval)
    pytest.fail(f"Timed out after {timeout}s waiting for {message}")


def is_not_found(func):
    """Return True if func raises a 404 ApiException."""
    from kubernetes.client.rest import ApiException

    try:
        func()
        return False
    except ApiException as e:
        if e.status == 404:
            return True
        raise


def make_job(name, ttl_seconds_after_finished=None):
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


def wait_for_job_pod(core, job_name):
    """Wait until the job controller creates a pod for the job."""

    def pod_created():
        pods = core.list_namespaced_pod(
            namespace="default", label_selector=f"job-name={job_name}"
        )
        return pods.items[0] if pods.items else None

    return wait_for(pod_created, message=f"pod of job {job_name}")


def mark_pod_succeeded(core, pod_name):
    """Patch the pod status to Succeeded, simulating a kubelet run."""
    core.patch_namespaced_pod_status(
        name=pod_name,
        namespace="default",
        body={"status": {"phase": "Succeeded"}},
    )
