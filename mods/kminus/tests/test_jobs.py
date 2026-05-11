#
# Copyright 2026 Tabsdata Inc.
#

"""Integration tests for K8s Jobs and CronJobs through the full kminus stack.

These tests bring up the whole stack (control-plane + virtual-kubelet +
pod-runner) via the km orchestrator and submit Jobs/CronJobs through
the K8s API. The kube-controller-manager creates pods, the
virtual-kubelet schedules them onto the pod-runner node, and pod-runner
executes them as native processes.
"""

import time
import uuid


def _unique_name(prefix):
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


class TestJob:
    """K8s batch/v1 Job executed through virtual-kubelet → pod-runner."""

    def test_job_completes(self, km, batch_client):
        """A simple Job should complete successfully."""
        from kubernetes.client import (
            V1Job, V1JobSpec, V1PodTemplateSpec, V1PodSpec,
            V1Container, V1ObjectMeta, V1Toleration,
        )

        name = _unique_name("job")
        job = V1Job(
            metadata=V1ObjectMeta(name=name, namespace="default"),
            spec=V1JobSpec(
                template=V1PodTemplateSpec(
                    spec=V1PodSpec(
                        node_name="tdknode",
                        restart_policy="Never",
                        containers=[
                            V1Container(
                                name="worker",
                                image="/dev/null",
                                command=["sh", "-c"],
                                args=["echo 'job done'"],
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

        batch_client.create_namespaced_job(namespace="default", body=job)

        deadline = time.time() + 60
        while time.time() < deadline:
            j = batch_client.read_namespaced_job(name=name, namespace="default")
            if j.status.succeeded and j.status.succeeded >= 1:
                break
            time.sleep(2)
        else:
            raise TimeoutError(f"Job {name} did not complete within 60s")

        j = batch_client.read_namespaced_job(name=name, namespace="default")
        assert j.status.succeeded >= 1

        batch_client.delete_namespaced_job(
            name=name, namespace="default",
            propagation_policy="Background",
        )


class TestCronJob:
    """K8s batch/v1 CronJob creates pods via virtual-kubelet → pod-runner."""

    def test_cronjob_creates_jobs(self, km, batch_client):
        """A CronJob should create at least one Job within its schedule."""
        from kubernetes.client import (
            V1CronJob, V1CronJobSpec, V1JobTemplateSpec, V1JobSpec,
            V1PodTemplateSpec, V1PodSpec, V1Container, V1ObjectMeta,
            V1Toleration,
        )

        name = _unique_name("cron")
        cron = V1CronJob(
            metadata=V1ObjectMeta(name=name, namespace="default"),
            spec=V1CronJobSpec(
                schedule="*/1 * * * *",
                job_template=V1JobTemplateSpec(
                    spec=V1JobSpec(
                        template=V1PodTemplateSpec(
                            spec=V1PodSpec(
                                node_name="tdknode",
                                restart_policy="Never",
                                containers=[
                                    V1Container(
                                        name="greeter",
                                        image="/dev/null",
                                        command=["sh", "-c"],
                                        args=["echo 'cron hello'"],
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
                ),
            ),
        )

        batch_client.create_namespaced_cron_job(namespace="default", body=cron)

        deadline = time.time() + 90
        found = False
        while time.time() < deadline:
            jobs = batch_client.list_namespaced_job(
                namespace="default",
                label_selector="batch.kubernetes.io/controller-uid",
            )
            cron_jobs = [j for j in jobs.items if j.metadata.name.startswith(name)]
            if cron_jobs:
                found = True
                break
            time.sleep(5)

        assert found, f"CronJob {name} did not create any Jobs within 90s"

        batch_client.delete_namespaced_cron_job(
            name=name, namespace="default",
            propagation_policy="Background",
        )
