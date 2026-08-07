#
# Copyright 2026 Tabsdata Inc.
#

"""Integration tests for pod dir retention.

Two clocks govern a finished pod's on-disk footprint. The reap deadline
covers the data dirs (`work/`, `volumes/`) and runs from pod completion; the
delete deadline covers the whole pod dir, including the `log/` and `store/`
that are the pod's only record, and runs from the moment the pod is removed
from the pod runner.

These tests drive the real binary, so they check what actually survives on
disk rather than what the logic layer thinks it did.
"""

import json
import os
import time

from conftest import load_pod_def, submit_pod, unique_name, wait_for_pod_phase

# Marker and deadline files the runner writes into each pod dir.
CLEANUP_TTLS_FILE = "_cleanup_ttls.json"
SUCCEEDED_MARKER = "_succeeded"
FAILED_MARKER = "_failed"
REAP_ON_FILE = "_reap_on"
DELETE_ON_FILE = "_delete_on"
REAPED_MARKER = "_reaped"
TTL_NEVER = "never"


def pod_dir(pod_runner, ns, name, uid):
    return os.path.join(
        pod_runner.work_dir, "work_pr", "data", "run", "pods",
        f"{ns}_{name}_{uid}",
    )


def wait_for_path(path, present=True, timeout=15):
    """Wait for a path to appear (or disappear). Returns whether it did."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        if os.path.exists(path) == present:
            return True
        time.sleep(0.2)
    return os.path.exists(path) == present


def read_marker(path):
    with open(path) as f:
        return f.read().strip()


def submit_annotated(mtls_client, pod_runner, annotations, args=None):
    """Submit a single-container pod carrying the given annotations."""
    pod = load_pod_def("single-container")
    pod["metadata"]["uid"] = unique_name("uid")
    pod["metadata"].setdefault("annotations", {}).update(annotations)
    if args is not None:
        pod["spec"]["containers"][0]["args"] = args
    name, ns = submit_pod(mtls_client, pod_runner, pod)
    return name, ns, pod["metadata"]["uid"]


class TestReaping:
    """The reap deadline: data dirs go, the record stays."""

    def test_succeeded_pod_data_dirs_are_reaped(self, pod_runner, mtls_client):
        name, ns, uid = submit_annotated(mtls_client, pod_runner, {})
        wait_for_pod_phase(mtls_client, pod_runner, ns, name, {"Succeeded"})

        d = pod_dir(pod_runner, ns, name, uid)
        # Default reap-on-success is 0s, applied inline at completion.
        assert wait_for_path(os.path.join(d, "work"), present=False), \
            "work/ should be reaped as soon as the pod succeeds"
        assert not os.path.exists(os.path.join(d, "volumes"))
        assert os.path.exists(os.path.join(d, REAPED_MARKER))
        assert os.path.exists(os.path.join(d, SUCCEEDED_MARKER))
        # The record survives the reap.
        assert os.path.exists(os.path.join(d, "log")), \
            "container logs are the record and must survive the reap"
        assert os.path.exists(
            os.path.join(d, "store", "original-spec.json")
        ), "the spec must survive the reap"

    def test_reap_ttl_annotation_defers_reaping(self, pod_runner, mtls_client):
        name, ns, uid = submit_annotated(
            mtls_client, pod_runner, {"prPodDataReapTtlOnPodSuccess": "1h"}
        )
        wait_for_pod_phase(mtls_client, pod_runner, ns, name, {"Succeeded"})

        d = pod_dir(pod_runner, ns, name, uid)
        assert wait_for_path(os.path.join(d, SUCCEEDED_MARKER))
        assert os.path.exists(os.path.join(d, "work")), \
            "the annotation defers the reap by an hour"
        assert not os.path.exists(os.path.join(d, REAPED_MARKER))
        # The deadline is an absolute instant an hour out.
        assert read_marker(os.path.join(d, REAP_ON_FILE)) != TTL_NEVER

    def test_never_reap_ttl_keeps_data_dirs(self, pod_runner, mtls_client):
        name, ns, uid = submit_annotated(
            mtls_client, pod_runner, {"prPodDataReapTtlOnPodSuccess": TTL_NEVER}
        )
        wait_for_pod_phase(mtls_client, pod_runner, ns, name, {"Succeeded"})

        d = pod_dir(pod_runner, ns, name, uid)
        assert wait_for_path(os.path.join(d, SUCCEEDED_MARKER))
        assert read_marker(os.path.join(d, REAP_ON_FILE)) == TTL_NEVER, \
            "never is written out literally, not as a missing file"
        assert os.path.exists(os.path.join(d, "work")), "never means never"

    def test_cleanup_ttls_record_where_each_value_came_from(
        self, pod_runner, mtls_client
    ):
        name, ns, uid = submit_annotated(
            mtls_client, pod_runner, {"prPodDirDeleteTtlPodFailureOnPodDelete": "90m"}
        )
        wait_for_pod_phase(mtls_client, pod_runner, ns, name, {"Succeeded"})

        d = pod_dir(pod_runner, ns, name, uid)
        assert wait_for_path(os.path.join(d, CLEANUP_TTLS_FILE))
        with open(os.path.join(d, CLEANUP_TTLS_FILE)) as f:
            ttls = json.load(f)

        assert ttls["deleteTtlOnFailure"] == {
            "raw": "90m", "secs": 5400, "source": "annotation",
        }
        assert ttls["reapTtlOnSuccess"]["source"] == "config"
        assert ttls["reapTtlOnSuccess"]["raw"] == "0s"


class TestDeleteDeadline:
    """The delete deadline starts when the pod leaves the runner."""

    def test_deleted_pod_keeps_its_record(self, pod_runner, mtls_client):
        name, ns, uid = submit_annotated(mtls_client, pod_runner, {})
        wait_for_pod_phase(mtls_client, pod_runner, ns, name, {"Succeeded"})
        d = pod_dir(pod_runner, ns, name, uid)
        assert wait_for_path(os.path.join(d, SUCCEEDED_MARKER))

        st, _ = mtls_client.delete(
            f"{pod_runner.url}/deletePod?namespace={ns}&name={name}"
        )
        assert st == 200

        # Default delete-on-success is 1h out, so the record stays.
        assert os.path.exists(d), "the pod dir outlives the delete"
        assert os.path.exists(os.path.join(d, "log")), \
            "logs outlive the delete"
        assert os.path.exists(os.path.join(d, "store", "original-spec.json")), \
            "the spec outlives the delete"
        assert os.path.exists(os.path.join(d, DELETE_ON_FILE)), \
            "the delete clock starts at eviction"

    def test_undeleted_pod_is_never_deleted(self, pod_runner, mtls_client):
        name, ns, uid = submit_annotated(mtls_client, pod_runner, {})
        wait_for_pod_phase(mtls_client, pod_runner, ns, name, {"Succeeded"})

        d = pod_dir(pod_runner, ns, name, uid)
        assert wait_for_path(os.path.join(d, SUCCEEDED_MARKER))
        assert not os.path.exists(os.path.join(d, DELETE_ON_FILE)), \
            "a pod the runner still reports on has no delete deadline"

    def test_zero_delete_ttl_removes_the_dir_on_delete(
        self, pod_runner, mtls_client
    ):
        name, ns, uid = submit_annotated(
            mtls_client, pod_runner, {"prPodDirDeleteTtlPodSuccessOnPodDelete": "0s"}
        )
        wait_for_pod_phase(mtls_client, pod_runner, ns, name, {"Succeeded"})
        d = pod_dir(pod_runner, ns, name, uid)
        assert wait_for_path(os.path.join(d, SUCCEEDED_MARKER))

        st, _ = mtls_client.delete(
            f"{pod_runner.url}/deletePod?namespace={ns}&name={name}"
        )
        assert st == 200

        assert wait_for_path(d, present=False), \
            "a 0s delete TTL is applied inline, not on the next scan"

    def test_never_delete_ttl_keeps_the_dir(self, pod_runner, mtls_client):
        name, ns, uid = submit_annotated(
            mtls_client, pod_runner, {"prPodDirDeleteTtlPodSuccessOnPodDelete": TTL_NEVER}
        )
        wait_for_pod_phase(mtls_client, pod_runner, ns, name, {"Succeeded"})
        d = pod_dir(pod_runner, ns, name, uid)
        assert wait_for_path(os.path.join(d, SUCCEEDED_MARKER))

        mtls_client.delete(
            f"{pod_runner.url}/deletePod?namespace={ns}&name={name}"
        )

        assert os.path.exists(d)
        assert read_marker(os.path.join(d, DELETE_ON_FILE)) == TTL_NEVER


class TestFailureRetention:
    """A failed pod's reason has to outlive the pod."""

    def test_failed_pod_keeps_its_data_dirs(self, pod_runner, mtls_client):
        name, ns, uid = submit_annotated(
            mtls_client, pod_runner, {}, args=["exit 1"]
        )
        wait_for_pod_phase(mtls_client, pod_runner, ns, name, {"Failed"})

        d = pod_dir(pod_runner, ns, name, uid)
        assert wait_for_path(os.path.join(d, FAILED_MARKER))
        assert not os.path.exists(os.path.join(d, SUCCEEDED_MARKER))
        assert os.path.exists(os.path.join(d, "work")), \
            "the default 1h reap-on-error keeps work/ for debugging"
        assert not os.path.exists(os.path.join(d, REAPED_MARKER))

    def test_deleting_a_failed_pod_keeps_the_reason(
        self, pod_runner, mtls_client
    ):
        name, ns, uid = submit_annotated(
            mtls_client, pod_runner, {}, args=["echo boom >&2; exit 3"]
        )
        wait_for_pod_phase(mtls_client, pod_runner, ns, name, {"Failed"})
        d = pod_dir(pod_runner, ns, name, uid)
        assert wait_for_path(os.path.join(d, FAILED_MARKER))

        st, _ = mtls_client.delete(
            f"{pod_runner.url}/deletePod?namespace={ns}&name={name}"
        )
        assert st == 200

        # The pod is gone from the API but its record is not.
        st, _ = mtls_client.get(
            f"{pod_runner.url}/getPodStatus?namespace={ns}&name={name}"
        )
        assert st == 404

        status_dir = os.path.join(d, "store", "status")
        assert os.path.isdir(status_dir), \
            "status snapshots must survive the delete — for a pod that " \
            "failed before any container ran they are the whole record"
        snapshots = os.listdir(status_dir)
        assert snapshots, f"no status snapshots in {status_dir}"
        assert os.path.exists(os.path.join(d, "store", "original-spec.json"))
        assert os.path.exists(os.path.join(d, "log")), \
            "container logs must survive the delete"
