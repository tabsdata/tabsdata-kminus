#
# Copyright 2026 Tabsdata Inc.
#

"""Integration tests for pod dir retention.

A pod's footprint is two dirs with a clock each. The data dir
(`data/pod-run/<uid>/`: `work/`, `volumes/`) goes on the reap deadline,
which runs from pod completion. The state dir
(`data/pod-state/<bucket>/<log_pod_id>/<stamp>_<pod_name>/`: the `log/` and
`store/` that are one pod's only record; the log pod id is the pod's
`tabsdata.com/log-id` label, or its name) goes on the delete deadline, which runs
from the moment the pod is removed from the pod runner. Both clocks survive
a restart.

These tests drive the real binary, so they check what actually survives on
disk rather than what the logic layer thinks it did.
"""

import json
import os
import re
import time

from itest_common import find_free_port, get_session_id

from conftest import (
    MtlsClient,
    PodRunnerProcess,
    TARGET_DIR,
    _write_file_resolver_prsysconfig,
    load_pod_def,
    submit_pod,
    unique_name,
    wait_for_pod_phase,
)

# Marker and deadline files the runner writes into each pod dir.
CLEANUP_TTLS_FILE = "_cleanup_ttls.json"
SUCCEEDED_MARKER = "_succeeded"
FAILED_MARKER = "_failed"
REAP_ON_FILE = "_reap_on"
DELETE_ON_FILE = "_delete_on"
TTL_NEVER = "never"

# Pod-state bucketing — part of the on-disk format (store/layout.rs).
POD_STATE_BUCKETS = 256
FNV_OFFSET_BASIS = 0xCBF29CE484222325
FNV_PRIME = 0x100000001B3
U64 = 0xFFFFFFFFFFFFFFFF


def state_bucket(pod_name):
    """FNV-1a 64 of the pod name, xor-folded to 8 bits, as two hex digits."""
    h = FNV_OFFSET_BASIS
    for byte in pod_name.encode():
        h = ((h ^ byte) * FNV_PRIME) & U64
    h ^= h >> 32
    h ^= h >> 16
    h ^= h >> 8
    return f"{h % POD_STATE_BUCKETS:02x}"


def data_root(pod_runner):
    return os.path.join(pod_runner.work_dir, "work_pr", "data")


def data_dir(pod_runner, ns, name, uid):
    """The pod's data dir: work/ and volumes/."""
    return os.path.join(data_root(pod_runner), "pod-run", uid)


def data_link(pod_runner, ns, name, uid):
    """The link naming the pod's data dir by pod id."""
    return os.path.join(data_root(pod_runner), "pod-run", f"{ns}_{name}_{uid}")


def name_dir(pod_runner, log_pod_id):
    """The dir every pod filed under a log pod id shares."""
    return os.path.join(
        data_root(pod_runner), "pod-state", state_bucket(log_pod_id), log_pod_id
    )


def state_dir(pod_runner, ns, name, uid):
    """The state dir of the pod's only incarnation: log/ and store/.

    The test pods carry no creationTimestamp, so admission stamps them and
    the stamp is read back from disk. The dir is created at admission.
    """
    parent = name_dir(pod_runner, name)
    assert wait_for_path(parent), f"no pod name dir {parent}"
    incarnations = [d for d in os.listdir(parent)
                    if os.path.isdir(os.path.join(parent, d))]
    assert len(incarnations) == 1, \
        f"one incarnation expected in {parent}: {incarnations}"
    assert re.fullmatch(rf"\d{{8}}T\d{{6}}Z_{re.escape(name)}", incarnations[0]), \
        incarnations[0]
    return os.path.join(parent, incarnations[0])


def wait_for_path(path, present=True, timeout=15):
    """Wait for a path to appear (or disappear). Returns whether it did."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        if os.path.exists(path) == present:
            return True
        time.sleep(0.2)
    return os.path.exists(path) == present


def read_marker(path):
    with open(path, encoding="utf-8") as f:
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

    def test_succeeded_pod_data_dir_is_reaped(self, pod_runner, mtls_client):
        name, ns, uid = submit_annotated(mtls_client, pod_runner, {})
        wait_for_pod_phase(mtls_client, pod_runner, ns, name, {"Succeeded"})

        d = data_dir(pod_runner, ns, name, uid)
        s = state_dir(pod_runner, ns, name, uid)
        # Default reap-on-success is 0s, applied inline at completion.
        assert wait_for_path(d, present=False), \
            "the data dir should be reaped as soon as the pod succeeds"
        assert not os.path.lexists(data_link(pod_runner, ns, name, uid)), \
            "the data dir's link goes with it"
        # The record lives in the state dir and survives the reap.
        assert os.path.exists(os.path.join(s, SUCCEEDED_MARKER))
        assert os.path.exists(os.path.join(s, "log")), \
            "container logs are the record and must survive the reap"
        assert os.path.exists(
            os.path.join(s, "store", "original-spec.json")
        ), "the spec must survive the reap"

    def test_reap_ttl_annotation_defers_reaping(self, pod_runner, mtls_client):
        name, ns, uid = submit_annotated(
            mtls_client, pod_runner, {"prPodDataReapTtlOnPodSuccess": "1h"}
        )
        wait_for_pod_phase(mtls_client, pod_runner, ns, name, {"Succeeded"})

        d = data_dir(pod_runner, ns, name, uid)
        assert wait_for_path(os.path.join(d, SUCCEEDED_MARKER))
        assert os.path.exists(os.path.join(d, "work")), \
            "the annotation defers the reap by an hour"
        link = data_link(pod_runner, ns, name, uid)
        assert os.path.islink(link) and os.readlink(link) == uid, \
            "the data dir is linked by pod id, relatively"
        # The deadline is an absolute instant an hour out.
        assert read_marker(os.path.join(d, REAP_ON_FILE)) != TTL_NEVER

    def test_never_reap_ttl_keeps_data_dirs(self, pod_runner, mtls_client):
        name, ns, uid = submit_annotated(
            mtls_client, pod_runner, {"prPodDataReapTtlOnPodSuccess": TTL_NEVER}
        )
        wait_for_pod_phase(mtls_client, pod_runner, ns, name, {"Succeeded"})

        d = data_dir(pod_runner, ns, name, uid)
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

        # Each half of the policy lives in the dir it governs. The data dir
        # is reaped at once (0s on success), so read the state dir's half.
        s = state_dir(pod_runner, ns, name, uid)
        assert wait_for_path(os.path.join(s, CLEANUP_TTLS_FILE))
        with open(os.path.join(s, CLEANUP_TTLS_FILE), encoding="utf-8") as f:
            ttls = json.load(f)

        assert ttls["deleteTtlOnFailure"] == {
            "raw": "90m", "secs": 5400, "source": "annotation",
        }
        assert ttls["deleteTtlOnSuccess"]["source"] == "config"
        assert "reapTtlOnSuccess" not in ttls, \
            "the reap TTLs belong to the data dir"

    def test_reap_ttls_live_in_the_data_dir(self, pod_runner, mtls_client):
        name, ns, uid = submit_annotated(
            mtls_client, pod_runner, {"prPodDataReapTtlOnPodSuccess": "1h"}
        )
        wait_for_pod_phase(mtls_client, pod_runner, ns, name, {"Succeeded"})

        d = data_dir(pod_runner, ns, name, uid)
        with open(os.path.join(d, CLEANUP_TTLS_FILE), encoding="utf-8") as f:
            ttls = json.load(f)
        assert ttls["reapTtlOnSuccess"] == {
            "raw": "1h", "secs": 3600, "source": "annotation",
        }
        assert ttls["reapTtlOnFailure"]["source"] == "config"
        assert "deleteTtlOnFailure" not in ttls


class TestDeleteDeadline:
    """The delete deadline starts when the pod leaves the runner."""

    def test_deleted_pod_keeps_its_record(self, pod_runner, mtls_client):
        name, ns, uid = submit_annotated(mtls_client, pod_runner, {})
        wait_for_pod_phase(mtls_client, pod_runner, ns, name, {"Succeeded"})
        d = state_dir(pod_runner, ns, name, uid)
        assert wait_for_path(os.path.join(d, SUCCEEDED_MARKER))

        st, _ = mtls_client.delete(
            f"{pod_runner.url}/deletePod?namespace={ns}&name={name}"
        )
        assert st == 200

        # Default delete-on-success is 1h out, so the record stays.
        assert os.path.exists(d), "the state dir outlives the delete"
        assert os.path.exists(os.path.join(d, "log")), \
            "logs outlive the delete"
        assert os.path.exists(os.path.join(d, "store", "original-spec.json")), \
            "the spec outlives the delete"
        assert os.path.exists(os.path.join(d, DELETE_ON_FILE)), \
            "the delete clock starts at eviction"

    def test_undeleted_pod_is_never_deleted(self, pod_runner, mtls_client):
        name, ns, uid = submit_annotated(mtls_client, pod_runner, {})
        wait_for_pod_phase(mtls_client, pod_runner, ns, name, {"Succeeded"})

        d = state_dir(pod_runner, ns, name, uid)
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
        d = state_dir(pod_runner, ns, name, uid)
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
        d = state_dir(pod_runner, ns, name, uid)
        assert wait_for_path(os.path.join(d, SUCCEEDED_MARKER))

        mtls_client.delete(
            f"{pod_runner.url}/deletePod?namespace={ns}&name={name}"
        )

        assert os.path.exists(d)
        assert read_marker(os.path.join(d, DELETE_ON_FILE)) == TTL_NEVER


class TestFailureRetention:
    """A failed pod's reason has to outlive the pod."""

    def test_failed_pod_keeps_its_data_dir(self, pod_runner, mtls_client):
        name, ns, uid = submit_annotated(
            mtls_client, pod_runner, {}, args=["import sys; sys.exit(1)"]
        )
        wait_for_pod_phase(mtls_client, pod_runner, ns, name, {"Failed"})

        d = data_dir(pod_runner, ns, name, uid)
        assert wait_for_path(os.path.join(d, FAILED_MARKER))
        assert not os.path.exists(os.path.join(d, SUCCEEDED_MARKER))
        assert os.path.exists(os.path.join(d, "work")), \
            "the default 1h reap-on-error keeps work/ for debugging"
        assert os.path.exists(
            os.path.join(state_dir(pod_runner, ns, name, uid), FAILED_MARKER)
        )

    def test_deleting_a_failed_pod_keeps_the_reason(
        self, pod_runner, mtls_client
    ):
        name, ns, uid = submit_annotated(
            mtls_client, pod_runner, {}, args=["import sys; print('boom', file=sys.stderr); sys.exit(3)"]
        )
        wait_for_pod_phase(mtls_client, pod_runner, ns, name, {"Failed"})
        d = state_dir(pod_runner, ns, name, uid)
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


class TestIncarnations:
    """Incarnations of a pod name share its dir, one stamp dir each."""

    def test_each_incarnation_gets_its_own_stamp_dir(
        self, pod_runner, mtls_client
    ):
        name = unique_name("inc")
        stamps = []
        for created, stamp in [
            ("2026-10-19T14:00:00Z", "20261019T140000Z"),
            ("2026-10-19T15:00:00Z", "20261019T150000Z"),
        ]:
            pod = load_pod_def("single-container")
            pod["metadata"]["name"] = name
            pod["metadata"]["uid"] = unique_name("uid")
            pod["metadata"]["creationTimestamp"] = created
            _, ns = submit_pod(mtls_client, pod_runner, pod)
            wait_for_pod_phase(mtls_client, pod_runner, ns, name, {"Succeeded"})
            st, _ = mtls_client.delete(
                f"{pod_runner.url}/deletePod?namespace={ns}&name={name}"
            )
            assert st == 200
            stamps.append(f"{stamp}_{name}")

        parent = name_dir(pod_runner, name)
        assert sorted(os.listdir(parent)) == stamps, \
            "the uid is not part of the path; the creation time and name are"
        for stamp in stamps:
            d = os.path.join(parent, stamp)
            assert os.path.isdir(os.path.join(d, "log"))
            assert os.path.exists(os.path.join(d, "store", "original-spec.json"))
            with open(os.path.join(d, CLEANUP_TTLS_FILE), encoding="utf-8") as f:
                assert "deleteTtlOnSuccess" in json.load(f), \
                    "each incarnation carries its own delete TTLs"
            assert os.path.exists(os.path.join(d, DELETE_ON_FILE))

        # Both incarnations are deleted; their logs are still served, oldest
        # first, each after its separator.
        st, body = mtls_client.get_text(
            f"{pod_runner.url}/getPodLogs?logPodId={name}"
        )
        assert st == 200, body
        first = body.index(f"POD STARTED 2026-10-19T14:00:00Z {name}")
        second = body.index(f"POD STARTED 2026-10-19T15:00:00Z {name}")
        assert first < second, body
        assert body.count("POD STARTED") == 2, body

        st, body = mtls_client.get_text(
            f"{pod_runner.url}/getPodLogs?logPodId={name}&previous=true"
        )
        assert st == 200
        assert body.count("POD STARTED") == 1
        assert "2026-10-19T14:00:00Z" in body

    def test_a_log_id_label_files_every_run_under_it(
        self, pod_runner, mtls_client
    ):
        """As a CronJob's runs would: generated pod names, one label value."""
        log_id = unique_name("cron")
        pods = []
        for created in ["2026-10-19T00:00:03Z", "2026-10-20T00:00:02Z"]:
            pod = load_pod_def("single-container")
            pod["metadata"]["name"] = unique_name(log_id)
            pod["metadata"]["uid"] = unique_name("uid")
            pod["metadata"]["creationTimestamp"] = created
            pod["metadata"].setdefault("labels", {})["tabsdata.com/log-id"] = log_id
            name, ns = submit_pod(mtls_client, pod_runner, pod)
            wait_for_pod_phase(mtls_client, pod_runner, ns, name, {"Succeeded"})
            mtls_client.delete(
                f"{pod_runner.url}/deletePod?namespace={ns}&name={name}"
            )
            pods.append(name)

        parent = name_dir(pod_runner, log_id)
        assert sorted(os.listdir(parent)) == [
            f"20261019T000003Z_{pods[0]}",
            f"20261020T000002Z_{pods[1]}",
        ]
        st, body = mtls_client.get_text(
            f"{pod_runner.url}/getPodLogs?logPodId={log_id}"
        )
        assert st == 200, body
        assert body.index(f"POD STARTED 2026-10-19T00:00:03Z {pods[0]}") < \
            body.index(f"POD STARTED 2026-10-20T00:00:02Z {pods[1]}"), body

        # One pod, no separator: how Tabsdata's multiplexer reads it.
        st, one = mtls_client.get_text(
            f"{pod_runner.url}/getPodLogs?logPodId={log_id}&podName={pods[1]}"
        )
        assert st == 200, one
        assert "POD STARTED" not in one, one
        assert "hello from single-container" in one, one

    def test_logs_of_an_unknown_pod_are_not_found(self, pod_runner, mtls_client):
        st, _ = mtls_client.get_text(
            f"{pod_runner.url}/getPodLogs?logPodId={unique_name('ghost')}"
        )
        assert st == 404


class TestRestart:
    """Both clocks survive a pod runner restart."""

    def test_deadlines_survive_a_restart(self, pod_runner_bin):
        work_dir = os.path.join(
            TARGET_DIR, f"testrun.{get_session_id()}", "_pod_runner_restart"
        )
        os.makedirs(work_dir, exist_ok=True)
        _write_file_resolver_prsysconfig(os.path.join(work_dir, "config"))
        pr = PodRunnerProcess(pod_runner_bin, work_dir, find_free_port())
        pr.start()
        try:
            client = MtlsClient(pr.vk_prconfig_path, pr.work_dir)

            # Finished and deleted: its record is on a 3s delete clock,
            # which a restart must not reset.
            gone, ns, gone_uid = submit_annotated(
                client, pr, {"prPodDirDeleteTtlPodSuccessOnPodDelete": "3s"}
            )
            wait_for_pod_phase(client, pr, ns, gone, {"Succeeded"})
            gone_state = state_dir(pr, ns, gone, gone_uid)
            assert wait_for_path(os.path.join(gone_state, SUCCEEDED_MARKER))
            st, _ = client.delete(
                f"{pr.url}/deletePod?namespace={ns}&name={gone}"
            )
            assert st == 200

            # Finished, never deleted: the runner forgets it on restart, so
            # its delete clock starts then.
            kept, ns, kept_uid = submit_annotated(client, pr, {})
            wait_for_pod_phase(client, pr, ns, kept, {"Succeeded"})
            kept_state = state_dir(pr, ns, kept, kept_uid)
            assert wait_for_path(os.path.join(kept_state, SUCCEEDED_MARKER))
            assert not os.path.exists(os.path.join(kept_state, DELETE_ON_FILE))

            pr.stop()
            pr.start()

            assert wait_for_path(gone_state, present=False, timeout=15), \
                "a delete deadline pending at shutdown is picked up again"
            assert not os.path.exists(os.path.dirname(gone_state)), \
                "the log pod id dir goes with its last incarnation"
            assert wait_for_path(os.path.join(kept_state, DELETE_ON_FILE)), \
                "a pod the restarted runner no longer knows starts its delete clock"
            assert os.path.exists(os.path.join(kept_state, "log")), \
                "and keeps its record until that clock runs out"

            data = data_root(pr)
            assert os.path.isdir(os.path.join(data, "pod-pid"))
            assert not [n for n in os.listdir(data) if n.startswith("run")], \
                "every dir is fixed across restarts; no per-run dirs"
        finally:
            pr.stop()
