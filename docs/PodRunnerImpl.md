<!--
  Copyright 2026 Tabsdata Inc.
-->

# pod-runner (Rust) -- Implementation

## TL;DR

The `pod-runner` is a Rust application that executes Kubernetes pods as native OS processes. It accepts a
supported subset of K8s pod specs via a REST/HTTPS API (mTLS), resolves secrets, configmaps, and environment
variables, sets up volumes, copies image directory templates, and runs each container as an OS subprocess.
It follows the standard K8s pod lifecycle for phases and conditions.

Two binaries are produced:

| Binary        | Path                 | Role                                       |
|---------------|----------------------|--------------------------------------------|
| `pod-runner`  | `src/server/main.rs` | Long-running HTTPS server                  |
| `kpr`         | `src/client/main.rs` | Thin CLI client for the pod-runner REST API |

---

## Architecture

The pod-runner is structured as four layers. Each layer talks only to
the one directly below it; it does not reach across:

```
              +-----------------------------------------+
              |                  REST                   |    server/ (Axum)
              |  HTTP handlers, mTLS, routing           |
              +-------------------+---------------------+
                                  |
                                  v
              +-----------------------------------------+
              |                 Logic                   |    logic/ (PodLogic)
              |  pod lifecycle, validation, cleanup,    |
              |  purge, snapshots, event reduction      |
              +---+---------------------------------+---+
                  |                                 |
                  v                                 v
        +-----------------+           +-----------------------------+
        |      Store      |           |           PodExec           |   pod/ (PodExecutor)
        |  memory + file  |           |  prepare, probes,           |
        |  (multiplexed)  |           |  init/main/sidecar phases   |
        +-----------------+           +--------------+--------------+
                                                     |
                                                     v
                                      +--------------------------------+
                                      |         ProcessExec            |   process/
                                      |  tokio::process, log rotation, |
                                      |  SIGTERM/SIGKILL, die-with-    |
                                      |  parent                        |
                                      +--------------------------------+
```

| Layer        | Module                      | Responsibility                                                                 |
|--------------|-----------------------------|--------------------------------------------------------------------------------|
| REST         | `server/`                   | Decode/encode HTTP, mTLS, route to `PodLogic`. No pod state here.              |
| Logic        | `logic/`                    | Pod lifecycle orchestration, spec validation, snapshots, cleanup and purge.    |
| Store        | `store/`                    | Persist pod specs + status. Multiplexed `memory` (hot path) + `file` (debug).  |
| PodExec      | `pod/`                      | Prepare pod dir, run init → main → sidecar containers, run probes.             |
| ProcessExec  | `process/`                  | Spawn and supervise a single OS process (stdout/stderr rotation, signals).     |

### Reactive Pattern

The runtime below the REST layer is **reactive**: handlers do not block
on long-running work, and there is no central polling loop. Instead,
each pod owns a small tree of tasks that communicate through tokio
channels, and each task reacts to whatever arrives next.

Concretely:

- **Commands in, events out.** When a `POST /createPod` arrives,
  `PodLogic::create_pod` validates, persists to the store, spawns a
  `PodExecutor` orchestrator task, and returns immediately. The
  orchestrator emits `PodEvent`s (`ContainerStarted`, `PhaseChanged`,
  `ContainerSucceeded`, `ContainerFailed`, `Completed`) on an `mpsc`
  channel. A `monitor_pod` task in `logic/` consumes those events and
  reduces them into store updates and metric counters.
- **Fan-out via `watch` channels.** Kill requests and the global
  shutdown are broadcast on `tokio::sync::watch` channels. Every
  container monitor, probe loop, and init-container wait holds a
  receiver and wakes from its `select!` the moment the value changes —
  no polling, no sleep-and-check.
- **`select!` as the control primitive.** Each container monitor task
  runs a `select!` over: the child process exit future, the kill
  `watch`, and the shutdown `watch`. Whichever fires first drives the
  next state transition.
- **Streaming instead of buffering.** `stream_container_logs` returns
  an `mpsc::Receiver<Bytes>` that is fed incrementally from the rotated
  log file; `follow=true` keeps the stream alive until the pod reaches
  a terminal phase.
- **Status is a derived view.** `PodStatus` is computed from the
  authoritative `PodLifecycle` state on demand, so store writes are
  triggered by lifecycle transitions rather than by periodic scans.

The net effect: REST calls stay short, pod work runs concurrently in
isolated task trees, and cancellation (delete, shutdown, probe failure)
propagates through channel signals rather than flags the tasks would
have to poll for.

---

## Configuration

Two YAML files configure the server, and pod-runner writes a third for its clients:

| File               | Lifetime                                                                     |
|--------------------|------------------------------------------------------------------------------|
| `prconfig.yaml`    | Endpoint URL + the server's TLS material. Server-only: it has no client certificate, so no client can use it. Regenerated on every start by the orchestrator (km). |
| `vk-prconfig.yaml` | Client config: URL, server CA and the `vk` client certificate. What every client uses (virtual-kubelet, `kpr --prconfig`, Tabsdata's log archive). Regenerated with `prconfig.yaml`. |
| `prsysconfig.yaml` | Node identity, pod limits, executor, cleanup, capacity, resolvers. Persistent and user-editable; auto-created with defaults on first start. The resolved `capacity` is reported to km at startup and printed with its banner — see [KmComponentMessages.md](KmComponentMessages.md). |

### CLI Arguments

```
pod-runner [--kubeconfig <file>] [--configdir <dir>] [--workdir <dir>] [--logdir <dir>]
           [--port <p>] [--metrics-port <p>] [--logmaxsizemb <n>] [--logmaxfiles <n>]
```

| Option           | Default       | Description                                                    |
|------------------|---------------|----------------------------------------------------------------|
| `--kubeconfig`   | `$KUBECONFIG` | Path to kubeconfig for K8s API access                          |
| `--configdir`    | `./config`    | Directory for `prconfig.yaml` and `prsysconfig.yaml`           |
| `--workdir`      | `.`           | Working directory (`run/` created under this)                  |
| `--logdir`       | `./log`       | Log directory                                                  |
| `--port`         | `3000`        | Server port (only used when auto-generating `prconfig.yaml`)   |
| `--metrics-port` | `metricsUrl`  | Metrics server port; overrides `metricsUrl` (`0` disables)     |
| `--logmaxsizemb` | `100`         | Max size of a pod-runner log file (MB) before rotation         |
| `--logmaxfiles`  | `3`           | Max number of rotated pod-runner log files to keep             |

### prconfig.yaml

```yaml
url: "https://127.0.0.1:3000"         # Server bind URL (https required)

server:
  tls:
    certificate: "/path/to/cert.pem"             # or certificate-data: "<base64>"
    certificate-key: "/path/to/key.pem"          # or certificate-key-data: "<base64>"
    certificate-authority: "/path/to/ca.pem"     # or certificate-authority-data: "<base64>"
```

Validation:
- URL scheme must be `https://`.
- Cannot set both the path and `-data` form for the same field.
- Server certificate, key, and client CA must all be present.

### prsysconfig.yaml

```yaml
nodename: "tdknode"                   # Virtual node name
namespace: "default"                  # Default namespace

capacity:
  cpu: 100
  memoryGB: 100
  pods: 100

maxPods: 100
defaultGracePeriodSecs: 120
maxRestartCount: 5
restartBackoffBaseSecs: 10
cleanUpForceKillWaitSecs: 10

propagateEnv: true                    # Inherit parent process environment
dieWithParent: true                   # Kill child processes when pod runner exits (Unix)
writeSecretsToContainerShellScript: false

podDataReapTtlOnPodSuccess: "0s"                 # Data dirs of a succeeded pod, from completion
podDataReapTtlOnPodFailure: "1h"                 # Data dirs of a failed pod, from completion
podDirDeleteTtlPodSuccessOnPodDelete: "1h"       # Pod dir of a succeeded pod, from removal from the runner
podDirDeleteTtlPodFailureOnPodDelete: "24h"      # Pod dir of a failed pod, from removal from the runner

metricsUrl: "http://127.0.0.1:3001"       # Overridden by --metrics-port when passed

secretResolver:
  type: "kube"                        # "file", "kube", or "none"
  configs:
    dir: "/path/to/secrets"           # for file resolver
    kubeconfig: "/path/to/kubeconfig" # for kube resolver

configMapResolver:
  type: "kube"
  configs:
    dir: "/path/to/configmaps"

nodeEnvsResolver:
  type: "none"                        # "podrunner" (inherit env), "file", or "none"
  configs:
    file: "/path/to/node-envs.json"
```

### Config Files Auto-Generation

On first start (no `prconfig.yaml` in configdir), the pod runner generates:

1. Self-signed CA (valid 3650 days)
2. Server certificate signed by CA (valid 365 days, SANs: `localhost`, `127.0.0.1`)
3. Client certificate signed by CA (valid 365 days, CN: `vk`)
4. `prconfig.yaml` with server certs embedded as base64
5. `vk-prconfig.yaml` with client certs for the virtual kubelet

`prsysconfig.yaml` is auto-created separately from defaults on first start and is **not** overwritten
on subsequent starts.

---

## REST API

All endpoints use query parameters for GET/DELETE and JSON body for POST.

### Pod Management

| Method     | Endpoint              | Description             | Parameters                                                                        |
|------------|-----------------------|-------------------------|-----------------------------------------------------------------------------------|
| **POST**   | `/createPod`          | Create and start a pod  | JSON body with pod spec                                                           |
| **DELETE** | `/deletePod`          | Kill and remove a pod   | `name` (req), `namespace`, `gracePeriod`                                          |
| **GET**    | `/getPod`             | Get full pod object     | `name` (req), `namespace`                                                         |
| **GET**    | `/getPodStatus`       | Get status only         | `podName` (req), `namespace`                                                      |
| **GET**    | `/getPods`            | List all pods           | `namespace` (optional)                                                            |
| **GET**    | `/getAllPodStatuses`  | Bulk status dump for sync loop — returns `[{namespace, name, uid, phase, status}]` |                                                                                   |
| **GET**    | `/getContainerLogs`   | Stream container logs   | `podName` (req), `namespace`, `containerName`, `follow`, `tailLines`, `previous`, `timestamps` |
| **GET**    | `/getPodLogs`         | Stream the logs of every pod filed under a log pod id, from disk | `logPodId` (req), `podName`, `namespace`, `containerName`, `tailLines`, `previous`, `timestamps` |

### Node Information

| Method  | Endpoint           | Description                                  |
|---------|--------------------|----------------------------------------------|
| **GET** | `/capacity`        | CPU, memory, pod limits (from prsysconfig)   |
| **GET** | `/nodeConditions`  | Ready, MemoryPressure, DiskPressure          |
| **GET** | `/nodeAddresses`   | InternalIP addresses                         |

### Response Format

Success responses return the requested object directly. Errors return `{ "error": "<message>" }`.

---

## Pod Lifecycle

### Execution Flow

```
1. Spec Validation
       Validate pod spec conforms to supported K8s schema.
             |
2. Preparation
       Rewrite paths to physical filesystem locations.
       Resolve environment variables and configmap-volume data from
       secrets/configmaps.
       Create container working directories.
       Copy image directory templates.
       Set up volume mounts (emptyDir dirs, hostPath symlinks, configMap
       per-key files).
             |
3. Init Container Phase
       Run init containers sequentially in declaration order.
       Sidecars (restartPolicy: Always) start async and run alongside main containers.
       If a non-sidecar init container fails, restart it in place when the
       pod's restartPolicy is Always (up to maxRestartCount, with backoff);
       otherwise, or past the maximum, mark the pod as Failed.
             |
4. Main Container Phase
       Run all main containers concurrently.
       Start health probes (startup -> liveness + readiness).
       Monitor process exits.
             |
5. Completion
       All containers exit code 0 -> Succeeded.
       Any non-sidecar container exits non-zero -> Failed.
```

### Pod Phases and Conditions

Follows the K8s specification.

| Phase         | Conditions                          | Meaning                                |
|---------------|-------------------------------------|----------------------------------------|
| **Pending**   | PodScheduled=True                   | Pod assigned to node                   |
| **Pending**   | Initialized=False                   | Init containers still running          |
| **Running**   | Initialized=True, ContainersReady   | Main containers running                |
| **Running**   | Ready=True                          | All readiness probes passed            |
| **Succeeded** | Ready=False                         | All containers exited code 0           |
| **Failed**    | Ready=False                         | At least one container exited non-zero |

### Container States

Each container status reports one of three states:

- **Waiting**: `{ "reason": "PodInitializing" }`
- **Running**: `{ "startedAt": "2026-04-08T12:34:56Z" }`
- **Terminated**: `{ "exitCode": 0, "reason": "Completed", "startedAt": "...", "finishedAt": "..." }`

---

## Native Process Execution

### Container Image Resolution

The `image` field in a container spec is treated as a **local path**: a
directory is recursively copied into the container's working directory; a
`.tgz` / `.tar.gz` archive is extracted into it. Relative paths are resolved
against the images root (`--imagesdir`, falling back to `imagesDir` in
`prsysconfig.yaml`, then `./images`); absolute paths are used as-is. Use
`/dev/null` or an empty string to skip the copy.

### Working Directory

Each container runs in `<data_dir>/work/<container-name>/` where
`<data_dir>` is the pod's data dir, `<workdir>/data/pod-run/<uid>/`.
The image directory template (if any) is copied here before the
container starts. All filesystem resources should be referenced
relative to this directory.

### Volume Mounts

Volumes are pod-level: each volume referenced by a `volumeMount` is
materialized exactly once under `<pod_dir>/volumes/<volume_name>`, and
every container mount path becomes a symlink to that volume directory.
Volumes are therefore shared across containers (an `emptyDir` written by
one container is visible to the others). A stale symlink or empty
directory already at a mount path (e.g. from an image template) is
replaced; any other existing content fails preparation with
`MountPathConflict`.

| Type        | Materialization under `volumes/<name>`                                    |
|-------------|---------------------------------------------------------------------------|
| `emptyDir`  | An empty directory                                                        |
| `hostPath`  | A symlink to the host directory                                           |
| `configMap` | A directory with one file per key (key=name, value=contents), or only the keys selected by `configMap.items` at their `path`. ConfigMap data is fetched once during pod preparation via the configured `ConfigMapResolver`. |
| `projected` | A directory merging one or more `configMap` sources flat (each source honoring `items`). On a path collision the later source wins and a warning is logged. Only `configMap` sources are supported; others are skipped with a warning. |

**Read-only mounts are not enforced** (mounts are plain symlinks).
Instead, a warning line is written at the very beginning of the
container's log for each `readOnly` mount, stating that not writing
into the volume is the container's responsibility.

**`configMap.items` is supported; `volumeMount.subPath` is not.** A configMap
volume writes either every key (file named after the key) or, when `items` is
set, only the listed keys at their relative `path`. A `projected` volume flat-
merges multiple configMap sources into one directory with last-source-wins on
collision (logged as a warning, since k8s does this silently). `mode` bits are
parsed but ignored; item paths that are absolute or contain `..` are skipped.

**ConfigMaps are not refreshed at runtime.** Unlike Kubernetes — where the
kubelet re-syncs mounted configmap volumes when the underlying configmap
changes — kminus resolves the configmap exactly once via the
`ConfigMapResolver` during pod preparation and materialises the resulting
files before the container starts. There is no watcher, no re-resolution, and
no on-disk refresh; subsequent edits to the configmap (file backend or
apiserver) are not propagated to running pods. The pod must be recreated to
pick up new values. The same one-shot resolution applies to env-referenced
configmaps (`configMapKeyRef` / `configMapRef`) and to secrets.

The recommended convention is to mount configmaps at
`/configmaps/<configmap-name>`, which gives a predictable location for both
spec authors and the processes reading the files. This is a convention, not
an enforced constraint — the mount path is taken verbatim from
`volumeMount.mountPath` and rewritten under the container root during
preparation.

### Environment Variable Resolution

Three sources, evaluated in priority order (lowest to highest):

1. **Node-level envs** -- from the nodeEnvsResolver (inherited host env or file)
2. **envFrom** -- bulk import from secrets/configmaps
3. **Individual env entries** -- literal values or references to secret/configmap keys

### Process Management

- Processes are spawned via `tokio::process::Command`
- Output is redirected to rotating log files (100MB per file, 5 files)
- On Unix, child processes can be configured to die with the parent (`prctl PR_SET_PDEATHSIG`)
- Graceful shutdown: SIGTERM, wait grace period, then SIGKILL
- Termination grace period (between SIGTERM and SIGKILL) is resolved as:
  an explicit delete `gracePeriod` &rarr; the pod's
  `terminationGracePeriodSeconds` &rarr; the runner's `defaultGracePeriodSecs`

### Restart Policies

| Policy      | Behavior                                                      |
|-------------|---------------------------------------------------------------|
| `Never`     | A failed init container fails the pod                         |
| `OnFailure` | As `Never`; a Job's init containers are never re-run          |
| `Always`    | A failed init container restarts in place, up to `maxRestartCount` times |

Only init containers restart in place; a main container's exit ends the pod.
While an init container backs off it waits in `CrashLoopBackOff`, its
`restartCount` grows and each run writes its own `<restartCount>.log`.

Restart backoff: `restartBackoffBaseSecs * 2^restart_count`, capped at
`restartBackoffBaseSecs * 16`.

---

## Health Probes

### Probe Types

| Probe        | Purpose                                    | On Failure                             |
|--------------|--------------------------------------------|----------------------------------------|
| **Startup**  | Gates liveness and readiness probes        | Kill container after failureThreshold  |
| **Liveness** | Detects deadlocked containers              | Kill container after failureThreshold  |
| **Readiness**| Gates traffic (Ready condition)            | Mark container not ready               |

### Probe Actions

| Action        | Mechanism                                                        |
|---------------|------------------------------------------------------------------|
| `exec`        | Spawn process, exit code 0 = success                             |
| `httpGet`     | HTTP GET to host:port/path, 2xx/3xx = success                    |
| `tcpSocket`   | TCP connect to host:port, connection success = probe success     |

### Probe Parameters

`initialDelaySeconds`, `periodSeconds`, `timeoutSeconds`, `successThreshold`, `failureThreshold` -- standard K8s
probe parameters are supported.

---

## mTLS

Pod Runner always uses mTLS with auto-generated certificates. Uses `rustls` (no OpenSSL dependency).

---

## Metrics

Prometheus metrics served on a separate port at `/metrics`. The port comes from `--metrics-port` when passed, otherwise from `metricsUrl` in `prsysconfig.yaml` (default `3001`).

### Gauges

- `pr_pods_running` -- current running pods
- `pr_containers_running` -- current running containers
- `pr_tokio_tasks_active` -- active async tasks
- `pr_memory_physical_bytes` -- RSS memory
- `pr_memory_virtual_bytes` -- virtual memory
- `pr_uptime_seconds` -- seconds since startup

### Counters

- `pr_pods_created_total`, `pr_pods_started_total`, `pr_pods_deleted_total`, `pr_pods_failed_total`
- `pr_containers_started_total`, `pr_containers_failed_total`

### Histograms

- `pr_pod_creation_seconds` -- pod creation latency
- `pr_pod_deletion_seconds` -- pod deletion latency
- `pr_http_request_duration_seconds` -- HTTP request latency by method, path, status

---

## Logging

### Container Logs

Follows K8s log naming conventions:

```
<workdir>/data/pod-state/<bucket>/<log_pod_id>/<stamp>_<pod_name>/
  log/<container-name>/
    0.log               # Current log (restart_count=0)
    0.log.1             # Rotated (most recent)
    0.log.2             # Older
    1.log               # Log after first restart
    probe-startup.log   # Startup probe output
    probe-liveness.log  # Liveness probe output
    probe-readiness.log # Readiness probe output
```

Log rotation: 100MB per file, 5 files max. Rotation happens after a newline boundary.

On-disk log files use the CRI format
(`<ts> <stdout|stderr> F <message>`); log collectors can consume them
directly.

### Log Retrieval

The `/getContainerLogs` endpoint serves kubectl-style output — like a
real kubelet, it parses the CRI lines and returns the messages only. It
supports:

- `tailLines` -- return only the last N lines
- `follow` -- stream logs until the pod reaches a terminal phase;
  followed streams survive log rotation (the reader drains the rotated
  file and reopens the fresh one)
- `previous` -- return logs from the previous restart
- `timestamps` -- prefix each line with its CRI timestamp (matches
  `kubectl logs --timestamps`)

The `/getPodLogs` endpoint serves the logs of every pod filed under a log
pod id: the value of the pod's `tabsdata.com/log-id` label, or its name when it has
none (see Reaping). It reads the pod-state dirs, so it works whether or not
the pods still exist. virtual-kubelet does not call it; `kubectl logs` goes
through `/getContainerLogs`. Apart from `logPodId`, it takes the query
string of `/getContainerLogs`:

```
GET /getPodLogs?logPodId=nightly[&podName=<pod>][&containerName=web][&tailLines=100][&previous=true][&timestamps=true]
```

Set `tabsdata.com/log-id` on a CronJob's, Job's or Deployment's pod template and
every pod it creates is filed under that id: all the runs of a CronJob,
all the retries of a Job, all the replicas and rollouts of a Deployment.
Pods (incarnations) come oldest first, each opening with a separator that
names its creation time and pod:

```
------------------------------------------------------------
POD STARTED 2026-10-19T14:30:05Z nightly-29001230-x7k2p
------------------------------------------------------------
--- container setup ---
...
--- container web ---
...
```

- Containers come in spec order, init containers first, one after the
  other (how `kubectl logs --all-containers` joins them). Each opens with
  a `--- container <name> ---` line unless `containerName` picks one.
- Lines are formatted as `/getContainerLogs` formats them. Restarts come
  oldest first and, within a restart, rotated files (`N.log.4` ...
  `N.log.1`) before `N.log`, so the whole history is there, not just the
  current file. Probe logs are left out.
- `podName` -- only that pod among those filed under the id, written
  without its `POD STARTED` separator (container headers stay unless
  `containerName` picks one). It is for a caller that reads one pod at a
  time and writes its own separators, such as Tabsdata's log
  multiplexer. A bare pod recreated under its name matches every
  incarnation, oldest first.
- `tailLines` -- the last N lines of each container, per pod.
- `previous` -- leave out the newest pod.
- `namespace` -- accepted; a pod runner serves a single namespace.
- `follow` -- refused with 400: this is a record, not a live stream.

A log pod id with nothing on disk, a `podName` not filed under it,
`previous` with only one pod, or a `containerName` no pod has logs for is
a 404. A log pod id, pod name or container name that is not a Kubernetes
name is a 400.

---

## Storage

Pod specs and statuses are stored in a multiplexed store:

| Store      | Purpose                            |
|------------|------------------------------------|
| **Memory** | For pod runner operation.          |
| **File**   | For troubleshooting purposes only. |

Both stores are updated on every state change.

---

## Reaping

A pod's footprint is split across two dirs with independent clocks:

- **data dir** -- `<workdir>/data/pod-run/<uid>/`: `work/` and `volumes/`. Everything the
  containers produced, usually the bulk of the footprint, and nothing reads it once the pod is
  done. It is **reaped** on a clock that runs from **pod completion**.
- **state dir** -- `<workdir>/data/pod-state/<bucket>/<log_pod_id>/<stamp>_<pod_name>/`: the
  `log/` and `store/` that are one pod's only record of what it did and why it ended. It is
  **deleted** on a clock that runs from the moment the pod is **removed from the pod runner**,
  because until then the runner still answers queries about it.

The data dir is named by the pod's uid alone: per uid, so a new pod never touches the data an
earlier one is still keeping for inspection, and as short as a unique name can be, so the paths
under it stay within Windows' 260 characters. Admission rejects a uid that is not ASCII letters,
digits and `-`, so it is always a plain path component. Beside it, `<workdir>/data/pod-run/<pod_id>`,
`<pod_id>` being `<namespace>_<name>_<uid>`, is a relative symlink to it for anyone looking for a
pod by name; the pod runner itself never goes through it. A uid has no `_` and a pod id always has
two, so a data dir and a link never share a name. Reaping a data dir removes its link, and startup
recovery removes links whose data dir is gone.

The state path is keyed by the **log pod id**: the value of the pod's `tabsdata.com/log-id` label when it
carries one that is a valid Kubernetes name, and the pod name otherwise. The label is the
`logIdLabel` setting in `prsysconfig.yaml`; empty disables it. A label value that is not a valid
name is ignored with a warning at admission, and the pod is filed under its name. The uid is never
part of the state path, and a pod runner serves a single namespace, so the id is unique as given.

Every pod filed under an id gets an incarnation dir of its own, `<stamp>_<pod_name>`:
- `<stamp>` is the pod's `metadata.creationTimestamp` in compact UTC (`20261019T143005Z`;
  RFC3339's `:` is not valid in a Windows path). Admission sets it to the admission time on a pod
  that has none (`kpr` submissions).
- The pod name keeps apart pods created in the same second: replicas, parallel Job pods.
  Kubernetes names never contain `_`, so the two halves split back unambiguously.
- Sorting the dir names sorts the pods by creation time.
- A pod replayed after a restart keeps its creation time and name, so it lands back in its own
  dir.

`<bucket>` is two lowercase hex digits: FNV-1a 64 of the log pod id, xor-folded to 8 bits (256
buckets). State dirs outlive their pods and accumulate far more entries than data dirs, so the
bucket keeps any one directory from growing unbounded -- and since the path is computed from the
id, a pod's records can be found whether or not the runner still knows the pod.

Each clock has a TTL per outcome, so four settings in total:

| Setting                                | Default | Governs                          |
|----------------------------------------|---------|----------------------------------|
| `podDataReapTtlOnPodSuccess`           | `0s`    | data dir of a succeeded pod      |
| `podDataReapTtlOnPodFailure`           | `1h`    | data dir of a failed pod         |
| `podDirDeleteTtlPodSuccessOnPodDelete` | `1h`    | state dir of a succeeded pod     |
| `podDirDeleteTtlPodFailureOnPodDelete` | `24h`   | state dir of a failed pod        |

A value is `<number><unit>` with unit `s`, `m` or `h`, or `never` to keep the files indefinitely.
There is no `d` unit -- `24h` says the same thing without a second spelling. An invalid value in
`prsysconfig.yaml` fails startup rather than silently falling back.

Each can be overridden per pod with the matching annotation: `prPodDataReapTtlOnPodSuccess`,
`prPodDataReapTtlOnPodFailure`, `prPodDirDeleteTtlPodSuccessOnPodDelete`,
`prPodDirDeleteTtlPodFailureOnPodDelete`. An annotation that doesn't parse is ignored with a
warning and the configured value is used.

### How it works on disk

Each dir carries everything its own clock needs, so the two never consult each other. At pod
admission the resolved values are written to a `_cleanup_ttls.json` in each dir, recording where
each one came from: the reap TTLs in the data dir, the delete TTLs in the incarnation's state dir.
The pods filed under one id can carry different annotations and end differently, so each has its
own policy, outcome marker and deadline; the `<log_pod_id>/` dir above them holds nothing but
them. Nothing on
the reaping path re-derives them from config, so each dir carries its own retention policy and
stays correct across a config edit or a runner restart.

At completion both dirs get an outcome marker (`_succeeded` or `_failed`, content the completion
time) and the data dir gets `_reap_on`. When the pod leaves the runner the state dir gets
`_delete_on`. Deadlines are stored as absolute instants, or the literal `never`, so a reader can
see exactly when a dir is due -- no TTL arithmetic, and "never expires" is distinguishable from
"the file is missing". Acting on a deadline removes the whole dir it lives in.

- **Inline**: whenever a pod is finalized or evicted, the deadline is applied immediately, so a
  `0s` TTL acts at once. A deadline that can't be parsed is never acted on. Removing an
  incarnation's state dir also removes its `<log_pod_id>/` dir once that is empty -- with a plain
  `rmdir`, never a recursive delete, so a new pod being admitted under the id at the same moment
  either keeps it from going or recreates it.
- **Timers**: a deadline still ahead gets one task that sleeps until it comes due. There is no
  scanning daemon: a deadline is known the moment it is written, so the runner waits on it
  directly instead of re-reading every finished pod dir to rediscover what it already knew. On
  waking, the task re-reads the deadline file and acts only if it still holds the deadline it was
  scheduled for -- a dir claimed again in the meantime (below) has had it cleared. Each sleep is
  raced against shutdown, so a pending deadline never holds the runner's exit.
- **Recovery**: the deadlines are on disk, so a restart loses none of them. At startup, before
  any pod is admitted, the runner walks both roots and applies or schedules every deadline it
  finds. A data dir with no outcome marker belonged to a pod the previous run never finalized; it
  is finalized as failed and reaped on its failure TTL. A state dir with no `_delete_on` belonged
  to a pod still in the runner when it went down; the runner keeps no pods across a restart, so
  that pod has left it and its delete clock starts now (as failed, if it never finished).
- **Re-admission**: a pod replayed after a restart has the same uid and creation time, so it
  claims its previous attempt's dirs back at admission. The data dir is removed outright: it is
  the previous attempt's, and would collide with this one's. The state dir is kept, so the
  previous attempt's logs (shifted into rotation) and status history stay readable, but its
  outcome marker and `_delete_on` are cleared, which stands any pending delete task down.

Process PID files live in the equally fixed `<workdir>/data/pod-pid/`: one `<pid>` file per
container process, content the program name, removed when the process exits. At startup any file
still there was left by a run that did not stop its containers; a still-running process with that
pid and program is killed, and the file removed. There are no per-run directories.

A pod that is never deleted keeps `log/` and `store/` for as long as the runner reports on it:
the control plane owns that pod's lifetime, and its record is live data until it doesn't.
Terminal pods hold no capacity (admission counts non-terminal pods only), so they cost nothing
but the record itself.

---

## Directory Layout

```
<workdir>/
  config/                                        # Config files (shared)
    prconfig.yaml                                # Regenerated each start
    prsysconfig.yaml                             # Persistent, user-editable
  log/
    pod-runner.log                               # pod-runner's own log (rotated)
  data/
    pod-run/<uid>/                               # Pod data dir -- removed on the reap deadline
      volumes/<volume_name>/                     # Pod-level volumes (emptyDir dir, hostPath symlink, configMap files)
      work/<container>/                          # Container working directory (mount paths symlink into volumes/)
      _cleanup_ttls.json                         # Resolved reap TTLs (written at admission)
      _succeeded | _failed                       # Outcome marker, content = completion time
      _reap_on                                   # Reap deadline, or "never"
      _size                                      # Cached disk usage
    pod-run/<namespace>_<name>_<uid>             # Symlink to pod-run/<uid>/, removed with it
    pod-state/<bucket>/<log_pod_id>/             # tabsdata.com/log-id label value, or pod name; removed with the last pod
      <stamp>_<pod_name>/                        # One pod's state dir -- removed on the delete deadline
        log/<container>/
          0.log                                  # Container stdout/stderr
          0.log.1                                # Rolled log
          probe-startup.log                      # Probe output
        store/
          original-spec.json                     # Original pod spec
          running-spec.json                      # Preprocessed spec (optional)
          status/<timestamp>.json                # Timestamped status snapshots
        _cleanup_ttls.json                       # Resolved delete TTLs (written at admission)
        _succeeded | _failed                     # Outcome marker, content = completion time
        _delete_on                               # Delete deadline, or "never" (written at eviction)
    pod-pid/<pid>                                # Process PID tracking, content = program name
```

---

## Constraints and Limitations

| Area                     | Status                                                            |
|--------------------------|-------------------------------------------------------------------|
| Workload types           | Jobs, CronJobs, Pods. No Deployments/StatefulSets/DaemonSets.     |
| Container images         | Local directory templates or `.tgz`/`.tar.gz` archives. No registry pulls. |
| Resource requests/limits | Stored and echoed; not enforced.                                  |
| Namespaces               | Accepted for lookup; no isolation.                                |
| Volumes                  | hostPath (Directory), emptyDir, configMap, and projected (configMap sources). No PVCs. |
| Secrets                  | Environment variables only. No file-mount support.                |
| ConfigMaps               | Environment variables, plus configMap / projected volume mounts (`items` supported, `subPath` not). |
| Probes                   | exec, httpGet, tcpSocket. No gRPC.                                |
| Restart policies         | Never, OnFailure. No Always (except for sidecar init containers). |
| Networking               | Host network only. No pod networking or service mesh.             |
| Security contexts        | Not enforced.                                                     |

Some unsupported features are **rejected at admission** (before the pod is
stored) rather than silently ignored, so they fail fast with a clear error:
an unsupported volume source (anything other than hostPath / emptyDir /
configMap / projected), a `gRPC` probe, `volumeMount.subPath` /
`subPathExpr`, and `optional: true` on a configmap/secret reference.
