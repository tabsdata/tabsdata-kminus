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

Two YAML files live in the config directory:

| File               | Lifetime                                                                     |
|--------------------|------------------------------------------------------------------------------|
| `prconfig.yaml`    | Endpoint URL + TLS material. Regenerated on every start by the orchestrator (km). |
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
| `--metrics-port` | `3001`        | Metrics server port (`0` disables)                             |
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
defaultGracePeriodSecs: 30
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

metricsUrl: "http://127.0.0.1:3001"

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
       If any non-sidecar init container fails, mark pod as Failed.
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

Each container runs in `<pod_dir>/work/<container-name>/` where
`<pod_dir>` is `<workdir>/run/pods/<namespace>_<name>_<uid>/`.
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
| `Never`     | Container is not restarted after exit                         |
| `OnFailure` | Restart only on non-zero exit, up to `maxRestartCount` times  |

Restart backoff: `restartBackoffBaseSecs * 2^restart_count`.

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

Prometheus metrics served on a separate port (configured via `--metrics-port`, default `3001`) at `/metrics`.

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
<workdir>/run/pods/<namespace>_<name>_<uid>/
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

Two independent clocks govern a finished pod's on-disk footprint:

- **reap** deletes the data dirs -- `work/` and `volumes/`. They hold everything the containers
  produced and are usually the bulk of the footprint, while nothing reads them once the pod is
  done. The clock runs from **pod completion**.
- **delete** deletes the whole pod dir, including the `log/` and `store/` that are the pod's only
  record of what it did and why it ended. The clock runs from the moment the pod is **removed from
  the pod runner**, because until then the runner still answers queries about it.

Each clock has a TTL per outcome, so four settings in total:

| Setting                                | Default | Governs                                 |
|----------------------------------------|---------|-----------------------------------------|
| `podDataReapTtlOnPodSuccess`           | `0s`    | `work/` + `volumes/` of a succeeded pod |
| `podDataReapTtlOnPodFailure`           | `1h`    | `work/` + `volumes/` of a failed pod    |
| `podDirDeleteTtlPodSuccessOnPodDelete` | `1h`    | pod dir of a succeeded pod              |
| `podDirDeleteTtlPodFailureOnPodDelete` | `24h`   | pod dir of a failed pod                 |

A value is `<number><unit>` with unit `s`, `m` or `h`, or `never` to keep the files indefinitely.
There is no `d` unit -- `24h` says the same thing without a second spelling. An invalid value in
`prsysconfig.yaml` fails startup rather than silently falling back.

Each can be overridden per pod with the matching annotation: `prPodDataReapTtlOnPodSuccess`,
`prPodDataReapTtlOnPodFailure`, `prPodDirDeleteTtlPodSuccessOnPodDelete`,
`prPodDirDeleteTtlPodFailureOnPodDelete`. An annotation that doesn't parse is ignored with a
warning and the configured value is used.

### How it works on disk

The resolved values are written to `_cleanup_ttls.json` in the pod dir at pod start, recording
where each one came from. Nothing on the reaping path re-derives them from config, so a pod dir
carries its own retention policy and stays correct across a config edit or a runner restart.

At completion the pod dir gets an outcome marker (`_succeeded` or `_failed`, content the
completion time) and `_reap_on`. When the pod leaves the runner it gets `_delete_on`. Deadlines
are stored as absolute instants, or the literal `never`, so a reader can see exactly when a dir
was due -- no TTL arithmetic, and "never expires" is distinguishable from "the file is missing".
These files are the record of what was scheduled; nothing reads them back to decide.

- **Inline**: whenever a pod is finalized or evicted, the deadlines are applied immediately, so a
  `0s` TTL acts at once. When both deadlines are due, delete wins -- there is no point reaping a
  dir that is about to go. A deadline that can't be parsed is never acted on.
- **Timers**: a deadline still ahead gets one task that sleeps until it comes due and then acts.
  There is no scanning daemon: a deadline is known the moment it is written, so the runner waits
  on it directly instead of re-reading every finished pod dir to rediscover what it already knew.
  Each sleep is raced against shutdown, so a pending deadline never holds the runner's exit --
  what it would have done is dropped, and rotation picks it up next run. A pod's two timers can
  come due at the same moment, since deleting a pod that is still live starts both clocks at
  once; the delete tolerates the reap dropping its marker into the dir mid-walk and simply walks
  it again.
- **Rotation**: on startup, every pod dir in the just-rotated `run.N` has its data dirs reaped
  regardless of its own deadline -- those pods belong to a runner that no longer exists. This is
  also what takes over the deadlines dropped at shutdown, and it covers pods a `SIGKILL`ed runner
  never finalized. An explicit `never` is honoured.

The runner keeps no pod state between runs, so in-memory timers lose nothing a later run needs:
a pod whose deadline outlives the process that scheduled it is reclaimed by rotation instead.

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
  run/                                           # Current run (rotated on restart)
    pods/<namespace>_<name>_<uid>/               # Per-pod, co-located
      volumes/<volume_name>/                     # Pod-level volumes (emptyDir dir, hostPath symlink, configMap files)
                                                 # -- reaped on the reap deadline
      work/<container>/                          # Container working directory (mount paths symlink into volumes/)
                                                 # -- reaped on the reap deadline
      log/<container>/
        0.log                                    # Container stdout/stderr
        0.log.1                                  # Rolled log
        probe-startup.log                        # Probe output
      store/
        original-spec.json                       # Original pod spec
        running-spec.json                        # Preprocessed spec (optional)
        status/<timestamp>.json                  # Timestamped status snapshots
      _cleanup_ttls.json                         # Resolved cleanup TTLs (written at pod start)
      _succeeded | _failed                       # Outcome marker, content = completion time
      _reap_on                                   # Reap deadline, or "never"
      _delete_on                                 # Delete deadline, or "never" (written at eviction)
      _reaped                                    # Written once the data dirs are reaped
      _size                                      # Cached disk usage
    pids/<pid>                                   # Process PID tracking
  run.0/                                         # Previous run (auto-rotated)
  run.1/                                         # Two runs ago
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
