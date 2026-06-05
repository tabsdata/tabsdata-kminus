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
| `prsysconfig.yaml` | Node identity, pod limits, executor, cleanup, capacity, resolvers. Persistent and user-editable; auto-created with defaults on first start. |

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

keepDeletedPodDir: false
keepFailedPodDir: true
deletedPodDirTtlHours: 24
podArchiveTtlDays: 7

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
| **GET**    | `/getAllPodStatuses`  | Bulk status dump for sync loop — returns `[{namespace, name, phase, status}]`   |                                                                                   |
| **GET**    | `/getContainerLogs`   | Stream container logs   | `podName` (req), `namespace`, `containerName`, `follow`, `tailLines`, `previous`  |

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

The `image` field in a container spec is treated as a **local directory path**. If the path exists and is a
directory, its contents are recursively copied into the container's working directory. Use `/dev/null` or an empty
string to skip the copy.

### Working Directory

Each container runs in `<pod_dir>/work/<container-name>/` where
`<pod_dir>` is `<workdir>/run/pods/<namespace>_<name>_<uid>/`.
The image directory template (if any) is copied here before the
container starts. All filesystem resources should be referenced
relative to this directory.

### Volume Mounts

| Type        | Behavior                                                                  |
|-------------|---------------------------------------------------------------------------|
| `emptyDir`  | Creates an empty directory at the mount path                              |
| `hostPath`  | Creates a symlink from the mount path to the host directory               |
| `configMap` | Creates a directory at the mount path; one file per key (key=name, value=contents), or only the keys selected by `configMap.items` at their `path`. ConfigMap data is fetched once during pod preparation via the configured `ConfigMapResolver`. |
| `projected` | Merges one or more `configMap` sources flat into the mount directory (each source honoring `items`). On a path collision the later source wins and a warning is logged. Only `configMap` sources are supported; others are skipped with a warning. |

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

### Log Retrieval

The `/getContainerLogs` endpoint supports:

- `tailLines` -- return only the last N lines
- `follow` -- stream logs until the pod reaches a terminal phase
- `previous` -- return logs from the previous restart

---

## Storage

Pod specs and statuses are stored in a multiplexed store:

| Store      | Purpose                            |
|------------|------------------------------------|
| **Memory** | For pod runner operation.          |
| **File**   | For troubleshooting purposes only. |

Both stores are updated on every state change.

---

## Cleanup and Purge

- **Cleanup**: on pod deletion or terminal phase, optionally archive pod directory to `.tgz`
- **Purge**: background daemon removes archived/deleted pod directories after TTL expiry
  (it also scans the live `run/pods/` directory for terminal pods past their TTL)
- `keepDeletedPodDir` / `keepFailedPodDir` -- control retention
- `deletedPodDirTtlHours` / `podArchiveTtlDays` -- TTL settings

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
      work/<container>/                          # Container working directory
      log/<container>/
        0.log                                    # Container stdout/stderr
        0.log.1                                  # Rolled log
        probe-startup.log                        # Probe output
      store/
        original-spec.json                       # Original pod spec
        running-spec.json                        # Preprocessed spec (optional)
        status/<timestamp>.json                  # Timestamped status snapshots
      archive.tgz                                # Per-pod archive (on cleanup)
      FINISHED                                   # Terminal sentinel
    pids/<pid>                                   # Process PID tracking
  run.0/                                         # Previous run (auto-rotated)
  run.1/                                         # Two runs ago
```

---

## Constraints and Limitations

| Area                     | Status                                                            |
|--------------------------|-------------------------------------------------------------------|
| Workload types           | Jobs, CronJobs, Pods. No Deployments/StatefulSets/DaemonSets.     |
| Container images         | Local directory templates only. No registry pulls.                |
| Resource requests/limits | Stored and echoed; not enforced.                                  |
| Namespaces               | Accepted for lookup; no isolation.                                |
| Volumes                  | hostPath (Directory), emptyDir, configMap, and projected (configMap sources). No PVCs. |
| Secrets                  | Environment variables only. No file-mount support.                |
| ConfigMaps               | Environment variables, plus configMap / projected volume mounts (`items` supported, `subPath` not). |
| Probes                   | exec, httpGet, tcpSocket. No gRPC.                                |
| Restart policies         | Never, OnFailure. No Always (except for sidecar init containers). |
| Networking               | Host network only. No pod networking or service mesh.             |
| Security contexts        | Not enforced.                                                     |
