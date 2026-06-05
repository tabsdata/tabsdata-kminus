<!--
  Copyright 2026 Tabsdata Inc.
-->

  <!--
  Copyright 2026 Tabs Data Inc.
  -->

# Pod Runner -- User Guide

## Quick Start

### Starting the Kminus Stack

```bash
# Build and add to PATH
kminus/build.sh
export PATH=$(pwd)/kminus/target/bin:$PATH

# Start all components (control plane, pod runner, virtual kubelet)
tdk start

# Set up kubectl
export KUBECONFIG=./config/kubeconfig.yaml
kubectl get nodes    # Should show "tdknode" in Ready state
```

### Running a Job

```yaml
# my-job.yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: hello
spec:
  template:
    spec:
      restartPolicy: Never
      containers:
        - name: greeter
          image: /dev/null
          command: ["/bin/echo"]
          args: ["Hello from Kminus!"]
```

```bash
kubectl apply -f my-job.yaml
kubectl get pods                  # Watch pod reach Succeeded
kubectl logs hello-<pod-id>      # View output
kubectl delete job hello
```

### Stopping the Stack

```bash
tdk stop          # Graceful shutdown
tdk stop-force    # Force kill (if needed)
```

---

## Pod Specs

### Container Images

Unlike standard Kubernetes, the `image` field points to a **local directory** or a **local archive** on disk.
The image contents are copied into the container's working directory before the process starts.

Two `image` forms are supported:

- A **directory** tree — copied recursively into the container working directory.
- A **gzip-compressed tar archive** ending in `.tgz` or `.tar.gz` — extracted into the container working directory.

```yaml
containers:
  - name: app
    image: /path/to/my/app        # Directory with binaries and configs
    command: ["./my-binary"]

  - name: archived
    image: /path/to/my/app.tgz    # tar.gz archive — extracted into the work dir
    command: ["./my-binary"]
```

If the path resolves to a missing file or directory, pod preparation fails with an error. Format is detected by file-name suffix
(`.tgz` or `.tar.gz` ⇒ archive; anything else ⇒ directory).

Use `/dev/null` or `""` when no template is needed (e.g., running system commands):

```yaml
containers:
  - name: worker
    image: /dev/null
    command: ["/usr/bin/python3"]
    args: ["script.py"]
```

#### Resolving relative `image` paths (`imagesDir`)

The `image` field may be either an **absolute** or a **relative** path:

- **Absolute** paths (e.g. `/path/to/app` or `/path/to/app.tgz`) are used as-is.
- **Relative** paths (e.g. `my-app` or `my-app.tgz`) are resolved against the pod runner's images root.

The images root is determined with the following precedence (highest first):

1. `--imagesdir <path>` on the pod-runner command line (forwarded by `km start --imagesdir <path>`).
2. `imagesDir` in `prsysconfig.yaml`:

   ```yaml
   # prsysconfig.yaml
   imagesDir: "/var/lib/kminus/images"
   ```

3. `./images` under the pod-runner's current working directory (default when neither of the above is set).

A relative images-root value is itself resolved against the pod-runner's current working directory, so an
absolute path is recommended when running pod-runner outside its install directory.

With `imagesDir: "/var/lib/kminus/images"`, `image: my-app/v1` resolves to `/var/lib/kminus/images/my-app/v1`,
and `image: my-app/v1.tgz` resolves to `/var/lib/kminus/images/my-app/v1.tgz`.

### Working Directory

Each container runs in its own working directory at `<pod_dir>/work/<container-name>/`. The image directory
template is copied here. All file references in the container should be **relative** to this directory.

If you need an absolute path at runtime, use the `KM_CONTAINER_ROOT` environment variable:

```yaml
env:
  - name: CONFIG_PATH
    value: "$(KM_CONTAINER_ROOT)/config/app.yaml"
```

`KM_CONTAINER_ROOT` is set by the pod runner to the container's physical working directory
(`<pod_dir>/work/<container-name>/`). Each container in the pod gets its own value.

The pod runner performs K8s-style `$(VAR)` substitution on env values, `command`, and `args`
during pod preparation, matching the kubelet's expansion semantics:

- `$(VAR)` is replaced with the value of `VAR`; `$$` escapes a literal `$`.
- `$(VAR)` referencing an undefined variable is left as a literal (e.g. `$(UNKNOWN)`).
- **Declaration order matters** for individual `env` entries: a value can only reference
  `envFrom` values, `KM_CONTAINER_ROOT`, or `env` entries declared *earlier* in the list.
  Forward references (e.g. `B=$(A)` before `A=val`) are left as the literal `$(A)`.
- `envFrom`-derived values (loaded from Secrets / ConfigMaps) are NOT substituted; they are
  passed to the container verbatim.
- `command` and `args` are substituted using the full container env after all entries are
  processed.

Substitution runs before (and in addition to) the container shell's own `$VAR` expansion.

### Init Containers

Init containers run sequentially before main containers:

```yaml
spec:
  initContainers:
    - name: setup-db
      image: /dev/null
      command: ["/usr/bin/python3"]
      args: ["init_db.py"]
  containers:
    - name: app
      image: /path/to/app
      command: ["./server"]
```

### Sidecars

An init container with `restartPolicy: Always` is treated as a sidecar. It starts during the init phase but runs
alongside main containers without blocking them:

```yaml
spec:
  initContainers:
    - name: log-collector
      image: /path/to/collector
      command: ["./collector"]
      restartPolicy: Always        # This makes it a sidecar
  containers:
    - name: app
      image: /path/to/app
      command: ["./server"]
```

### Restart Policies

| Policy      | Behavior                                                           |
|-------------|--------------------------------------------------------------------|
| `Never`     | Container is not restarted after exit (default for Jobs)           |
| `OnFailure` | Restart on non-zero exit, up to `maxRestartCount` times with exponential backoff |

```yaml
spec:
  restartPolicy: OnFailure
  containers:
    - name: worker
      image: /path/to/worker
      command: ["./process"]
```

---

## Volumes

Volumes are pod-level: each volume is materialized once under the pod
directory (`<pod_dir>/volumes/<volume_name>`) and every container mount
path is a symlink to it, so a volume mounted by several containers is
genuinely shared. `readOnly` mounts cannot be enforced (mounts are
plain symlinks); instead, a warning is written at the very beginning of
the container's log stating that not writing into the volume is the
container's responsibility.

### emptyDir

Creates an empty directory shared between containers in the same pod:

```yaml
spec:
  volumes:
    - name: shared-data
      emptyDir: {}
  containers:
    - name: producer
      image: /dev/null
      command: ["./produce"]
      volumeMounts:
        - name: shared-data
          mountPath: /mounts/data
    - name: consumer
      image: /dev/null
      command: ["./consume"]
      volumeMounts:
        - name: shared-data
          mountPath: /mounts/data
```

### hostPath

Creates a symlink to a directory on the host:

```yaml
spec:
  volumes:
    - name: host-data
      hostPath:
        path: /data/datasets
        type: Directory
  containers:
    - name: app
      image: /dev/null
      command: ["./analyze"]
      volumeMounts:
        - name: host-data
          mountPath: /mounts/datasets
          readOnly: true
```

### configMap

Creates a directory at the mount path and writes one file per configmap key
(filename = key, contents = value). Requires a `ConfigMapResolver` to be
configured.

**`configMap.items` is supported.** When `items` is set, only the listed keys
are projected, each written at its `items[].path` (relative to the mount,
subdirectories allowed). When `items` is omitted, every key is written at a
file named after the key. `mode` (file permission bits) is parsed but ignored,
and `volumeMount.subPath` is **not** supported.

**Convention:** mount configmap volumes at `/configmaps/<configmap-name>`.
This makes the location predictable for both producers (writing pod specs)
and consumers (reading the files at runtime).

**Not refreshed at runtime.** Unlike Kubernetes — where the kubelet re-syncs
configmap volumes when the configmap changes — kminus resolves the configmap
**once** during pod preparation and writes the files to disk before the
container starts. Subsequent edits to the configmap are **not** propagated to
running pods; the pod must be recreated to pick up new values. The same
applies to env-referenced configmaps (`configMapKeyRef` / `configMapRef`).

```yaml
spec:
  volumes:
    - name: config
      configMap:
        name: app-config
  containers:
    - name: app
      image: /dev/null
      command: ["./server"]
      volumeMounts:
        - name: config
          mountPath: /configmaps/app-config
          readOnly: true
```

Given a configmap with `data: { app.conf: "...", logging.yaml: "..." }`, the
container sees `/configmaps/app-config/app.conf` and
`/configmaps/app-config/logging.yaml` populated with the corresponding values.

### projected

Merges one or more sources into a single mount directory. **Only `configMap`
sources are supported** — other projection source types (`secret`,
`downwardAPI`, `serviceAccountToken`) are skipped with a warning logged by
pod-runner. Each `configMap` source behaves like a standalone configMap volume
(honoring `items`), but all sources are flattened into the **same** target
directory.

**Flat merge with last-source-wins on collision.** All keys from all sources
land in the mount directory at their `items[].path` (or key name when `items`
is omitted). If two sources write the same target path, the source listed
**later** in `sources` overwrites the earlier one. This matches Kubernetes'
silent last-wins behavior — but pod-runner **logs a warning** identifying the
colliding path and the winning/overwritten `configmap/key` so the collision is
not invisible.

Keys named by `items` but absent from their configMap, and item paths that are
absolute or contain `..`, are skipped (each with a warning).

```yaml
spec:
  volumes:
    - name: configmap-configs
      projected:
        sources:
          - configMap:
              name: tabsdata-configs-apiserver.yaml
              items:
                - key: apiserver.yaml
                  path: apiserver/apiserver.yaml
          - configMap:
              name: tabsdata-configs-apiserver-log
              items:
                - key: apiserver.log.yaml
                  path: apiserver/apiserver.log.yaml
  containers:
    - name: app
      image: /dev/null
      command: ["./server"]
      volumeMounts:
        - name: configmap-configs
          mountPath: /configs
          readOnly: true
```

The container sees `/configs/apiserver/apiserver.yaml` and
`/configs/apiserver/apiserver.log.yaml`. The same once-at-preparation
resolution caveat as `configMap` volumes applies (not refreshed at runtime).

---

## Environment Variables

### Literal Values

```yaml
env:
  - name: LOG_LEVEL
    value: "info"
  - name: PORT
    value: "8080"
```

### From Secrets

```yaml
env:
  - name: DB_PASSWORD
    valueFrom:
      secretKeyRef:
        name: db-credentials
        key: password
```

### From ConfigMaps

```yaml
env:
  - name: APP_CONFIG
    valueFrom:
      configMapKeyRef:
        name: app-settings
        key: config_value
```

### Bulk Import (envFrom)

```yaml
envFrom:
  - secretRef:
      name: db-credentials       # All keys become env vars
  - configMapRef:
      name: app-settings         # All keys become env vars
```

### Priority Order

When the same variable is defined in multiple sources, the highest-priority source wins:

1. Individual `env` entries (highest)
2. `envFrom` entries (in declaration order, later overrides earlier)
3. Node-level environment variables (lowest)

---

## Health Probes

### Startup Probe

Gates liveness and readiness probes. Use for slow-starting applications:

```yaml
startupProbe:
  httpGet:
    path: /healthz
    port: 8080
  initialDelaySeconds: 5
  periodSeconds: 5
  failureThreshold: 30          # Allow up to 150s to start
```

### Liveness Probe

Detects deadlocked processes. The container is killed after `failureThreshold` consecutive failures:

```yaml
livenessProbe:
  exec:
    command: ["/bin/sh", "-c", "curl -s http://localhost:8080/health"]
  initialDelaySeconds: 15
  periodSeconds: 10
  timeoutSeconds: 5
  failureThreshold: 3
```

### Readiness Probe

Controls the pod's Ready condition. Does not kill the container on failure:

```yaml
readinessProbe:
  tcpSocket:
    port: 5432
  initialDelaySeconds: 5
  periodSeconds: 10
```

### Supported Actions

| Action      | Example                                          |
|-------------|--------------------------------------------------|
| `exec`      | Run a command, exit code 0 = healthy             |
| `httpGet`   | HTTP GET, 2xx/3xx = healthy                      |
| `tcpSocket` | TCP connect, connection success = healthy        |

---

## Logs

### Viewing Logs

```bash
# Standard kubectl (via virtual kubelet)
kubectl logs <pod-name>
kubectl logs <pod-name> -c <container-name>
kubectl logs <pod-name> --follow
kubectl logs <pod-name> --tail=100
kubectl logs <pod-name> --previous    # Logs from previous restart

# Direct via the pod-runner client
kpr logs <pod-name>
kpr logs <pod-name> --container <name> --follow --tail 50
```

### Log Files on Disk

Container logs are stored under the pod's log directory:

```
<workdir>/run/pods/<namespace>_<name>_<uid>/log/<container-name>/
  0.log               # Current log (restart 0)
  0.log.1             # Most recent rotation
  0.log.2             # Older rotation
  1.log               # Log after first restart
  probe-startup.log   # Startup probe output
  probe-liveness.log  # Liveness probe output
  probe-readiness.log # Readiness probe output
```

Logs rotate at 100MB with up to 5 files kept per rotation.

---

## Secrets and ConfigMaps

Secrets are exposed as environment variables. ConfigMaps are exposed as
environment variables or as `configMap` volume mounts (see
[Volumes](#configmap)).

### File-Based Resolver

Store secrets/configmaps as YAML or JSON files:

```
secrets/
  default/
    db-credentials.yaml     # { password: "s3cret", username: "admin" }
configmaps/
  default/
    app-settings.yaml       # { log_level: "info", workers: "4" }
```

Configure in `prconfig.yaml`:

```yaml
secretResolver:
  type: "file"
  configs:
    dir: "/path/to/secrets"

configMapResolver:
  type: "file"
  configs:
    dir: "/path/to/configmaps"
```

### Kubernetes API Resolver

Fetch secrets/configmaps from the K8s API server:

```yaml
secretResolver:
  type: "kube"
  configs:
    kubeconfig: "/path/to/kubeconfig"
```

---

## pod-runner Client (`kpr`)

Direct interaction with the pod-runner, bypassing kubectl:

```bash
# Connection setup
export PRCONFIG=./config/vk-prconfig.yaml

# Pod operations
kpr create -f pod.yaml
kpr get <pod-name>
kpr status <pod-name>
kpr list
kpr delete <pod-name>
kpr logs <pod-name>

# Node information
kpr capacity
kpr node-conditions
kpr node-addresses

# Health checks
kpr status-check
kpr probes-check

# Maintenance
kpr archive              # Archive completed pods
kpr purge                # Remove expired archives
```

---

## Metrics

Prometheus metrics are served at `http://localhost:3001/metrics` (configurable via `metricsPort`).

Key metrics:

| Metric                         | Type      | Description                  |
|--------------------------------|-----------|------------------------------|
| `pr_pods_running`              | Gauge     | Current running pods         |
| `pr_containers_running`        | Gauge     | Current running containers   |
| `pr_memory_physical_bytes`     | Gauge     | RSS memory usage             |
| `pr_pods_created_total`        | Counter   | Total pods created           |
| `pr_pods_failed_total`         | Counter   | Total pods failed            |
| `pr_pod_creation_seconds`      | Histogram | Pod creation latency         |
| `pr_http_request_duration`     | Histogram | API request latency          |

---

## Configuration Reference

### Directory Structure

```
<workdir>/
  config/
    kubeconfig.yaml              # Admin kubeconfig (generated by control-plane)
    vk-kubeconfig.yaml           # virtual-kubelet kubeconfig (generated by control-plane)
    prconfig.yaml                # pod-runner config with TLS certs (generated by pod-runner)
    vk-prconfig.yaml             # Client config with TLS certs (generated by pod-runner)

  log/
    km.log                       # kminus orchestrator log
    control-plane.log            # control-plane log
    pod-runner.log               # pod-runner log
    virtual-kubelet.log          # virtual-kubelet log

  data/
    control-plane/               # control-plane runtime data
    virtual-kubelet/             # virtual-kubelet runtime data
    km/                          # kminus PID files

  run/                           # pod-runner current run
    pods/<pod_id>/               # per-pod co-located data
    pids/                        # process PID tracking
  run.0/                         # previous run (rotated)
  run.1/                         # two runs ago
```

### TLS

By default, all communication uses mTLS with auto-generated certificates. On first start, the pod runner generates
a CA, server certificate, and client certificate. All certs are embedded inline (base64) in the config files.

To use custom certificates, create `prconfig.yaml` before first start with your cert paths or inline data.

---

## Limitations

- **No container isolation** -- processes share the host filesystem and network
- **Local images only** -- supports local directories and `.tgz` / `.tar.gz` archives; no container image registry pulls
- **Single host** -- no multi-node scheduling
- **No gRPC probes** -- exec, httpGet, and tcpSocket only
- **No PVCs** -- hostPath, emptyDir, configMap, and projected (configMap sources) volumes only
- **No `volumeMount.subPath`** -- `configMap.items` is supported, but a mount always exposes the whole volume directory
- **No network policies** -- all containers share the host network
- **No resource enforcement** -- CPU/memory limits are stored but not enforced
- **No security contexts** -- all processes run as the pod runner's user

---

## Troubleshooting

### Pod stuck in Pending

Check pod status for details:

```bash
kubectl describe pod <name>
# or
kpr status <name>
```

Common causes: init container failed, image directory or archive doesn't exist, secret/configmap not found.

### Pod Failed

Check container logs:

```bash
kubectl logs <name> -c <container>
```

Failed pod directories are kept by default (`keepFailedPodDir: true`) for debugging. Inspect the working directory
directly:

```bash
ls run/pods/<namespace>_<name>_<uid>/
```

### Connection Refused

Verify the pod runner is running:

```bash
tdk status
```

Check TLS configuration matches between `prconfig.yaml` and `vk-prconfig.yaml`.

### Stack Won't Start

Delete the work directory for a clean start:

```bash
tdk stop-force
rm -rf work/
tdk start
```
