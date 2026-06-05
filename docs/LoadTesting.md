<!--
  Copyright 2026 Tabsdata Inc.
-->

> **Historical design notes.** This file explores the full load-test
> measurement space considered at design time. The implemented harness
> is simpler — three metrics (`nominal_time_secs`, `actual_time_secs`,
> `start_latency_secs`) — and lives in `ltest/`; see `ltest/LoadTest.md`
> for current usage.

Good problem. Here's how I'd think about the full measurement space:

## Time Dimensions

**Submission → Running (scheduling latency)**
- `schedulingLatency`: time from pod creation timestamp to `status.conditions[PodScheduled].lastTransitionTime`
- `initStartLatency`: time from `PodScheduled` to first init container `startedAt`
- Captures: scheduler queue depth, node selection, image pull (cold vs warm)

**Running → Completion (execution time)**
- `wallTime`: `finishedAt - startedAt` of the longest container (critical path)
- `nominalTime`: sum of all container `finishedAt - startedAt` (aggregate CPU time)
- For init containers: sequential wall time vs. their individual durations

**Completion → Observable (propagation latency)**
- `completionPropagationDelay`: time from last container `finishedAt` to when `kubectl get pod` returns `Succeeded`/`Failed`
- This captures kubelet → API server → etcd → watch propagation
- Measure by polling with a tight loop and timestamping when status becomes terminal

**End-to-end**
- `e2eLatency`: pod `creationTimestamp` → phase becomes `Succeeded/Failed` as seen by client
- This is the number operators actually care about

---

## Container Decomposition

Since you have init containers + sidecars + main containers, break down:

- Per-container `startedAt` / `finishedAt` from `status.initContainerStatuses` / `status.containerStatuses`
- **Init chain duration**: last init `finishedAt - firstInit startedAt` (sequential, so this is real blocking time)
- **Sidecar startup contribution**: time from pod scheduled to all sidecars in `Running` state
- **Main container start delay**: time from last init finishing to main container `startedAt`
- **Injected failure impact**: for containers with expected failures, measure retry count, backoff time (`restartCount`, `lastState.terminated.finishedAt` gaps)

---

## System-Level Dimensions

**Concurrency & load shape**
- Batch size (how many pods submitted simultaneously)
- Submission rate (pods/sec if staggered)
- Node count and pod-per-node ratio at peak

**Resource pressure signals**
- Node where pod landed (to detect hot nodes)
- Image pull: cold (not cached) vs. warm — huge variance source; tag pods with a label
- PVC binding time if volumes are involved

**Failure dimensions**
- Exit codes per container
- `restartCount` per container
- Time lost to failed init containers (since they block the chain)
- Whether failure was in init vs. sidecar vs. main — very different impact

**Scheduler & API server**
- Time-in-pending before scheduled (queue depth proxy)
- Gap between pod creation and first kubelet event (API server → kubelet latency)

---

## What to Report Per Pod

```
podName, node, batchId, submittedAt (client clock)
schedulingLatency        # creationTimestamp → PodScheduled
imagePullDuration        # warm=0, cold=measured
initChainDuration        # first init start → last init finish
sidecarReadyDuration     # scheduled → sidecars Running
mainStartDelay           # last init finish → main container startedAt
wallTime                 # main container startedAt → finishedAt
nominalTime              # sum of all container durations
e2eLatency               # submitted → terminal phase observed
propagationDelay         # container finishedAt → client-observable
exitCode                 # per container
restartCount             # per container
failurePhase             # init|sidecar|main|none
```

---

## Aggregate Statistics to Report

For each metric above, don't just mean/median — the interesting stuff lives in the tail:

- p50, p90, p95, p99, max
- Histogram buckets (useful for scheduling latency and e2e)
- Batch-level: time-to-first-completion, time-to-last-completion, throughput (pods/sec)
- Cohort comparison: cold-pull pods vs. warm, failure pods vs. clean

---

## A Few Things That Bite You

1. **Clock skew**: `submittedAt` is your client clock; everything else is API server clock. Use `creationTimestamp` as your t0 for internal deltas, and only compare client clock to API server clock for propagation delay (accept ~10ms noise).
2. **Watch vs. poll**: if you use `kubectl get` polling to detect completion, your `propagationDelay` measurement includes your poll interval — use `--watch` or a proper informer loop for accurate timestamps.
3. **Image pull as a confounder**: it's huge and bimodal. Always separate cold/warm cohorts or your p99 is meaningless.
4. **Init container failures**: a failing init container with `restartPolicy: Never` ends the pod immediately; with `Always` it retries. The retry gaps are dead time that inflates your e2e without showing up in wall time.

Want me to sketch a bash/Python harness that collects these metrics from `kubectl get pod -o json`?