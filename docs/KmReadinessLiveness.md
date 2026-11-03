<!--
  Copyright 2026 Tabsdata Inc.
-->

# Kminus Readiness and Liveness

## Scope

Recovering or restarting an individual component is **not** a goal here.
There is no restart, no backoff, no partial-service mode. The mechanism
answers one question — is the whole stack working? — and has exactly one
remedy when the answer is no: log why, and shut the whole stack down. A
kminus instance is either fully up or fully down; bringing it back is a
deliberate `km start`.

That choice is what makes the rest of this document simple. Nothing has to
decide whether a failure is recoverable, negotiate a restart order, or
reason about a stack running with one component missing.

## How it works

Every component publishes a heartbeat file that it rewrites once per
interval, and only after verifying itself. km reads those files: their
appearance is the readiness gate for starting the next component, their
freshness is the liveness signal it supervises. km speaks neither HTTP nor
TLS to any component — it stats a file.

A fresh beat therefore means *serving*, not merely *process alive*, which a
bound socket cannot distinguish.

Heartbeats are the only thing readiness and liveness depend on. A component
with something to *say* rather than to prove — a sizing figure, a warning not
worth failing over — uses the separate message channel in
[KmComponentMessages.md](KmComponentMessages.md), which km prints but never
waits for.

## The file

`<workdir>/data/<component>/heartbeat.json`, one line of JSON:

```json
{"ts":1787354884,"status":"ready","detail":""}
```

`status` is `ready` or `degraded`; `detail` carries the component's own
explanation when it is not ready. Only `ready` counts as healthy. The file
is written to `<name>.tmp` and renamed into place, so a reader never sees a
half-written beat (rename replaces the target on Windows as well as POSIX).

km passes `--heartbeat-file` and `--heartbeat-secs` to each component and
deletes any stale file before spawning it, so a previous run's beat cannot
satisfy this run's gate. A component given no heartbeat file does not beat —
that is how a standalone run behaves.

## What each component verifies before beating

- **`pod-runner`** — calls its own `/capacity` over mTLS, using the
  credentials from `vk-prconfig.yaml`: byte for byte the request the
  virtual-kubelet's node controller makes.
- **`control-plane`** — probes etcd `/health` (on the URL envtest allocated,
  which only it knows), the API server's `/readyz` through the authenticated
  client, and kube-controller-manager `/healthz`.
- **`virtual-kubelet`** — its node is still registered with the API server,
  and its pod-status sync loop completed a round recently (the loop stamps
  that after the pod-runner answers).

Each component grades itself on the work it actually does, so the health
judgement lives where the knowledge is. The pod-runner's probe covers the
serving path only — it does not exercise pod execution.

## Two levels

km supervises the three processes it spawns: control-plane, pod-runner,
virtual-kubelet. It never probes the control-plane's children: km allocates
every port the stack binds, but watching the inner three is the
control-plane's business.

The control-plane supervises its own three (etcd, kube-apiserver,
kube-controller-manager) with the same pattern, and collapses their verdicts
into its single heartbeat. So a failure inside the control plane reaches km
as `degraded` with the failing child named, or as silence, and takes two
hops to become a teardown: the control-plane gives up first, then km
notices.

`km status` shows each component's beat age; it also lists the
control-plane's children, but only for display — no probe, no verdict.

## What `km status` reports

Two independent signals, and neither is recomputed by `status`:

- **Live** — km's own pid is alive. An instance exists.
- **Ready** — the age of `<workdir>/data/km/ready`, a marker km stamps on
  every supervision round in which the whole stack answered. Only its
  modification time carries meaning; nothing reads its contents.

km already decides readiness once per round, across every component. Stamping
the marker records that decision, and `status` ages it with the same
`intervalSecs` km supervises on. Re-deriving readiness in `status` would be a
second opinion, free to disagree with the one actually driving the stack.

The marker is removed at startup before anything is spawned, and again during
teardown, so a stopped instance cannot read as ready and a new run cannot
inherit the last one's answer. km stamps it as soon as the stack comes up
rather than waiting for the first supervision round, so a `status` in that
first interval says `ready` rather than "not reported yet".

| Exit | Meaning     | Condition                                      |
|------|-------------|------------------------------------------------|
| `0`  | ready       | km live, marker fresher than `2 × interval`    |
| `1`  | live        | km live, marker stale or never written         |
| `2`  | not running | no live km pid                                 |

The three cases exist because a caller polling for readiness needs to tell
"still coming up" from "never started": one code for both makes a script
either wait on something that will never be ready, or give up on something
that is about to be.

## Timing

Ports are allocated for the whole stack from `--portstart`, seven of them:

    +0 control-plane API server      +4 virtual-kubelet
    +1 kube-controller-manager       +5 etcd (client)
    +2 pod-runner                    +6 etcd (peer)
    +3 pod-runner metrics

etcd's two come last so that adding them left every existing assignment where
it was. They are pinned explicitly — `Etcd.URL` for the client port, and the
`listen-peer-urls` flag for the peer, since envtest keeps that one in a
private field it overwrites on every start — so no port the stack binds is
left unaccounted for, and none of them move between runs. An instance created
before km managed these simply has no `etcd`/`etcdPeer` entries: they default
to 0, which already means "pick a free one at startup", so km chooses them and
still knows what they are.

The knobs live in `kmsysconfig.yaml` and are forwarded to every component, so
the whole stack shares one cadence:

```yaml
heartbeat:
  intervalSecs: 15   # how often a component re-verifies itself and beats
  maxMissed: 4       # consecutive misses tolerated while running
  startupSecs: 120   # how long a component gets to become ready
```

- **Stale after** = `2 × interval` — the age at which a poll counts as a
  miss. The extra interval absorbs lateness, which grows with load: a
  component beats only after its own self-check round trip, so a merely late
  beat does not trip the counter. When km's own poll is itself late, the
  limit stretches to the gap since its previous poll: a beat written since
  km last looked is not counted against the component for km's starvation.
- **Detection** = `interval × maxMissed` — consecutive misses before a
  running component is declared dead.
- **Startup budget** = `startupSecs` — how long a component gets to report
  ready. Also what the control-plane allows etcd, the API server and
  kube-controller-manager to all answer their health probes in, and its
  envtest start timeout.

Until its first successful beat a component retries every 250ms, so readiness
is not rounded up to a whole interval.

## Why startup has its own budget

Starting is not steady state. The control-plane brings up etcd, an API server
and a controller manager; the virtual-kubelet registers a node and waits for a
status sync. None of that resembles the work a running component does between
two beats, so a budget sized from the running one does not fit it.

It used to be sized that way — `interval × maxMissed` — and worse, the
control-plane applied that *miss count* to its own startup rounds, which are
250ms apart rather than a full interval. That gave its children under a
second: enough that a kube-controller-manager needing ~800ms to answer
`/healthz` on a busy machine would take the whole control plane down with it,
intermittently and for no real reason.

`startupSecs` is therefore generous rather than tuned, because **overrunning
it is not how a failed start is normally caught**. Every component's pid is
watched from the moment it is spawned — km's process watcher every 250ms, the
control-plane's own children by exit status and pid — and a component that
dies ends the wait at once, naming what died. Nothing restarts a failed
component, so there is nothing to gain by waiting for it. In practice a
crashed start is reported in a few seconds; the timeout only bounds the
narrower case of a component that stays alive and never becomes ready.

One floor to respect when tuning: `startupSecs` must exceed the slowest
component's real startup time. The virtual-kubelet is the slowest — it waits
5s for its node to register before it can serve.

## Two cadences

Liveness and readiness are different questions, and they are monitored at
different rates because they cost different amounts to answer.

**Liveness — is it still there?** A pid check, a non-blocking wait on a child
handle, or a loopback dial at a child's port. Cheap, immediate and definitive:
neither an exited process nor a closed listener recovers on its own, so no
further evidence is needed. These run on a **tick** — 250ms in km, 1s in the
control-plane — and a failure acts at once without spending the miss budget.

Not every liveness check is equally cheap, so they are not all on the tick.
Measured on macOS: km's pid check is 73µs (sysinfo reads the process table
directly), and a loopback dial is 52µs — both nothing. But gopsutil, which
the control-plane must use, reads a process's name and state by invoking `ps`
on some platforms: 4.4ms and two subprocesses per child, 85x the price. So the
control-plane dials its children's ports every tick and checks their pids once
per round, where milliseconds do not matter.

A closed port also catches what a pid cannot. A child shutting down gracefully
closes its listener **first** and can stay alive for a long time afterwards —
which is exactly what a plain `kill` on kube-apiserver looks like: the port
refuses immediately while the process lingers, so a pid check correctly reports
it alive and only the port check sees the truth.

**Readiness — is it actually serving?** Only the component can answer that,
and only by doing real work: the pod-runner calls its own `/capacity`, the
control-plane probes three children, the virtual-kubelet checks its node
registration and sync. That costs a round trip, can fail transiently, and is
therefore judged **on the heartbeat interval**, over a budget of consecutive
misses.

An interval probe can be definitive too: a *refused* connection from a child
that has already served means the same thing as a failed dial, so it is acted
on at once rather than over four rounds. An unhealthy answer (a 503) or a slow
one is **not** definitive and does go through the budget — that is the
distinction the budget exists for. Only a refused connection counts, never a
timeout, so a busy child is never mistaken for a departed one.

Same split at both levels, and the cheap check carries most of the load:
crashes are the common failure and they are caught in milliseconds, while the
expensive check is reserved for the case a pid cannot detect — alive but no
longer working.

In km the two run in different places. A dedicated watcher thread checks
every 250ms that each component km has spawned is still a process, and the
heartbeat monitor runs its rounds on the interval. An exited child fails the
stack outright rather than going through the miss budget: there is nothing to
wait for and no beat that could redeem it.

The watcher starts **before the first spawn**, which is the point of it being
separate. It reads the PID files km writes immediately after each spawn, so
its list grows as the stack comes up and a component not started yet is simply
absent. Without it, a component dying while km is still gating on a later one
would go unnoticed until that gate timed out — the whole startup budget spent
waiting, and the failure then reported against the wrong component. Killing
the control-plane while km is starting the virtual-kubelet is now caught in
about 200ms, and named correctly.

The control-plane runs the same two cadences over its own three: a process
tick every second, and the probe round on the interval. It owns
kube-controller-manager's process, so it waits on it in a goroutine and gets
the **exit status** into the log (`kube-controller-manager exited: signal:
killed`) instead of only the probe's `connection refused` — and that wait
reaps the child, which previously stayed a zombie until the control-plane
exited. etcd and the API server belong to envtest, so they can only be
checked by pid; that check guards against both a zombie (which still
"exists") and a recycled pid (by comparing the process name).

## Portability

The mechanism is filesystem and loopback only, so it behaves the same on
Linux, macOS and Windows. Two places needed explicit care:

- **Publishing a beat** writes `<file>.tmp` and renames it into place. Rename
  replaces the target on Windows as well as POSIX, so a reader never sees a
  half-written file on any platform.
- **Recognising a refused connection** is platform-split. Go's `syscall`
  package does not export `WSAECONNREFUSED`, and on Windows
  `syscall.ECONNREFUSED` is a placeholder that no real error matches — so
  comparing against it alone would silently disable every definitive-loss
  check on Windows. `connectionRefused` therefore has a Windows build that
  also matches Winsock's code.

Process names differ too: Windows reports `etcd.exe`, so recorded names are
normalised before comparison. Both Go binaries cross-compile clean for
`windows/amd64` and `linux/amd64`, and km type-checks for
`x86_64-pc-windows-msvc`.

## Failure taxonomy

- **No file** — never came up. Fails the readiness gate after the startup
  budget.
- **Fresh, `degraded`** — the component reports its own trouble and `detail`
  says what. Counts as a miss, but against a limit five times the configured
  `maxMissed`: publishing the beat at all proves the component is running and
  able to report, so it is given room to recover rather than torn down for
  being slow.
- **Stale, process alive** — wedged: alive but no longer serving. Counts as a
  miss against `maxMissed`, with no extra tolerance — silence says nothing
  about whether the component is still working. A stale `degraded` beat is
  silence too: only a fresh one proves the component is still reporting.

The two kinds of miss keep separate counts, so degraded beats never spend the
silent budget: `maxMissed` silent misses in a row are needed to fail on
silence, however many degraded beats came first. A fresh `ready` beat clears
both counts; a fresh `degraded` beat ends a run of silence; silence leaves a
run of `degraded` beats standing, since it proves nothing about recovery.
- **Process exited** — caught by the process check, typically within a
  second, and acted on at once: no staleness to wait for, no miss budget to
  spend.

A missed beat is logged as a warning with its running count
(`Missed heartbeat (2/4): last beat 37s ago`). On the last one km tears the
stack down in reverse start order and exits 1. The control-plane does the same
for its children and exits 1, which km then observes.

Note that the component km names is the one that failed a check, not
necessarily the one at fault. A wedged pod-runner usually surfaces as a
*virtual-kubelet* failure, because the virtual-kubelet self-reports its broken
dependency (`pod status sync last completed a round over 30s ago`) a round
before the pod-runner's own beat has aged out. The reason travels with the
failure, so the log and the console both name the real cause.

## Why km exited

Every exit path prints one block on stdout, where the startup banner was, and
logs the same thing:

```
Tabsdata K- (K minus) stopping: SIGINT (Ctrl+C) received

Tabsdata K- (K minus) stopping: virtual-kubelet failed readiness
  did not report ready within 60s (…/data/virtual-kubelet/heartbeat.json)

Tabsdata K- (K minus) stopping: pod-runner failed liveness
  process 26311 is gone

Tabsdata K- (K minus) stopping: virtual-kubelet failed liveness
  missed 20 consecutive heartbeats — reported 'degraded': pod status sync last
  completed a round over 3s ago
```

The headline says which component and which check — readiness or liveness —
and the second line says what was observed. Exactly one reason is ever
printed: the readiness gate, the process watcher, the heartbeat monitor and
the signal thread can all reach an exit, so the first to arrive claims it and
the others stand down. Without that, a gate that kills the child it gave up on
would be followed by the watcher reporting that kill as a separate liveness
failure.

## Shutdown

Every probe and heartbeat loop is wired to its process's termination
notifier, not to a timeout: the pod-runner selects on the shutdown watch
channel, the virtual-kubelet on its signal context, the control-plane on its
signal channel (cancelling a separate probe context so an in-flight probe
aborts without killing kube-controller-manager before its graceful stop),
and km's monitor stands down when the signal thread raises its shutdown
flag, so a normal stop never races into a spurious failure teardown.

Teardown is also bounded so a wedged child cannot stretch it. envtest's stop
timeout is set explicitly rather than inherited (its 20s default was the
largest single delay in an unclean shutdown: a killed etcd leaves the API
server unable to exit, and every second of that wait is pure latency because
the control-plane force-kills whatever envtest could not stop anyway). Killing
etcd under a running stack now takes about seven seconds end to end — under a
second for the control-plane to notice, five for it to stop the rest, and a
fraction for km to see it go.

One caveat: the pod-runner's shutdown grace is 90s because it drains pods on
SIGTERM. That is a ceiling a healthy pod-runner cuts short by exiting, but a
wedged one cannot, so a monitor-detected pod-runner failure pays the full 90s
before km escalates to SIGKILL.
