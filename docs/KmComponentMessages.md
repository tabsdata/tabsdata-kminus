<!--
  Copyright 2026 Tabsdata Inc.
-->

<div align="center">
    <img src="../assets/images/tabsdata-logo-for-light-theme.svg#gh-light-mode-only" alt="Tabsdata" width="500">
    <img src="../assets/images/tabsdata-logo-for-dark-theme.svg#gh-dark-mode-only" alt="Tabsdata" width="500">
</div>

# KMinus: Component Messages

How a component tells the operator something at startup or shutdown, and how
km collects and prints it.

This is a **reporting** channel. It decides nothing. Readiness and liveness
stay entirely with the heartbeat files described in
[KmReadinessLiveness.md](KmReadinessLiveness.md): a component with nothing to
say is not unhealthy, and a message that never arrives never holds up a start.
Every failure on this path — an unwritable file, a malformed line, a missing
directory — is swallowed on both sides.

## Why it exists

Each component already writes a log nobody opens until something has gone
wrong. A few facts deserve to be in front of whoever ran `km start`:

- the etcd backend size and the quota it is measured against
- `capacity.pods`, whose only other symptom is a wave of `ProviderFailed`
  pods that read as pod faults rather than a configured ceiling

Both are cheap to print once and expensive to discover later.

## The directory

```
<workdir>/messages/
├── start-control-plane.json
├── start-pod-runner.json
├── stop-control-plane.json
└── stop-pod-runner.json
```

km **empties and recreates** `messages/` at startup, before anything is
spawned, so a previous run's messages can never be read as this run's. It is
not cleared at shutdown: the files are left for a post-mortem.

## The file

One file per component per phase, named `<phase><binary>.json` where `<phase>`
is `start-` or `stop-`. Two phases means the startup set is not reprinted as
the stack comes down.

Contents are **newline-delimited JSON** — one object per line — so a component
can append as it goes without holding or re-reading what it already said:

```json
{"time":"2026-08-22T14:58:29.326Z","binary":"control-plane","level":"INFO","message":"etcd backend: 336.0KiB on disk, ..."}
{"time":"2026-08-22T14:58:29.400Z","binary":"control-plane","level":"WARN","message":"..."}
```

| Field     | Meaning                                                        |
|-----------|----------------------------------------------------------------|
| `time`    | RFC 3339, **UTC**, **fixed millisecond precision** — see below |
| `binary`  | The reporting component, shown in the printed line             |
| `level`   | `INFO`, `WARN` or `ERROR`; anything else is shown as written   |
| `message` | The text, already formatted for a human                        |

**The timestamp format is load-bearing.** km orders lines by comparing `time`
as a plain string, which is chronological only if every writer uses the same
precision and the same zone. That choice keeps km free of a date library for
what is purely a display ordering — but it means a writer emitting a different
width, or a local offset, silently sorts wrong. Both current writers pin the
format and have a test asserting it:

- Go: `2006-01-02T15:04:05.000Z07:00` formatted from `time.Now().UTC()`
- Rust: `%Y-%m-%dT%H:%M:%S%.3fZ` formatted from `chrono::Utc::now()`

A line that fails to parse is skipped; the rest of the file is still read, so
one bad writer loses its own message and nobody else's. Missing fields default
to empty rather than discarding the line.

## What km does

1. **At startup**, before spawning anything: empty and recreate `messages/`.
2. **Once the stack is ready**: read every `start-*.json`, sort by `time` then
   `binary`, print to the terminal and log each line at its own level.
3. **On the way down**, after the components have exited so anything written
   on their way out is already there: read every `stop-*.json` and print it
   the same way. The startup set is printed here too if it never was — a stack
   that failed before it was ready never reached step 2, and those messages are
   usually the ones that explain why.

Each phase prints at most once.

Terminal:

```
  Component messages (startup):
    INFO   control-plane    etcd backend: 336.0KiB on disk, 336.0KiB live, 4.0GiB quota, fsync on, defrag every 60m
    INFO   pod-runner       node capacity: 250 pods, 100 cpu, 100GB memory (prsysconfig.yaml 'capacity'); pods scheduled beyond the pod ceiling fail as ProviderFailed
```

Log (`km.log`), one record per message, keeping the reporter and the
component's own timestamp:

```
INFO km::system::messages: etcd backend: 336.0KiB on disk, ... binary=control-plane time=2026-08-22T14:58:29.326Z phase="startup"
```

The component's timestamp is not shown on the terminal. It exists to order the
lines, and they all land within a second or two of each other; the exact value
stays in the file and on the logged record.

## Adding a message from a component

Write a line into `<workdir>/<phase><binary>.json` under `messages/`, opened
append-only, and ignore any error. The two existing writers are:

| Component     | Writer                                          |
|---------------|--------------------------------------------------|
| control-plane | `writeMessage` in `mods/control-plane/src/main/go/main.go` |
| pod-runner    | `mods/pod-runner/src/server/messages.rs`         |

Reserve `WARN` and `ERROR` for something the operator can act on. The etcd
summary is `INFO` normally and `WARN` only when the live data is near the
quota, because that is the point at which a setting needs changing.

## What this is not

- **Not readiness.** km never waits for a message. See
  [KmReadinessLiveness.md](KmReadinessLiveness.md).
- **Not a live view.** The messages are a startup and a shutdown snapshot.
  `km status` reads its own live sources — for etcd, the separate
  `data/control-plane/etcd-storage.json`, refreshed every probe round.
- **Not a log.** Components keep logging everything they always did; this is
  the short list worth surfacing.
