<!--
  Copyright 2026 Tabsdata Inc.
-->

<div align="center">
    <img src="assets/images/tabsdata-logo-for-light-theme.svg#gh-light-mode-only" alt="Tabsdata" width="500">
    <img src="assets/images/tabsdata-logo-for-dark-theme.svg#gh-dark-mode-only" alt="Tabsdata" width="500">
</div>

# Developing KMinus

This guide covers what is specific to building and testing **kminus** from this repository. Read it
together with [Developing Tabsdata](https://github.com/tabsdata/tabsdata/blob/main/DEVELOPING.md),
which this guide builds on.

## Contents

- [1. Prepare your machine](#1-prepare-your-machine)
- [2. Find your way around the repository](#2-find-your-way-around-the-repository)
- [3. Build](#3-build)
- [4. Run the tests](#4-run-the-tests)
- [5. Find your way around the tasks](#5-find-your-way-around-the-tasks)
- [6. The Kubernetes binaries](#6-the-kubernetes-binaries)
- [7. Good to know](#7-good-to-know)

## 1. Prepare your machine

Follow steps 1 to 5 of
[Developing Tabsdata](https://github.com/tabsdata/tabsdata/blob/main/DEVELOPING.md#contents): install
the system software, clone the three repositories side by side, create the build environment,
install **tdd** and prepare the toolchain. This repository is the one cloned into `tabsdata-km`.

> [!IMPORTANT]
> On macOS and Linux, raise the open-file limit to 65535, system-wide or in your shell profile, as
> [Open files, on macOS and Linux](https://github.com/tabsdata/tabsdata/blob/main/DEVELOPING.md#open-files-on-macos-and-linux)
> describes. With a lower limit, **kminus** pods fail with "Too many open files", and
> **tdkserver** refuses to start a local instance. Windows needs no change: it handles processes
> and files differently, and sets no such limit.

Run every task in this guide from `tabsdata-km`, with the build environment active. The
**kminus** build reads helper scripts from the `tabsdata-os` folder next to it, so keep both
checkouts in place.

Building tabsdata also builds **kminus**: the **build** task of `tabsdata-os` runs the
**assembly** task of this repository and refreshes `target/dist`. Build here only when you work
on **kminus** itself.

## 2. Find your way around the repository

```text
├── Makefile.toml             cargo-make entry point (extends Makeroot / Maketask)
├── Makeroot.toml             root environment (env files, scripts, hooks)
├── Maketask.toml             top-level tasks (build / test / clean / assembly / ...)
├── assets/                   images for the documentation
├── docs/                     design documents
├── examples/                 runnable manifests
├── make/
│   └── make-km/
│       ├── fragments/        task / hook / plugin fragments; standalone/ is km's own
│       │                     make root; shared/ and the exported tasks/ are loaded by os/ee
│       ├── libraries/        duckscript helpers (log.ds, term.py)
│       ├── resources/        banners, fonts, images
│       ├── scripts/          build-time scripts (e.g. fetch_k8s_deps.py)
│       ├── settings/         *.env defaults loaded by Makeroot
│       └── tests/            tests of the build-time scripts
├── mods/
│   ├── control-plane/        Go — Kubernetes control plane
│   ├── virtual-kubelet/      Go — Virtual Kubelet provider
│   ├── pod-runner/           Rust — pod execution runtime
│   └── kminus/               Rust — distribution binary (km)
├── ltest/                    Python load-test harness
└── test/                     shared integration-test helpers (itest_common)
```

The tasks are defined in `Maketask.toml` and, per module, in `mods/<module>/Maketask.toml`, with
shared fragments under `make/make-km/`.

Each module builds into its own `target/` folder, next to its sources. The **assembly** task
collects every binary into the root `target/dist/`:

```text
mods/control-plane/target/
  ├── control-plane                Go binary
  ├── kube-apiserver               ┐
  ├── kube-controller-manager      │ Kubernetes binaries copied from
  ├── etcd                         │ mods/control-plane/cache/
  └── kubectl                      ┘
mods/virtual-kubelet/target/
  └── virtual-kubelet              Go binary
mods/pod-runner/target/<profile>/
  └── pod-runner                   Rust binary
mods/kminus/target/<profile>/
  └── km                           Rust binary

target/
  └── dist/                        every binary above, collected by assembly
```

## 3. Build

Build every module:

```bash
cargo make build
```

Build every module and collect the binaries into `target/dist/`:

```bash
cargo make assembly
```

The modules build in this order, and those without dependencies on each other build in parallel:

1. The Go modules, **control-plane** and **virtual-kubelet**, independent of each other
2. The Rust **pod-runner**
3. The Rust **kminus**, which brings everything together
4. The **assembly** task, which collects the module binaries and the cached Kubernetes binaries
   into `target/dist/`

Builds are always native to the current platform; cross-compilation is not supported.

### Build profiles

By default, the build uses the `dev` profile. Choose another profile with the `profile`
environment variable, as in tabsdata. The `assembly` profile builds optimized binaries.

macOS and Linux:

```bash
profile=assembly cargo make assembly
```

Windows:

```powershell
$env:profile = "assembly"; cargo make assembly; Remove-Item Env:profile
```

The `assembly` profile applies:

- Go builds get `-ldflags=-s -w -trimpath`.
- Rust builds use the Cargo `assembly` profile and the flags in `.cargo/rustflags-<os>.cfg`.

### Build a single module

Each module exposes the same task names for itself alone. Run them from the module's folder:

```bash
cd mods/control-plane
```

```bash
cargo make build
```

## 4. Run the tests

Run the unit tests of every module:

```bash
cargo make test
```

Run the integration tests of every module, after building:

```bash
cargo make itest
```

The integration tests of **kminus** also run as part of the Python integration tests of tabsdata
(`cargo make itest_py` in `tabsdata-os`).

## 5. Find your way around the tasks

| Task | What it does |
|---|---|
| `cargo make assembly` | Builds every module<br>Collects the binaries into `target/dist/` |
| `cargo make build` | Builds every module |
| `cargo make clean` | Removes the `target/` folder of every module and the root `target/` |
| `cargo make clean --all` | Does the same as `clean`<br>Also removes the control-plane cache of Kubernetes binaries |
| `cargo make itest` | Builds, then runs the integration tests |
| `cargo make lock` | Regenerates the Cargo lock files of the Rust modules |
| `cargo make permit` | Checks that Python, Rust and Go are available |
| `cargo make test` | Runs the unit tests |

List every task:

```bash
cargo make --list-all-steps
```

## 6. The Kubernetes binaries

The control-plane needs four Kubernetes binaries at runtime, **kube-apiserver**,
**kube-controller-manager**, **etcd** and **kubectl**. The **build** task fetches them and caches
them, so later builds neither download nor compile them again. After a build, they are copied
into `mods/control-plane/target/`, next to the `control-plane` binary.

| Binary | Obtained from |
|---|---|
| etcd | Downloaded with [setup-envtest](https://pkg.go.dev/sigs.k8s.io/controller-runtime/tools/setup-envtest) |
| kube-apiserver | Downloaded with **setup-envtest** |
| kube-controller-manager | Built from source<br>The Kubernetes repository is cloned into `cache/k8s-source/kubernetes/` |
| kubectl | Downloaded with **setup-envtest** |

The Kubernetes version is set by `K8S_VERSION` in `Makeroot.toml`. The **setup-envtest** tool
resolves it to the latest patch release, and the source clone uses the same tag. The first build
installs **setup-envtest** itself.

The cache lives in `mods/control-plane/cache/`, and holds only the binaries for the current
platform:

```text
mods/control-plane/cache/
├── k8s-source/                   Kubernetes sources (cloned once, shared)
│   └── kubernetes/
└── <os>-<arch>/                  host-platform binaries (e.g. darwin-arm64)
    ├── kube-apiserver
    ├── kube-controller-manager
    ├── etcd
    └── kubectl
```

The **clean** task keeps the cache, and the `cargo make clean --all` command removes it too. The cache is ignored
by git and safe to delete at any time; the next build fills it again.

## 7. Good to know

- The Go modules always build with Go 1.26.11. Any installed version of **Go** works: the build
  sets `GOTOOLCHAIN`, and **Go** downloads that exact version once.
- The control-plane and the virtual-kubelet build without cgo (`CGO_ENABLED=0`), so both are
  statically linked and need no C toolchain. The control-plane uses gopsutil v4, which reaches
  the macOS system libraries without cgo. Don't go back to gopsutil v3: its cgo dependency
  `go-m1cpu` crashes before `main()` on some Apple Silicon machines.
- The Rust modules build with `--locked`, so a stale `Cargo.lock` fails the build. Regenerate the
  lock files with `cargo make lock`.
