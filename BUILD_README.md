<!--
  Copyright 2026 Tabsdata Inc.
-->

# kMinus — Build System

## Project Layout

```
├── Makefile                 Root build orchestrator
├── requirements.txt         Python dependencies (build scripts + tests)
├── mk/
│   └── common.mk           Shared build variables (platforms, paths, helpers)
├── control-plane/           Go — Kubernetes control plane
├── virtual-kubelet/         Go — Virtual Kubelet provider
├── pod-runner/              Rust — Pod execution runtime
└── kminus/                  Rust — Distribution binary (brings all components together)
```

Build output goes to `target/<platform>/`:

```
target/
├── darwin-arm64/            Platform-specific binaries (built natively)
│   ├── control-plane
│   ├── virtual-kubelet
│   ├── pod-runner
│   ├── kpr                 (pod-runner REST client CLI)
│   ├── km
│   ├── kube-apiserver
│   ├── kube-controller-manager
│   ├── etcd
│   └── kubectl
└── dist/                    Distribution archives (from assembly)
    └── tabsdata-kminus-darwin-arm64.tar.gz
```

## Control Plane Build

The `control-plane` build fetches/cached some prebuilt binaries. This is explained in the 
`control-plane/BUILD_README.md` document.

## Build Targets

| Target          | Description                                                                  |
|-----------------|------------------------------------------------------------------------------|
| `make build`    | Compile all subprojects (debug, fast compile)                                |
| `make test`     | Run all unit tests across the four subprojects                               |
| `make release`  | Compile all subprojects (optimized: Go strips symbols, Rust release profile) |
| `make assembly` | Optimized build + create distribution archive                                |
| `make itest`    | Build + run integration tests (no-op if a subproject has no itest dir)       |
| `make clean`    | Remove all build artifacts (preserves caches)                                |
| `make clean-all`| Remove all build artifacts **and** subproject caches                         |

All targets are **incremental** — unchanged sources are not recompiled.

Builds are done **natively** on each platform (no cross-compilation).
The Makefiles and `mk/` scripts will be made fully multiplatform in a future
iteration (Windows/cmd support, platform-specific toolchain detection, etc.).

Use `make -j` to parallelize independent subprojects within each tier.

## Build Order

Subprojects are built respecting their dependency order:

1. **control-plane** and **virtual-kubelet** — independent, built in parallel with `-j`
2. **pod-runner** — depends on tier 1
3. **kminus** — depends on tier 2; assembly also packages all component binaries
   into a distribution archive

## Prerequisites

- **Go** 1.23+ (brew install go)
- **Rust** 1.94+ (edition 2024)
- **Python 3.12+** with a virtual environment

## Python Environment

Build scripts and integration tests require Python with specific packages.
A `requirements.txt` at the project root lists all dependencies.

Setup (using conda):

```
conda activate km
pip install -r requirements.txt
```

The `make itest` target will fail with a clear error if the required Python
packages are not available. The virtual environment must be activated before
running `make itest`. The `make build` target does not require the virtual
environment.

## Integration Tests

Each subproject has its own integration tests under `src/itest/`. Test
output (logs, data dirs) goes to `<subproject>/target/testrun.<timestamp>/`,
not the root `target/` directory. This keeps test artifacts per subproject
and separated from build output.
