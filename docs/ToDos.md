<!--
  Copyright 2026 Tabsdata Inc.
-->

# ToDos

## KMinus

It should check/raise the soft ulimit ``ulimit -n 65536``

## Control Plane

### Version / About
- A `-version` flag should print KMinus build version and exit.
- The version should be set at build time via `-ldflags`.

## Virtual Kubelet

### Version / About
- Same as control-plane: `-version` flag with build-time version.

## Pod Runner

### Disk Utilization

Per pod Disk utilization. The idea would be to have a `DiskUsageMonitor`
that has collects the utilization of the pod root directory. The implementation
could be something like:

A `DirSize` scanner computes the size of a directory recursively using the
following rules:
  * 
  * Starts from a base dir.
  * symlinks are ignored.
  * collects the size of all files.
  * for sub-dirs
    * if the sub-dir has an `_ignore` file, the sub-dir is not scanned, and it does not count towards usage.
    * if the sub-dir has a `_usage` file, the sub-dir is not scanned, and the value in the `_usage` file is
      taken as the size of the sub-dir.
  * it writes a `_usage` file with the current utilization.

A `DirCheck` read the `_usage` file of a directory, and if it exceeds a preconfigured capacity it reports it
back to take action on it (warning, kill, etc).

## Build

### Multiplatform Makefiles
- The Makefiles and `mk/` scripts currently work on macOS and Linux
  only. Windows support (cmd/PowerShell) is a future iteration.
