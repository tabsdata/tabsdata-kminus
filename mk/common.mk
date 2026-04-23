#
# Copyright 2026 Tabsdata Inc.
#

BUILD_DIR ?= $(CURDIR)/../target
DIST_DIR := $(BUILD_DIR)/dist

# Detect host platform
HOST_OS := $(shell uname -s | tr '[:upper:]' '[:lower:]')
HOST_ARCH_RAW := $(shell uname -m)
ifeq ($(HOST_ARCH_RAW),x86_64)
  HOST_ARCH := amd64
else ifeq ($(HOST_ARCH_RAW),aarch64)
  HOST_ARCH := arm64
else
  HOST_ARCH := $(HOST_ARCH_RAW)
endif
HOST_PLATFORM := $(HOST_OS)-$(HOST_ARCH)

# Output directory: target/<platform>/
BIN_DIR := $(BUILD_DIR)/$(HOST_PLATFORM)

# Helper: file extension for platform (windows -> .exe)
ext = $(if $(findstring windows,$(1)),.exe)
HOST_EXT := $(call ext,$(HOST_PLATFORM))

# Release mode — set RELEASE=1 for optimized builds.
# Default is debug (faster compile, slower runtime).
RELEASE ?=
ifdef RELEASE
  GO_BUILD_FLAGS := -ldflags="-s -w" -trimpath
  CARGO_PROFILE := --release
  CARGO_OUT_DIR := release
else
  GO_BUILD_FLAGS :=
  CARGO_PROFILE :=
  CARGO_OUT_DIR := debug
endif

# Python environment check — verifies required packages are available.
# Called by targets that use Python (itest, build scripts).
ROOT_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))..
REQUIREMENTS := $(ROOT_DIR)/requirements.txt
ITEST_COMMON_DIR := $(ROOT_DIR)/mk

.PHONY: check-python-env
check-python-env:
	@python3 -c "import pytest, psutil, yaml" 2>/dev/null || \
	  (echo "ERROR: Python environment missing required packages." && \
	   echo "Activate your virtual env and run: pip install -r $(REQUIREMENTS)" && \
	   exit 1)

# Wrapper around pytest that treats "no tests collected" (exit code 5)
# as success.  Use this in subprojects' `itest` targets so a project
# with no integration tests is a true no-op rather than a failure.
#
# Usage:  $(PYTEST_NOOP) [path] [extra-args...]
PYTEST_NOOP := $(ROOT_DIR)/mk/pytest-noop.sh
