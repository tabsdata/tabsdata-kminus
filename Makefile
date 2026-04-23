#
# Copyright 2026 Tabsdata Inc.
#

BUILD_DIR := $(CURDIR)/target

MAKE_ARGS := BUILD_DIR=$(BUILD_DIR) $(if $(RELEASE),RELEASE=1)

.PHONY: all build test release clean clean-all assembly itest

all: build

# Dependency order:
#   Tier 1: control-plane, virtual-kubelet (independent, parallel with -j)
#   Tier 2: pod-runner (depends on tier 1)
#   Tier 3: kminus distribution (depends on tier 2)

# --- Build (current platform) ---

build: build-kminus

build-control-plane:
	@$(MAKE) -C control-plane build $(MAKE_ARGS)

build-virtual-kubelet:
	@$(MAKE) -C virtual-kubelet build $(MAKE_ARGS)

build-pod-runner: build-control-plane build-virtual-kubelet
	@$(MAKE) -C pod-runner build $(MAKE_ARGS)

build-kminus: build-pod-runner
	@$(MAKE) -C kminus build $(MAKE_ARGS)

# --- Release (optimized build) ---

release:
	@$(MAKE) build RELEASE=1

# --- Assembly (build + distribution archive) ---

assembly:
	@$(MAKE) assembly-kminus RELEASE=1

assembly-control-plane:
	@$(MAKE) -C control-plane assembly $(MAKE_ARGS)

assembly-virtual-kubelet:
	@$(MAKE) -C virtual-kubelet assembly $(MAKE_ARGS)

assembly-pod-runner: assembly-control-plane assembly-virtual-kubelet
	@$(MAKE) -C pod-runner assembly $(MAKE_ARGS)

assembly-kminus: assembly-pod-runner
	@$(MAKE) -C kminus assembly $(MAKE_ARGS)

# --- Unit Tests ---
# Per-subproject unit tests. Each subproject's `test` target is a no-op
# when it has no test sources (e.g. Go components without *_test.go files).

test: test-control-plane test-virtual-kubelet test-pod-runner test-kminus

test-control-plane:
	@$(MAKE) -C control-plane test $(MAKE_ARGS)

test-virtual-kubelet:
	@$(MAKE) -C virtual-kubelet test $(MAKE_ARGS)

test-pod-runner:
	@$(MAKE) -C pod-runner test $(MAKE_ARGS)

test-kminus:
	@$(MAKE) -C kminus test $(MAKE_ARGS)

# --- Integration Tests ---

itest: build itest-control-plane itest-virtual-kubelet itest-pod-runner itest-kminus

itest-control-plane:
	@$(MAKE) -C control-plane itest $(MAKE_ARGS)

itest-virtual-kubelet:
	@$(MAKE) -C virtual-kubelet itest $(MAKE_ARGS)

itest-pod-runner:
	@$(MAKE) -C pod-runner itest $(MAKE_ARGS)

itest-kminus:
	@$(MAKE) -C kminus itest $(MAKE_ARGS)

# --- Clean ---

clean:
	@$(MAKE) -C control-plane clean $(MAKE_ARGS)
	@$(MAKE) -C virtual-kubelet clean $(MAKE_ARGS)
	@$(MAKE) -C pod-runner clean $(MAKE_ARGS)
	@$(MAKE) -C kminus clean $(MAKE_ARGS)
	rm -rf $(BUILD_DIR)

# --- Clean All (includes caches) ---

clean-all:
	@$(MAKE) -C control-plane clean-all $(MAKE_ARGS)
	@$(MAKE) -C virtual-kubelet clean-all $(MAKE_ARGS)
	@$(MAKE) -C pod-runner clean-all $(MAKE_ARGS)
	@$(MAKE) -C kminus clean-all $(MAKE_ARGS)
	rm -rf $(BUILD_DIR)
