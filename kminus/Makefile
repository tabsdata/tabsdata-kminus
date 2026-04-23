#
# Copyright 2026 Tabsdata Inc.
#

PROJECT := km

include ../mk/common.mk

SOURCES := $(shell find src -name '*.rs' 2>/dev/null) Cargo.toml $(wildcard Cargo.lock)

DIST_NAME := tabsdata-kminus
DIST_STAGING := $(DIST_DIR)/$(DIST_NAME)
COMPONENTS := control-plane virtual-kubelet pod-runner $(PROJECT)
K8S_DEPS := kube-apiserver kube-controller-manager etcd kubectl
ALL_BINARIES := $(COMPONENTS) $(K8S_DEPS)

.PHONY: build test clean clean-all assembly itest

# --- Build (current platform) ---

build: $(BIN_DIR)/$(PROJECT)$(HOST_EXT)

$(BIN_DIR)/$(PROJECT)$(HOST_EXT): $(SOURCES)
	@mkdir -p $(dir $@)
	cargo build $(CARGO_PROFILE)
	@cp target/$(CARGO_OUT_DIR)/$(PROJECT)$(HOST_EXT) $@

# --- Unit tests ---

test:
	cargo test $(CARGO_PROFILE)

# --- Assembly (build + distribution archive) ---

ifeq ($(findstring windows,$(HOST_PLATFORM)),windows)
DIST_ARCHIVE := $(DIST_DIR)/$(DIST_NAME)-$(HOST_PLATFORM).zip
$(DIST_ARCHIVE): $(foreach c,$(ALL_BINARIES),$(BIN_DIR)/$(c).exe)
	@rm -rf $(DIST_STAGING)
	@mkdir -p $(DIST_STAGING)/bin
	@cp $(foreach c,$(ALL_BINARIES),$(BIN_DIR)/$(c).exe) $(DIST_STAGING)/bin/
	cd $(DIST_DIR) && zip -r $(abspath $@) $(DIST_NAME)
	@rm -rf $(DIST_STAGING)
else
DIST_ARCHIVE := $(DIST_DIR)/$(DIST_NAME)-$(HOST_PLATFORM).tar.gz
$(DIST_ARCHIVE): $(foreach c,$(ALL_BINARIES),$(BIN_DIR)/$(c))
	@rm -rf $(DIST_STAGING)
	@mkdir -p $(DIST_STAGING)/bin
	@cp $(foreach c,$(ALL_BINARIES),$(BIN_DIR)/$(c)) $(DIST_STAGING)/bin/
	tar -czf $@ -C $(DIST_DIR) $(DIST_NAME)
	@rm -rf $(DIST_STAGING)
endif

assembly: build $(DIST_ARCHIVE)

# --- Clean ---

clean:
	cargo clean
	rm -f $(BIN_DIR)/$(PROJECT)$(HOST_EXT)
	rm -rf $(DIST_DIR)

ITEST_DIR := $(CURDIR)/tests

# Integration tests. No-op if the itest dir is missing or contains no
# pytest-collectable tests.
itest: check-python-env build
	@if [ -d $(ITEST_DIR) ]; then \
	  cd $(ITEST_DIR) && BIN_DIR=$(BIN_DIR) PYTHONPATH=$(ITEST_COMMON_DIR) $(PYTEST_NOOP) -v; \
	else \
	  echo "(no integration test dir — skipping)"; \
	fi

clean-all: clean
