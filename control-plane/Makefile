#
# Copyright 2026 Tabsdata Inc.
#

PROJECT := control-plane

include ../mk/common.mk

GO_SRC_DIR := $(CURDIR)/src/main/go
SOURCES := $(shell find $(GO_SRC_DIR) -name '*.go' -not -path '*/vendor/*')
GO_DEPS := $(SOURCES) $(GO_SRC_DIR)/go.mod $(wildcard $(GO_SRC_DIR)/go.sum)

K8S_VERSION := 1.29
K8S_DEPS := kube-apiserver kube-controller-manager etcd kubectl
FETCH_SCRIPT := $(CURDIR)/mk/fetch_k8s_deps.py
LOCAL_CACHE_DIR := $(CURDIR)/cache

ITEST_DIR := $(CURDIR)/src/itest
ITEST_TARGET := bin/

.PHONY: build test clean clean-all assembly itest

# --- Build (current platform) ---

build: $(BIN_DIR)/$(PROJECT)$(HOST_EXT)
	@python3 $(FETCH_SCRIPT) $(HOST_PLATFORM) $(LOCAL_CACHE_DIR) $(K8S_VERSION)
	@for dep in $(K8S_DEPS); do \
	  if [ -f "$(LOCAL_CACHE_DIR)/$(HOST_PLATFORM)/$$dep$(HOST_EXT)" ]; then \
	    cp "$(LOCAL_CACHE_DIR)/$(HOST_PLATFORM)/$$dep$(HOST_EXT)" "$(BIN_DIR)/"; \
	  fi; \
	done

$(BIN_DIR)/$(PROJECT)$(HOST_EXT): $(GO_DEPS)
	@mkdir -p $(dir $@)
	cd $(GO_SRC_DIR) && go build $(GO_BUILD_FLAGS) -o $@ .

# --- Unit tests (no-op if no _test.go files) ---

test:
	cd $(GO_SRC_DIR) && go test ./...

# --- Assembly ---

assembly: build

# --- Integration Tests ---

# No-op if the itest dir or target subdir is missing.
itest: check-python-env build
	@if [ -d $(ITEST_DIR)/$(ITEST_TARGET) ]; then \
	  cd $(ITEST_DIR) && BIN_DIR=$(BIN_DIR) PYTHONPATH=$(ITEST_COMMON_DIR) $(PYTEST_NOOP) $(ITEST_TARGET) -v; \
	else \
	  echo "(no integration test dir — skipping)"; \
	fi

# --- Clean (preserves cache) ---

clean:
	rm -f $(BIN_DIR)/$(PROJECT)$(HOST_EXT)
	rm -f $(foreach d,$(K8S_DEPS),$(BIN_DIR)/$(d)$(HOST_EXT))

# --- Clean All (includes cache) ---

clean-all: clean
	rm -rf $(LOCAL_CACHE_DIR)
