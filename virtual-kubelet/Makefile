#
# Copyright 2026 Tabsdata Inc.
#

PROJECT := virtual-kubelet

include ../mk/common.mk

GO_SRC_DIR := $(CURDIR)/src/main/go
SOURCES := $(shell find $(GO_SRC_DIR) -name '*.go' -not -path '*/vendor/*')
GO_DEPS := $(SOURCES) $(GO_SRC_DIR)/go.mod $(wildcard $(GO_SRC_DIR)/go.sum)

.PHONY: build test clean clean-all assembly itest

# --- Build (current platform) ---

build: $(BIN_DIR)/$(PROJECT)$(HOST_EXT)

$(BIN_DIR)/$(PROJECT)$(HOST_EXT): $(GO_DEPS)
	@mkdir -p $(dir $@)
	cd $(GO_SRC_DIR) && go build $(GO_BUILD_FLAGS) -o $@ .

# --- Unit tests (no-op if no _test.go files) ---

test:
	cd $(GO_SRC_DIR) && go test ./...

# --- Assembly ---

assembly: build

ITEST_DIR := $(CURDIR)/src/itest
ITEST_TARGET := bin/

# --- Integration Tests ---

# No-op if the itest dir or target subdir is missing.
itest: check-python-env build
	@if [ -d $(ITEST_DIR)/$(ITEST_TARGET) ]; then \
	  cd $(ITEST_DIR) && BIN_DIR=$(BIN_DIR) PYTHONPATH=$(ITEST_COMMON_DIR) $(PYTEST_NOOP) $(ITEST_TARGET) -v; \
	else \
	  echo "(no integration test dir — skipping)"; \
	fi

# --- Clean ---

clean:
	rm -f $(BIN_DIR)/$(PROJECT)$(HOST_EXT)

clean-all: clean
