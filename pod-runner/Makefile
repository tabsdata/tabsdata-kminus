#
# Copyright 2026 Tabsdata Inc.
#

PROJECT := pod-runner

include ../mk/common.mk

SOURCES := $(shell find src -name '*.rs' 2>/dev/null) Cargo.toml $(wildcard Cargo.lock)

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

# --- Assembly ---

assembly: build

# --- Clean ---

clean:
	cargo clean
	rm -f $(BIN_DIR)/$(PROJECT)$(HOST_EXT)

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
