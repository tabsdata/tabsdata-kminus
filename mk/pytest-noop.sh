#!/usr/bin/env bash
#
# Copyright 2026 Tabsdata Inc.
#
# Run pytest, treating "no tests collected" (exit code 5) as success.
# All arguments are forwarded to pytest verbatim.
#
# This lets a subproject's `make itest` target be a true no-op when
# the project has no integration tests, instead of failing.

set -u

pytest "$@"
rc=$?

if [ "$rc" -eq 5 ]; then
    echo "(no integration tests collected — skipping)"
    exit 0
fi

exit "$rc"
