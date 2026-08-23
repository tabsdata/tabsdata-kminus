//go:build !windows

/*
 * Copyright 2026 Tabsdata Inc.
 */

package main

import (
	"errors"
	"syscall"
)

// connectionRefused reports whether err means nothing was listening.
func connectionRefused(err error) bool {
	return errors.Is(err, syscall.ECONNREFUSED)
}
