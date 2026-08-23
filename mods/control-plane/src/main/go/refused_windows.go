//go:build windows

/*
 * Copyright 2026 Tabsdata Inc.
 */

package main

import (
	"errors"
	"syscall"
)

// wsaeconnrefused is Winsock's connection-refused code.
//
// It has to be spelled out: Go's syscall package does not export
// WSAECONNREFUSED, and on Windows `syscall.ECONNREFUSED` is a placeholder
// (APPLICATION_ERROR + n) that no real error ever matches — Errno.Is maps only
// the permission/exist/not-exist families. Comparing against ECONNREFUSED
// alone would therefore make every refused connection look like some other
// failure, and the definitive-loss path would never fire on Windows.
const wsaeconnrefused = syscall.Errno(10061)

// connectionRefused reports whether err means nothing was listening.
func connectionRefused(err error) bool {
	return errors.Is(err, wsaeconnrefused) || errors.Is(err, syscall.ECONNREFUSED)
}
