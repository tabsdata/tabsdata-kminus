//go:build !windows

/*
 * Copyright 2026 Tabsdata Inc.
 */

package main

import (
	"os"
	"syscall"
)

// ownGroup leaves a child's process attributes as they are: on Unix a
// SIGTERM reaches it by pid.
func ownGroup() *syscall.SysProcAttr {
	return nil
}

// requestStop asks a child to shut down.
func requestStop(p *os.Process) error {
	return p.Signal(syscall.SIGTERM)
}

// requestStopPid asks the process at pid to shut down.
func requestStopPid(pid int) {
	if p, err := os.FindProcess(pid); err == nil {
		_ = p.Signal(syscall.SIGTERM)
	}
}
