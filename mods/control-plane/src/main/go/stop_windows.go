//go:build windows

/*
 * Copyright 2026 Tabsdata Inc.
 */

package main

import (
	"os"
	"syscall"
)

var generateConsoleCtrlEvent = syscall.NewLazyDLL("kernel32.dll").NewProc("GenerateConsoleCtrlEvent")

// ownGroup gives a child its own process group, which is what a
// CTRL_BREAK can be aimed at. It shares this process's console, the one
// km created hidden for it, so the event can reach it.
func ownGroup() *syscall.SysProcAttr {
	return &syscall.SysProcAttr{CreationFlags: syscall.CREATE_NEW_PROCESS_GROUP}
}

// requestStop asks a child started with ownGroup to shut down. Windows
// has no SIGTERM: the ask is a CTRL_BREAK raised for the child's group,
// which Go programs receive as os.Interrupt.
func requestStop(p *os.Process) error {
	if ok, _, err := generateConsoleCtrlEvent.Call(syscall.CTRL_BREAK_EVENT, uintptr(p.Pid)); ok == 0 {
		return err
	}
	return nil
}

// requestStopPid is a no-op: the only processes stopped by pid are etcd and
// the API server, which envtest starts in this process's own group. A stop
// from km already reached them with the CTRL_BREAK that reached this
// process; any other stop ends them by the force stage.
func requestStopPid(int) {}
