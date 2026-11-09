//go:build !windows

/*
 * Copyright 2026 Tabsdata Inc.
 */

package main

import (
	"errors"
	"os/exec"
	"syscall"
	"testing"
	"time"
)

func TestAHangupDoesNotEndTheProcess(t *testing.T) {
	ignoreHangup()
	if err := syscall.Kill(syscall.Getpid(), syscall.SIGHUP); err != nil {
		t.Fatalf("send SIGHUP: %v", err)
	}
	time.Sleep(200 * time.Millisecond)
}

func TestAChildStillEndsOnAHangup(t *testing.T) {
	ignoreHangup()
	err := exec.Command("sh", "-c", "kill -HUP $$; sleep 10").Run()
	var exited *exec.ExitError
	if !errors.As(err, &exited) {
		t.Fatalf("the child outlived its SIGHUP: %v", err)
	}
	status, ok := exited.Sys().(syscall.WaitStatus)
	if !ok || !status.Signaled() || status.Signal() != syscall.SIGHUP {
		t.Fatalf("the child did not end by SIGHUP: %v", err)
	}
}
