/*
 * Copyright 2026 Tabsdata Inc.
 */

package main

import (
	"encoding/json"
	"errors"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
)

func testLogger() *logrus.Logger {
	logger := logrus.New()
	logger.SetOutput(io.Discard)
	return logger
}

// km treats an unparseable beat as a miss, so the file must always be valid
// JSON — including when the detail carries quotes or Windows path separators.
func TestWriteHeartbeatEmitsParseableBeat(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "nested", "heartbeat.json")

	writeHeartbeat(testLogger(), file, statusDegraded, `node "tdknode" gone: c:\x`)

	body, err := os.ReadFile(file)
	if err != nil {
		t.Fatalf("heartbeat not written: %v", err)
	}
	var beat struct {
		TS     int64  `json:"ts"`
		Status string `json:"status"`
		Detail string `json:"detail"`
	}
	if err := json.Unmarshal(body, &beat); err != nil {
		t.Fatalf("heartbeat is not valid JSON: %v (%s)", err, body)
	}
	if beat.Status != statusDegraded {
		t.Errorf("status = %q, want %q", beat.Status, statusDegraded)
	}
	if beat.Detail != `node "tdknode" gone: c:\x` {
		t.Errorf("detail = %q, want the message verbatim", beat.Detail)
	}
	if beat.TS == 0 {
		t.Error("ts not stamped")
	}
}

// The beat is published by rename so km never reads a half-written file.
func TestWriteHeartbeatLeavesNoStagingFile(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "heartbeat.json")

	writeHeartbeat(testLogger(), file, statusReady, "")

	if _, err := os.Stat(file + heartbeatTmpSuffix); !os.IsNotExist(err) {
		t.Error("staging file must be renamed into place, not left behind")
	}
}

// An empty path is how a standalone run disables the heartbeat.
func TestWriteHeartbeatSkipsEmptyPath(t *testing.T) {
	dir := t.TempDir()
	writeHeartbeat(testLogger(), "", statusReady, "")

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Errorf("wrote %d files for an empty heartbeat path", len(entries))
	}
}

// Before the first sync round the age must be large enough to hold the
// kubelet un-ready: a zero stamp must not read as "just synced".
func TestSinceLastSyncIsUnboundedBeforeTheFirstRound(t *testing.T) {
	lastSyncUnixNano.Store(0)
	if got := sinceLastSync(); got != time.Duration(math.MaxInt64) {
		t.Errorf("sinceLastSync() = %v before any round, want the max duration", got)
	}
}

func TestMarkSyncCompletedMakesTheRoundRecent(t *testing.T) {
	lastSyncUnixNano.Store(0)
	markSyncCompleted()
	if got := sinceLastSync(); got > time.Second {
		t.Errorf("sinceLastSync() = %v just after a round, want it near zero", got)
	}
}

// A missing node is the failure this whole mechanism exists to catch, so it
// must win over every other consideration.
func TestBeatDetailReportsAMissingNodeFirst(t *testing.T) {
	detail := beatDetail("tdknode", errors.New("not found"), 0, 3*time.Second)
	if !strings.Contains(detail, "tdknode") || !strings.Contains(detail, "not found") {
		t.Errorf("detail = %q, want the node name and the lookup error", detail)
	}
}

func TestBeatDetailReportsAStalledSyncLoop(t *testing.T) {
	syncStale := 3 * time.Second
	if detail := beatDetail("tdknode", nil, 4*time.Second, syncStale); detail == "" {
		t.Error("a sync round older than the limit must not be ready")
	}
	// Never synced: sinceLastSync is unbounded before the first round.
	if detail := beatDetail("tdknode", nil, time.Duration(math.MaxInt64), syncStale); detail == "" {
		t.Error("a kubelet that has never synced must not be ready")
	}
}

func TestBeatDetailIsEmptyWhenRegisteredAndSyncing(t *testing.T) {
	syncStale := 3 * time.Second
	if detail := beatDetail("tdknode", nil, time.Second, syncStale); detail != "" {
		t.Errorf("detail = %q, want ready", detail)
	}
	// Exactly at the limit is still ready; only older is not.
	if detail := beatDetail("tdknode", nil, syncStale, syncStale); detail != "" {
		t.Errorf("detail = %q at the limit, want ready", detail)
	}
}

func TestHeartbeatWaitIsFastUntilTheFirstHealthyBeat(t *testing.T) {
	interval := 15 * time.Second
	if got := heartbeatWait(false, interval); got != heartbeatStartupRetry {
		t.Errorf("heartbeatWait(not yet healthy) = %v, want %v", got, heartbeatStartupRetry)
	}
	if got := heartbeatWait(true, interval); got != interval {
		t.Errorf("heartbeatWait(healthy) = %v, want %v", got, interval)
	}
}
