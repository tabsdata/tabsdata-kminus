/*
 * Copyright 2026 Tabsdata Inc.
 */

package main

import (
	"encoding/csv"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

var hexToken = regexp.MustCompile(`^[0-9a-f]{64}$`)

// seededToken must be deterministic and shaped like the random tokens
// (64 lowercase hex chars = 32 bytes).
func TestSeededTokenDeterministicAndShaped(t *testing.T) {
	a := seededToken("alice")
	b := seededToken("alice")
	if a != b {
		t.Fatalf("seededToken not deterministic: %q != %q", a, b)
	}
	if !hexToken.MatchString(a) {
		t.Fatalf("seededToken has unexpected shape: %q", a)
	}
	if seededToken("alice") == seededToken("bob") {
		t.Fatalf("seededToken collided for different seeds")
	}
}

// fixedAdminToken is off unless the env var is exactly "true" (case-insensitive),
// and when on it matches the username-seeded token.
func TestFixedAdminToken(t *testing.T) {
	t.Setenv(fixedAuthTokenEnv, "")
	if got := fixedAdminToken(); got != "" {
		t.Fatalf("expected no fixed token when env unset, got %q", got)
	}
	t.Setenv(fixedAuthTokenEnv, "false")
	if got := fixedAdminToken(); got != "" {
		t.Fatalf("expected no fixed token when env=false, got %q", got)
	}
	t.Setenv(fixedAuthTokenEnv, "TRUE")
	got := fixedAdminToken()
	if got == "" {
		t.Fatalf("expected a fixed token when env=TRUE")
	}
	if want := seededToken(currentUsername()); got != want {
		t.Fatalf("fixed token %q != seededToken(username) %q", got, want)
	}
}

func readTokensCSV(t *testing.T, path string) map[string]string {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open tokens.csv: %v", err)
	}
	defer f.Close()
	r := csv.NewReader(f)
	r.FieldsPerRecord = -1
	records, err := r.ReadAll()
	if err != nil {
		t.Fatalf("parse tokens.csv: %v", err)
	}
	m := make(map[string]string)
	for _, rec := range records {
		if len(rec) >= 2 {
			m[rec[1]] = rec[0]
		}
	}
	return m
}

// With the dev override on, a freshly created tokens.csv gets the deterministic
// admin token while the other users get random ones.
func TestLoadOrCreateTokensFixedAdminOnCreate(t *testing.T) {
	dir := t.TempDir()
	t.Setenv(fixedAuthTokenEnv, "true")

	tokens, err := loadOrCreateTokens(dir)
	if err != nil {
		t.Fatalf("loadOrCreateTokens: %v", err)
	}
	want := seededToken(currentUsername())
	if tokens[adminUserName] != want {
		t.Fatalf("admin token %q != fixed %q", tokens[adminUserName], want)
	}
	if !hexToken.MatchString(tokens[vkUserName]) || tokens[vkUserName] == want {
		t.Fatalf("vk token should be random hex, got %q", tokens[vkUserName])
	}
	// The persisted file must match the returned map (apiserver auth == kubeconfig).
	onDisk := readTokensCSV(t, filepath.Join(dir, tokensFile))
	if onDisk[adminUserName] != want {
		t.Fatalf("tokens.csv admin %q != fixed %q", onDisk[adminUserName], want)
	}
}

// With the override on over an EXISTING tokens.csv, the admin token is rewritten
// to the fixed value while the other users' tokens are preserved.
func TestLoadOrCreateTokensFixedAdminOverridesExisting(t *testing.T) {
	dir := t.TempDir()

	// First run without the override: fully random tokens.
	first, err := loadOrCreateTokens(dir)
	if err != nil {
		t.Fatalf("loadOrCreateTokens (first): %v", err)
	}
	vkBefore := first[vkUserName]

	// Second run with the override: admin becomes fixed, vk unchanged.
	t.Setenv(fixedAuthTokenEnv, "true")
	second, err := loadOrCreateTokens(dir)
	if err != nil {
		t.Fatalf("loadOrCreateTokens (second): %v", err)
	}
	want := seededToken(currentUsername())
	if second[adminUserName] != want {
		t.Fatalf("admin token not overridden: %q != %q", second[adminUserName], want)
	}
	if second[vkUserName] != vkBefore {
		t.Fatalf("vk token changed: %q != %q", second[vkUserName], vkBefore)
	}
	onDisk := readTokensCSV(t, filepath.Join(dir, tokensFile))
	if onDisk[adminUserName] != want {
		t.Fatalf("tokens.csv not rewritten with fixed admin: %q", onDisk[adminUserName])
	}
	if onDisk[vkUserName] != vkBefore {
		t.Fatalf("tokens.csv vk token changed: %q != %q", onDisk[vkUserName], vkBefore)
	}
}

// End-to-end: with the override on, the deterministic admin token flows all
// the way into kubeconfig.yaml (the client credential the user consumes).
func TestFixedAdminTokenReachesKubeconfig(t *testing.T) {
	dir := t.TempDir()
	t.Setenv(fixedAuthTokenEnv, "true")

	tokens, err := loadOrCreateTokens(dir)
	if err != nil {
		t.Fatalf("loadOrCreateTokens: %v", err)
	}
	kubeconfig := filepath.Join(dir, "kubeconfig.yaml")
	if err := writeKubeconfig(kubeconfig, "https://127.0.0.1:6443", adminUserName, tokens[adminUserName]); err != nil {
		t.Fatalf("writeKubeconfig: %v", err)
	}
	data, err := os.ReadFile(kubeconfig)
	if err != nil {
		t.Fatalf("read kubeconfig: %v", err)
	}
	want := "token: " + seededToken(currentUsername())
	if !strings.Contains(string(data), want) {
		t.Fatalf("kubeconfig.yaml missing fixed admin token %q:\n%s", want, data)
	}
}

// Without the override, tokens stay random and the file is created once and
// reused verbatim across calls.
func TestLoadOrCreateTokensRandomStable(t *testing.T) {
	dir := t.TempDir()
	t.Setenv(fixedAuthTokenEnv, "")

	first, err := loadOrCreateTokens(dir)
	if err != nil {
		t.Fatalf("loadOrCreateTokens (first): %v", err)
	}
	second, err := loadOrCreateTokens(dir)
	if err != nil {
		t.Fatalf("loadOrCreateTokens (second): %v", err)
	}
	for _, u := range tokenUsers {
		if !hexToken.MatchString(first[u.name]) {
			t.Fatalf("%s token not random hex: %q", u.name, first[u.name])
		}
		if first[u.name] != second[u.name] {
			t.Fatalf("%s token not stable across calls: %q != %q", u.name, first[u.name], second[u.name])
		}
	}
}
