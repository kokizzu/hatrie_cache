package main

import (
	"fmt"
	"io"
	"strings"
	"testing"
)

func TestParseConfigJournalCursorSecretDefaultsOffAndIsRedacted(t *testing.T) {
	defaultConfig, err := parseConfig(nil, io.Discard)
	if err != nil {
		t.Fatalf("parseConfig(default) error = %v", err)
	}
	if defaultConfig.monitoringJournalCursorSecret != "" {
		t.Fatalf("default journal cursor secret = %q, want disabled", defaultConfig.monitoringJournalCursorSecret)
	}

	const secret = "0123456789abcdef"
	configured, err := parseConfig([]string{"-monitoring-journal-cursor-secret", secret}, io.Discard)
	if err != nil {
		t.Fatalf("parseConfig(configured) error = %v", err)
	}
	if configured.monitoringJournalCursorSecret != secret {
		t.Fatalf("journal cursor secret = %q, want configured secret", configured.monitoringJournalCursorSecret)
	}
	if strings.Contains(fmt.Sprint(redactedConfig(configured)), secret) {
		t.Fatal("redacted config contains the journal cursor secret")
	}
	if _, err := parseConfig([]string{"-monitoring-journal-cursor-secret", "too-short"}, io.Discard); err == nil {
		t.Fatal("parseConfig accepted an undersized journal cursor secret")
	}
}
