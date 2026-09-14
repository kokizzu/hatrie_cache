package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	hatriecache "hatrie_cache"
)

func TestParseConfigAcceptsAuditSuccessSampleRate(t *testing.T) {
	cfg, err := parseConfig([]string{"-audit-success-sample-rate", "0.25"}, &bytes.Buffer{})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.auditSuccessSampleRate != 0.25 {
		t.Fatalf("audit success sample rate = %v, want 0.25", cfg.auditSuccessSampleRate)
	}
}

func TestParseConfigRejectsInvalidAuditSuccessSampleRate(t *testing.T) {
	for _, value := range []string{"-0.1", "1.1"} {
		if _, err := parseConfig([]string{"-audit-success-sample-rate", value}, &bytes.Buffer{}); err == nil {
			t.Fatalf("audit success sample rate %s accepted", value)
		}
	}
}

func TestOpenAuditLogIfConfiguredUsesSuccessSampling(t *testing.T) {
	path := filepath.Join(t.TempDir(), "audit.jsonl")
	logger, err := openAuditLogIfConfigured(path, 0.1)
	if err != nil {
		t.Fatal(err)
	}
	defer logger.Close()
	for index := 0; index < 1000; index++ {
		if err := logger.Log(hatriecache.AuditEvent{Action: "success", OK: true}); err != nil {
			t.Fatal(err)
		}
	}
	if logger.SampledSuccessEvents() < 800 || logger.SampledSuccessEvents() > 950 {
		t.Fatalf("sampled successes = %d, want most successes sampled", logger.SampledSuccessEvents())
	}
}

func TestMonitoringWrapperPassesAuditSuccessSampleRate(t *testing.T) {
	makefile, err := os.ReadFile(filepath.Join("..", "..", "Makefile"))
	if err != nil {
		t.Fatal(err)
	}
	monitoringScript, err := os.ReadFile(filepath.Join("..", "..", "scripts", "monitoring-server.sh"))
	if err != nil {
		t.Fatal(err)
	}
	for _, token := range []string{"AUDIT_SUCCESS_SAMPLE_RATE", "audit-success-sample-rate"} {
		if !strings.Contains(string(makefile), token) && !strings.Contains(string(monitoringScript), token) {
			t.Fatalf("monitoring configuration missing %q", token)
		}
	}
}
