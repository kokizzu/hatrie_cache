package main

import (
	"io"
	"os"
	"path/filepath"
	"testing"
)

func TestParseConfigMaintenanceReadOnlyFlagAndFile(t *testing.T) {
	cfg, err := parseConfig([]string{"--maintenance-read-only"}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.maintenanceReadOnly {
		t.Fatal("maintenance read-only flag was not enabled")
	}

	path := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(path, []byte(`{"maintenance_read_only":true}`), 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err = parseConfig([]string{"--config", path}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.maintenanceReadOnly {
		t.Fatal("maintenance read-only config-file option was not enabled")
	}

	redacted := redactedConfig(cfg)
	if value, ok := redacted["maintenance_read_only"].(bool); !ok || !value {
		t.Fatalf("redacted maintenance read-only config = %#v, want true", redacted["maintenance_read_only"])
	}
}
