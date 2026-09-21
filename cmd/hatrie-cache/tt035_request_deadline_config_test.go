package main

import (
	"bytes"
	"strings"
	"testing"
	"time"
)

func TestTT035ParseConfigRequestTimeoutDisabledByDefault(t *testing.T) {
	cfg, err := parseConfig(nil, &bytes.Buffer{})
	if err != nil {
		t.Fatalf("parse default config: %v", err)
	}
	if cfg.commandRequestTimeout != 0 {
		t.Fatalf("default command request timeout = %s, want disabled", cfg.commandRequestTimeout)
	}
}

func TestTT035ParseConfigRequestTimeout(t *testing.T) {
	const want = 250 * time.Millisecond
	cfg, err := parseConfig([]string{"-command-request-timeout", want.String()}, &bytes.Buffer{})
	if err != nil {
		t.Fatalf("parse command request timeout: %v", err)
	}
	if cfg.commandRequestTimeout != want {
		t.Fatalf("command request timeout = %s, want %s", cfg.commandRequestTimeout, want)
	}
}

func TestTT035ParseConfigRejectsNegativeRequestTimeout(t *testing.T) {
	_, err := parseConfig([]string{"-command-request-timeout", "-1s"}, &bytes.Buffer{})
	if err == nil {
		t.Fatal("parse negative command request timeout succeeded")
	}
	if !strings.Contains(err.Error(), "command request timeout must be non-negative") {
		t.Fatalf("parse negative command request timeout error = %q", err)
	}
}
