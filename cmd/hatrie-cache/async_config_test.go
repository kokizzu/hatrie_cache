package main

import (
	"bytes"
	"strings"
	"testing"

	hatriecache "hatrie_cache/hat/hatCache"
)

func TestParseConfigMonitoringAsyncCommandsDefaultsOff(t *testing.T) {
	cfg, err := parseConfig(nil, &bytes.Buffer{})
	if err != nil {
		t.Fatalf("parseConfig(defaults) error = %v", err)
	}
	if cfg.monitoringAsyncCommands {
		t.Fatal("monitoring async commands defaulted on")
	}
	if cfg.monitoringAsyncCommandStatusCapacity != hatriecache.DefaultMonitoringAsyncCommandStatusCapacity {
		t.Fatalf("async command status capacity = %d, want %d", cfg.monitoringAsyncCommandStatusCapacity, hatriecache.DefaultMonitoringAsyncCommandStatusCapacity)
	}
}

func TestParseConfigMonitoringAsyncCommandsAcceptsCompleteConfiguration(t *testing.T) {
	cfg, err := parseConfig([]string{
		"-monitoring-async-commands",
		"-journal-path", t.TempDir() + "/commands.journal",
		"-journal-idempotency-capacity", "8",
		"-journal-group-commit-max-batch", "2",
		"-monitoring-async-command-status-capacity", "17",
	}, &bytes.Buffer{})
	if err != nil {
		t.Fatalf("parseConfig(async) error = %v", err)
	}
	if !cfg.monitoringAsyncCommands {
		t.Fatal("monitoring async commands = false, want true")
	}
	if cfg.monitoringAsyncCommandStatusCapacity != 17 {
		t.Fatalf("async command status capacity = %d, want 17", cfg.monitoringAsyncCommandStatusCapacity)
	}
	if cfg.journalIdempotencyCapacity != 8 || cfg.journalGroupCommitMaxBatch != 2 {
		t.Fatalf("async journal prerequisites = capacity %d/batch %d, want 8/2", cfg.journalIdempotencyCapacity, cfg.journalGroupCommitMaxBatch)
	}
}

func TestParseConfigMonitoringAsyncCommandsRejectsIncompleteConfiguration(t *testing.T) {
	base := []string{
		"-monitoring-async-commands",
		"-journal-path", t.TempDir() + "/commands.journal",
		"-journal-idempotency-capacity", "8",
		"-journal-group-commit-max-batch", "2",
	}
	tests := []struct {
		name string
		args []string
		want string
	}{
		{
			name: "journal path",
			args: append([]string{base[0]}, base[3:]...),
			want: "require -journal-path",
		},
		{
			name: "idempotency",
			args: func() []string {
				args := append([]string{}, base...)
				args[4] = "0"
				return args
			}(),
			want: "require -journal-idempotency-capacity",
		},
		{
			name: "group commit",
			args: func() []string {
				args := append([]string{}, base...)
				args[6] = "1"
				return args
			}(),
			want: "require -journal-group-commit-max-batch",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := parseConfig(tt.args, &bytes.Buffer{})
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("parseConfig() error = %v, want substring %q", err, tt.want)
			}
		})
	}
}
