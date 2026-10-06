package hatJournal

import (
	"errors"
	"testing"
	"time"
)

func TestParseSyncMode(t *testing.T) {
	tests := []struct {
		input string
		want  SyncMode
	}{
		{input: "", want: SyncModeImmediate},
		{input: "sync", want: SyncModeImmediate},
		{input: "immediate", want: SyncModeImmediate},
		{input: "periodic", want: SyncModePeriodic},
		{input: "disabled", want: SyncModeDisabled},
		{input: "none", want: SyncModeDisabled},
	}
	for _, test := range tests {
		got, err := ParseSyncMode(test.input)
		if err != nil {
			t.Fatalf("ParseSyncMode(%q) error = %v", test.input, err)
		}
		if got != test.want {
			t.Fatalf("ParseSyncMode(%q) = %q, want %q", test.input, got, test.want)
		}
	}
	if _, err := ParseSyncMode("eventual"); err == nil {
		t.Fatal("ParseSyncMode(eventual) error = nil")
	}
}

func TestValidateOptionsNormalizesSyncPolicy(t *testing.T) {
	defaultOptions, err := ValidateOptions(Options{GroupCommitMaxBatch: 1})
	if err != nil {
		t.Fatal(err)
	}
	if defaultOptions.SyncMode != SyncModeImmediate || defaultOptions.SyncInterval != 0 {
		t.Fatalf("default sync options = %#v", defaultOptions)
	}

	periodic, err := ValidateOptions(Options{
		GroupCommitMaxBatch: 1,
		SyncMode:            SyncModePeriodic,
	})
	if err != nil {
		t.Fatal(err)
	}
	if periodic.SyncInterval != DefaultSyncInterval {
		t.Fatalf("periodic default interval = %s, want %s", periodic.SyncInterval, DefaultSyncInterval)
	}

	if _, err := ValidateOptions(Options{GroupCommitMaxBatch: 1, SyncMode: SyncModePeriodic, SyncInterval: -time.Second}); err == nil {
		t.Fatal("negative periodic interval error = nil")
	}
}

func TestSyncPolicyTracksPeriodicDueTime(t *testing.T) {
	now := time.Unix(100, 0)
	periodic, err := NewSyncPolicy(SyncModePeriodic, 50*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if !periodic.ShouldSync(now) {
		t.Fatal("periodic policy did not require its initial sync")
	}
	periodic.MarkSynced(now)
	if periodic.ShouldSync(now.Add(49 * time.Millisecond)) {
		t.Fatal("periodic policy synced before interval")
	}
	if !periodic.ShouldSync(now.Add(50 * time.Millisecond)) {
		t.Fatal("periodic policy did not sync at interval")
	}

	disabled, err := NewSyncPolicy(SyncModeDisabled, 0)
	if err != nil {
		t.Fatal(err)
	}
	if disabled.ShouldSync(now) {
		t.Fatal("disabled policy requested a sync")
	}
}

func TestSyncPolicyRejectsInvalidMode(t *testing.T) {
	if _, err := NewSyncPolicy(SyncMode("unknown"), time.Second); err == nil {
		t.Fatal("unknown mode error = nil")
	} else if !errors.Is(err, ErrInvalidSyncMode) {
		t.Fatalf("unknown mode error = %v, want ErrInvalidSyncMode", err)
	}
}
