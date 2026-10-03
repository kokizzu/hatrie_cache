package hatJournal

import (
	"errors"
	"testing"
	"time"
)

func TestT018SyncModeValidation(t *testing.T) {
	defaultOptions, err := ValidateOptions(Options{GroupCommitMaxBatch: DefaultGroupCommitMaxBatch})
	if err != nil {
		t.Fatal(err)
	}
	if defaultOptions.SyncMode != SyncModeDurable || defaultOptions.SyncInterval != 0 {
		t.Fatalf("default sync options = %q/%s, want durable/0", defaultOptions.SyncMode, defaultOptions.SyncInterval)
	}

	periodic, err := ValidateOptions(Options{
		GroupCommitMaxBatch: DefaultGroupCommitMaxBatch,
		SyncMode:            SyncModePeriodic,
	})
	if err != nil {
		t.Fatal(err)
	}
	if periodic.SyncInterval != DefaultSyncInterval {
		t.Fatalf("periodic default interval = %s, want %s", periodic.SyncInterval, DefaultSyncInterval)
	}

	for _, options := range []Options{
		{GroupCommitMaxBatch: DefaultGroupCommitMaxBatch, SyncMode: SyncModePeriodic, SyncInterval: MinSyncInterval - 1},
		{GroupCommitMaxBatch: DefaultGroupCommitMaxBatch, SyncMode: SyncModePeriodic, SyncInterval: MaxSyncInterval + time.Nanosecond},
		{GroupCommitMaxBatch: DefaultGroupCommitMaxBatch, SyncInterval: -time.Nanosecond},
	} {
		if _, err := ValidateOptions(options); err == nil {
			t.Fatalf("ValidateOptions(%#v) error = nil", options)
		}
	}
	if _, err := ParseSyncMode("bad"); !errors.Is(err, ErrSyncModeInvalid) {
		t.Fatalf("ParseSyncMode(bad) error = %v, want ErrSyncModeInvalid", err)
	}
}
