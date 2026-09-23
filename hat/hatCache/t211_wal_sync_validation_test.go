package hatCache

import (
	"errors"
	"testing"

	"hatrie_cache/hat/hatJournal"
)

func TestT211JournalSyncOptionsDefaultAndRejectInvalidMode(t *testing.T) {
	options, err := hatJournal.ValidateOptions(hatJournal.Options{GroupCommitMaxBatch: 1})
	if err != nil {
		t.Fatalf("ValidateOptions() error = %v", err)
	}
	if options.SyncMode != hatJournal.SyncModePeriodic {
		t.Fatalf("default sync mode = %v, want periodic", options.SyncMode)
	}

	if _, err := hatJournal.ValidateOptions(hatJournal.Options{
		GroupCommitMaxBatch: 1,
		SyncMode:            hatJournal.SyncMode(99),
	}); !errors.Is(err, hatJournal.ErrSpaceSyncPolicyInvalid) {
		t.Fatalf("invalid sync mode error = %v, want ErrSpaceSyncPolicyInvalid", err)
	}
}
