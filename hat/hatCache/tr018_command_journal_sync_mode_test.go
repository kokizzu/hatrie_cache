package hatCache

import (
	"errors"
	"path/filepath"
	"testing"
	"time"
)

func TestCommandJournalSyncModeDefaultIsImmediate(t *testing.T) {
	journal, err := OpenCommandJournalWithOptions(filepath.Join(t.TempDir(), "commands.journal"), CommandJournalOptions{
		GroupCommitMaxBatch: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()
	var syncs int
	journal.syncHook = func() error {
		syncs++
		return nil
	}
	journal.mu.Lock()
	if err := journal.syncLocked(); err != nil {
		journal.mu.Unlock()
		t.Fatal(err)
	}
	if err := journal.syncLocked(); err != nil {
		journal.mu.Unlock()
		t.Fatal(err)
	}
	journal.mu.Unlock()
	if syncs != 2 {
		t.Fatalf("immediate sync count = %d, want 2", syncs)
	}
}

func TestCommandJournalSyncModePeriodicAndDisabled(t *testing.T) {
	periodic, err := OpenCommandJournalWithOptions(filepath.Join(t.TempDir(), "periodic.journal"), CommandJournalOptions{
		GroupCommitMaxBatch: 1,
		SyncMode:            CommandJournalSyncModePeriodic,
		SyncInterval:        time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer periodic.Close()
	var periodicSyncs int
	periodic.syncHook = func() error {
		periodicSyncs++
		return nil
	}
	periodic.mu.Lock()
	if err := periodic.syncLocked(); err != nil {
		periodic.mu.Unlock()
		t.Fatal(err)
	}
	if err := periodic.syncLocked(); err != nil {
		periodic.mu.Unlock()
		t.Fatal(err)
	}
	periodic.syncPolicy.MarkSynced(time.Now().Add(-2 * time.Hour))
	if err := periodic.syncLocked(); err != nil {
		periodic.mu.Unlock()
		t.Fatal(err)
	}
	periodic.mu.Unlock()
	if periodicSyncs != 2 {
		t.Fatalf("periodic sync count = %d, want 2", periodicSyncs)
	}

	disabled, err := OpenCommandJournalWithOptions(filepath.Join(t.TempDir(), "disabled.journal"), CommandJournalOptions{
		GroupCommitMaxBatch: 1,
		SyncMode:            CommandJournalSyncModeDisabled,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer disabled.Close()
	var disabledSyncs int
	disabled.syncHook = func() error {
		disabledSyncs++
		return errors.New("sync hook must not run in disabled mode")
	}
	disabled.mu.Lock()
	err = disabled.syncLocked()
	disabled.mu.Unlock()
	if err != nil {
		t.Fatalf("disabled sync error = %v", err)
	}
	if disabledSyncs != 0 {
		t.Fatalf("disabled sync count = %d, want 0", disabledSyncs)
	}
}
