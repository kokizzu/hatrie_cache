package hatCache

import (
	"errors"
	"path/filepath"
	"testing"
	"time"

	"hatrie_cache/hat/hatJournal"
)

func TestT018JournalSyncModePolicy(t *testing.T) {
	defaultOptions, err := hatJournal.ValidateOptions(hatJournal.Options{
		Format:              hatJournal.DefaultFormat,
		GroupCommitMaxBatch: hatJournal.DefaultGroupCommitMaxBatch,
	})
	if err != nil {
		t.Fatal(err)
	}
	if defaultOptions.SyncMode != hatJournal.SyncModeDurable {
		t.Fatalf("default sync mode = %q, want %q", defaultOptions.SyncMode, hatJournal.SyncModeDurable)
	}

	periodic, err := hatJournal.ValidateOptions(hatJournal.Options{
		Format:              hatJournal.DefaultFormat,
		GroupCommitMaxBatch: hatJournal.DefaultGroupCommitMaxBatch,
		SyncMode:            hatJournal.SyncModePeriodic,
		SyncInterval:        time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	if periodic.SyncInterval != time.Hour {
		t.Fatalf("periodic sync interval = %s, want %s", periodic.SyncInterval, time.Hour)
	}

	for _, mode := range []hatJournal.SyncMode{hatJournal.SyncModeDurable, hatJournal.SyncModePeriodic, hatJournal.SyncModeNone} {
		if _, err := hatJournal.ParseSyncMode(string(mode)); err != nil {
			t.Fatalf("ParseSyncMode(%q) error = %v", mode, err)
		}
	}
	if _, err := hatJournal.ParseSyncMode("invalid"); !errors.Is(err, hatJournal.ErrSyncModeInvalid) {
		t.Fatalf("ParseSyncMode(invalid) error = %v, want ErrSyncModeInvalid", err)
	}
}

func TestT018JournalPeriodicAndDisabledSync(t *testing.T) {
	periodic, err := OpenCommandJournalWithOptions(filepath.Join(t.TempDir(), "periodic.journal"), CommandJournalOptions{
		Format:              DefaultCommandJournalFormat,
		GroupCommitMaxBatch: DefaultJournalGroupCommitMaxBatch,
		SyncMode:            CommandJournalSyncModePeriodic,
		SyncInterval:        time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	var periodicSyncs int
	periodic.syncHook = func() error {
		periodicSyncs++
		return nil
	}
	trie := CreateHatTrie()
	defer trie.Destroy()
	defer periodic.Close()
	if response := periodic.ExecuteCommand(trie, CacheCommandRequest{Command: "SETSTR", Key: "one", Value: "1"}); !response.OK {
		t.Fatalf("periodic first write = %#v", response)
	}
	if response := periodic.ExecuteCommand(trie, CacheCommandRequest{Command: "SETSTR", Key: "two", Value: "2"}); !response.OK {
		t.Fatalf("periodic second write = %#v", response)
	}
	if periodicSyncs != 1 {
		t.Fatalf("periodic sync calls = %d, want 1 before explicit flush", periodicSyncs)
	}
	if err := periodic.Sync(); err != nil {
		t.Fatal(err)
	}
	if periodicSyncs != 2 {
		t.Fatalf("periodic sync calls after flush = %d, want 2", periodicSyncs)
	}

	none, err := OpenCommandJournalWithOptions(filepath.Join(t.TempDir(), "none.journal"), CommandJournalOptions{
		Format:              DefaultCommandJournalFormat,
		GroupCommitMaxBatch: DefaultJournalGroupCommitMaxBatch,
		SyncMode:            CommandJournalSyncModeNone,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer none.Close()
	var noneSyncs int
	none.syncHook = func() error {
		noneSyncs++
		return nil
	}
	if response := none.ExecuteCommand(trie, CacheCommandRequest{Command: "SETSTR", Key: "three", Value: "3"}); !response.OK {
		t.Fatalf("none write = %#v", response)
	}
	if noneSyncs != 0 {
		t.Fatalf("disabled sync calls = %d, want 0", noneSyncs)
	}
	if err := none.Sync(); err != nil {
		t.Fatal(err)
	}
	if noneSyncs != 1 {
		t.Fatalf("disabled explicit sync calls = %d, want 1", noneSyncs)
	}
}

func TestT018JournalPeriodicCloseFlushesPendingWrite(t *testing.T) {
	journal, err := OpenCommandJournalWithOptions(filepath.Join(t.TempDir(), "periodic-close.journal"), CommandJournalOptions{
		Format:              DefaultCommandJournalFormat,
		GroupCommitMaxBatch: DefaultJournalGroupCommitMaxBatch,
		SyncMode:            CommandJournalSyncModePeriodic,
		SyncInterval:        time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	var syncs int
	journal.syncHook = func() error {
		syncs++
		return nil
	}
	trie := CreateHatTrie()
	defer trie.Destroy()
	if response := journal.ExecuteCommand(trie, CacheCommandRequest{Command: "SETSTR", Key: "one", Value: "1"}); !response.OK {
		t.Fatalf("first write = %#v", response)
	}
	if response := journal.ExecuteCommand(trie, CacheCommandRequest{Command: "SETSTR", Key: "two", Value: "2"}); !response.OK {
		t.Fatalf("second write = %#v", response)
	}
	if syncs != 1 {
		t.Fatalf("sync calls before close = %d, want 1", syncs)
	}
	if err := journal.Close(); err != nil {
		t.Fatal(err)
	}
	if syncs != 2 {
		t.Fatalf("sync calls after close = %d, want 2", syncs)
	}
}
