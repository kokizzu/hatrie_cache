package hatCache

import (
	"testing"

	"hatrie_cache/hat/hatJournal"
)

func TestT211JournalSyncModesReportDurability(t *testing.T) {
	for _, test := range []struct {
		name          string
		mode          hatJournal.SyncMode
		wantSyncCalls int
		wantDurable   bool
	}{
		{name: "periodic", mode: hatJournal.SyncModePeriodic, wantSyncCalls: 1, wantDurable: true},
		{name: "immediate", mode: hatJournal.SyncModeImmediate, wantSyncCalls: 1, wantDurable: true},
		{name: "disabled", mode: hatJournal.SyncModeDisabled, wantSyncCalls: 0, wantDurable: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			journal, err := OpenCommandJournalWithOptions(t.TempDir()+"/commands.journal", CommandJournalOptions{
				Format:              CommandJournalFormatBinary,
				GroupCommitMaxBatch: 64,
				SyncMode:            test.mode,
			})
			if err != nil {
				t.Fatalf("OpenCommandJournalWithOptions() error = %v", err)
			}
			defer journal.Close()
			trie := newTestTrie(t)
			syncCalls := 0
			journal.syncHook = func() error {
				syncCalls++
				return nil
			}

			response := journal.ExecuteCommand(trie, CacheCommandRequest{Command: "SETSTR", Key: "t211:key", Value: "value"})
			if !response.OK {
				t.Fatalf("ExecuteCommand() = %#v", response)
			}
			report := journal.DurabilityReport()
			if report.SyncMode != test.mode {
				t.Fatalf("SyncMode = %v, want %v", report.SyncMode, test.mode)
			}
			if syncCalls != test.wantSyncCalls {
				t.Fatalf("sync calls = %d, want %d", syncCalls, test.wantSyncCalls)
			}
			if report.Durable != test.wantDurable {
				t.Fatalf("Durable = %v, want %v; report = %#v", report.Durable, test.wantDurable, report)
			}
			if report.LastSequence != 1 {
				t.Fatalf("LastSequence = %d, want 1", report.LastSequence)
			}
			if test.wantDurable {
				if report.LastSyncedSequence != 1 || report.UnsyncedSequences != 0 {
					t.Fatalf("durable report = %#v, want synced sequence 1", report)
				}
			} else if report.LastSyncedSequence != 0 || report.UnsyncedSequences != 1 {
				t.Fatalf("non-durable report = %#v, want unsynced sequence 1", report)
			}
		})
	}
}

func TestT211ImmediateModeOverridesGroupBatchAndDefaultIsPeriodic(t *testing.T) {
	journal, err := OpenCommandJournalWithOptions(t.TempDir()+"/commands.journal", CommandJournalOptions{
		Format:              CommandJournalFormatBinary,
		GroupCommitMaxBatch: 64,
		SyncMode:            hatJournal.SyncModeImmediate,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()
	trie := newTestTrie(t)
	syncCalls := 0
	journal.syncHook = func() error {
		syncCalls++
		return nil
	}
	for index := 0; index < 3; index++ {
		response := journal.ExecuteCommand(trie, CacheCommandRequest{Command: "SETSTR", Key: "t211:immediate", Value: "value"})
		if !response.OK {
			t.Fatalf("ExecuteCommand(%d) = %#v", index, response)
		}
	}
	if syncCalls != 3 {
		t.Fatalf("immediate sync calls = %d, want 3", syncCalls)
	}

	defaultJournal, err := OpenCommandJournalWithOptions(t.TempDir()+"/commands.journal", CommandJournalOptions{
		Format:              CommandJournalFormatBinary,
		GroupCommitMaxBatch: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer defaultJournal.Close()
	if report := defaultJournal.DurabilityReport(); report.SyncMode != hatJournal.SyncModePeriodic {
		t.Fatalf("default SyncMode = %v, want periodic", report.SyncMode)
	}
}
