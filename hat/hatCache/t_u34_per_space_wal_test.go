package hatCache

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

func TestTU34DefaultSyncPolicyRemainsSynchronous(t *testing.T) {
	trie := CreateHatTrie()
	defer trie.Destroy()
	journal, err := OpenCommandJournalWithOptions(filepath.Join(t.TempDir(), "commands.journal"), CommandJournalOptions{
		GroupCommitMaxBatch: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()
	syncs := 0
	journal.syncHook = func() error {
		syncs++
		return nil
	}
	if response := journal.ExecuteCommand(trie, CacheCommandRequest{Command: "SET", Key: "durable:key", Value: "value"}); !response.OK {
		t.Fatalf("ExecuteCommand() = %#v", response)
	}
	if syncs != 1 {
		t.Fatalf("default sync count = %d, want 1", syncs)
	}
	if got := journal.Durability(); got.AppliedSequence != 1 || got.DurableSequence != 1 || got.Pending {
		t.Fatalf("default durability = %#v, want applied/durable 1/no pending", got)
	}
}

func TestTU34DisabledSpaceNeedsExplicitSyncAndReplays(t *testing.T) {
	path := filepath.Join(t.TempDir(), "commands.journal")
	trie := CreateHatTrie()
	defer trie.Destroy()
	journal, err := OpenCommandJournalWithOptions(path, CommandJournalOptions{
		GroupCommitMaxBatch: 1,
		SyncPolicy: CommandJournalSyncPolicy{
			Rules: []CommandJournalSyncPolicyRule{{SpacePrefix: "volatile:", Mode: CommandJournalSyncDisabled}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	syncs := 0
	journal.syncHook = func() error {
		syncs++
		return nil
	}
	if response := journal.ExecuteCommand(trie, CacheCommandRequest{Command: "SET", Key: "volatile:key", Value: "value"}); !response.OK {
		t.Fatalf("ExecuteCommand() = %#v", response)
	}
	if syncs != 0 {
		t.Fatalf("disabled sync count = %d, want 0", syncs)
	}
	if got := journal.Durability(); got.AppliedSequence != 1 || got.DurableSequence != 0 || !got.Pending {
		t.Fatalf("disabled durability = %#v, want applied 1/durable 0/pending", got)
	}
	if durable, err := journal.Sync(); err != nil || durable != 1 {
		t.Fatalf("Sync() = %d/%v, want 1/nil", durable, err)
	}
	if err := journal.Close(); err != nil {
		t.Fatal(err)
	}

	restored := CreateHatTrie()
	defer restored.Destroy()
	reopened, err := OpenCommandJournalWithOptions(path, CommandJournalOptions{GroupCommitMaxBatch: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if _, err := reopened.Replay(restored, 0); err != nil {
		t.Fatalf("Replay() error = %v", err)
	}
	if response := restored.ExecuteCommand(CacheCommandRequest{Command: "GET", Key: "volatile:key"}); !response.OK || response.Value != "value" {
		t.Fatalf("replayed GET = %#v, want value", response)
	}
}

func TestTU34PeriodicSpaceSyncsAtBoundary(t *testing.T) {
	trie := CreateHatTrie()
	defer trie.Destroy()
	journal, err := OpenCommandJournalWithOptions(filepath.Join(t.TempDir(), "commands.journal"), CommandJournalOptions{
		GroupCommitMaxBatch: 1,
		SyncPolicy: CommandJournalSyncPolicy{
			PeriodicInterval: time.Hour,
			Rules:            []CommandJournalSyncPolicyRule{{SpacePrefix: "periodic:", Mode: CommandJournalSyncPeriodic}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()
	syncs := 0
	journal.syncHook = func() error {
		syncs++
		return nil
	}
	if response := journal.ExecuteCommand(trie, CacheCommandRequest{Command: "SET", Key: "periodic:first", Value: "one"}); !response.OK {
		t.Fatalf("first ExecuteCommand() = %#v", response)
	}
	if syncs != 0 {
		t.Fatalf("periodic initial sync count = %d, want 0", syncs)
	}
	journal.lastSyncAt = time.Now().Add(-2 * time.Hour)
	if response := journal.ExecuteCommand(trie, CacheCommandRequest{Command: "SET", Key: "periodic:second", Value: "two"}); !response.OK {
		t.Fatalf("second ExecuteCommand() = %#v", response)
	}
	if syncs != 1 {
		t.Fatalf("periodic boundary sync count = %d, want 1", syncs)
	}
	if got := journal.Durability(); got.AppliedSequence != 2 || got.DurableSequence != 2 || got.Pending {
		t.Fatalf("periodic durability = %#v, want applied/durable 2/no pending", got)
	}
}

func TestTU34DisabledGroupCommitAndBatchAvoidAutomaticSync(t *testing.T) {
	policy := CommandJournalSyncPolicy{
		Rules: []CommandJournalSyncPolicyRule{{SpacePrefix: "volatile:", Mode: CommandJournalSyncDisabled}},
	}
	for _, test := range []struct {
		name  string
		batch int
	}{
		{name: "group commit", batch: 4},
		{name: "prepared batch", batch: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			trie := CreateHatTrie()
			defer trie.Destroy()
			journal, err := OpenCommandJournalWithOptions(filepath.Join(t.TempDir(), "commands.journal"), CommandJournalOptions{
				GroupCommitMaxBatch: test.batch,
				SyncPolicy:          policy,
			})
			if err != nil {
				t.Fatal(err)
			}
			defer journal.Close()
			syncs := 0
			journal.syncHook = func() error {
				syncs++
				return nil
			}
			if test.batch > 1 {
				if response := journal.ExecuteCommand(trie, CacheCommandRequest{Command: "SET", Key: "volatile:group", Value: "value"}); !response.OK {
					t.Fatalf("group ExecuteCommand() = %#v", response)
				}
			} else {
				applied, response := journal.executeJournalRecordsBatch(trie, []CommandJournalRecord{{Request: CacheCommandRequest{Command: "SET", Key: "volatile:batch", Value: "value"}}})
				if applied != 1 || !response.OK {
					t.Fatalf("batch result = %d/%#v", applied, response)
				}
			}
			if syncs != 0 {
				t.Fatalf("disabled %s sync count = %d, want 0", test.name, syncs)
			}
			if _, err := journal.Sync(); err != nil {
				t.Fatalf("Sync() error = %v", err)
			}
		})
	}
}

func TestTU34PublicBatchUsesStrongestSpaceMode(t *testing.T) {
	trie := CreateHatTrie()
	defer trie.Destroy()
	journal, err := OpenCommandJournalWithOptions(filepath.Join(t.TempDir(), "commands.journal"), CommandJournalOptions{
		GroupCommitMaxBatch: 1,
		SyncPolicy: CommandJournalSyncPolicy{
			Rules: []CommandJournalSyncPolicyRule{{SpacePrefix: "volatile:", Mode: CommandJournalSyncDisabled}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()
	syncs := 0
	journal.syncHook = func() error {
		syncs++
		return nil
	}
	response, rejected := executePublicCommandBatch(context.Background(), trie, CacheCommandRequest{
		Command: "BATCH",
		Atomic:  true,
		Batch: []CacheCommandRequest{
			{Command: "SET", Key: "volatile:cache", Value: "value"},
			{Command: "SET", Key: "durable:catalog", Value: "value"},
		},
	}, commandExecutionOptions{Journal: journal})
	if rejected || !response.OK {
		t.Fatalf("executePublicCommandBatch() = %#v/%v", response, rejected)
	}
	if syncs != 1 {
		t.Fatalf("mixed public batch sync count = %d, want 1", syncs)
	}
	if got := journal.Durability(); got.AppliedSequence != 2 || got.DurableSequence != 2 || got.Pending {
		t.Fatalf("mixed public batch durability = %#v, want applied/durable 2/no pending", got)
	}
}
