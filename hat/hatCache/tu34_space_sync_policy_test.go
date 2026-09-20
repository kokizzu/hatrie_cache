package hatCache

import (
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"hatrie_cache/hat/hatJournal"
)

func TestTU34SpaceSyncPolicyAppliesToNamedJournalCommands(t *testing.T) {
	registry, err := hatJournal.NewSpaceSyncPolicyRegistry(hatJournal.SpaceSyncPolicyOptions{
		DefaultPolicy: hatJournal.SpaceSyncPolicyPeriodic,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Register("ephemeral", hatJournal.SpaceSyncPolicyDisabled); err != nil {
		t.Fatal(err)
	}
	if err := registry.Register("durable", hatJournal.SpaceSyncPolicyImmediate); err != nil {
		t.Fatal(err)
	}

	journal, err := OpenCommandJournalWithOptions(filepath.Join(t.TempDir(), "commands.journal"), CommandJournalOptions{
		GroupCommitMaxBatch: 1,
		SpaceSyncPolicies:   registry,
		GroupCommitWindow:   0,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()
	var syncs atomic.Int64
	journal.syncHook = func() error {
		syncs.Add(1)
		return nil
	}
	trie := newTestTrie(t)

	if response := journal.ExecuteCommandInSpace(trie, "ephemeral", CacheCommandRequest{Command: "SETSTR", Key: "ephemeral:key", Value: "v"}); !response.OK {
		t.Fatalf("disabled command = %#v", response)
	}
	if got := syncs.Load(); got != 0 {
		t.Fatalf("disabled sync count = %d, want 0", got)
	}
	if response := journal.ExecuteCommandInSpace(trie, "durable", CacheCommandRequest{Command: "SETSTR", Key: "durable:key", Value: "v"}); !response.OK {
		t.Fatalf("immediate command = %#v", response)
	}
	if got := syncs.Load(); got != 1 {
		t.Fatalf("immediate sync count = %d, want 1", got)
	}
	if response := journal.ExecuteCommandInSpace(trie, "unknown", CacheCommandRequest{Command: "SETSTR", Key: "periodic:key", Value: "v"}); !response.OK {
		t.Fatalf("periodic command = %#v", response)
	}
	if got := syncs.Load(); got != 2 {
		t.Fatalf("periodic sync count = %d, want 2", got)
	}
}

func TestTU34DisabledSpaceSyncPolicySkipsGroupedSync(t *testing.T) {
	registry, err := hatJournal.NewSpaceSyncPolicyRegistry(hatJournal.SpaceSyncPolicyOptions{
		DefaultPolicy: hatJournal.SpaceSyncPolicyDisabled,
	})
	if err != nil {
		t.Fatal(err)
	}
	journal, err := OpenCommandJournalWithOptions(filepath.Join(t.TempDir(), "commands.journal"), CommandJournalOptions{
		GroupCommitMaxBatch: 4,
		GroupCommitWindow:   time.Millisecond,
		SpaceSyncPolicies:   registry,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()
	var syncs atomic.Int64
	journal.syncHook = func() error {
		syncs.Add(1)
		return nil
	}
	trie := newTestTrie(t)
	for index := 0; index < 4; index++ {
		response := journal.ExecuteCommandInSpace(trie, "ephemeral", CacheCommandRequest{
			Command: "SETSTR",
			Key:     "ephemeral:" + string(rune('a'+index)),
			Value:   "v",
		})
		if !response.OK {
			t.Fatalf("grouped disabled command %d = %#v", index, response)
		}
	}
	if got := syncs.Load(); got != 0 {
		t.Fatalf("grouped disabled sync count = %d, want 0", got)
	}
}
