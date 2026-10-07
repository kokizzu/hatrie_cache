package hatCache

import (
	"sync/atomic"
	"testing"
	"time"
)

func TestTU34ExecuteCommandWithSpaceUsesPerSpaceSyncPolicy(t *testing.T) {
	journal, err := OpenCommandJournalWithOptions(t.TempDir()+"/commands.journal", CommandJournalOptions{
		GroupCommitMaxBatch: 1,
		SpaceSyncPolicies: map[string]CommandJournalSpaceSyncPolicy{
			"bulk":    {Mode: CommandJournalSpaceSyncDisabled},
			"reports": {Mode: CommandJournalSpaceSyncPeriodic, Interval: time.Hour},
		},
	})
	if err != nil {
		t.Fatalf("OpenCommandJournalWithOptions() error = %v", err)
	}
	defer journal.Close()
	var syncs atomic.Int32
	journal.syncHook = func() error {
		syncs.Add(1)
		return nil
	}

	trie := newTestTrie(t)
	if response := journal.ExecuteCommandWithSpace(trie, "bulk", CacheCommandRequest{Command: "SETSTR", Key: "bulk:1", Value: "ok"}); !response.OK {
		t.Fatalf("buffered command response = %#v", response)
	}
	if got := syncs.Load(); got != 0 {
		t.Fatalf("buffered sync count = %d, want 0", got)
	}
	if response := journal.ExecuteCommandWithSpace(trie, "reports", CacheCommandRequest{Command: "SETSTR", Key: "reports:1", Value: "ok"}); !response.OK {
		t.Fatalf("periodic command response = %#v", response)
	}
	if got := syncs.Load(); got != 1 {
		t.Fatalf("first periodic sync count = %d, want 1", got)
	}
	if response := journal.ExecuteCommandWithSpace(trie, "reports", CacheCommandRequest{Command: "SETSTR", Key: "reports:2", Value: "ok"}); !response.OK {
		t.Fatalf("second periodic command response = %#v", response)
	}
	if got := syncs.Load(); got != 1 {
		t.Fatalf("unexpired periodic sync count = %d, want 1", got)
	}
	if response := journal.ExecuteCommand(trie, CacheCommandRequest{Command: "SETSTR", Key: "default:1", Value: "ok"}); !response.OK {
		t.Fatalf("legacy command response = %#v", response)
	}
	if got := syncs.Load(); got != 2 {
		t.Fatalf("legacy sync count = %d, want 2", got)
	}
}

func TestTU34GroupCommitUsesStrongestSpaceSyncPolicy(t *testing.T) {
	journal, err := OpenCommandJournalWithOptions(t.TempDir()+"/commands.journal", CommandJournalOptions{
		GroupCommitWindow:   20 * time.Millisecond,
		GroupCommitMaxBatch: 8,
		SpaceSyncPolicies: map[string]CommandJournalSpaceSyncPolicy{
			"bulk": {Mode: CommandJournalSpaceSyncDisabled},
		},
	})
	if err != nil {
		t.Fatalf("OpenCommandJournalWithOptions() error = %v", err)
	}
	defer journal.Close()
	var syncs atomic.Int32
	journal.syncHook = func() error {
		syncs.Add(1)
		return nil
	}
	trie := newTestTrie(t)
	responses := make(chan CacheCommandResponse, 2)
	go func() {
		responses <- journal.ExecuteCommandWithSpace(trie, "bulk", CacheCommandRequest{Command: "SETSTR", Key: "bulk:1", Value: "ok"})
	}()
	go func() {
		responses <- journal.ExecuteCommandWithSpace(trie, "critical", CacheCommandRequest{Command: "SETSTR", Key: "critical:1", Value: "ok"})
	}()
	for range 2 {
		if response := <-responses; !response.OK {
			t.Fatalf("grouped response = %#v", response)
		}
	}
	if got := syncs.Load(); got != 1 {
		t.Fatalf("mixed policy sync count = %d, want 1", got)
	}
}

func TestTU34IdempotentGroupCommitUsesSpaceSyncPolicy(t *testing.T) {
	journal, err := OpenCommandJournalWithOptions(t.TempDir()+"/commands.journal", CommandJournalOptions{
		GroupCommitWindow:   20 * time.Millisecond,
		GroupCommitMaxBatch: 8,
		IdempotencyCapacity: 8,
		SpaceSyncPolicies: map[string]CommandJournalSpaceSyncPolicy{
			"bulk": {Mode: CommandJournalSpaceSyncDisabled},
		},
	})
	if err != nil {
		t.Fatalf("OpenCommandJournalWithOptions() error = %v", err)
	}
	defer journal.Close()
	var syncs atomic.Int32
	journal.syncHook = func() error {
		syncs.Add(1)
		return nil
	}
	response := journal.ExecuteCommandWithSpace(newTestTrie(t), "bulk", CacheCommandRequest{
		Command:        "SETSTR",
		Key:            "bulk:idempotent",
		Value:          "ok",
		IdempotencyKey: "bulk-request-1",
	})
	if !response.OK {
		t.Fatalf("idempotent response = %#v", response)
	}
	if got := syncs.Load(); got != 0 {
		t.Fatalf("idempotent buffered sync count = %d, want 0", got)
	}
}
