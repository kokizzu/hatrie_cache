package hatCache

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

func TestCHU01IdempotencyStatsExposeDuplicateBytesAndEvictionHorizon(t *testing.T) {
	trie := CreateHatTrie()
	defer trie.Destroy()

	journal, err := OpenCommandJournalWithOptions(filepath.Join(t.TempDir(), "commands.journal"), CommandJournalOptions{
		GroupCommitMaxBatch: 2,
		IdempotencyCapacity: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()

	buffer, err := NewAsyncInsertBuffer(journal, trie, AsyncInsertBufferOptions{
		BatchSize:     1,
		Capacity:      4,
		FlushInterval: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer buffer.Close(context.Background())

	submit := func(request CacheCommandRequest) {
		submission, submitErr := buffer.Submit(context.Background(), request)
		if submitErr != nil {
			t.Fatal(submitErr)
		}
		if flushErr := buffer.Flush(context.Background()); flushErr != nil {
			t.Fatal(flushErr)
		}
		response, waitErr := submission.Wait(context.Background())
		if waitErr != nil || !response.OK {
			t.Fatalf("async insert response = %#v, error = %v", response, waitErr)
		}
	}

	first := CacheCommandRequest{
		Command:        "SET",
		Key:            "chu01:stats:first",
		Value:          "first-value",
		IdempotencyKey: "chu01-stats-1",
	}
	submit(first)
	submit(first)
	submit(CacheCommandRequest{
		Command:        "SET",
		Key:            "chu01:stats:second",
		Value:          "second-value",
		IdempotencyKey: "chu01-stats-2",
	})
	submit(CacheCommandRequest{
		Command:        "SET",
		Key:            "chu01:stats:third",
		Value:          "third-value",
		IdempotencyKey: "chu01-stats-3",
	})

	stats := journal.IdempotencyStats()
	if !stats.Enabled || stats.Capacity != 2 || stats.Entries != 2 {
		t.Fatalf("idempotency stats capacity/state = %#v, want enabled capacity=2 entries=2", stats)
	}
	if stats.Duplicates != 1 {
		t.Fatalf("idempotency duplicate count = %d, want 1", stats.Duplicates)
	}
	if stats.DuplicateEstimatedBytes == 0 {
		t.Fatal("idempotency duplicate estimated bytes = 0, want positive")
	}
	if stats.Evictions != 1 {
		t.Fatalf("idempotency eviction count = %d, want 1", stats.Evictions)
	}
	if stats.OldestSequence < 2 || stats.NewestSequence < stats.OldestSequence {
		t.Fatalf("idempotency sequence horizon = %#v, want oldest >= 2 and ordered", stats)
	}
}
