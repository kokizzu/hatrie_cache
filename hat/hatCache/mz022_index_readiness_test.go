package hatCache

import (
	"context"
	"errors"
	"testing"
)

func TestWaitSQLJSONIndexReadyBuildsAndTracksCurrentIndex(t *testing.T) {
	trie := newTestTrie(t)
	trie.UpsertString("jobs", `[{"id":1,"state":"queued"}]`)
	if err := trie.CreateSQLJSONFieldIndex("jobs", "state"); err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	stats, available, err := trie.WaitSQLJSONIndexReady(ctx, "jobs", "state")
	if err != nil || !available || !stats.Current || stats.Rebuilds != 1 {
		t.Fatalf("WaitSQLJSONIndexReady() = %#v, %v, %v", stats, available, err)
	}

	trie.UpsertString("jobs", `[{"id":2,"state":"running"}]`)
	stats, available, err = trie.WaitSQLJSONIndexReady(ctx, "jobs", "state")
	if err != nil || !available || !stats.Current || stats.Rebuilds != 2 {
		t.Fatalf("WaitSQLJSONIndexReady() after update = %#v, %v, %v", stats, available, err)
	}
}

func TestWaitSQLJSONIndexReadyHonorsContextAndAvailability(t *testing.T) {
	trie := newTestTrie(t)
	if _, available, err := trie.WaitSQLJSONIndexReady(context.Background(), "missing", "state"); err != nil || available {
		t.Fatalf("unconfigured WaitSQLJSONIndexReady() = %v, %v", available, err)
	}
	trie.UpsertString("jobs", `[{"state":"queued"}]`)
	if err := trie.CreateSQLJSONFieldIndex("jobs", "state"); err != nil {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, available, err := trie.WaitSQLJSONIndexReady(canceled, "jobs", "state"); !errors.Is(err, context.Canceled) || !available {
		t.Fatalf("canceled WaitSQLJSONIndexReady() = %v, %v", available, err)
	}
	if _, available, err := trie.WaitSQLJSONIndexReady(nil, "jobs", "state"); err == nil || available {
		t.Fatalf("nil-context WaitSQLJSONIndexReady() = %v, %v", available, err)
	}
}

func TestWaitSQLJSONIndexReadyReturnsRebuildError(t *testing.T) {
	trie := newTestTrie(t)
	trie.UpsertString("jobs", `not-json`)
	if err := trie.CreateSQLJSONFieldIndex("jobs", "state"); err != nil {
		t.Fatal(err)
	}
	if _, available, err := trie.WaitSQLJSONIndexReady(context.Background(), "jobs", "state"); err == nil || !available {
		t.Fatalf("invalid-source WaitSQLJSONIndexReady() = %v, %v", available, err)
	}
}
