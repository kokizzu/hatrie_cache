package hatPipeline_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"hatrie_cache/hat/hatPipeline"
)

func TestAsyncInsertDedupFileStoreRejectsCorruptFrame(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ledger")
	store, err := hatPipeline.NewAsyncInsertDedupFileStore(hatPipeline.AsyncInsertDedupFileStoreOptions{Path: path})
	if err != nil {
		t.Fatalf("NewAsyncInsertDedupFileStore() error = %v", err)
	}
	if err := store.Append(context.Background(), hatPipeline.AsyncInsertDedupRecord{Source: "orders", ID: "one", ExpiresAt: time.Now().Add(time.Hour)}); err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	data[len(data)-1] ^= 1
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	if _, err := store.Load(context.Background()); !errors.Is(err, hatPipeline.ErrAsyncInsertDedupCorrupt) {
		t.Fatalf("Load() error = %v, want %v", err, hatPipeline.ErrAsyncInsertDedupCorrupt)
	}
}

func TestAsyncInsertDeduplicatorCancellationDoesNotAdmit(t *testing.T) {
	deduplicator, err := hatPipeline.NewAsyncInsertDeduplicator(hatPipeline.AsyncInsertDeduplicatorOptions{Capacity: 1, TTL: time.Hour})
	if err != nil {
		t.Fatalf("NewAsyncInsertDeduplicator() error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := deduplicator.Accept(ctx, "orders", "one", []byte("payload")); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Accept() error = %v, want %v", err, context.Canceled)
	}
	if got := deduplicator.Stats().Entries; got != 0 {
		t.Fatalf("canceled Accept() entries = %d, want 0", got)
	}
}
