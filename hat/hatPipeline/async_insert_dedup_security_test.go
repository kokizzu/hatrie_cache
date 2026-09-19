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

func TestAsyncInsertDedupFileStoreRejectsOversizedLedger(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ledger")
	if err := os.WriteFile(path, make([]byte, 32), 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	store, err := hatPipeline.NewAsyncInsertDedupFileStore(hatPipeline.AsyncInsertDedupFileStoreOptions{Path: path, MaxBytes: 16})
	if err != nil {
		t.Fatalf("NewAsyncInsertDedupFileStore() error = %v", err)
	}
	if _, err := store.Load(context.Background()); !errors.Is(err, hatPipeline.ErrAsyncInsertDedupFileTooLarge) {
		t.Fatalf("Load() error = %v, want %v", err, hatPipeline.ErrAsyncInsertDedupFileTooLarge)
	}
}

func TestAsyncInsertDedupFileStoreRejectsSymlink(t *testing.T) {
	directory := t.TempDir()
	target := filepath.Join(directory, "target")
	path := filepath.Join(directory, "ledger")
	if err := os.WriteFile(target, nil, 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	if err := os.Symlink(target, path); err != nil {
		t.Fatalf("Symlink() error = %v", err)
	}
	store, err := hatPipeline.NewAsyncInsertDedupFileStore(hatPipeline.AsyncInsertDedupFileStoreOptions{Path: path})
	if err != nil {
		t.Fatalf("NewAsyncInsertDedupFileStore() error = %v", err)
	}
	if _, err := store.Load(context.Background()); !errors.Is(err, hatPipeline.ErrAsyncInsertDedupSymlink) {
		t.Fatalf("Load() error = %v, want %v", err, hatPipeline.ErrAsyncInsertDedupSymlink)
	}
}

func TestAsyncInsertDeduplicatorCompactsExpiredFileRecords(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ledger")
	store, err := hatPipeline.NewAsyncInsertDedupFileStore(hatPipeline.AsyncInsertDedupFileStoreOptions{Path: path})
	if err != nil {
		t.Fatalf("NewAsyncInsertDedupFileStore() error = %v", err)
	}
	now := time.Unix(400, 0)
	deduplicator, err := hatPipeline.NewAsyncInsertDeduplicator(hatPipeline.AsyncInsertDeduplicatorOptions{
		Capacity: 4,
		TTL:      time.Minute,
		Now:      func() time.Time { return now },
		Store:    store,
	})
	if err != nil {
		t.Fatalf("NewAsyncInsertDeduplicator() error = %v", err)
	}
	if _, err := deduplicator.Accept(context.Background(), "orders", "old", []byte("old")); err != nil {
		t.Fatalf("Accept() error = %v", err)
	}
	now = now.Add(2 * time.Minute)
	if err := deduplicator.Compact(context.Background()); err != nil {
		t.Fatalf("Compact() error = %v", err)
	}
	if records, err := store.Load(context.Background()); err != nil || len(records) != 0 {
		t.Fatalf("Load() = %d/%v, want 0/nil", len(records), err)
	}
}
