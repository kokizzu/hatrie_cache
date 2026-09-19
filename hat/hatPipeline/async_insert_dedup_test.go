package hatPipeline_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"hatrie_cache/hat/hatPipeline"
)

func TestAsyncInsertDeduplicatorAcceptsDuplicatesAndDetectsConflicts(t *testing.T) {
	now := time.Unix(100, 0)
	deduplicator, err := hatPipeline.NewAsyncInsertDeduplicator(hatPipeline.AsyncInsertDeduplicatorOptions{
		Capacity: 2,
		TTL:      time.Hour,
		Now:      func() time.Time { return now },
	})
	if err != nil {
		t.Fatalf("NewAsyncInsertDeduplicator() error = %v", err)
	}
	decision, err := deduplicator.Accept(context.Background(), "orders", "insert-1", []byte("one"))
	if err != nil || decision != hatPipeline.AsyncInsertAccepted {
		t.Fatalf("first Accept() = %v/%v, want accepted/nil", decision, err)
	}
	decision, err = deduplicator.Accept(context.Background(), "orders", "insert-1", []byte("one"))
	if err != nil || decision != hatPipeline.AsyncInsertDuplicate {
		t.Fatalf("duplicate Accept() = %v/%v, want duplicate/nil", decision, err)
	}
	if _, err := deduplicator.Accept(context.Background(), "orders", "insert-1", []byte("changed")); !errors.Is(err, hatPipeline.ErrAsyncInsertConflict) {
		t.Fatalf("conflicting Accept() error = %v, want %v", err, hatPipeline.ErrAsyncInsertConflict)
	}
}

func TestAsyncInsertDeduplicatorExpiresEntriesAndEnforcesCapacity(t *testing.T) {
	now := time.Unix(200, 0)
	deduplicator, err := hatPipeline.NewAsyncInsertDeduplicator(hatPipeline.AsyncInsertDeduplicatorOptions{
		Capacity: 1,
		TTL:      time.Minute,
		Now:      func() time.Time { return now },
	})
	if err != nil {
		t.Fatalf("NewAsyncInsertDeduplicator() error = %v", err)
	}
	if _, err := deduplicator.Accept(context.Background(), "orders", "one", []byte("one")); err != nil {
		t.Fatalf("first Accept() error = %v", err)
	}
	if _, err := deduplicator.Accept(context.Background(), "orders", "two", []byte("two")); !errors.Is(err, hatPipeline.ErrAsyncInsertCapacity) {
		t.Fatalf("capacity Accept() error = %v, want %v", err, hatPipeline.ErrAsyncInsertCapacity)
	}
	now = now.Add(2 * time.Minute)
	if decision, err := deduplicator.Accept(context.Background(), "orders", "two", []byte("two")); err != nil || decision != hatPipeline.AsyncInsertAccepted {
		t.Fatalf("expired Accept() = %v/%v, want accepted/nil", decision, err)
	}
}

func TestAsyncInsertDeduplicatorFileStoreSurvivesRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "async-insert-ledger")
	store, err := hatPipeline.NewAsyncInsertDedupFileStore(hatPipeline.AsyncInsertDedupFileStoreOptions{Path: path})
	if err != nil {
		t.Fatalf("NewAsyncInsertDedupFileStore() error = %v", err)
	}
	now := time.Unix(300, 0)
	options := hatPipeline.AsyncInsertDeduplicatorOptions{
		Capacity: 4,
		TTL:      time.Hour,
		Now:      func() time.Time { return now },
		Store:    store,
	}
	first, err := hatPipeline.NewAsyncInsertDeduplicator(options)
	if err != nil {
		t.Fatalf("first NewAsyncInsertDeduplicator() error = %v", err)
	}
	if _, err := first.Accept(context.Background(), "payments", "insert-7", []byte("payload")); err != nil {
		t.Fatalf("durable Accept() error = %v", err)
	}
	reopened, err := hatPipeline.NewAsyncInsertDeduplicator(options)
	if err != nil {
		t.Fatalf("reopened NewAsyncInsertDeduplicator() error = %v", err)
	}
	decision, err := reopened.Accept(context.Background(), "payments", "insert-7", []byte("payload"))
	if err != nil || decision != hatPipeline.AsyncInsertDuplicate {
		t.Fatalf("reopened duplicate Accept() = %v/%v, want duplicate/nil", decision, err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat() error = %v", err)
	}
	if got := info.Mode().Perm(); got != 0o600 {
		t.Fatalf("ledger mode = %o, want 600", got)
	}
}

func TestAsyncInsertDeduplicatorAcceptsOneConcurrentDuplicate(t *testing.T) {
	deduplicator, err := hatPipeline.NewAsyncInsertDeduplicator(hatPipeline.AsyncInsertDeduplicatorOptions{Capacity: 8, TTL: time.Hour})
	if err != nil {
		t.Fatalf("NewAsyncInsertDeduplicator() error = %v", err)
	}
	const workers = 32
	decisions := make(chan hatPipeline.AsyncInsertDedupDecision, workers)
	errorsSeen := make(chan error, workers)
	var group sync.WaitGroup
	for i := 0; i < workers; i++ {
		group.Add(1)
		go func() {
			defer group.Done()
			decision, err := deduplicator.Accept(context.Background(), "orders", "same", []byte("payload"))
			decisions <- decision
			errorsSeen <- err
		}()
	}
	group.Wait()
	close(decisions)
	close(errorsSeen)
	accepted := 0
	duplicates := 0
	for decision := range decisions {
		switch decision {
		case hatPipeline.AsyncInsertAccepted:
			accepted++
		case hatPipeline.AsyncInsertDuplicate:
			duplicates++
		}
	}
	for err := range errorsSeen {
		if err != nil {
			t.Fatalf("concurrent Accept() error = %v", err)
		}
	}
	if accepted != 1 || duplicates != workers-1 {
		t.Fatalf("concurrent decisions accepted=%d duplicates=%d", accepted, duplicates)
	}
}
