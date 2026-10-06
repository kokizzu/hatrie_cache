package hatPipeline

import (
	"context"
	"errors"
	"os"
	"sync/atomic"
	"testing"
	"time"
)

func TestAsyncInsertDedupPersistsCommittedIDs(t *testing.T) {
	path := t.TempDir() + "/insert-ids.hid"
	ledger, err := NewAsyncInsertLedger(AsyncInsertLedgerOptions{
		Path:       path,
		MaxEntries: 16,
		TTL:        time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	var handled atomic.Int64
	batcher, err := NewDurableAsyncBatcher(DurableAsyncBatcherOptions[string]{
		Ledger: ledger,
		Batcher: AsyncBatcherOptions[AsyncInsert[string]]{
			Capacity:      8,
			MaxBatchSize:  2,
			FlushInterval: time.Hour,
		},
		Handler: func(_ context.Context, batch []AsyncInsert[string]) error {
			handled.Add(int64(len(batch)))
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	accepted, err := batcher.Submit(context.Background(), "insert-1", "alpha")
	if err != nil || !accepted {
		t.Fatalf("first Submit() = %t/%v, want accepted", accepted, err)
	}
	if err := batcher.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := batcher.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := ledger.Close(); err != nil {
		t.Fatal(err)
	}
	if handled.Load() != 1 {
		t.Fatalf("handled = %d, want 1", handled.Load())
	}

	ledger, err = NewAsyncInsertLedger(AsyncInsertLedgerOptions{Path: path, TTL: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	defer ledger.Close()
	batcher, err = NewDurableAsyncBatcher(DurableAsyncBatcherOptions[string]{
		Ledger: ledger,
		Batcher: AsyncBatcherOptions[AsyncInsert[string]]{
			Capacity:      8,
			MaxBatchSize:  2,
			FlushInterval: time.Hour,
		},
		Handler: func(_ context.Context, batch []AsyncInsert[string]) error {
			handled.Add(int64(len(batch)))
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	accepted, err = batcher.Submit(context.Background(), "insert-1", "replayed")
	if err != nil || accepted {
		t.Fatalf("replayed Submit() = %t/%v, want duplicate", accepted, err)
	}
	if err := batcher.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if handled.Load() != 1 {
		t.Fatalf("duplicate handler calls changed count to %d", handled.Load())
	}
}

func TestAsyncInsertDedupAllowsRetryAfterHandlerFailure(t *testing.T) {
	ledger, err := NewAsyncInsertLedger(AsyncInsertLedgerOptions{Path: t.TempDir() + "/ids.hid"})
	if err != nil {
		t.Fatal(err)
	}
	defer ledger.Close()
	var attempts atomic.Int64
	batcher, err := NewDurableAsyncBatcher(DurableAsyncBatcherOptions[string]{
		Ledger:  ledger,
		Batcher: AsyncBatcherOptions[AsyncInsert[string]]{Capacity: 4, MaxBatchSize: 1, FlushInterval: time.Hour},
		Handler: func(_ context.Context, _ []AsyncInsert[string]) error {
			if attempts.Add(1) == 1 {
				return errors.New("temporary sink failure")
			}
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	accepted, err := batcher.Submit(context.Background(), "retry-me", "value")
	if err != nil || !accepted {
		t.Fatalf("first Submit() = %t/%v, want accepted", accepted, err)
	}
	if err := batcher.Flush(context.Background()); err == nil {
		t.Fatal("first Flush() error = nil, want sink failure")
	}
	accepted, err = batcher.Submit(context.Background(), "retry-me", "value")
	if err != nil || !accepted {
		t.Fatalf("retry Submit() = %t/%v, want accepted", accepted, err)
	}
	if err := batcher.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := batcher.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if attempts.Load() != 2 {
		t.Fatalf("handler attempts = %d, want 2", attempts.Load())
	}
}

func TestAsyncInsertLedgerReleasesUncommittedIDsOnRestart(t *testing.T) {
	path := t.TempDir() + "/ids.hid"
	ledger, err := NewAsyncInsertLedger(AsyncInsertLedgerOptions{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	acquired, err := ledger.Acquire("crash-before-handler")
	if err != nil || !acquired {
		t.Fatalf("Acquire() = %t/%v, want acquired", acquired, err)
	}
	if err := ledger.Close(); err != nil {
		t.Fatal(err)
	}
	ledger, err = NewAsyncInsertLedger(AsyncInsertLedgerOptions{Path: path})

	if err != nil {
		t.Fatal(err)
	}
	defer ledger.Close()
	acquired, err = ledger.Acquire("crash-before-handler")
	if err != nil || !acquired {
		t.Fatalf("restart Acquire() = %t/%v, want replayable", acquired, err)
	}
	if err := ledger.Release("crash-before-handler"); err != nil {
		t.Fatal(err)
	}
}

func TestAsyncInsertLedgerBoundsAndExpiresEntries(t *testing.T) {
	ledger, err := NewAsyncInsertLedger(AsyncInsertLedgerOptions{
		Path:       t.TempDir() + "/ids.hid",
		MaxEntries: 1,
		TTL:        50 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer ledger.Close()
	acquired, err := ledger.Acquire("first")
	if err != nil || !acquired {
		t.Fatalf("first Acquire() = %t/%v", acquired, err)
	}
	if err := ledger.Commit("first"); err != nil {
		t.Fatal(err)
	}
	if _, err := ledger.Acquire("second"); !errors.Is(err, ErrAsyncInsertLedgerFull) {
		t.Fatalf("full Acquire() error = %v, want %v", err, ErrAsyncInsertLedgerFull)
	}
	time.Sleep(75 * time.Millisecond)
	removed, err := ledger.PurgeExpired()
	if err != nil || removed != 1 {
		t.Fatalf("PurgeExpired() = %d/%v, want 1/nil", removed, err)
	}
	acquired, err = ledger.Acquire("second")
	if err != nil || !acquired {
		t.Fatalf("expired Acquire() = %t/%v, want acquired", acquired, err)
	}
	if err := ledger.Release("second"); err != nil {
		t.Fatal(err)
	}
}

func TestAsyncInsertLedgerRejectsInvalidIDs(t *testing.T) {
	ledger, err := NewAsyncInsertLedger(AsyncInsertLedgerOptions{Path: t.TempDir() + "/ids.hid"})
	if err != nil {
		t.Fatal(err)
	}
	defer ledger.Close()
	if _, err := ledger.Acquire(""); !errors.Is(err, ErrAsyncInsertIDRequired) {
		t.Fatalf("empty Acquire() error = %v, want %v", err, ErrAsyncInsertIDRequired)
	}
}

func TestAsyncInsertLedgerTruncatesTornTail(t *testing.T) {
	path := t.TempDir() + "/ids.hid"
	ledger, err := NewAsyncInsertLedger(AsyncInsertLedgerOptions{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	acquired, err := ledger.Acquire("committed")
	if err != nil || !acquired {
		t.Fatalf("Acquire() = %t/%v", acquired, err)
	}
	if err := ledger.Commit("committed"); err != nil {
		t.Fatal(err)
	}
	if err := ledger.Close(); err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write([]byte(asyncInsertLedgerMagic)); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	ledger, err = NewAsyncInsertLedger(AsyncInsertLedgerOptions{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	defer ledger.Close()
	acquired, err = ledger.Acquire("committed")
	if err != nil || acquired {
		t.Fatalf("replayed Acquire() = %t/%v, want duplicate", acquired, err)
	}
}

func TestAsyncInsertLedgerCompactsExpiredRecords(t *testing.T) {
	ledger, err := NewAsyncInsertLedger(AsyncInsertLedgerOptions{
		Path:       t.TempDir() + "/ids.hid",
		MaxEntries: 1,
		TTL:        time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer ledger.Close()
	past := time.Now().Add(-2 * time.Hour)
	compacted := false
	for index := 0; index < 20; index++ {
		id := "expired-" + time.Duration(index).String()
		acquired, err := ledger.Acquire(id)
		if err != nil || !acquired {
			t.Fatalf("Acquire(%q) = %t/%v", id, acquired, err)
		}
		if index == 17 {
			stats := ledger.Stats()
			if stats.Entries != 0 || stats.Records != 0 || stats.FileBytes != 0 {
				t.Fatalf("compacted ledger stats = %#v, want empty compact ledger", stats)
			}
			compacted = true
		}
		if err := ledger.CommitAt(past, []string{id}); err != nil {
			t.Fatal(err)
		}
	}
	if !compacted {
		t.Fatal("expired ledger did not compact")
	}
}

func TestDurableAsyncBatcherCommitsOriginalIDsBeforeHandlerMutation(t *testing.T) {
	ledger, err := NewAsyncInsertLedger(AsyncInsertLedgerOptions{Path: t.TempDir() + "/ids.hid"})
	if err != nil {
		t.Fatal(err)
	}
	defer ledger.Close()
	batcher, err := NewDurableAsyncBatcher(DurableAsyncBatcherOptions[string]{
		Ledger:  ledger,
		Batcher: AsyncBatcherOptions[AsyncInsert[string]]{Capacity: 2, MaxBatchSize: 1, FlushInterval: time.Hour},
		Handler: func(_ context.Context, batch []AsyncInsert[string]) error {
			batch[0].ID = "mutated-by-handler"
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	accepted, err := batcher.Submit(context.Background(), "original-id", "value")
	if err != nil || !accepted {
		t.Fatalf("Submit() = %t/%v", accepted, err)
	}
	if err := batcher.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	accepted, err = batcher.Submit(context.Background(), "original-id", "retry")
	if err != nil || accepted {
		t.Fatalf("original retry = %t/%v, want duplicate", accepted, err)
	}
	if err := batcher.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}
