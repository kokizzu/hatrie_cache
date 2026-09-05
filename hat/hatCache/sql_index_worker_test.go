package hatCache

import (
	"context"
	"testing"
	"time"
)

func TestSQLJSONIndexRebuildWorkerBuildsQueuedIndex(t *testing.T) {
	trie := newTestTrie(t)
	trie.UpsertString("jobs", `[{"id":1,"state":"queued"}]`)
	if err := trie.CreateSQLJSONFieldIndex("jobs", "state"); err != nil {
		t.Fatal(err)
	}
	if err := trie.ScheduleSQLJSONIndexRebuild("jobs", "state"); err != nil {
		t.Fatal(err)
	}

	completed := make(chan SQLJSONIndexRebuildProgress, 1)
	worker, err := trie.StartSQLJSONIndexRebuildWorker(context.Background(), time.Millisecond, func(progress SQLJSONIndexRebuildProgress) {
		if progress.State == SQLJSONIndexRebuildStateCompleted {
			completed <- progress
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case progress := <-completed:
		if progress.Key != "jobs" || progress.Field != "state" || progress.Processed != 1 {
			t.Fatalf("completed progress = %#v", progress)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("index rebuild worker did not complete")
	}
	worker.Stop()
	worker.Wait()
	select {
	case <-worker.Done():
	default:
		t.Fatal("stopped index rebuild worker is not done")
	}
	status, available, err := trie.SQLJSONIndexMaintenanceStats("jobs", "state")
	if err != nil || !available || status.Pending || !status.Current || status.Rebuilds != 1 {
		t.Fatalf("worker maintenance status = %#v, %v, %v", status, available, err)
	}
}

func TestSQLJSONIndexRebuildWorkerStopsWithContext(t *testing.T) {
	trie := newTestTrie(t)
	ctx, cancel := context.WithCancel(context.Background())
	worker, err := trie.StartSQLJSONIndexRebuildWorker(ctx, time.Millisecond, nil)
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	select {
	case <-worker.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("index rebuild worker did not stop with context")
	}
	worker.Wait()
}

func TestSQLJSONIndexRebuildWorkerRetriesFailedRebuild(t *testing.T) {
	trie := newTestTrie(t)
	trie.UpsertString("jobs", `not-json`)
	if err := trie.CreateSQLJSONFieldIndex("jobs", "state"); err != nil {
		t.Fatal(err)
	}
	if err := trie.ScheduleSQLJSONIndexRebuild("jobs", "state"); err != nil {
		t.Fatal(err)
	}

	failed := make(chan struct{}, 1)
	completed := make(chan struct{}, 1)
	worker, err := trie.StartSQLJSONIndexRebuildWorker(context.Background(), time.Millisecond, func(progress SQLJSONIndexRebuildProgress) {
		switch progress.State {
		case SQLJSONIndexRebuildStateFailed:
			select {
			case failed <- struct{}{}:
			default:
			}
		case SQLJSONIndexRebuildStateCompleted:
			select {
			case completed <- struct{}{}:
			default:
			}
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-failed:
	case <-time.After(2 * time.Second):
		worker.Stop()
		worker.Wait()
		t.Fatal("index rebuild worker did not report failed rebuild")
	}
	trie.UpsertString("jobs", `[{"id":1,"state":"ready"}]`)
	select {
	case <-completed:
	case <-time.After(2 * time.Second):
		worker.Stop()
		worker.Wait()
		t.Fatal("index rebuild worker did not retry repaired source")
	}
	worker.Stop()
	worker.Wait()
	status, available, err := trie.SQLJSONIndexMaintenanceStats("jobs", "state")
	if err != nil || !available || status.Pending || !status.Current || status.Rebuilds != 1 {
		t.Fatalf("retried worker maintenance status = %#v, %v, %v", status, available, err)
	}
}
