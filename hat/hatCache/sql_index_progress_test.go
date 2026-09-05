package hatCache

import (
	"context"
	"errors"
	"testing"
)

func TestSQLJSONIndexRebuildProgressCanCancelAndResume(t *testing.T) {
	trie := newTestTrie(t)
	trie.UpsertString("jobs", `[{"id":1,"state":"queued"}]`)
	if err := trie.CreateSQLJSONFieldIndex("jobs", "state"); err != nil {
		t.Fatal(err)
	}
	if err := trie.ScheduleSQLJSONIndexRebuild("jobs", "state"); err != nil {
		t.Fatal(err)
	}

	var canceled []SQLJSONIndexRebuildProgress
	canceledContext, cancel := context.WithCancel(context.Background())
	cancel()
	processed, err := trie.RunScheduledSQLJSONIndexRebuildsWithProgress(canceledContext, 1, func(progress SQLJSONIndexRebuildProgress) {
		canceled = append(canceled, progress)
	})
	if !errors.Is(err, context.Canceled) || processed != 0 {
		t.Fatalf("canceled rebuild = processed %d, error %v", processed, err)
	}
	if got := progressStates(canceled); len(got) != 2 || got[0] != SQLJSONIndexRebuildStateQueued || got[1] != SQLJSONIndexRebuildStateCanceled {
		t.Fatalf("canceled progress states = %v", got)
	}
	status, available, err := trie.SQLJSONIndexMaintenanceStats("jobs", "state")
	if err != nil || !available || !status.Pending || status.Current {
		t.Fatalf("canceled maintenance status = %#v, %v, %v", status, available, err)
	}

	var resumed []SQLJSONIndexRebuildProgress
	processed, err = trie.RunScheduledSQLJSONIndexRebuildsWithProgress(context.Background(), 1, func(progress SQLJSONIndexRebuildProgress) {
		resumed = append(resumed, progress)
	})
	if err != nil || processed != 1 {
		t.Fatalf("resumed rebuild = processed %d, error %v", processed, err)
	}
	if got := progressStates(resumed); len(got) != 3 || got[0] != SQLJSONIndexRebuildStateQueued || got[1] != SQLJSONIndexRebuildStateRunning || got[2] != SQLJSONIndexRebuildStateCompleted {
		t.Fatalf("resumed progress states = %v", got)
	}
	for _, progress := range resumed {
		if progress.Key != "jobs" || progress.Field != "state" || progress.Total != 1 {
			t.Fatalf("resumed progress = %#v", progress)
		}
	}
	status, available, err = trie.SQLJSONIndexMaintenanceStats("jobs", "state")
	if err != nil || !available || status.Pending || !status.Current || status.Rebuilds != 1 {
		t.Fatalf("resumed maintenance status = %#v, %v, %v", status, available, err)
	}
}

func progressStates(progress []SQLJSONIndexRebuildProgress) []SQLJSONIndexRebuildState {
	states := make([]SQLJSONIndexRebuildState, len(progress))
	for index, item := range progress {
		states[index] = item.State
	}
	return states
}
