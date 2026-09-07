package hatSql_test

import (
	"errors"
	"sync/atomic"
	"testing"

	hatSql "hatrie_cache/hat/hatSql"
)

func TestSQLSinkCommitCoordinatorRunsCommitOnceAndRestores(t *testing.T) {
	commit := hatSql.SQLSinkCommit{
		Sink:          "warehouse",
		TransactionID: "txn-1",
		Progress: []hatSql.SQLSinkProgress{
			{Sink: "warehouse", Partition: "1", Frontier: 20},
			{Sink: "warehouse", Partition: "0", Frontier: 10},
		},
	}
	coordinator := hatSql.NewSQLSinkCommitCoordinator()
	calls := 0
	if committed, err := coordinator.Commit(commit, func() error {
		calls++
		return nil
	}); err != nil || !committed {
		t.Fatalf("first Commit() = %t/%v, want true/nil", committed, err)
	}
	if committed, err := coordinator.Commit(commit, func() error {
		calls++
		return nil
	}); err != nil || committed {
		t.Fatalf("duplicate Commit() = %t/%v, want false/nil", committed, err)
	}
	if calls != 1 {
		t.Fatalf("commit callback calls = %d, want 1", calls)
	}

	snapshot := coordinator.Snapshot()
	if len(snapshot) != 1 || snapshot[0].Sink != "warehouse" || snapshot[0].TransactionID != "txn-1" || len(snapshot[0].Progress) != 2 || snapshot[0].Progress[0].Partition != "0" {
		t.Fatalf("Snapshot() = %#v, want deterministic committed snapshot", snapshot)
	}
	restored := hatSql.NewSQLSinkCommitCoordinator()
	if err := restored.Restore(snapshot); err != nil {
		t.Fatalf("Restore() error = %v", err)
	}
	if committed, err := restored.Commit(commit, func() error {
		calls++
		return nil
	}); err != nil || committed {
		t.Fatalf("restored duplicate Commit() = %t/%v, want false/nil", committed, err)
	}
	if calls != 1 {
		t.Fatalf("restored commit callback calls = %d, want 1", calls)
	}
}

func TestSQLSinkCommitCoordinatorRejectsConflictsAndRetriesFailures(t *testing.T) {
	coordinator := hatSql.NewSQLSinkCommitCoordinator()
	commit := hatSql.SQLSinkCommit{
		Sink:          "warehouse",
		TransactionID: "txn-1",
		Progress:      []hatSql.SQLSinkProgress{{Sink: "warehouse", Partition: "0", Frontier: 10}},
	}
	boom := errors.New("sink unavailable")
	if committed, err := coordinator.Commit(commit, func() error { return boom }); !errors.Is(err, boom) || committed {
		t.Fatalf("failed Commit() = %t/%v, want false/boom", committed, err)
	}
	if committed, err := coordinator.Commit(commit, func() error { return nil }); err != nil || !committed {
		t.Fatalf("retry Commit() = %t/%v, want true/nil", committed, err)
	}
	conflict := commit
	conflict.Progress = []hatSql.SQLSinkProgress{{Sink: "warehouse", Partition: "0", Frontier: 11}}
	if _, err := coordinator.Commit(conflict, func() error { return nil }); !errors.Is(err, hatSql.ErrSQLSinkCommitConflict) {
		t.Fatalf("conflicting Commit() error = %v, want conflict", err)
	}
}

func TestSQLSinkCommitCoordinatorRejectsInvalidCommit(t *testing.T) {
	coordinator := hatSql.NewSQLSinkCommitCoordinator()
	for _, commit := range []hatSql.SQLSinkCommit{
		{Progress: []hatSql.SQLSinkProgress{{Sink: "warehouse", Partition: "0", Frontier: 1}}},
		{Sink: "warehouse", TransactionID: "txn-empty"},
		{Sink: "warehouse", TransactionID: "txn-invalid", Progress: []hatSql.SQLSinkProgress{{Sink: "other", Partition: "0", Frontier: 1}}},
	} {
		if _, err := coordinator.Commit(commit, func() error { return nil }); !errors.Is(err, hatSql.ErrSQLSinkCommitInvalid) {
			t.Fatalf("invalid Commit(%#v) error = %v, want invalid", commit, err)
		}
	}
	if len(coordinator.Snapshot()) != 0 {
		t.Fatal("invalid commits changed coordinator state")
	}
}

func TestSQLSinkCommitCoordinatorSingleFlightsConcurrentCommit(t *testing.T) {
	coordinator := hatSql.NewSQLSinkCommitCoordinator()
	commit := hatSql.SQLSinkCommit{
		Sink:          "warehouse",
		TransactionID: "txn-concurrent",
		Progress:      []hatSql.SQLSinkProgress{{Sink: "warehouse", Partition: "0", Frontier: 10}},
	}
	var calls atomic.Int32
	started := make(chan struct{})
	release := make(chan struct{})
	apply := func() error {
		if calls.Add(1) == 1 {
			close(started)
			<-release
		}
		return nil
	}
	results := make(chan bool, 2)
	errors := make(chan error, 2)
	for index := 0; index < 2; index++ {
		go func() {
			committed, err := coordinator.Commit(commit, apply)
			results <- committed
			errors <- err
		}()
	}
	<-started
	close(release)
	firstCommitted, firstErr := <-results, <-errors
	secondCommitted, secondErr := <-results, <-errors
	if firstErr != nil || secondErr != nil {
		t.Fatalf("concurrent Commit() errors = %v/%v", firstErr, secondErr)
	}
	if firstCommitted == secondCommitted {
		t.Fatalf("concurrent Commit() results = %t/%t, want one true and one false", firstCommitted, secondCommitted)
	}
	if calls.Load() != 1 {
		t.Fatalf("concurrent commit callback calls = %d, want 1", calls.Load())
	}
}

func TestSQLSinkCommitCoordinatorCleansUpPanickingCallback(t *testing.T) {
	coordinator := hatSql.NewSQLSinkCommitCoordinator()
	commit := hatSql.SQLSinkCommit{
		Sink:          "warehouse",
		TransactionID: "txn-panic",
		Progress:      []hatSql.SQLSinkProgress{{Sink: "warehouse", Partition: "0", Frontier: 10}},
	}
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("Commit() did not propagate callback panic")
			}
		}()
		_, _ = coordinator.Commit(commit, func() error { panic("sink callback panic") })
	}()
	if err := coordinator.Restore(nil); err != nil {
		t.Fatalf("Restore() after panic error = %v, want nil", err)
	}
	if committed, err := coordinator.Commit(commit, func() error { return nil }); err != nil || !committed {
		t.Fatalf("retry after panic = %t/%v, want true/nil", committed, err)
	}
}
