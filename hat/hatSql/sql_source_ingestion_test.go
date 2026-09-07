package hatSql_test

import (
	"errors"
	"sync/atomic"
	"testing"

	hatSql "hatrie_cache/hat/hatSql"
)

func TestSQLSourceIngestionCoordinatorRunsOnceAndRestores(t *testing.T) {
	ingestion := hatSql.SQLSourceIngestion{
		Source: "events",
		Transaction: hatSql.SQLSourceTransaction{
			ID: "txn-1",
			Offsets: []hatSql.SQLSourceOffset{
				{Source: "events", Partition: "1", Offset: 20},
				{Source: "events", Partition: "0", Offset: 10},
			},
		},
	}
	coordinator := hatSql.NewSQLSourceIngestionCoordinator()
	calls := 0
	if ingested, err := coordinator.Ingest(ingestion, func() error {
		calls++
		return nil
	}); err != nil || !ingested {
		t.Fatalf("first Ingest() = %t/%v, want true/nil", ingested, err)
	}
	if ingested, err := coordinator.Ingest(ingestion, func() error {
		calls++
		return nil
	}); err != nil || ingested {
		t.Fatalf("duplicate Ingest() = %t/%v, want false/nil", ingested, err)
	}
	if calls != 1 {
		t.Fatalf("ingest callback calls = %d, want 1", calls)
	}
	snapshot := coordinator.Snapshot()
	if len(snapshot) != 1 || snapshot[0].Source != "events" || snapshot[0].Transaction.ID != "txn-1" || snapshot[0].Transaction.Offsets[0].Partition != "0" {
		t.Fatalf("Snapshot() = %#v, want deterministic committed snapshot", snapshot)
	}
	restored := hatSql.NewSQLSourceIngestionCoordinator()
	if err := restored.Restore(snapshot); err != nil {
		t.Fatalf("Restore() error = %v", err)
	}
	if ingested, err := restored.Ingest(ingestion, func() error {
		calls++
		return nil
	}); err != nil || ingested {
		t.Fatalf("restored duplicate Ingest() = %t/%v, want false/nil", ingested, err)
	}
	if calls != 1 {
		t.Fatalf("restored ingest callback calls = %d, want 1", calls)
	}
}

func TestSQLSourceIngestionCoordinatorRejectsConflictsAndRetriesFailures(t *testing.T) {
	coordinator := hatSql.NewSQLSourceIngestionCoordinator()
	ingestion := hatSql.SQLSourceIngestion{
		Source: "events",
		Transaction: hatSql.SQLSourceTransaction{
			ID:      "txn-1",
			Offsets: []hatSql.SQLSourceOffset{{Source: "events", Partition: "0", Offset: 10}},
		},
	}
	boom := errors.New("source unavailable")
	if ingested, err := coordinator.Ingest(ingestion, func() error { return boom }); !errors.Is(err, boom) || ingested {
		t.Fatalf("failed Ingest() = %t/%v, want false/boom", ingested, err)
	}
	if ingested, err := coordinator.Ingest(ingestion, func() error { return nil }); err != nil || !ingested {
		t.Fatalf("retry Ingest() = %t/%v, want true/nil", ingested, err)
	}
	conflict := ingestion
	conflict.Transaction.Offsets = []hatSql.SQLSourceOffset{{Source: "events", Partition: "0", Offset: 11}}
	if _, err := coordinator.Ingest(conflict, func() error { return nil }); !errors.Is(err, hatSql.ErrSQLSourceIngestionConflict) {
		t.Fatalf("conflicting Ingest() error = %v, want conflict", err)
	}
}

func TestSQLSourceIngestionCoordinatorRejectsInvalidInput(t *testing.T) {
	coordinator := hatSql.NewSQLSourceIngestionCoordinator()
	for _, ingestion := range []hatSql.SQLSourceIngestion{
		{Transaction: hatSql.SQLSourceTransaction{ID: "txn-empty", Offsets: []hatSql.SQLSourceOffset{{Source: "events", Partition: "0", Offset: 1}}}},
		{Source: "events", Transaction: hatSql.SQLSourceTransaction{Offsets: []hatSql.SQLSourceOffset{{Source: "events", Partition: "0", Offset: 1}}}},
		{Source: "events", Transaction: hatSql.SQLSourceTransaction{ID: "txn-source-mismatch", Offsets: []hatSql.SQLSourceOffset{{Source: "other", Partition: "0", Offset: 1}}}},
		{Source: "events", Transaction: hatSql.SQLSourceTransaction{ID: "txn-duplicate", Offsets: []hatSql.SQLSourceOffset{
			{Source: "events", Partition: "0", Offset: 1},
			{Source: "events", Partition: "0", Offset: 2},
		}}},
	} {
		if _, err := coordinator.Ingest(ingestion, func() error { return nil }); !errors.Is(err, hatSql.ErrSQLSourceIngestionInvalid) {
			t.Fatalf("invalid Ingest(%#v) error = %v, want invalid", ingestion, err)
		}
	}
	if len(coordinator.Snapshot()) != 0 {
		t.Fatal("invalid ingestions changed coordinator state")
	}
}

func TestSQLSourceIngestionCoordinatorSingleFlightsConcurrentIngest(t *testing.T) {
	coordinator := hatSql.NewSQLSourceIngestionCoordinator()
	ingestion := hatSql.SQLSourceIngestion{
		Source: "events",
		Transaction: hatSql.SQLSourceTransaction{
			ID:      "txn-concurrent",
			Offsets: []hatSql.SQLSourceOffset{{Source: "events", Partition: "0", Offset: 10}},
		},
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
			ingested, err := coordinator.Ingest(ingestion, apply)
			results <- ingested
			errors <- err
		}()
	}
	<-started
	close(release)
	firstIngested, firstErr := <-results, <-errors
	secondIngested, secondErr := <-results, <-errors
	if firstErr != nil || secondErr != nil {
		t.Fatalf("concurrent Ingest() errors = %v/%v", firstErr, secondErr)
	}
	if firstIngested == secondIngested {
		t.Fatalf("concurrent Ingest() results = %t/%t, want one true and one false", firstIngested, secondIngested)
	}
	if calls.Load() != 1 {
		t.Fatalf("concurrent ingest callback calls = %d, want 1", calls.Load())
	}
}

func TestSQLSourceIngestionCoordinatorCleansUpPanickingCallback(t *testing.T) {
	coordinator := hatSql.NewSQLSourceIngestionCoordinator()
	ingestion := hatSql.SQLSourceIngestion{
		Source: "events",
		Transaction: hatSql.SQLSourceTransaction{
			ID:      "txn-panic",
			Offsets: []hatSql.SQLSourceOffset{{Source: "events", Partition: "0", Offset: 10}},
		},
	}
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("Ingest() did not propagate callback panic")
			}
		}()
		_, _ = coordinator.Ingest(ingestion, func() error { panic("source callback panic") })
	}()
	if err := coordinator.Restore(nil); err != nil {
		t.Fatalf("Restore() after panic error = %v, want nil", err)
	}
	if ingested, err := coordinator.Ingest(ingestion, func() error { return nil }); err != nil || !ingested {
		t.Fatalf("retry after panic = %t/%v, want true/nil", ingested, err)
	}
}
