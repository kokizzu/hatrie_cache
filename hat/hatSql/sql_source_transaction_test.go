package hatSql_test

import (
	"errors"
	"testing"

	hatSql "hatrie_cache/hat/hatSql"
)

func TestSQLSourceOffsetTrackerAdvanceTransactionIsAtomic(t *testing.T) {
	tracker := hatSql.NewSQLSourceOffsetTracker()
	transaction := hatSql.SQLSourceTransaction{
		ID: "txn-1",
		Offsets: []hatSql.SQLSourceOffset{
			{Source: "events", Partition: "0", Offset: 10},
			{Source: "events", Partition: "1", Offset: 20},
		},
	}
	if advanced, err := tracker.AdvanceTransaction(transaction); err != nil || !advanced {
		t.Fatalf("AdvanceTransaction() = %t/%v, want true/nil", advanced, err)
	}

	stale := hatSql.SQLSourceTransaction{
		ID: "txn-2",
		Offsets: []hatSql.SQLSourceOffset{
			{Source: "events", Partition: "0", Offset: 11},
			{Source: "events", Partition: "1", Offset: 19},
		},
	}
	if advanced, err := tracker.AdvanceTransaction(stale); err != nil || advanced {
		t.Fatalf("stale AdvanceTransaction() = %t/%v, want false/nil", advanced, err)
	}
	if offset, found := tracker.Offset("events", "0"); !found || offset != 10 {
		t.Fatalf("partition 0 offset = %d/%t, want 10/true", offset, found)
	}
	if offset, found := tracker.Offset("events", "1"); !found || offset != 20 {
		t.Fatalf("partition 1 offset = %d/%t, want 20/true", offset, found)
	}
}

func TestSQLSourceOffsetTrackerAdvanceTransactionRejectsInvalidInput(t *testing.T) {
	tracker := hatSql.NewSQLSourceOffsetTracker()
	for _, transaction := range []hatSql.SQLSourceTransaction{
		{Offsets: []hatSql.SQLSourceOffset{{Source: "events", Partition: "0", Offset: 1}}},
		{ID: "txn-empty"},
		{ID: "txn-invalid", Offsets: []hatSql.SQLSourceOffset{{Source: "events", Partition: "", Offset: 1}}},
		{ID: "txn-duplicate", Offsets: []hatSql.SQLSourceOffset{
			{Source: "events", Partition: "0", Offset: 1},
			{Source: "events", Partition: "0", Offset: 2},
		}},
	} {
		if _, err := tracker.AdvanceTransaction(transaction); !errors.Is(err, hatSql.ErrSQLSourceTransactionInvalid) && !errors.Is(err, hatSql.ErrSQLSourceOffsetInvalid) && !errors.Is(err, hatSql.ErrSQLSourceOffsetDuplicate) {
			t.Fatalf("AdvanceTransaction(%#v) error = %v, want validation error", transaction, err)
		}
	}
	if snapshot := tracker.Snapshot(); len(snapshot) != 0 {
		t.Fatalf("snapshot after invalid transactions = %#v, want empty", snapshot)
	}
}
