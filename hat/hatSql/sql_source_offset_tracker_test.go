package hatSql_test

import (
	"errors"
	"reflect"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestSQLSourceOffsetTrackerAdvancesEachPartitionMonotonically(t *testing.T) {
	tracker := hatSql.NewSQLSourceOffsetTracker()
	if advanced, err := tracker.Advance(hatSql.SQLSourceOffset{Source: "events", Partition: "0", Offset: 4}); err != nil || !advanced {
		t.Fatalf("first Advance() = %t, %v; want true, nil", advanced, err)
	}
	for _, offset := range []uint64{4, 3} {
		if advanced, err := tracker.Advance(hatSql.SQLSourceOffset{Source: "events", Partition: "0", Offset: offset}); err != nil || advanced {
			t.Fatalf("replayed Advance(%d) = %t, %v; want false, nil", offset, advanced, err)
		}
	}
	if advanced, err := tracker.Advance(hatSql.SQLSourceOffset{Source: "events", Partition: "0", Offset: 5}); err != nil || !advanced {
		t.Fatalf("next Advance() = %t, %v; want true, nil", advanced, err)
	}
	if advanced, err := tracker.Advance(hatSql.SQLSourceOffset{Source: "events", Partition: "1", Offset: 1}); err != nil || !advanced {
		t.Fatalf("independent partition Advance() = %t, %v; want true, nil", advanced, err)
	}

	if offset, found := tracker.Offset("events", "0"); !found || offset != 5 {
		t.Fatalf("partition 0 offset = %d, %t; want 5, true", offset, found)
	}
	want := []hatSql.SQLSourceOffset{
		{Source: "events", Partition: "0", Offset: 5},
		{Source: "events", Partition: "1", Offset: 1},
	}
	if got := tracker.Snapshot(); !reflect.DeepEqual(got, want) {
		t.Fatalf("Snapshot() = %#v, want %#v", got, want)
	}
}

func TestSQLSourceOffsetTrackerBatchIsAtomicAndRestorable(t *testing.T) {
	tracker := hatrieSQLSourceOffsetTracker(t)
	if advanced, err := tracker.AdvanceBatch([]hatSql.SQLSourceOffset{
		{Source: "events", Partition: "0", Offset: 8},
		{Source: "events", Partition: "1", Offset: 2},
	}); err != nil || advanced != 2 {
		t.Fatalf("AdvanceBatch() = %d, %v; want 2, nil", advanced, err)
	}
	before := tracker.Snapshot()
	if _, err := tracker.AdvanceBatch([]hatSql.SQLSourceOffset{
		{Source: "events", Partition: "0", Offset: 9},
		{Source: "events", Partition: "0", Offset: 10},
	}); !errors.Is(err, hatSql.ErrSQLSourceOffsetDuplicate) {
		t.Fatalf("duplicate batch error = %v, want duplicate error", err)
	}
	if got := tracker.Snapshot(); !reflect.DeepEqual(got, before) {
		t.Fatalf("snapshot after rejected batch = %#v, want %#v", got, before)
	}

	restored := hatSql.NewSQLSourceOffsetTracker()
	if err := restored.Restore(before); err != nil {
		t.Fatal(err)
	}
	if got := restored.Snapshot(); !reflect.DeepEqual(got, before) {
		t.Fatalf("restored snapshot = %#v, want %#v", got, before)
	}
}

func TestSQLSourceOffsetTrackerRejectsInvalidInputWithoutMutation(t *testing.T) {
	tracker := hatSql.NewSQLSourceOffsetTracker()
	for _, offset := range []hatSql.SQLSourceOffset{
		{Partition: "0", Offset: 1},
		{Source: "events", Offset: 1},
	} {
		if _, err := tracker.Advance(offset); !errors.Is(err, hatSql.ErrSQLSourceOffsetInvalid) {
			t.Fatalf("invalid Advance(%#v) error = %v, want invalid error", offset, err)
		}
	}
	if got := tracker.Snapshot(); len(got) != 0 {
		t.Fatalf("snapshot after invalid input = %#v, want empty", got)
	}
}

func hatrieSQLSourceOffsetTracker(t *testing.T) *hatSql.SQLSourceOffsetTracker {
	t.Helper()
	return hatSql.NewSQLSourceOffsetTracker()
}
