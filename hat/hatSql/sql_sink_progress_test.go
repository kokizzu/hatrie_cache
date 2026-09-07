package hatSql_test

import (
	"errors"
	"testing"

	hatSql "hatrie_cache/hat/hatSql"
)

func TestSQLSinkProgressTrackerAcknowledgeIsMonotoneAndRestorable(t *testing.T) {
	tracker := hatSql.NewSQLSinkProgressTracker()
	if acknowledged, err := tracker.Acknowledge(hatSql.SQLSinkProgress{Sink: "warehouse", Partition: "1", Frontier: 20}); err != nil || !acknowledged {
		t.Fatalf("Acknowledge() = %t/%v, want true/nil", acknowledged, err)
	}
	if acknowledged, err := tracker.Acknowledge(hatSql.SQLSinkProgress{Sink: "warehouse", Partition: "0", Frontier: 10}); err != nil || !acknowledged {
		t.Fatalf("Acknowledge() = %t/%v, want true/nil", acknowledged, err)
	}
	if acknowledged, err := tracker.Acknowledge(hatSql.SQLSinkProgress{Sink: "warehouse", Partition: "1", Frontier: 19}); err != nil || acknowledged {
		t.Fatalf("stale Acknowledge() = %t/%v, want false/nil", acknowledged, err)
	}
	if frontier, found := tracker.Frontier("warehouse", "1"); !found || frontier != 20 {
		t.Fatalf("Frontier() = %d/%t, want 20/true", frontier, found)
	}
	want := []hatSql.SQLSinkProgress{
		{Sink: "warehouse", Partition: "0", Frontier: 10},
		{Sink: "warehouse", Partition: "1", Frontier: 20},
	}
	if got := tracker.Snapshot(); len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("Snapshot() = %#v, want %#v", got, want)
	}

	restored := hatSql.NewSQLSinkProgressTracker()
	if err := restored.Restore(tracker.Snapshot()); err != nil {
		t.Fatalf("Restore() error = %v", err)
	}
	if got := restored.Snapshot(); len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("restored Snapshot() = %#v, want %#v", got, want)
	}
}

func TestSQLSinkProgressTrackerBatchIsAtomicAndValidatesInput(t *testing.T) {
	tracker := hatSql.NewSQLSinkProgressTracker()
	if count, err := tracker.AcknowledgeBatch([]hatSql.SQLSinkProgress{
		{Sink: "warehouse", Partition: "0", Frontier: 8},
		{Sink: "warehouse", Partition: "1", Frontier: 9},
	}); err != nil || count != 2 {
		t.Fatalf("AcknowledgeBatch() = %d/%v, want 2/nil", count, err)
	}
	if _, err := tracker.AcknowledgeBatch([]hatSql.SQLSinkProgress{
		{Sink: "warehouse", Partition: "0", Frontier: 10},
		{Sink: "warehouse", Partition: "0", Frontier: 11},
	}); !errors.Is(err, hatSql.ErrSQLSinkProgressDuplicate) {
		t.Fatalf("duplicate batch error = %v, want duplicate error", err)
	}
	if _, err := tracker.AcknowledgeBatch([]hatSql.SQLSinkProgress{
		{Sink: "warehouse", Partition: "0", Frontier: 10},
		{Sink: "", Partition: "1", Frontier: 11},
	}); !errors.Is(err, hatSql.ErrSQLSinkProgressInvalid) {
		t.Fatalf("invalid batch error = %v, want invalid error", err)
	}
	if frontier, found := tracker.Frontier("warehouse", "0"); !found || frontier != 8 {
		t.Fatalf("frontier after rejected batches = %d/%t, want 8/true", frontier, found)
	}
}
