package hatSql

import (
	"errors"
	"reflect"
	"testing"
)

func TestSQLSourceFrontierTrackerReportsCommonFrontier(t *testing.T) {
	tracker, err := NewSQLSourceFrontierTracker([]SQLSourceFrontierPartition{
		{Source: "orders", Partition: "1"},
		{Source: "orders", Partition: "0"},
	})
	if err != nil {
		t.Fatalf("NewSQLSourceFrontierTracker() error = %v", err)
	}
	if frontier, ready := tracker.CommonFrontier(); ready || frontier != 0 {
		t.Fatalf("initial CommonFrontier() = %d/%v, want 0/false", frontier, ready)
	}
	if changed, err := tracker.Observe(SQLSourceFrontier{Source: "orders", Partition: "0", Frontier: 10}); err != nil || !changed {
		t.Fatalf("Observe(partition 0) = %v/%v, want true/nil", changed, err)
	}
	if frontier, ready := tracker.CommonFrontier(); ready || frontier != 0 {
		t.Fatalf("partial CommonFrontier() = %d/%v, want 0/false", frontier, ready)
	}
	if changed, err := tracker.ObserveBatch([]SQLSourceFrontier{{Source: "orders", Partition: "1", Frontier: 12}}); err != nil || changed != 1 {
		t.Fatalf("ObserveBatch(partition 1) = %d/%v, want 1/nil", changed, err)
	}
	if frontier, ready := tracker.CommonFrontier(); !ready || frontier != 10 {
		t.Fatalf("CommonFrontier() = %d/%v, want 10/true", frontier, ready)
	}
	if !tracker.ReadyAt(10) || tracker.ReadyAt(11) {
		t.Fatal("ReadyAt() did not enforce the minimum observed frontier")
	}
	if changed, err := tracker.Observe(SQLSourceFrontier{Source: "orders", Partition: "0", Frontier: 10}); err != nil || changed {
		t.Fatalf("replayed Observe() = %v/%v, want false/nil", changed, err)
	}
	if changed, err := tracker.Observe(SQLSourceFrontier{Source: "orders", Partition: "0", Frontier: 15}); err != nil || !changed {
		t.Fatalf("advanced Observe() = %v/%v, want true/nil", changed, err)
	}
	if frontier, ready := tracker.CommonFrontier(); !ready || frontier != 12 {
		t.Fatalf("advanced CommonFrontier() = %d/%v, want 12/true", frontier, ready)
	}
}

func TestSQLSourceFrontierTrackerSnapshotIsSortedAndBatchValidationIsAtomic(t *testing.T) {
	tracker, err := NewSQLSourceFrontierTracker([]SQLSourceFrontierPartition{
		{Source: "z", Partition: "0"},
		{Source: "a", Partition: "1"},
	})
	if err != nil {
		t.Fatalf("NewSQLSourceFrontierTracker() error = %v", err)
	}
	if _, err := tracker.Observe(SQLSourceFrontier{Source: "a", Partition: "1", Frontier: 4}); err != nil {
		t.Fatalf("Observe() error = %v", err)
	}
	before := tracker.Snapshot()
	_, err = tracker.ObserveBatch([]SQLSourceFrontier{
		{Source: "z", Partition: "0", Frontier: 5},
		{Source: "missing", Partition: "0", Frontier: 8},
	})
	if !errors.Is(err, ErrSQLSourceFrontierUnknownPartition) {
		t.Fatalf("ObserveBatch(unknown) error = %v, want unknown partition", err)
	}
	if got := tracker.Snapshot(); !reflect.DeepEqual(got, before) {
		t.Fatalf("snapshot changed after rejected batch: got %#v, before %#v", got, before)
	}
	_, err = tracker.ObserveBatch([]SQLSourceFrontier{
		{Source: "a", Partition: "1", Frontier: 5},
		{Source: "a", Partition: "1", Frontier: 6},
	})
	if !errors.Is(err, ErrSQLSourceFrontierDuplicate) {
		t.Fatalf("ObserveBatch(duplicate) error = %v, want duplicate error", err)
	}
	if got := tracker.Snapshot(); !reflect.DeepEqual(got, before) {
		t.Fatalf("snapshot changed after duplicate batch: got %#v, before %#v", got, before)
	}
	if got := tracker.Snapshot(); !reflect.DeepEqual(got, []SQLSourceFrontierSnapshot{
		{Source: "a", Partition: "1", Frontier: 4, Observed: true},
		{Source: "z", Partition: "0", Observed: false},
	}) {
		t.Fatalf("Snapshot() = %#v, want deterministic partition order", got)
	}
}

func TestSQLSourceFrontierTrackerValidatesPartitionsAndNilReceiver(t *testing.T) {
	for name, partitions := range map[string][]SQLSourceFrontierPartition{
		"empty":   nil,
		"invalid": {{Source: "", Partition: "0"}},
		"dupe":    {{Source: "a", Partition: "0"}, {Source: "a", Partition: "0"}},
	} {
		if _, err := NewSQLSourceFrontierTracker(partitions); !errors.Is(err, ErrSQLSourceFrontierInvalidPartitions) {
			t.Errorf("%s constructor error = %v, want invalid partitions", name, err)
		}
	}
	var tracker *SQLSourceFrontierTracker
	if changed, err := tracker.Observe(SQLSourceFrontier{Source: "a", Partition: "0", Frontier: 1}); !errors.Is(err, ErrSQLSourceFrontierTrackerNil) || changed {
		t.Fatalf("nil Observe() = %v/%v, want false/nil tracker error", changed, err)
	}
	if changed, err := tracker.ObserveBatch(nil); !errors.Is(err, ErrSQLSourceFrontierTrackerNil) || changed != 0 {
		t.Fatalf("nil ObserveBatch() = %d/%v, want zero/nil tracker error", changed, err)
	}
	if frontier, ready := tracker.CommonFrontier(); ready || frontier != 0 {
		t.Fatalf("nil CommonFrontier() = %d/%v, want 0/false", frontier, ready)
	}
}
