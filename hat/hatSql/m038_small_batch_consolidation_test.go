package hatSql

import (
	"errors"
	"math"
	"reflect"
	"testing"
)

func TestMZ038SmallBatchConsolidationMatchesGenericAndIsAtomic(t *testing.T) {
	updates := []DifferentialRow{
		{Key: "b", Time: 21, Diff: 2, Row: Row{"id": int64(2), "value": "beta"}},
		{Key: "a", Time: 22, Diff: 1, Row: Row{"id": int64(1), "value": "alpha"}},
		{Key: "b", Time: 23, Diff: -1, Row: Row{"id": int64(2), "value": "beta"}},
		{Key: "a", Time: 24, Diff: 2, Row: Row{"id": int64(1), "value": "alpha"}},
	}

	generic := NewIncrementalMultiset()
	slice := NewIncrementalMultiset()
	want, err := generic.applyGenericBatch(updates)
	if err != nil {
		t.Fatalf("generic batch error = %v", err)
	}
	got, err := slice.applySmallBatch(updates)
	if err != nil {
		t.Fatalf("small batch error = %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("small batch changes = %#v, want %#v", got, want)
	}
	if !reflect.DeepEqual(slice.Snapshot(), generic.Snapshot()) {
		t.Fatalf("small batch state = %#v, want %#v", slice.Snapshot(), generic.Snapshot())
	}

	before := slice.Snapshot()
	_, err = slice.applySmallBatch([]DifferentialRow{
		{Key: "a", Diff: -100, Row: Row{"id": int64(1), "value": "alpha"}},
		{Key: "b", Diff: 1, Row: Row{"id": int64(2), "value": "beta"}},
	})
	if !errors.Is(err, ErrIncrementalMultisetNegativeMultiplicity) {
		t.Fatalf("invalid small batch error = %v, want negative multiplicity", err)
	}
	if after := slice.Snapshot(); !reflect.DeepEqual(after, before) {
		t.Fatalf("rejected small batch changed state: %#v -> %#v", before, after)
	}
}

func TestMZ038SmallBatchConsolidationPreservesInputIsolationAndOverflowAtomicity(t *testing.T) {
	multiset := NewIncrementalMultiset()
	first := Row{"id": int64(1), "value": "alpha"}
	second := Row{"id": int64(2), "value": "beta"}
	if _, err := multiset.Apply([]DifferentialRow{
		{Key: "b", Diff: 1, Row: first},
		{Key: "a", Diff: 1, Row: second},
	}); err != nil {
		t.Fatal(err)
	}
	first["value"] = "changed"
	second["value"] = "changed"
	if snapshot := multiset.Snapshot(); snapshot[0].Row["value"] != "beta" || snapshot[1].Row["value"] != "alpha" {
		t.Fatalf("small batch retained caller rows: %#v", snapshot)
	}

	before := multiset.Snapshot()
	multiset.counts["c"] = math.MaxInt64
	multiset.rows["c"] = Row{"id": int64(3), "value": "gamma"}
	multiset.times["c"] = 3
	_, err := multiset.applySmallBatch([]DifferentialRow{
		{Key: "c", Diff: 1, Row: Row{"id": int64(3), "value": "gamma"}},
		{Key: "a", Diff: 1, Row: Row{"id": int64(2), "value": "beta"}},
	})
	if !errors.Is(err, ErrIncrementalMultisetOverflow) {
		t.Fatalf("overflow small batch error = %v, want overflow", err)
	}
	if got := multiset.Snapshot(); !reflect.DeepEqual(got[:2], before) || got[2].Diff != math.MaxInt64 {
		t.Fatalf("overflow rejection changed state: %#v", got)
	}
}
