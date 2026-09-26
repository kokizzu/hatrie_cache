package hatSql

import (
	"errors"
	"reflect"
	"testing"
)

func TestMZ035IncrementalMultisetSameKeyBatchAtomicAndNetDelta(t *testing.T) {
	multiset := NewIncrementalMultiset()
	row := Row{"id": int64(1), "value": "alpha"}
	if _, err := multiset.Apply([]DifferentialRow{{Key: "a", Diff: 2, Row: row}}); err != nil {
		t.Fatal(err)
	}

	changes, err := multiset.applySameKeyBatch([]DifferentialRow{
		{Key: "a", Time: 11, Diff: 3, Row: Row{"id": int64(1), "value": "alpha"}},
		{Key: "a", Time: 12, Diff: -1, Row: Row{"id": int64(1), "value": "alpha"}},
	})
	if err != nil {
		t.Fatalf("same-key batch error = %v", err)
	}
	if want := []DifferentialRow{{Key: "a", Time: 12, Diff: 2, Row: row}}; !reflect.DeepEqual(changes, want) {
		t.Fatalf("same-key changes = %#v, want %#v", changes, want)
	}
	if count, ok := multiset.Count("a"); !ok || count != 4 {
		t.Fatalf("same-key count = %d/%v, want 4/true", count, ok)
	}

	before := multiset.Snapshot()
	_, err = multiset.applySameKeyBatch([]DifferentialRow{
		{Key: "a", Diff: -5, Row: row},
		{Key: "a", Diff: 1, Row: row},
	})
	if !errors.Is(err, ErrIncrementalMultisetNegativeMultiplicity) {
		t.Fatalf("invalid same-key batch error = %v, want negative multiplicity", err)
	}
	if after := multiset.Snapshot(); !reflect.DeepEqual(after, before) {
		t.Fatalf("rejected same-key batch changed state: %#v -> %#v", before, after)
	}

	input := Row{"id": int64(2), "value": "beta"}
	if _, err := multiset.applySameKeyBatch([]DifferentialRow{{Key: "b", Diff: 2, Row: input}}); err != nil {
		t.Fatalf("published same-key batch error = %v", err)
	}
	input["value"] = "mutated"
	if got := multiset.Snapshot(); len(got) != 2 || got[1].Row["value"] != "beta" {
		t.Fatalf("same-key batch retained caller row: %#v", got)
	}
}
