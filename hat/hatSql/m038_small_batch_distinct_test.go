package hatSql

import (
	"errors"
	"reflect"
	"testing"
)

func TestMZ038IncrementalDistinctSmallBatchMatchesGenericAndIsAtomic(t *testing.T) {
	updates := []DifferentialRow{
		{Key: "b", Time: 21, Diff: 1, Row: Row{"id": int64(2), "value": "beta"}},
		{Key: "b", Time: 22, Diff: -1, Row: Row{"id": int64(2), "value": "beta"}},
		{Key: "a", Time: 23, Diff: 1, Row: Row{"id": int64(1), "value": "alpha"}},
		{Key: "a", Time: 24, Diff: 1, Row: Row{"id": int64(1), "value": "alpha"}},
	}

	generic := NewIncrementalDistinct()
	slice := NewIncrementalDistinct()
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
	if !reflect.DeepEqual(slice.counts, generic.counts) || !reflect.DeepEqual(slice.rows, generic.rows) || !reflect.DeepEqual(slice.times, generic.times) {
		t.Fatalf("small batch state differs: counts=%#v/%#v rows=%#v/%#v times=%#v/%#v", slice.counts, generic.counts, slice.rows, generic.rows, slice.times, generic.times)
	}
	if got[0].Key != "b" || got[0].Diff != 1 || got[1].Key != "b" || got[1].Diff != -1 {
		t.Fatalf("small batch changed input order: %#v", got)
	}

	before := slice.Snapshot()
	_, err = slice.applySmallBatch([]DifferentialRow{
		{Key: "a", Diff: -100},
		{Key: "b", Diff: 1},
	})
	if !errors.Is(err, ErrIncrementalDistinctNegativeMultiplicity) {
		t.Fatalf("invalid small batch error = %v, want negative multiplicity", err)
	}
	if after := slice.Snapshot(); !reflect.DeepEqual(after, before) {
		t.Fatalf("rejected small batch changed state: %#v -> %#v", before, after)
	}
}

func TestMZ038IncrementalDistinctSmallBatchOwnsRows(t *testing.T) {
	distinct := NewIncrementalDistinct()
	input := Row{"id": int64(1), "value": []byte("alpha")}
	if _, err := distinct.applySmallBatch([]DifferentialRow{{Key: "a", Diff: 1, Row: input}, {Key: "b", Diff: 1, Row: Row{"id": int64(2)}}}); err != nil {
		t.Fatal(err)
	}
	input["value"].([]byte)[0] = 'x'
	if got := distinct.Snapshot()[0].Row["value"].([]byte); string(got) != "alpha" {
		t.Fatalf("small batch retained caller row = %q, want alpha", got)
	}
}
