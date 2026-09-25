package hatSql

import (
	"errors"
	"reflect"
	"testing"
)

func TestMZ035IncrementalMultisetPreservesMultiplicityAndEmitsDeltas(t *testing.T) {
	multiset := NewIncrementalMultiset()
	row := Row{"id": int64(1), "value": "alpha"}

	changes, err := multiset.Apply([]DifferentialRow{{Key: "a", Time: 7, Diff: 2, Row: row}})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(changes, []DifferentialRow{{Key: "a", Time: 7, Diff: 2, Row: row}}) {
		t.Fatalf("initial changes = %#v", changes)
	}
	row["value"] = "mutated"

	changes, err = multiset.Apply([]DifferentialRow{{Key: "a", Time: 8, Diff: 3, Row: Row{"id": int64(1), "value": "alpha"}}})
	if err != nil {
		t.Fatal(err)
	}
	if len(changes) != 1 || changes[0].Diff != 3 {
		t.Fatalf("second changes = %#v, want +3", changes)
	}
	if count, ok := multiset.Count("a"); !ok || count != 5 {
		t.Fatalf("Count(a) = %d/%v, want 5/true", count, ok)
	}

	changes, err = multiset.Apply([]DifferentialRow{
		{Key: "a", Time: 9, Diff: -1, Row: Row{"id": int64(1), "value": "alpha"}},
		{Key: "a", Time: 9, Diff: -1, Row: Row{"id": int64(1), "value": "alpha"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(changes) != 1 || changes[0].Diff != -2 {
		t.Fatalf("retraction changes = %#v, want -2", changes)
	}
	snapshot := multiset.Snapshot()
	if len(snapshot) != 1 || snapshot[0].Diff != 3 || snapshot[0].Row["value"] != "alpha" {
		t.Fatalf("snapshot = %#v, want multiplicity 3 and cloned alpha row", snapshot)
	}

	snapshot[0].Row["value"] = "changed"
	if multiset.Snapshot()[0].Row["value"] != "alpha" {
		t.Fatal("Snapshot() leaked mutable row state")
	}
}

func TestMZ035IncrementalMultisetAppliesBatchesAtomically(t *testing.T) {
	multiset := NewIncrementalMultiset()
	if _, err := multiset.Apply([]DifferentialRow{{Key: "a", Diff: 2, Row: Row{"id": int64(1)}}}); err != nil {
		t.Fatal(err)
	}

	_, err := multiset.Apply([]DifferentialRow{
		{Key: "b", Diff: 1, Row: Row{"id": int64(2)}},
		{Key: "a", Diff: -3, Row: Row{"id": int64(1)}},
	})
	if !errors.Is(err, ErrIncrementalMultisetNegativeMultiplicity) {
		t.Fatalf("invalid batch error = %v, want negative multiplicity", err)
	}
	if count, ok := multiset.Count("a"); !ok || count != 2 {
		t.Fatalf("a after rejected batch = %d/%v, want 2/true", count, ok)
	}
	if _, ok := multiset.Count("b"); ok {
		t.Fatal("rejected batch published key b")
	}
}

func TestMZ035IncrementalMultisetRejectsConflictsAndOverflow(t *testing.T) {
	multiset := NewIncrementalMultiset()
	if _, err := multiset.Apply([]DifferentialRow{{Key: "a", Diff: 1, Row: Row{"id": int64(1)}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := multiset.Apply([]DifferentialRow{{Key: "a", Diff: 1, Row: Row{"id": int64(2)}}}); !errors.Is(err, ErrIncrementalMultisetRowConflict) {
		t.Fatalf("row conflict error = %v", err)
	}
	if _, err := multiset.Apply([]DifferentialRow{{Key: "a", Diff: -1, Row: Row{"id": int64(2)}}}); !errors.Is(err, ErrIncrementalMultisetRowConflict) {
		t.Fatalf("retraction row conflict error = %v", err)
	}

	max := NewIncrementalMultiset()
	if _, err := max.Apply([]DifferentialRow{{Key: "max", Diff: int64(^uint64(0) >> 1), Row: Row{"id": int64(3)}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := max.Apply([]DifferentialRow{{Key: "max", Diff: 1, Row: Row{"id": int64(3)}}}); !errors.Is(err, ErrIncrementalMultisetOverflow) {
		t.Fatalf("overflow error = %v", err)
	}
}

func TestMZ035IncrementalMultisetRejectsInvalidKeysAndTreatsZeroDiffAsNoOp(t *testing.T) {
	multiset := NewIncrementalMultiset()

	if _, err := multiset.Apply([]DifferentialRow{{Diff: 1}}); err != ErrIncrementalMultisetInvalidKey {
		t.Fatalf("empty key error = %v, want %v", err, ErrIncrementalMultisetInvalidKey)
	}
	if changes, err := multiset.Apply([]DifferentialRow{{Key: "ignored", Diff: 0}}); err != nil || changes != nil {
		t.Fatalf("zero-diff update = %#v, %v; want nil, nil", changes, err)
	}
	if count, ok := multiset.Count("ignored"); ok || count != 0 {
		t.Fatalf("zero-diff count = %d, %v; want 0, false", count, ok)
	}
}
