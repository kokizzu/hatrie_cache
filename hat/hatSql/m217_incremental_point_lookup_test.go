package hatSql

import (
	"errors"
	"reflect"
	"strings"
	"testing"
)

func TestM217IncrementalPointLookupMaintainsCompleteRowsAndMultiplicity(t *testing.T) {
	index, err := NewIncrementalPointLookup(IncrementalPointLookupDefinition{
		IndexKey: func(row Row) (string, error) {
			return row["team"].(string), nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	rowA := Row{"id": "a", "team": "red", "score": int64(10)}
	rowB := Row{"id": "b", "team": "red", "score": int64(20)}
	if err := index.Apply([]DifferentialRow{
		{Key: "a", Time: 1, Diff: 2, Row: rowA},
		{Key: "b", Time: 1, Diff: 1, Row: rowB},
	}); err != nil {
		t.Fatal(err)
	}
	rowA["score"] = int64(999)
	got := index.Lookup("red")
	want := []DifferentialRow{
		{Key: "a", Time: 1, Diff: 2, Row: Row{"id": "a", "team": "red", "score": int64(10)}},
		{Key: "b", Time: 1, Diff: 1, Row: rowB},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Lookup(red) = %#v, want %#v", got, want)
	}
	got[0].Row["score"] = int64(-1)
	if score := index.Lookup("red")[0].Row["score"]; score != int64(10) {
		t.Fatalf("lookup result mutated retained row: score = %#v", score)
	}

	if err := index.Apply([]DifferentialRow{{Key: "a", Time: 2, Diff: -1}}); err != nil {
		t.Fatal(err)
	}
	if got := index.Lookup("red")[0].Diff; got != 1 {
		t.Fatalf("remaining multiplicity = %d, want 1", got)
	}
	if err := index.Apply([]DifferentialRow{
		{Key: "a", Time: 3, Diff: -1},
		{Key: "a", Time: 4, Diff: 1, Row: Row{"id": "a", "team": "blue", "score": int64(30)}},
	}); err != nil {
		t.Fatal(err)
	}
	if got := index.Lookup("red"); len(got) != 1 || got[0].Key != "b" {
		t.Fatalf("red after key move = %#v, want only b", got)
	}
	if got := index.Lookup("blue"); len(got) != 1 || got[0].Key != "a" {
		t.Fatalf("blue after key move = %#v, want a", got)
	}
}

func TestM217IncrementalPointLookupRejectsInvalidBatchAtomically(t *testing.T) {
	index, err := NewIncrementalPointLookup(IncrementalPointLookupDefinition{
		IndexKey: func(row Row) (string, error) {
			value, ok := row["team"].(string)
			if !ok {
				return "", errors.New("team must be a string")
			}
			return value, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := index.Apply([]DifferentialRow{{Key: "a", Diff: 1, Row: Row{"team": "red"}}}); err != nil {
		t.Fatal(err)
	}
	before := index.Snapshot()
	if err := index.Apply([]DifferentialRow{
		{Key: "a", Diff: -1},
		{Key: "missing", Diff: -1},
	}); !errors.Is(err, ErrIncrementalPointLookupNegativeMultiplicity) {
		t.Fatalf("invalid batch error = %v, want negative multiplicity", err)
	}
	if got := index.Snapshot(); !reflect.DeepEqual(got, before) {
		t.Fatalf("snapshot after rejected batch = %#v, want %#v", got, before)
	}
	if err := index.Apply([]DifferentialRow{{Key: "bad", Diff: 1, Row: Row{"team": "red"}}}); err != nil {
		t.Fatal(err)
	}
	if err := index.Apply([]DifferentialRow{{Key: "bad", Diff: 1, Row: Row{"team": "blue"}}}); !errors.Is(err, ErrIncrementalPointLookupRowConflict) {
		t.Fatalf("row conflict error = %v, want row conflict", err)
	}
}

func TestM217IncrementalPointLookupRequiresKeyAndHandlesEmptyLookup(t *testing.T) {
	if _, err := NewIncrementalPointLookup(IncrementalPointLookupDefinition{}); !errors.Is(err, ErrIncrementalPointLookupKeyRequired) {
		t.Fatalf("missing key error = %v, want key required", err)
	}
	index, err := NewIncrementalPointLookup(IncrementalPointLookupDefinition{
		IndexKey: func(Row) (string, error) { return "", nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := index.Lookup("missing"); got != nil {
		t.Fatalf("empty lookup = %#v, want nil", got)
	}
	if err := index.Apply([]DifferentialRow{{Key: "a", Diff: 1, Row: Row{}}}); err != nil {
		t.Fatal(err)
	}
	if got := index.Lookup(""); len(got) != 1 {
		t.Fatalf("empty index-key lookup length = %d, want 1", len(got))
	}
	if !strings.Contains(index.String(), "entries=1") {
		t.Fatalf("String() = %q, want entry count", index.String())
	}
}
