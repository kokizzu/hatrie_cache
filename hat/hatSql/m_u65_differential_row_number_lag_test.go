package hatSql

import (
	"errors"
	"reflect"
	"testing"
)

func TestM065DifferentialRowNumberLagRetractsAndCorrects(t *testing.T) {
	window, err := NewDifferentialRowNumberLagWindow(DifferentialRowNumberLagWindowOptions{
		PartitionKey: func(row SQLRow) string { return row["partition"].(string) },
		Lag:          1,
		MaxRows:      16,
	})
	if err != nil {
		t.Fatal(err)
	}
	rowA := Row{"partition": "p", "value": "a"}
	rowB := Row{"partition": "p", "value": "b"}
	got, err := window.Apply([]DifferentialRow{
		{Key: "a", Time: 10, Diff: 1, Row: rowA},
		{Key: "b", Time: 20, Diff: 1, Row: rowB},
	})
	if err != nil {
		t.Fatal(err)
	}
	want := []DifferentialRowNumberLagRow{
		{Key: "a", Time: 10, Ordinal: 0, Diff: 1, Partition: "p", RowNumber: 1, Row: rowA},
		{Key: "b", Time: 20, Ordinal: 0, Diff: 1, Partition: "p", RowNumber: 2, Row: rowB, HasLag: true, LagRow: rowA},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("insert output = %#v, want %#v", got, want)
	}

	got, err = window.Apply([]DifferentialRow{{Key: "a", Time: 10, Diff: -1}})
	if err != nil {
		t.Fatal(err)
	}
	want = []DifferentialRowNumberLagRow{
		{Key: "a", Time: 10, Ordinal: 0, Diff: -1, Partition: "p", RowNumber: 1, Row: rowA},
		{Key: "b", Time: 20, Ordinal: 0, Diff: -1, Partition: "p", RowNumber: 2, Row: rowB, HasLag: true, LagRow: rowA},
		{Key: "b", Time: 20, Ordinal: 0, Diff: 1, Partition: "p", RowNumber: 1, Row: rowB},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("retraction output = %#v, want %#v", got, want)
	}
	wantSnapshot := []DifferentialRowNumberLagRow{
		{Key: "b", Time: 20, Ordinal: 0, Diff: 1, Partition: "p", RowNumber: 1, Row: rowB},
	}
	if got := window.Snapshot(); !reflect.DeepEqual(got, wantSnapshot) {
		t.Fatalf("snapshot = %#v, want %#v", got, wantSnapshot)
	}
}

func TestM065DifferentialRowNumberLagSupportsWeightedCopies(t *testing.T) {
	window, err := NewDifferentialRowNumberLagWindow(DifferentialRowNumberLagWindowOptions{Lag: 1, MaxRows: 4})
	if err != nil {
		t.Fatal(err)
	}
	row := Row{"value": "same"}
	got, err := window.Apply([]DifferentialRow{{Key: "same", Time: 1, Diff: 2, Row: row}})
	if err != nil {
		t.Fatal(err)
	}
	want := []DifferentialRowNumberLagRow{
		{Key: "same", Time: 1, Ordinal: 0, Diff: 1, RowNumber: 1, Row: row},
		{Key: "same", Time: 1, Ordinal: 1, Diff: 1, RowNumber: 2, Row: row, HasLag: true, LagRow: row},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("weighted insert output = %#v, want %#v", got, want)
	}
	got, err = window.Apply([]DifferentialRow{{Key: "same", Time: 1, Diff: -1}})
	if err != nil {
		t.Fatal(err)
	}
	want = []DifferentialRowNumberLagRow{
		{Key: "same", Time: 1, Ordinal: 1, Diff: -1, RowNumber: 2, Row: row, HasLag: true, LagRow: row},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("weighted retraction output = %#v, want %#v", got, want)
	}
}

func TestM065DifferentialRowNumberLagIsAtomicAndBounded(t *testing.T) {
	if _, err := NewDifferentialRowNumberLagWindow(DifferentialRowNumberLagWindowOptions{Lag: -1}); err != ErrDifferentialRowNumberLagInvalid {
		t.Fatalf("negative lag error = %v, want %v", err, ErrDifferentialRowNumberLagInvalid)
	}
	if _, err := NewDifferentialRowNumberLagWindow(DifferentialRowNumberLagWindowOptions{MaxRows: -1}); err != ErrDifferentialRowNumberLagMaxRowsInvalid {
		t.Fatalf("negative max rows error = %v, want %v", err, ErrDifferentialRowNumberLagMaxRowsInvalid)
	}
	window, err := NewDifferentialRowNumberLagWindow(DifferentialRowNumberLagWindowOptions{MaxRows: 2})
	if err != nil {
		t.Fatal(err)
	}
	row := Row{"value": "original"}
	if _, err := window.Apply([]DifferentialRow{{Key: "a", Time: 1, Diff: 1, Row: row}}); err != nil {
		t.Fatal(err)
	}
	row["value"] = "caller mutation"
	if got := window.Snapshot()[0].Row["value"]; got != "original" {
		t.Fatalf("snapshot retained caller mutation: %v", got)
	}
	before := window.Snapshot()
	if _, err := window.Apply([]DifferentialRow{{Key: "missing", Time: 2, Diff: -1}}); err == nil {
		t.Fatal("missing retraction succeeded")
	}
	if got := window.Snapshot(); !reflect.DeepEqual(got, before) {
		t.Fatalf("missing retraction mutated state: %#v, want %#v", got, before)
	}
	if _, err := window.Apply([]DifferentialRow{{Key: "b", Time: 2, Diff: 2, Row: Row{"value": "b"}}}); err != ErrDifferentialRowNumberLagMaxRows {
		t.Fatalf("limit error = %v, want %v", err, ErrDifferentialRowNumberLagMaxRows)
	}
	if got := window.Snapshot(); !reflect.DeepEqual(got, before) {
		t.Fatalf("limit failure mutated state: %#v, want %#v", got, before)
	}
	if _, err := window.Apply([]DifferentialRow{{Key: "a", Time: 1, Diff: 1, Row: Row{"value": "wrong"}}}); !errors.Is(err, ErrDifferentialRowNumberLagRowMismatch) {
		t.Fatalf("row mismatch error = %v, want %v", err, ErrDifferentialRowNumberLagRowMismatch)
	}
}

func TestM065DifferentialRowNumberLagOrdersPartitionsAndEqualTimesDeterministically(t *testing.T) {
	window, err := NewDifferentialRowNumberLagWindow(DifferentialRowNumberLagWindowOptions{
		PartitionKey: func(row SQLRow) string { return row["partition"].(string) },
		Lag:          1,
		MaxRows:      8,
	})
	if err != nil {
		t.Fatal(err)
	}
	updates := []DifferentialRow{
		{Key: "z", Time: 1, Diff: 1, Row: Row{"partition": "b", "value": "z"}},
		{Key: "b", Time: 1, Diff: 1, Row: Row{"partition": "a", "value": "b"}},
		{Key: "a", Time: 1, Diff: 1, Row: Row{"partition": "a", "value": "a"}},
	}
	got, err := window.Apply(updates)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 || got[0].Partition != "a" || got[0].Key != "a" || got[1].Partition != "a" || got[1].Key != "b" || got[2].Partition != "b" || got[2].Key != "z" {
		t.Fatalf("deterministic output order = %#v", got)
	}
	if got[1].RowNumber != 2 || !got[1].HasLag || got[1].LagRow["value"] != "a" {
		t.Fatalf("equal-time lag result = %#v", got[1])
	}
}
