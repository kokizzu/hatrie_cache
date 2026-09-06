package hatSql

import (
	"errors"
	"fmt"
	"reflect"
	"testing"
)

func TestDistinctDifferentialRowsEmitsOnlyMultiplicityTransitions(t *testing.T) {
	rows := []DifferentialRow{
		{Key: "a", Time: 1, Diff: 2, Row: Row{"value": "first"}},
		{Key: "a", Time: 2, Diff: -1, Row: Row{"value": "middle"}},
		{Key: "b", Time: 3, Diff: 1, Row: Row{"value": "b"}},
		{Key: "a", Time: 4, Diff: -1, Row: Row{"value": "last"}},
		{Key: "b", Time: 5, Diff: -1, Row: Row{"value": "remove-b"}},
	}

	got, err := DistinctDifferentialRows(rows)
	if err != nil {
		t.Fatalf("DistinctDifferentialRows() error = %v", err)
	}
	want := []DifferentialRow{
		{Key: "a", Time: 1, Diff: 1, Row: Row{"value": "first"}},
		{Key: "b", Time: 3, Diff: 1, Row: Row{"value": "b"}},
		{Key: "a", Time: 4, Diff: -1, Row: Row{"value": "last"}},
		{Key: "b", Time: 5, Diff: -1, Row: Row{"value": "remove-b"}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got = %#v, want %#v", got, want)
	}
	got[0].Row["value"] = "changed-output"
	if rows[0].Row["value"] != "first" {
		t.Fatal("distinct output aliases input row")
	}
}

func TestDistinctDifferentialRowsHandlesZeroAndRejectsInvalidMultiplicity(t *testing.T) {
	if got, err := DistinctDifferentialRows([]DifferentialRow{{Key: "a", Diff: 0}}); err != nil || got != nil {
		t.Fatalf("zero diff result = %#v, error = %v; want nil, nil", got, err)
	}
	for name, rows := range map[string][]DifferentialRow{
		"underflow": {{Key: "a", Diff: -1}},
		"overflow":  {{Key: "a", Diff: int64(^uint64(0) >> 1)}, {Key: "a", Diff: int64(^uint64(0) >> 1)}, {Key: "a", Diff: 1}, {Key: "a", Diff: 1}},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := DistinctDifferentialRows(rows); err == nil {
				t.Fatal("DistinctDifferentialRows() error = nil")
			}
		})
	}
}

func TestDistinctDifferentialRowsHandlesEmptyInput(t *testing.T) {
	got, err := DistinctDifferentialRows(nil)
	if err != nil {
		t.Fatalf("DistinctDifferentialRows() error = %v", err)
	}
	if got != nil {
		t.Fatalf("got = %#v, want nil", got)
	}
}

func TestDistinctDifferentialRowsWrapsInvalidStateErrors(t *testing.T) {
	_, err := DistinctDifferentialRows([]DifferentialRow{{Key: "a", Diff: -1}})
	if !errors.Is(err, ErrDifferentialDistinctNegativeMultiplicity) {
		t.Fatalf("error = %v, want ErrDifferentialDistinctNegativeMultiplicity", err)
	}
}

func ExampleDistinctDifferentialRows() {
	updates := []DifferentialRow{
		{Key: "alice", Time: 1, Diff: 2, Row: Row{"name": "Alice"}},
		{Key: "alice", Time: 2, Diff: -1, Row: Row{"name": "Alice"}},
		{Key: "alice", Time: 3, Diff: -1, Row: Row{"name": "Alice"}},
	}
	transitions, err := DistinctDifferentialRows(updates)
	if err != nil {
		panic(err)
	}
	for _, transition := range transitions {
		fmt.Printf("%s %d %v\n", transition.Key, transition.Diff, transition.Row["name"])
	}
	// Output:
	// alice 1 Alice
	// alice -1 Alice
}

func BenchmarkDistinctDifferentialRows(b *testing.B) {
	rows := make([]DifferentialRow, 1024)
	for index := range rows {
		rows[index] = DifferentialRow{Key: string(rune(index % 256)), Time: uint64(index), Diff: 1, Row: Row{"value": index}}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := DistinctDifferentialRows(rows); err != nil {
			b.Fatal(err)
		}
	}
}
