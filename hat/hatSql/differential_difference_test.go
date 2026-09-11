package hatSql

import (
	"errors"
	"fmt"
	"math"
	"reflect"
	"testing"
)

func TestNegateDifferentialRowsPreservesWeightsAndClonesRows(t *testing.T) {
	input := []DifferentialRow{
		{Key: "first", Time: 1, Diff: 2, Row: Row{"value": []byte("first")}},
		{Key: "second", Time: 2, Diff: -3, Row: Row{"value": "second"}},
	}
	want := []DifferentialRow{
		{Key: "first", Time: 1, Diff: -2, Row: Row{"value": []byte("first")}},
		{Key: "second", Time: 2, Diff: 3, Row: Row{"value": "second"}},
	}

	got, err := NegateDifferentialRows(input)
	if err != nil {
		t.Fatalf("NegateDifferentialRows() error = %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("NegateDifferentialRows() = %#v, want %#v", got, want)
	}
	got[0].Row["value"].([]byte)[0] = 'X'
	if string(input[0].Row["value"].([]byte)) != "first" {
		t.Fatal("NegateDifferentialRows() did not clone byte rows")
	}
}

func TestNegateDifferentialRowsRejectsInvalidInput(t *testing.T) {
	tests := []struct {
		name string
		rows []DifferentialRow
		want error
	}{
		{name: "missing key", rows: []DifferentialRow{{Diff: 1}}, want: ErrDifferentialRowKeyRequired},
		{name: "minimum integer", rows: []DifferentialRow{{Key: "key", Diff: math.MinInt64}}, want: ErrDifferentialDifferenceOverflow},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := NegateDifferentialRows(test.rows)
			if got != nil {
				t.Fatalf("NegateDifferentialRows() result = %#v, want nil", got)
			}
			if !errors.Is(err, test.want) {
				t.Fatalf("NegateDifferentialRows() error = %v, want %v", err, test.want)
			}
		})
	}
}

func TestExceptDifferentialRowsConsolidatesSignedWeights(t *testing.T) {
	left := []DifferentialRow{
		{Key: "a", Time: 1, Diff: 3, Row: Row{"value": "a"}},
		{Key: "b", Time: 2, Diff: 2, Row: Row{"value": "b"}},
		{Key: "a", Time: 1, Diff: -1, Row: Row{"value": "a"}},
	}
	right := []DifferentialRow{
		{Key: "a", Time: 1, Diff: 1, Row: Row{"value": "a"}},
		{Key: "b", Time: 2, Diff: 2, Row: Row{"value": "b"}},
		{Key: "c", Time: 3, Diff: 1, Row: Row{"value": "c"}},
	}
	want := []DifferentialRow{
		{Key: "a", Time: 1, Diff: 1, Row: Row{"value": "a"}},
		{Key: "c", Time: 3, Diff: -1, Row: Row{"value": "c"}},
	}

	got, err := ExceptDifferentialRows(left, right)
	if err != nil {
		t.Fatalf("ExceptDifferentialRows() error = %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ExceptDifferentialRows() = %#v, want %#v", got, want)
	}
	if left[0].Diff != 3 || right[0].Diff != 1 {
		t.Fatal("ExceptDifferentialRows() mutated input weights")
	}
}

func TestExceptDifferentialRowsFailsAtomicallyOnOverflow(t *testing.T) {
	left := []DifferentialRow{{Key: "left", Time: 1, Diff: 1, Row: Row{"value": "left"}}}
	right := []DifferentialRow{{Key: "right", Time: 2, Diff: math.MinInt64, Row: Row{"value": "right"}}}

	got, err := ExceptDifferentialRows(left, right)
	if got != nil {
		t.Fatalf("ExceptDifferentialRows() result = %#v, want nil", got)
	}
	if !errors.Is(err, ErrDifferentialDifferenceOverflow) {
		t.Fatalf("ExceptDifferentialRows() error = %v, want ErrDifferentialDifferenceOverflow", err)
	}
	if left[0].Diff != 1 || right[0].Diff != math.MinInt64 {
		t.Fatal("ExceptDifferentialRows() mutated inputs after failure")
	}
}

func TestExceptDifferentialRowsRejectsConsolidationOverflow(t *testing.T) {
	left := []DifferentialRow{{Key: "key", Time: 1, Diff: math.MaxInt64, Row: Row{"value": "key"}}}
	right := []DifferentialRow{{Key: "key", Time: 1, Diff: -1, Row: Row{"value": "key"}}}

	got, err := ExceptDifferentialRows(left, right)
	if got != nil {
		t.Fatalf("ExceptDifferentialRows() result = %#v, want nil", got)
	}
	if !errors.Is(err, ErrDifferentialDifferenceOverflow) {
		t.Fatalf("ExceptDifferentialRows() error = %v, want ErrDifferentialDifferenceOverflow", err)
	}
}

func ExampleExceptDifferentialRows() {
	left := []DifferentialRow{
		{Key: "alice", Time: 1, Diff: 2, Row: Row{"team": "red"}},
		{Key: "bob", Time: 1, Diff: 1, Row: Row{"team": "blue"}},
	}
	right := []DifferentialRow{
		{Key: "alice", Time: 1, Diff: 1, Row: Row{"team": "red"}},
	}
	updates, err := ExceptDifferentialRows(left, right)
	if err != nil {
		panic(err)
	}
	for _, update := range updates {
		fmt.Printf("%s %d\n", update.Key, update.Diff)
	}
	// Output:
	// alice 1
	// bob 1
}
