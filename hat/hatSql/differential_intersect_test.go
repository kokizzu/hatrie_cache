package hatSql

import (
	"errors"
	"fmt"
	"math"
	"reflect"
	"testing"
)

func TestDifferentialIntersectPreservesMultisetTransitions(t *testing.T) {
	operator := NewDifferentialIntersect()
	initial, err := operator.Apply(
		[]DifferentialRow{
			{Key: "a", Time: 1, Diff: 3, Row: Row{"value": "a"}},
			{Key: "b", Time: 1, Diff: 1, Row: Row{"value": "b"}},
		},
		[]DifferentialRow{
			{Key: "a", Time: 2, Diff: 2, Row: Row{"value": "a"}},
			{Key: "b", Time: 2, Diff: 1, Row: Row{"value": "b"}},
		},
	)
	if err != nil {
		t.Fatalf("initial Apply() error = %v", err)
	}
	wantInitial := []DifferentialRow{
		{Key: "a", Time: 2, Diff: 2, Row: Row{"value": "a"}},
		{Key: "b", Time: 2, Diff: 1, Row: Row{"value": "b"}},
	}
	if !reflect.DeepEqual(initial, wantInitial) {
		t.Fatalf("initial Apply() = %#v, want %#v", initial, wantInitial)
	}

	changes, err := operator.Apply(
		[]DifferentialRow{{Key: "a", Time: 3, Diff: -1, Row: Row{"value": "a"}}},
		[]DifferentialRow{{Key: "b", Time: 3, Diff: -1, Row: Row{"value": "b"}}},
	)
	if err != nil {
		t.Fatalf("second Apply() error = %v", err)
	}
	wantChanges := []DifferentialRow{{Key: "b", Time: 3, Diff: -1, Row: Row{"value": "b"}}}
	if !reflect.DeepEqual(changes, wantChanges) {
		t.Fatalf("second Apply() = %#v, want %#v", changes, wantChanges)
	}

	changes, err = operator.Apply(nil, []DifferentialRow{{Key: "a", Time: 4, Diff: -2, Row: Row{"value": "a"}}})
	if err != nil {
		t.Fatalf("third Apply() error = %v", err)
	}
	wantChanges = []DifferentialRow{{Key: "a", Time: 4, Diff: -2, Row: Row{"value": "a"}}}
	if !reflect.DeepEqual(changes, wantChanges) {
		t.Fatalf("third Apply() = %#v, want %#v", changes, wantChanges)
	}
}

func TestDifferentialIntersectClonesOutputRows(t *testing.T) {
	input := []DifferentialRow{{Key: "key", Time: 1, Diff: 1, Row: Row{"value": []byte("value")}}}
	operator := NewDifferentialIntersect()
	output, err := operator.Apply(input, input)
	if err != nil {
		t.Fatalf("Apply() error = %v", err)
	}
	output[0].Row["value"].([]byte)[0] = 'X'
	if string(input[0].Row["value"].([]byte)) != "value" {
		t.Fatal("Apply() aliased input row bytes")
	}
	removal, err := operator.Apply([]DifferentialRow{{Key: "key", Time: 2, Diff: -1}}, nil)
	if err != nil {
		t.Fatalf("removal Apply() error = %v", err)
	}
	if string(removal[0].Row["value"].([]byte)) != "value" {
		t.Fatal("Apply() retained mutated output row")
	}
}

func TestDifferentialIntersectRejectsInvalidBatchAtomically(t *testing.T) {
	operator := NewDifferentialIntersect()
	if _, err := operator.Apply(
		[]DifferentialRow{{Key: "key", Time: 1, Diff: 2, Row: Row{"value": "value"}}},
		[]DifferentialRow{{Key: "key", Time: 1, Diff: 2, Row: Row{"value": "value"}}},
	); err != nil {
		t.Fatalf("initial Apply() error = %v", err)
	}
	if output, err := operator.Apply(
		[]DifferentialRow{{Key: "key", Time: 2, Diff: -1}},
		[]DifferentialRow{{Time: 2, Diff: -1}},
	); output != nil || !errors.Is(err, ErrDifferentialRowKeyRequired) {
		t.Fatalf("invalid Apply() = %#v, %v; want nil and ErrDifferentialRowKeyRequired", output, err)
	}
	output, err := operator.Apply(nil, []DifferentialRow{{Key: "key", Time: 3, Diff: -2}})
	if err != nil {
		t.Fatalf("post-error Apply() error = %v", err)
	}
	want := []DifferentialRow{{Key: "key", Time: 3, Diff: -2, Row: Row{"value": "value"}}}
	if !reflect.DeepEqual(output, want) {
		t.Fatalf("post-error Apply() = %#v, want %#v", output, want)
	}
}

func TestDifferentialIntersectRejectsUnderflowAndOverflow(t *testing.T) {
	operator := NewDifferentialIntersect()
	if output, err := operator.Apply([]DifferentialRow{{Key: "key", Diff: -1}}, nil); output != nil || !errors.Is(err, ErrDifferentialIntersectNegativeMultiplicity) {
		t.Fatalf("underflow Apply() = %#v, %v; want nil and negative multiplicity", output, err)
	}
	if output, err := operator.Apply([]DifferentialRow{{Key: "key", Diff: math.MaxInt64}, {Key: "key", Diff: math.MaxInt64}, {Key: "key", Diff: 2}}, nil); output != nil || !errors.Is(err, ErrDifferentialIntersectCountOverflow) {
		t.Fatalf("count overflow Apply() = %#v, %v; want nil and count overflow", output, err)
	}
}

func TestDifferentialIntersectRejectsUnrepresentableOutputDelta(t *testing.T) {
	operator := NewDifferentialIntersect()
	large := []DifferentialRow{
		{Key: "key", Diff: math.MaxInt64},
		{Key: "key", Diff: 1},
	}
	if _, err := operator.Apply(large, large); err != nil {
		t.Fatalf("initial Apply() error = %v", err)
	}
	output, err := operator.Apply([]DifferentialRow{{Key: "key", Diff: math.MinInt64}}, nil)
	if output != nil || !errors.Is(err, ErrDifferentialIntersectDiffOverflow) {
		t.Fatalf("unrepresentable Apply() = %#v, %v; want nil and diff overflow", output, err)
	}
}

func TestDifferentialIntersectResetClearsState(t *testing.T) {
	operator := NewDifferentialIntersect()
	if _, err := operator.Apply(
		[]DifferentialRow{{Key: "key", Diff: 1, Row: Row{"value": "value"}}},
		[]DifferentialRow{{Key: "key", Diff: 1}},
	); err != nil {
		t.Fatalf("initial Apply() error = %v", err)
	}
	operator.Reset()
	if output, err := operator.Apply(nil, []DifferentialRow{{Key: "key", Diff: -1}}); output != nil || !errors.Is(err, ErrDifferentialIntersectNegativeMultiplicity) {
		t.Fatalf("post-reset Apply() = %#v, %v; want nil and negative multiplicity", output, err)
	}
}

func ExampleDifferentialIntersect() {
	operator := NewDifferentialIntersect()
	updates, err := operator.Apply(
		[]DifferentialRow{{Key: "alice", Time: 1, Diff: 2, Row: Row{"name": "Alice"}}},
		[]DifferentialRow{{Key: "alice", Time: 1, Diff: 1}},
	)
	if err != nil {
		panic(err)
	}
	for _, update := range updates {
		fmt.Printf("%s %d\n", update.Key, update.Diff)
	}
	// Output:
	// alice 1
}
