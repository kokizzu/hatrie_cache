package hatSql

import (
	"errors"
	"reflect"
	"testing"
)

func TestMZ038DifferentialIntersectSameKeyBatchPreservesTransitions(t *testing.T) {
	operator := NewDifferentialIntersect()
	leftRow := Row{"id": int64(1), "side": "left"}
	rightRow := Row{"id": int64(1), "side": "right"}

	if _, err := operator.Apply(
		[]DifferentialRow{{Key: "k", Time: 1, Diff: 1, Row: leftRow}},
		[]DifferentialRow{{Key: "k", Time: 2, Diff: 1, Row: rightRow}},
	); err != nil {
		t.Fatalf("initial intersection error = %v", err)
	}

	changes, err := operator.Apply(
		[]DifferentialRow{{Key: "k", Time: 3, Diff: 1, Row: leftRow}},
		[]DifferentialRow{{Key: "k", Time: 4, Diff: -1, Row: rightRow}},
	)
	if err != nil {
		t.Fatalf("same-key transition error = %v", err)
	}
	want := []DifferentialRow{{Key: "k", Time: 4, Diff: -1, Row: leftRow}}
	if !reflect.DeepEqual(changes, want) {
		t.Fatalf("same-key transition changes = %#v, want %#v", changes, want)
	}

	changes, err = operator.Apply(
		[]DifferentialRow{{Key: "k", Time: 5, Diff: -1, Row: leftRow}},
		[]DifferentialRow{{Key: "k", Time: 6, Diff: 1, Row: rightRow}},
	)
	if err != nil {
		t.Fatalf("same-key restoration error = %v", err)
	}
	want = []DifferentialRow{
		{Key: "k", Time: 5, Diff: -1, Row: leftRow},
		{Key: "k", Time: 6, Diff: 1, Row: leftRow},
	}
	if !reflect.DeepEqual(changes, want) {
		t.Fatalf("same-key restoration changes = %#v, want %#v", changes, want)
	}
}

func TestMZ038DifferentialIntersectSameKeyBatchIsAtomicOnError(t *testing.T) {
	operator := NewDifferentialIntersect()
	row := Row{"id": int64(1)}
	if _, err := operator.Apply(
		[]DifferentialRow{{Key: "k", Diff: 1, Row: row}},
		[]DifferentialRow{{Key: "k", Diff: 1, Row: row}},
	); err != nil {
		t.Fatalf("initial intersection error = %v", err)
	}

	_, err := operator.Apply(
		[]DifferentialRow{
			{Key: "k", Diff: 1, Row: row},
			{Key: "k", Diff: -3, Row: row},
		},
		nil,
	)
	if !errors.Is(err, ErrDifferentialIntersectNegativeMultiplicity) {
		t.Fatalf("invalid same-key batch error = %v, want negative multiplicity", err)
	}

	changes, err := operator.Apply(nil, []DifferentialRow{{Key: "k", Time: 9, Diff: -1, Row: row}})
	if err != nil {
		t.Fatalf("post-error retract error = %v", err)
	}
	want := []DifferentialRow{{Key: "k", Time: 9, Diff: -1, Row: row}}
	if !reflect.DeepEqual(changes, want) {
		t.Fatalf("post-error retract changes = %#v, want %#v", changes, want)
	}
}
