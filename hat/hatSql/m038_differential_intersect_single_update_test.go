package hatSql

import (
	"reflect"
	"testing"
)

func TestMZ038DifferentialIntersectSingleUpdatePreservesTransitions(t *testing.T) {
	operator := NewDifferentialIntersect()
	leftRow := Row{"id": int64(1), "side": "left"}
	rightRow := Row{"id": int64(1), "side": "right"}

	if changes, err := operator.Apply([]DifferentialRow{{Key: "k", Time: 1, Diff: 1, Row: leftRow}}, nil); err != nil || changes != nil {
		t.Fatalf("left-only insert = %#v, %v; want no output", changes, err)
	}

	changes, err := operator.Apply(nil, []DifferentialRow{{Key: "k", Time: 2, Diff: 1, Row: rightRow}})
	if err != nil {
		t.Fatalf("right insert error = %v", err)
	}
	want := []DifferentialRow{{Key: "k", Time: 2, Diff: 1, Row: leftRow}}
	if !reflect.DeepEqual(changes, want) {
		t.Fatalf("right insert changes = %#v, want %#v", changes, want)
	}

	changes, err = operator.Apply(nil, []DifferentialRow{{Key: "k", Time: 3, Diff: -1, Row: rightRow}})
	if err != nil {
		t.Fatalf("right retract error = %v", err)
	}
	want = []DifferentialRow{{Key: "k", Time: 3, Diff: -1, Row: leftRow}}
	if !reflect.DeepEqual(changes, want) {
		t.Fatalf("right retract changes = %#v, want %#v", changes, want)
	}

	if changes, err := operator.Apply(nil, nil); err != nil || changes != nil {
		t.Fatalf("empty update = %#v, %v; want no output", changes, err)
	}
}
