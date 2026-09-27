package hatSql

import (
	"errors"
	"reflect"
	"testing"
)

func TestM037IncrementalLeftJoinKeepsStateWhenUnmatchedCallbackFails(t *testing.T) {
	callbackErr := errors.New("unmatched projection failed")
	failUnmatched := false
	join, err := NewIncrementalLeftJoin(IncrementalLeftJoinDefinition{
		LeftKey: func(row Row) (string, error) {
			return row["group"].(string), nil
		},
		RightKey: func(row Row) (string, error) {
			return row["group"].(string), nil
		},
		Merge: func(left, right Row) (Row, error) {
			return Row{
				"left":  left["id"],
				"right": right["id"],
			}, nil
		},
		Unmatched: func(left Row) (Row, error) {
			if failUnmatched && left["id"] == "bad" {
				return nil, callbackErr
			}
			return Row{
				"left": left["id"],
			}, nil
		},
	})
	if err != nil {
		t.Fatalf("NewIncrementalLeftJoin() error = %v", err)
	}

	_, err = join.Apply([]IncrementalJoinUpdate{
		{Side: IncrementalJoinLeft, Row: DifferentialRow{Key: "left-good", Time: 1, Diff: 1, Row: Row{"id": "good", "group": "a"}}},
		{Side: IncrementalJoinLeft, Row: DifferentialRow{Key: "left-bad", Time: 1, Diff: 1, Row: Row{"id": "bad", "group": "a"}}},
	})
	if err != nil {
		t.Fatalf("seed Apply() error = %v", err)
	}
	before, err := join.Snapshot()
	if err != nil {
		t.Fatalf("seed Snapshot() error = %v", err)
	}
	failUnmatched = true

	_, err = join.Apply([]IncrementalJoinUpdate{{
		Side: IncrementalJoinRight,
		Row: DifferentialRow{
			Key:  "right-a",
			Time: 2,
			Diff: 1,
			Row:  Row{"id": "right", "group": "a"},
		},
	}})
	if !errors.Is(err, callbackErr) {
		t.Fatalf("Apply() error = %v, want callback error", err)
	}
	failUnmatched = false
	after, snapshotErr := join.Snapshot()
	if snapshotErr != nil {
		t.Fatalf("failed Snapshot() error = %v", snapshotErr)
	}
	if !reflect.DeepEqual(after, before) {
		t.Fatalf("state changed after callback failure: got %#v, want %#v", after, before)
	}
}
