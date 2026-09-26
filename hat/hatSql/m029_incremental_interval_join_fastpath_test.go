package hatSql

import (
	"reflect"
	"testing"
)

func TestMZ029IncrementalIntervalJoinReplacementFastPath(t *testing.T) {
	definition := m029TestIntervalJoinDefinition()
	definition.Merge = func(left, right Row) (Row, error) {
		return Row{
			"left_start":  left["start"],
			"right_start": right["start"],
		}, nil
	}
	join, err := NewIncrementalIntervalJoin(definition)
	if err != nil {
		t.Fatalf("create interval join: %v", err)
	}
	if _, err := join.Apply([]IncrementalIntervalJoinUpdate{
		{Side: IncrementalIntervalJoinLeft, Row: DifferentialRow{
			Key: "l1", Diff: 1, Row: Row{"id": "l1", "group": "g", "start": int64(0), "end": int64(3)},
		}},
		{Side: IncrementalIntervalJoinRight, Row: DifferentialRow{
			Key: "r1", Diff: 1, Row: Row{"id": "r1", "group": "g", "start": int64(1), "end": int64(2)},
		}},
	}); err != nil {
		t.Fatalf("seed interval join: %v", err)
	}

	updates := []IncrementalIntervalJoinUpdate{
		{Side: IncrementalIntervalJoinLeft, Row: DifferentialRow{Key: "l1", Diff: -1}},
		{Side: IncrementalIntervalJoinLeft, Row: DifferentialRow{
			Key: "l1", Diff: 1, Row: Row{"id": "l1", "group": "g", "start": int64(1), "end": int64(4)},
		}},
	}
	deltas, err := join.applyReplacement(updates)
	if err != nil {
		t.Fatalf("apply replacement: %v", err)
	}
	wantDeltas := []DifferentialRow{
		{Key: "l1\x00r1", Diff: -1, Row: Row{"left_start": int64(0), "right_start": int64(1)}},
		{Key: "l1\x00r1", Diff: 1, Row: Row{"left_start": int64(1), "right_start": int64(1)}},
	}
	if !reflect.DeepEqual(deltas, wantDeltas) {
		t.Fatalf("replacement deltas = %#v, want %#v", deltas, wantDeltas)
	}

	snapshot, err := join.Snapshot()
	if err != nil {
		t.Fatalf("snapshot after replacement: %v", err)
	}
	wantSnapshot := []DifferentialRow{
		{Key: "l1\x00r1", Diff: 1, Row: Row{"left_start": int64(1), "right_start": int64(1)}},
	}
	if !reflect.DeepEqual(snapshot, wantSnapshot) {
		t.Fatalf("snapshot after replacement = %#v, want %#v", snapshot, wantSnapshot)
	}
}
