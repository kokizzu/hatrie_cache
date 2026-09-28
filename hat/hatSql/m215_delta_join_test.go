package hatSql

import (
	"errors"
	"reflect"
	"testing"
)

func TestM215IncrementalJoinApplyConsolidatedProducesNetDelta(t *testing.T) {
	join, err := NewIncrementalJoin(IncrementalJoinDefinition{
		LeftKey:  m215JoinKey,
		RightKey: m215JoinKey,
		Merge:    m215JoinMerge,
	})
	if err != nil {
		t.Fatalf("NewIncrementalJoin() error = %v", err)
	}
	if _, err := join.Apply([]IncrementalJoinUpdate{
		{Side: IncrementalJoinLeft, Row: DifferentialRow{Key: "left", Diff: 2, Row: Row{"id": "left", "group": "old"}}},
		{Side: IncrementalJoinRight, Row: DifferentialRow{Key: "right-old", Diff: 3, Row: Row{"id": "right-old", "group": "old"}}},
		{Side: IncrementalJoinRight, Row: DifferentialRow{Key: "right-new", Diff: 1, Row: Row{"id": "right-new", "group": "new"}}},
	}); err != nil {
		t.Fatalf("seed Apply() error = %v", err)
	}

	output, err := join.ApplyConsolidated([]IncrementalJoinUpdate{
		{Side: IncrementalJoinLeft, Row: DifferentialRow{Key: "left", Diff: -2}},
		{Side: IncrementalJoinLeft, Row: DifferentialRow{Key: "left", Diff: 1, Row: Row{"id": "left", "group": "new"}}},
	})
	if err != nil {
		t.Fatalf("ApplyConsolidated() error = %v", err)
	}
	want := []DifferentialRow{
		{Key: "left\x00right-new", Diff: 1, Row: Row{"left_id": "left", "right_id": "right-new", "group": "new"}},
		{Key: "left\x00right-old", Diff: -6, Row: Row{"left_id": "left", "right_id": "right-old", "group": "old"}},
	}
	if !reflect.DeepEqual(output, want) {
		t.Fatalf("ApplyConsolidated() = %#v, want %#v", output, want)
	}
}

func TestM215IncrementalJoinApplyConsolidatedSkipsNetZeroChurn(t *testing.T) {
	join, updates := m215HighChurnJoinFixture(t)
	before, err := join.Snapshot()
	if err != nil {
		t.Fatalf("before Snapshot() error = %v", err)
	}
	output, err := join.ApplyConsolidated(updates)
	if err != nil {
		t.Fatalf("ApplyConsolidated() error = %v", err)
	}
	if output != nil {
		t.Fatalf("net-zero output = %#v, want nil", output)
	}
	after, err := join.Snapshot()
	if err != nil {
		t.Fatalf("after Snapshot() error = %v", err)
	}
	if !reflect.DeepEqual(after, before) {
		t.Fatalf("net-zero snapshot changed: before=%#v after=%#v", before, after)
	}
}

func TestM215IncrementalJoinApplyConsolidatedIsAtomic(t *testing.T) {
	join, err := NewIncrementalJoin(IncrementalJoinDefinition{
		LeftKey:  m215JoinKey,
		RightKey: m215JoinKey,
		Merge:    m215JoinMerge,
	})
	if err != nil {
		t.Fatalf("NewIncrementalJoin() error = %v", err)
	}
	if _, err := join.Apply([]IncrementalJoinUpdate{
		{Side: IncrementalJoinLeft, Row: DifferentialRow{Key: "left", Diff: 1, Row: Row{"id": "left", "group": "stable"}}},
		{Side: IncrementalJoinRight, Row: DifferentialRow{Key: "right", Diff: 1, Row: Row{"id": "right", "group": "stable"}}},
	}); err != nil {
		t.Fatalf("seed Apply() error = %v", err)
	}
	before, err := join.Snapshot()
	if err != nil {
		t.Fatalf("before Snapshot() error = %v", err)
	}
	if _, err := join.ApplyConsolidated([]IncrementalJoinUpdate{{
		Side: IncrementalJoinLeft,
		Row:  DifferentialRow{Key: "missing", Diff: -1},
	}}); !errors.Is(err, ErrIncrementalJoinNegativeMultiplicity) {
		t.Fatalf("invalid ApplyConsolidated() error = %v, want %v", err, ErrIncrementalJoinNegativeMultiplicity)
	}
	after, err := join.Snapshot()
	if err != nil {
		t.Fatalf("after Snapshot() error = %v", err)
	}
	if !reflect.DeepEqual(after, before) {
		t.Fatalf("rejected batch changed state: before=%#v after=%#v", before, after)
	}
}

func TestM215IncrementalJoinApplyConsolidatedHandlesBothSidesChanging(t *testing.T) {
	join, err := NewIncrementalJoin(IncrementalJoinDefinition{
		LeftKey:  m215JoinKey,
		RightKey: m215JoinKey,
		Merge:    m215JoinMerge,
	})
	if err != nil {
		t.Fatalf("NewIncrementalJoin() error = %v", err)
	}
	if _, err := join.Apply([]IncrementalJoinUpdate{
		{Side: IncrementalJoinLeft, Row: DifferentialRow{Key: "left", Diff: 1, Row: Row{"id": "left", "group": "old"}}},
		{Side: IncrementalJoinRight, Row: DifferentialRow{Key: "right", Diff: 1, Row: Row{"id": "right", "group": "old"}}},
	}); err != nil {
		t.Fatalf("seed Apply() error = %v", err)
	}
	output, err := join.ApplyConsolidated([]IncrementalJoinUpdate{
		{Side: IncrementalJoinLeft, Row: DifferentialRow{Key: "left", Diff: -1}},
		{Side: IncrementalJoinLeft, Row: DifferentialRow{Key: "left", Diff: 1, Row: Row{"id": "left", "group": "new"}}},
		{Side: IncrementalJoinRight, Row: DifferentialRow{Key: "right", Diff: -1}},
		{Side: IncrementalJoinRight, Row: DifferentialRow{Key: "right", Diff: 1, Row: Row{"id": "right", "group": "new"}}},
	})
	if err != nil {
		t.Fatalf("ApplyConsolidated() error = %v", err)
	}
	want := []DifferentialRow{
		{Key: "left\x00right", Diff: -1, Row: Row{"left_id": "left", "right_id": "right", "group": "old"}},
		{Key: "left\x00right", Diff: 1, Row: Row{"left_id": "left", "right_id": "right", "group": "new"}},
	}
	if !reflect.DeepEqual(output, want) {
		t.Fatalf("ApplyConsolidated() = %#v, want %#v", output, want)
	}
}
