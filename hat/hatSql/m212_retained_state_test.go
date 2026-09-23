package hatSql

import (
	"context"
	"errors"
	"testing"
)

func TestM212RetainedStateCompactsHistoryWithoutRewritingLiveState(t *testing.T) {
	state, err := NewSQLRetainedState(SQLRetainedStateOptions{MaxFrontiers: 8})
	if err != nil {
		t.Fatal(err)
	}
	versions := []struct {
		frontier uint64
		value    string
	}{
		{frontier: 1, value: "Ada"},
		{frontier: 2, value: "Grace"},
		{frontier: 3, value: "Lin"},
	}
	for _, version := range versions {
		if err := state.Publish(version.frontier, []SQLRetainedStateSource{{
			Name: "CACHE",
			Key:  "items",
			Rows: []Row{{"id": int64(version.frontier), "value": version.value}},
		}}); err != nil {
			t.Fatal(err)
		}
	}
	before, err := state.ResolveSQLSource("CACHE", "items")
	if err != nil {
		t.Fatal(err)
	}
	dropped, err := state.Compact(2)
	if err != nil {
		t.Fatal(err)
	}
	if dropped != 2 {
		t.Fatalf("compacted frontier count = %d, want 2", dropped)
	}
	if snapshot := state.Snapshot(); snapshot.EarliestFrontier != 3 || snapshot.LatestFrontier != 3 || snapshot.HistoryFrontiers != 1 {
		t.Fatalf("metadata after compaction = %#v, want only latest frontier 3", snapshot)
	}
	after, err := state.ResolveSQLSource("CACHE", "items")
	if err != nil {
		t.Fatal(err)
	}
	if len(before) != len(after) || after[0]["value"] != "Lin" {
		t.Fatalf("live state after compaction = %#v, want unchanged latest row Lin", after)
	}
	if _, _, err := state.BeginSQLSnapshotAt(context.Background(), 1); !errors.Is(err, ErrSQLRetainedStateFrontierUnavailable) {
		t.Fatalf("compacted frontier 1 error = %v, want unavailable", err)
	}
	if _, _, err := state.BeginSQLSnapshotAt(context.Background(), 2); !errors.Is(err, ErrSQLRetainedStateFrontierUnavailable) {
		t.Fatalf("compacted frontier 2 error = %v, want unavailable", err)
	}
	rows, err := state.ResolveSQLSourceAt("CACHE", "items", 3)
	if err != nil || len(rows) != 1 || rows[0]["value"] != "Lin" {
		t.Fatalf("latest retained state after compaction = %#v/%v, want Lin", rows, err)
	}
}

func TestM212RetainedStateCompactionPreservesLatestWhenTargetIsBeyondIt(t *testing.T) {
	state, err := NewSQLRetainedState(SQLRetainedStateOptions{MaxFrontiers: 4})
	if err != nil {
		t.Fatal(err)
	}
	if err := state.Publish(7, []SQLRetainedStateSource{{Name: "CACHE", Key: "items", Rows: []Row{{"id": int64(7)}}}}); err != nil {
		t.Fatal(err)
	}
	dropped, err := state.Compact(99)
	if err != nil {
		t.Fatal(err)
	}
	if dropped != 0 {
		t.Fatalf("compacted latest-only history count = %d, want zero", dropped)
	}
	if _, _, err := state.BeginSQLSnapshotAt(context.Background(), 7); err != nil {
		t.Fatalf("latest frontier after beyond-latest compaction = %v", err)
	}
}

func TestM212RetainedStateCompactionIsNoopAtZero(t *testing.T) {
	state, err := NewSQLRetainedState(SQLRetainedStateOptions{MaxFrontiers: 4})
	if err != nil {
		t.Fatal(err)
	}
	if err := state.Publish(1, nil); err != nil {
		t.Fatal(err)
	}
	dropped, err := state.Compact(0)
	if err != nil || dropped != 0 {
		t.Fatalf("zero compaction = %d/%v, want zero/nil", dropped, err)
	}
	if snapshot := state.Snapshot(); snapshot.HistoryFrontiers != 1 || snapshot.EarliestFrontier != 1 {
		t.Fatalf("metadata after zero compaction = %#v", snapshot)
	}
}
