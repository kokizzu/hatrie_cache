package hatSql

import (
	"context"
	"errors"
	"testing"
)

func TestM210RetainedStateHistoricalAsOfRead(t *testing.T) {
	state, err := NewSQLRetainedState(SQLRetainedStateOptions{MaxFrontiers: 4})
	if err != nil {
		t.Fatal(err)
	}
	if err := state.Publish(1, []SQLRetainedStateSource{{
		Name: "CACHE",
		Key:  "items",
		Rows: []Row{{"id": int64(1), "value": "Ada"}},
	}}); err != nil {
		t.Fatal(err)
	}
	if err := state.Publish(2, []SQLRetainedStateSource{{
		Name: "CACHE",
		Key:  "items",
		Rows: []Row{{"id": int64(1), "value": "Ada"}, {"id": int64(2), "value": "Grace"}},
	}}); err != nil {
		t.Fatal(err)
	}

	frontier := uint64(1)
	historical, err := ExecuteSQLQueryContext(context.Background(), "FROM CACHE('items') SELECT id, value", state, SQLQueryOptions{AsOfFrontier: &frontier})
	if err != nil {
		t.Fatalf("historical query error = %v", err)
	}
	if len(historical.Rows) != 1 || historical.Rows[0]["value"] != "Ada" {
		t.Fatalf("historical rows = %#v, want the frontier-1 row", historical.Rows)
	}

	live, err := ExecuteSQLQueryContext(context.Background(), "FROM CACHE('items') SELECT id, value", state, SQLQueryOptions{})
	if err != nil {
		t.Fatalf("live query error = %v", err)
	}
	if len(live.Rows) != 2 {
		t.Fatalf("live rows = %#v, want two current rows", live.Rows)
	}
}

func TestM210RetainedStateSupportsHistoricalSubscriptionAsOf(t *testing.T) {
	state, err := NewSQLRetainedState(SQLRetainedStateOptions{MaxFrontiers: 4})
	if err != nil {
		t.Fatal(err)
	}
	if err := state.Publish(1, []SQLRetainedStateSource{{
		Name: "CACHE",
		Key:  "items",
		Rows: []Row{{"id": int64(1), "value": "Ada"}},
	}}); err != nil {
		t.Fatal(err)
	}
	if err := state.Publish(2, []SQLRetainedStateSource{{
		Name: "CACHE",
		Key:  "items",
		Rows: []Row{{"id": int64(2), "value": "Grace"}},
	}}); err != nil {
		t.Fatal(err)
	}
	registry := NewQuerySubscriptions(2)
	subscription, err := registry.Subscribe(context.Background(), QuerySubscriptionDefinition{
		Query:        "FROM CACHE('items') SELECT id, value",
		Dependencies: []string{"items"},
		AsOf:         1,
	}, state, QueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer subscription.Close()
	snapshot, ok := subscription.Snapshot()
	if !ok {
		t.Fatal("subscription snapshot is unavailable")
	}
	if len(snapshot.Result.Rows) != 1 || snapshot.Result.Rows[0]["value"] != "Ada" {
		t.Fatalf("subscription AS OF rows = %#v, want frontier-1 row", snapshot.Result.Rows)
	}
}

func TestM210RetainedStateSnapshotIsImmutableAndInputIsolated(t *testing.T) {
	state, err := NewSQLRetainedState(SQLRetainedStateOptions{MaxFrontiers: 4})
	if err != nil {
		t.Fatal(err)
	}
	input := []Row{{"id": int64(1), "value": "Ada"}}
	if err := state.Publish(1, []SQLRetainedStateSource{{Name: "CACHE", Key: "items", Rows: input}}); err != nil {
		t.Fatal(err)
	}
	input[0]["value"] = "mutated by caller"

	resolver, release, err := state.BeginSQLSnapshotAt(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if release == nil {
		t.Fatal("historical snapshot release is nil")
	}
	defer release()
	rows, err := resolver.ResolveSQLSource("CACHE", "items")
	if err != nil {
		t.Fatal(err)
	}
	if rows[0]["value"] != "Ada" {
		t.Fatalf("historical input value = %#v, want Ada", rows[0]["value"])
	}
	rows[0]["value"] = "mutated by snapshot caller"

	if err := state.Publish(2, []SQLRetainedStateSource{{
		Name: "CACHE",
		Key:  "items",
		Rows: []Row{{"id": int64(1), "value": "Grace"}},
	}}); err != nil {
		t.Fatal(err)
	}
	rows, err = resolver.ResolveSQLSource("CACHE", "items")
	if err != nil {
		t.Fatal(err)
	}
	if rows[0]["value"] != "Ada" {
		t.Fatalf("historical snapshot changed = %#v, want Ada", rows[0]["value"])
	}
}

func TestM210RetainedStateRetentionBoundary(t *testing.T) {
	state, err := NewSQLRetainedState(SQLRetainedStateOptions{MaxFrontiers: 2})
	if err != nil {
		t.Fatal(err)
	}
	for frontier := uint64(1); frontier <= 3; frontier++ {
		if err := state.Publish(frontier, []SQLRetainedStateSource{{
			Name: "CACHE",
			Key:  "items",
			Rows: []Row{{"id": int64(frontier)}},
		}}); err != nil {
			t.Fatal(err)
		}
	}
	if _, _, err := state.BeginSQLSnapshotAt(context.Background(), 1); !errors.Is(err, ErrSQLRetainedStateFrontierUnavailable) {
		t.Fatalf("evicted frontier error = %v, want ErrSQLRetainedStateFrontierUnavailable", err)
	}
	if snapshot := state.Snapshot(); snapshot.EarliestFrontier != 2 || snapshot.LatestFrontier != 3 {
		t.Fatalf("retained snapshot metadata = %#v, want earliest 2/latest 3", snapshot)
	}
	if _, _, err := state.BeginSQLSnapshotAt(context.Background(), 2); err != nil {
		t.Fatalf("retained frontier 2 error = %v", err)
	}
}

func TestM210RetainedStatePublishIsAtomic(t *testing.T) {
	state, err := NewSQLRetainedState(SQLRetainedStateOptions{MaxFrontiers: 4, MaxSources: 2})
	if err != nil {
		t.Fatal(err)
	}
	if err := state.Publish(1, []SQLRetainedStateSource{{Name: "CACHE", Key: "items", Rows: []Row{{"id": int64(1)}}}}); err != nil {
		t.Fatal(err)
	}
	err = state.Publish(2, []SQLRetainedStateSource{
		{Name: "CACHE", Key: "items", Rows: []Row{{"id": int64(2)}}},
		{Name: "CACHE", Key: "items", Rows: []Row{{"id": int64(3)}}},
	})
	if !errors.Is(err, ErrSQLRetainedStateDuplicateSource) {
		t.Fatalf("duplicate source error = %v, want ErrSQLRetainedStateDuplicateSource", err)
	}
	if snapshot := state.Snapshot(); snapshot.LatestFrontier != 1 || snapshot.HistoryFrontiers != 1 {
		t.Fatalf("metadata after rejected publish = %#v, want only frontier 1", snapshot)
	}
	rows, err := state.ResolveSQLSource("CACHE", "items")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0]["id"] != int64(1) {
		t.Fatalf("current rows after rejected publish = %#v, want id 1", rows)
	}
}

func TestM210RetainedStateRejectsInvalidContextAndFrontier(t *testing.T) {
	state, err := NewSQLRetainedState(SQLRetainedStateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := state.Publish(0, nil); !errors.Is(err, ErrSQLRetainedStateFrontierInvalid) {
		t.Fatalf("zero frontier error = %v, want ErrSQLRetainedStateFrontierInvalid", err)
	}
	if err := state.Publish(1, nil); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err := state.BeginSQLSnapshotAt(ctx, 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled snapshot error = %v, want context.Canceled", err)
	}
}
