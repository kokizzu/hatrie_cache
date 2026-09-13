package hatSql

import (
	"context"
	"fmt"
	"testing"
)

type mz026HistoricalResolver struct {
	history map[uint64][]Row
}

func (resolver *mz026HistoricalResolver) ResolveSQLSource(name, key string) ([]Row, error) {
	return resolver.ResolveSQLSourceAt(name, key, 10)
}

func (resolver *mz026HistoricalResolver) ResolveSQLSourceAt(name, key string, frontier uint64) ([]Row, error) {
	rows, ok := resolver.history[frontier]
	if !ok {
		return nil, fmt.Errorf("frontier %d is unavailable", frontier)
	}
	return rows, nil
}

func TestQuerySubscriptionsExportSnapshotsAtExactFrontier(t *testing.T) {
	resolver := &mz026HistoricalResolver{history: map[uint64][]Row{
		10: {{"id": int64(1), "name": "Ada"}},
		20: {{"id": int64(1), "name": "Lin"}},
	}}
	registry := NewQuerySubscriptions(2)
	first, err := registry.Subscribe(context.Background(), QuerySubscriptionDefinition{
		Query:        "FROM CACHE('people') SELECT id, name",
		Dependencies: []string{"people"},
		AsOf:         10,
	}, resolver, QueryOptions{})
	if err != nil {
		t.Fatalf("first Subscribe() error = %v", err)
	}
	defer first.Close()
	second, err := registry.Subscribe(context.Background(), QuerySubscriptionDefinition{
		Query:        "FROM CACHE('people') SELECT id, name",
		Dependencies: []string{"people"},
		AsOf:         10,
		UpTo:         20,
	}, resolver, QueryOptions{})
	if err != nil {
		t.Fatalf("second Subscribe() error = %v", err)
	}
	defer second.Close()

	exported, err := registry.ExportSnapshotsAt(context.Background(), 20, resolver, QueryOptions{})
	if err != nil {
		t.Fatalf("ExportSnapshotsAt() error = %v", err)
	}
	if len(exported) != 2 || exported[0].ID >= exported[1].ID {
		t.Fatalf("exported snapshots = %#v, want sorted subscription IDs", exported)
	}
	for index, snapshot := range exported {
		if snapshot.Frontier != 20 || snapshot.Progress || snapshot.Result.Rows[0]["name"] != "Lin" {
			t.Fatalf("exported snapshot %d = %#v, want frontier 20 and Lin", index, snapshot)
		}
		if index == 1 && (!snapshot.Complete || snapshot.Result.Rows[0]["id"] != int64(1)) {
			t.Fatalf("bounded exported snapshot = %#v, want complete at UpTo", snapshot)
		}
		if index == 0 && snapshot.Complete {
			t.Fatalf("unbounded exported snapshot = %#v, want incomplete", snapshot)
		}
	}

	exported[0].Result.Rows[0]["name"] = "mutated"
	repeated, err := registry.ExportSnapshotsAt(context.Background(), 20, resolver, QueryOptions{})
	if err != nil {
		t.Fatalf("repeated ExportSnapshotsAt() error = %v", err)
	}
	if repeated[0].Result.Rows[0]["name"] != "Lin" {
		t.Fatalf("exported result aliases internal data: %#v", repeated[0].Result.Rows)
	}
	current, ok := first.Snapshot()
	if !ok || current.Frontier != 10 || current.Result.Rows[0]["name"] != "Ada" {
		t.Fatalf("live subscription changed after export: %#v/%t", current, ok)
	}
}

func TestQuerySubscriptionsExportSnapshotsAtRejectsInvalidFrontier(t *testing.T) {
	resolver := &mz026HistoricalResolver{history: map[uint64][]Row{
		10: {{"id": int64(1)}},
	}}
	registry := NewQuerySubscriptions(1)
	subscription, err := registry.Subscribe(context.Background(), QuerySubscriptionDefinition{
		Query:        "FROM CACHE('people') SELECT id",
		Dependencies: []string{"people"},
		AsOf:         10,
		UpTo:         10,
	}, resolver, QueryOptions{})
	if err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}
	defer subscription.Close()

	for _, frontier := range []uint64{0, 9, 11} {
		if _, err := registry.ExportSnapshotsAt(context.Background(), frontier, resolver, QueryOptions{}); err == nil {
			t.Fatalf("ExportSnapshotsAt(frontier=%d) error = nil", frontier)
		}
	}
}
