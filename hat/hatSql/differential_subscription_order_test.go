package hatSql

import (
	"context"
	"reflect"
	"testing"
)

func TestQuerySubscriptionDeltaBatchWithDeterministicOrder(t *testing.T) {
	snapshot := func(rows []Row) QuerySubscriptionSnapshot {
		return QuerySubscriptionSnapshot{
			ID:       1,
			Revision: 2,
			Frontier: 7,
			Result: QueryResult{
				Columns: []string{"id"},
				Rows:    rows,
			},
		}
	}
	rowsInResolverOrder := []Row{{"id": "b"}, {"id": "a"}}
	rowsInAnotherOrder := []Row{{"id": "a"}, {"id": "b"}}

	defaultBatch := querySubscriptionDeltaBatch(snapshot(rowsInResolverOrder), QueryResult{}, false)
	if got := differentialSubscriptionIDs(defaultBatch.Deltas); !reflect.DeepEqual(got, []string{"b", "a"}) {
		t.Fatalf("default delta order = %v, want resolver order", got)
	}

	orderedBatch := querySubscriptionDeltaBatchWithOrder(snapshot(rowsInResolverOrder), QueryResult{}, false, true)
	if got := differentialSubscriptionIDs(orderedBatch.Deltas); !reflect.DeepEqual(got, []string{"a", "b"}) {
		t.Fatalf("deterministic delta order = %v, want canonical order", got)
	}

	orderedAgain := querySubscriptionDeltaBatchWithOrder(snapshot(rowsInAnotherOrder), QueryResult{}, false, true)
	if !reflect.DeepEqual(orderedBatch.Deltas, orderedAgain.Deltas) {
		t.Fatalf("deterministic batches differ: first=%v second=%v", orderedBatch.Deltas, orderedAgain.Deltas)
	}
}

func TestQuerySubscriptionDeltaBatchWithDeterministicOrderPreservesPhases(t *testing.T) {
	previous := QueryResult{Rows: []Row{
		{"id": "z", "state": "old"},
		{"id": "b", "state": "old"},
	}}
	snapshot := QuerySubscriptionSnapshot{Result: QueryResult{Rows: []Row{
		{"id": "c", "state": "new"},
		{"id": "a", "state": "new"},
	}}}

	batch := querySubscriptionDeltaBatchWithOrder(snapshot, previous, true, true)
	if got := differentialSubscriptionIDs(batch.Deltas); !reflect.DeepEqual(got, []string{"b", "z", "a", "c"}) {
		t.Fatalf("delta rows = %v, want sorted removals followed by sorted additions", got)
	}
	gotDiffs := make([]int64, 0, len(batch.Deltas))
	for _, delta := range batch.Deltas {
		gotDiffs = append(gotDiffs, delta.Diff)
	}
	if want := []int64{-1, -1, 1, 1}; !reflect.DeepEqual(gotDiffs, want) {
		t.Fatalf("delta signs = %v, want %v", gotDiffs, want)
	}

	reset := querySubscriptionResetDeltaWithOrder(QuerySubscriptionSnapshot{}, QueryResult{Rows: []Row{
		{"id": "d"},
		{"id": "a"},
	}}, true)
	if got := differentialSubscriptionIDs(reset.Deltas); !reflect.DeepEqual(got, []string{"a", "d"}) {
		t.Fatalf("reset delta order = %v, want canonical order", got)
	}
}

func TestQuerySubscriptionDifferentialUsesDeterministicOrder(t *testing.T) {
	resolver := SourceResolverFunc(func(string, string) ([]Row, error) {
		return []Row{{"id": "b"}, {"id": "a"}}, nil
	})
	registry := NewQuerySubscriptions(1)
	subscription, err := registry.SubscribeDifferential(context.Background(), QuerySubscriptionDefinition{
		Query:              "FROM CACHE('people') SELECT id",
		Dependencies:       []string{"people"},
		DeterministicOrder: true,
	}, resolver, QueryOptions{})
	if err != nil {
		t.Fatalf("SubscribeDifferential() error = %v", err)
	}
	defer subscription.Close()

	initial := receiveDifferentialBatch(t, subscription)
	if got := differentialSubscriptionIDs(initial.Deltas); !reflect.DeepEqual(got, []string{"a", "b"}) {
		t.Fatalf("initial delta order = %v, want canonical order", got)
	}
}

func differentialSubscriptionIDs(deltas []QuerySubscriptionDelta) []string {
	ids := make([]string, 0, len(deltas))
	for _, delta := range deltas {
		ids = append(ids, delta.Row["id"].(string))
	}
	return ids
}
