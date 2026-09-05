package hatSql_test

import (
	"context"
	"reflect"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestQuerySubscriptionsStatusReportsFrontierLagAndQueue(t *testing.T) {
	resolver := hatSql.SourceResolverFunc(func(_ string, key string) ([]hatSql.Row, error) {
		return []hatSql.Row{{"name": key}}, nil
	})
	registry := hatSql.NewQuerySubscriptions(2)
	progress, err := registry.Subscribe(context.Background(), hatSql.QuerySubscriptionDefinition{
		Query:        "FROM CACHE('people') SELECT name",
		Dependencies: []string{"people"},
		EmitProgress: true,
	}, resolver, hatSql.QueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	regular, err := registry.Subscribe(context.Background(), hatSql.QuerySubscriptionDefinition{
		Query:        "FROM CACHE('teams') SELECT name",
		Dependencies: []string{"teams"},
	}, resolver, hatSql.QueryOptions{})
	if err != nil {
		t.Fatal(err)
	}

	initial := registry.Status(10)
	if len(initial) != 2 {
		t.Fatalf("initial status count = %d", len(initial))
	}
	if !reflect.DeepEqual(initial, []hatSql.QuerySubscriptionStatus{
		{ID: 1, Revision: 1, Frontier: 0, Lag: 10, QueuedUpdates: 0},
		{ID: 2, Revision: 1, Frontier: 0, Lag: 10, QueuedUpdates: 0},
	}) {
		t.Fatalf("initial status = %#v", initial)
	}

	if err := registry.NotifyChangedAt(context.Background(), 4, nil, resolver, hatSql.QueryOptions{}); err != nil {
		t.Fatal(err)
	}
	statuses := registry.Status(10)
	if !reflect.DeepEqual(statuses, []hatSql.QuerySubscriptionStatus{
		{ID: 1, Revision: 1, Frontier: 4, Lag: 6, QueuedUpdates: 1},
		{ID: 2, Revision: 1, Frontier: 4, Lag: 6, QueuedUpdates: 0},
	}) {
		t.Fatalf("status after frontier = %#v", statuses)
	}
	if got := registry.Status(2)[0].Lag; got != 0 {
		t.Fatalf("status with an older observed frontier lag = %d, want 0", got)
	}

	progress.Close()
	statuses = registry.Status(10)
	if len(statuses) != 1 || statuses[0].ID != 2 {
		t.Fatalf("status after close = %#v", statuses)
	}
	regular.Close()
	if statuses = registry.Status(10); statuses != nil {
		t.Fatalf("empty status = %#v, want nil", statuses)
	}
}

func TestQuerySubscriptionsStatusDoesNotExposeClosedSubscriptions(t *testing.T) {
	registry := hatSql.NewQuerySubscriptions(1)
	if got := registry.Status(1); got != nil {
		t.Fatalf("nil registry status = %#v", got)
	}
	var nilRegistry *hatSql.QuerySubscriptions
	if got := nilRegistry.Status(1); got != nil {
		t.Fatalf("nil registry pointer status = %#v", got)
	}
}

func BenchmarkQuerySubscriptionsStatus(b *testing.B) {
	resolver := hatSql.SourceResolverFunc(func(_ string, key string) ([]hatSql.Row, error) {
		return []hatSql.Row{{"name": key}}, nil
	})
	registry := hatSql.NewQuerySubscriptions(1)
	for index := 0; index < 64; index++ {
		if _, err := registry.Subscribe(context.Background(), hatSql.QuerySubscriptionDefinition{
			Query:        "FROM CACHE('people') SELECT name",
			Dependencies: []string{"people"},
		}, resolver, hatSql.QueryOptions{}); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	for range b.N {
		statuses := registry.Status(100)
		if len(statuses) != 64 {
			b.Fatalf("status count = %d", len(statuses))
		}
	}
}
