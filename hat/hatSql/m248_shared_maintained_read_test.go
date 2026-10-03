package hatSql_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"hatrie_cache/hat/hatSql"
)

type m248CountingResolver struct {
	rows  []hatSql.Row
	calls int64
}

type m248KeyedCountingResolver struct {
	rows  map[string][]hatSql.Row
	calls int64
}

func (resolver *m248CountingResolver) ResolveSQLSource(_ string, _ string) ([]hatSql.Row, error) {
	atomic.AddInt64(&resolver.calls, 1)
	return hatSql.CloneRows(resolver.rows), nil
}

func (resolver *m248KeyedCountingResolver) ResolveSQLSource(_ string, key string) ([]hatSql.Row, error) {
	atomic.AddInt64(&resolver.calls, 1)
	return hatSql.CloneRows(resolver.rows[key]), nil
}

func TestM248SharedMaintainedReadMemoizesWithinRefresh(t *testing.T) {
	resolver := &m248CountingResolver{rows: []hatSql.Row{{"id": 1}, {"id": 2}}}
	registry := hatSql.NewQuerySubscriptions(2)
	definition := hatSql.QuerySubscriptionDefinition{
		Query:               "FROM CACHE('people') SELECT id ORDER BY id",
		Dependencies:        []string{"people"},
		ShareIdenticalReads: true,
	}
	first, err := registry.Subscribe(context.Background(), definition, resolver, hatSql.QueryOptions{})
	if err != nil {
		t.Fatalf("first Subscribe() error = %v", err)
	}
	second, err := registry.Subscribe(context.Background(), definition, resolver, hatSql.QueryOptions{})
	if err != nil {
		t.Fatalf("second Subscribe() error = %v", err)
	}
	defer first.Close()
	defer second.Close()
	atomic.StoreInt64(&resolver.calls, 0)
	resolver.rows = []hatSql.Row{{"id": 3}}

	if err := registry.NotifyChanged(context.Background(), []string{"people"}, resolver, hatSql.QueryOptions{}); err != nil {
		t.Fatalf("NotifyChanged() error = %v", err)
	}
	if got := atomic.LoadInt64(&resolver.calls); got != 1 {
		t.Fatalf("source resolve calls = %d, want one shared read", got)
	}
	for index, subscription := range []*hatSql.QuerySubscription{first, second} {
		select {
		case update := <-subscription.Updates():
			if len(update.Result.Rows) != 1 || update.Result.Rows[0]["id"] != 3 {
				t.Fatalf("subscription %d update = %#v", index, update)
			}
		case <-time.After(time.Second):
			t.Fatalf("subscription %d did not receive update", index)
		}
	}
}

func TestM248SharedMaintainedReadIsOptIn(t *testing.T) {
	resolver := &m248CountingResolver{rows: []hatSql.Row{{"id": 1}}}
	registry := hatSql.NewQuerySubscriptions(1)
	definition := hatSql.QuerySubscriptionDefinition{
		Query:        "FROM CACHE('people') SELECT id",
		Dependencies: []string{"people"},
	}
	first, err := registry.Subscribe(context.Background(), definition, resolver, hatSql.QueryOptions{})
	if err != nil {
		t.Fatalf("first Subscribe() error = %v", err)
	}
	second, err := registry.Subscribe(context.Background(), definition, resolver, hatSql.QueryOptions{})
	if err != nil {
		t.Fatalf("second Subscribe() error = %v", err)
	}
	defer first.Close()
	defer second.Close()
	atomic.StoreInt64(&resolver.calls, 0)
	if err := registry.NotifyChanged(context.Background(), []string{"people"}, resolver, hatSql.QueryOptions{}); err != nil {
		t.Fatalf("NotifyChanged() error = %v", err)
	}
	if got := atomic.LoadInt64(&resolver.calls); got != 2 {
		t.Fatalf("default source resolve calls = %d, want two", got)
	}
}

func TestM248SharedMaintainedReadSeparatesParameters(t *testing.T) {
	resolver := &m248KeyedCountingResolver{rows: map[string][]hatSql.Row{
		"people": {{"id": 1}},
		"teams":  {{"id": 2}},
	}}
	registry := hatSql.NewQuerySubscriptions(1)
	first, err := registry.Subscribe(context.Background(), hatSql.QuerySubscriptionDefinition{
		Query:               "FROM CACHE($1) SELECT id",
		Parameters:          []interface{}{"people"},
		Dependencies:        []string{"people"},
		ShareIdenticalReads: true,
	}, resolver, hatSql.QueryOptions{})
	if err != nil {
		t.Fatalf("first Subscribe() error = %v", err)
	}
	second, err := registry.Subscribe(context.Background(), hatSql.QuerySubscriptionDefinition{
		Query:               "FROM CACHE($1) SELECT id",
		Parameters:          []interface{}{"teams"},
		Dependencies:        []string{"teams"},
		ShareIdenticalReads: true,
	}, resolver, hatSql.QueryOptions{})
	if err != nil {
		t.Fatalf("second Subscribe() error = %v", err)
	}
	defer first.Close()
	defer second.Close()
	atomic.StoreInt64(&resolver.calls, 0)
	if err := registry.NotifyChanged(context.Background(), []string{"people", "teams"}, resolver, hatSql.QueryOptions{}); err != nil {
		t.Fatalf("NotifyChanged() error = %v", err)
	}
	if got := atomic.LoadInt64(&resolver.calls); got != 2 {
		t.Fatalf("parameter-separated source resolve calls = %d, want two", got)
	}
}
