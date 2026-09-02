package hatSql_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"hatrie_cache/hat/hatSql"
)

type frontierSubscriptionResolver struct {
	rows    []hatSql.Row
	history map[uint64][]hatSql.Row
}

func (resolver *frontierSubscriptionResolver) ResolveSQLSource(name, key string) ([]hatSql.Row, error) {
	if name != "CACHE" || key != "people" {
		return nil, nil
	}
	return hatSql.CloneRows(resolver.rows), nil
}

func (resolver *frontierSubscriptionResolver) ResolveSQLSourceAt(name, key string, frontier uint64) ([]hatSql.Row, error) {
	if name != "CACHE" || key != "people" {
		return nil, nil
	}
	rows, ok := resolver.history[frontier]
	if !ok {
		return nil, fmt.Errorf("frontier %d is unavailable", frontier)
	}
	return hatSql.CloneRows(rows), nil
}

func TestQuerySubscriptionFrontierHonorsAsOfUpToAndProgress(t *testing.T) {
	resolver := &frontierSubscriptionResolver{
		rows:    []hatSql.Row{{"name": "Lin"}},
		history: map[uint64][]hatSql.Row{10: {{"name": "Ada"}}, 15: {{"name": "Lin"}}},
	}
	registry := hatSql.NewQuerySubscriptions(4)
	subscription, err := registry.Subscribe(context.Background(), hatSql.QuerySubscriptionDefinition{
		Query:        "FROM CACHE('people') SELECT name",
		Dependencies: []string{"people"},
		AsOf:         10,
		UpTo:         20,
		EmitProgress: true,
	}, resolver, hatSql.QueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer subscription.Close()
	initial, ok := subscription.Snapshot()
	if !ok || initial.Frontier != 10 || initial.Progress || initial.Complete || initial.Result.Rows[0]["name"] != "Ada" {
		t.Fatalf("initial snapshot = %#v, %v", initial, ok)
	}

	if err := registry.NotifyChangedAt(context.Background(), 10, []string{"people"}, resolver, hatSql.QueryOptions{}); err != nil {
		t.Fatal(err)
	}
	select {
	case update := <-subscription.Updates():
		t.Fatalf("as-of update = %#v", update)
	default:
	}

	if err := registry.NotifyChangedAt(context.Background(), 15, []string{"people"}, resolver, hatSql.QueryOptions{}); err != nil {
		t.Fatal(err)
	}
	update := receiveSubscriptionUpdate(t, subscription)
	if update.Progress || update.Complete || update.Frontier != 15 || update.Result.Rows[0]["name"] != "Lin" {
		t.Fatalf("data update = %#v", update)
	}
	progress := receiveSubscriptionUpdate(t, subscription)
	if !progress.Progress || progress.Complete || progress.Frontier != 15 || len(progress.Result.Rows) != 0 || len(progress.Result.Columns) != 0 {
		t.Fatalf("progress update = %#v", progress)
	}

	if err := registry.NotifyChangedAt(context.Background(), 25, []string{"people"}, resolver, hatSql.QueryOptions{}); err != nil {
		t.Fatal(err)
	}
	completion := receiveSubscriptionUpdate(t, subscription)
	if !completion.Progress || !completion.Complete || completion.Frontier != 20 {
		t.Fatalf("completion update = %#v", completion)
	}
	select {
	case _, open := <-subscription.Updates():
		if open {
			t.Fatal("completed subscription channel remains open")
		}
	case <-time.After(time.Second):
		t.Fatal("completed subscription channel did not close")
	}
}

func TestQuerySubscriptionFrontierRejectsUnsafeHistoricalRequests(t *testing.T) {
	resolver := hatSql.SourceResolverFunc(func(string, string) ([]hatSql.Row, error) {
		return []hatSql.Row{{"name": "Ada"}}, nil
	})
	registry := hatSql.NewQuerySubscriptions(1)
	if _, err := registry.Subscribe(context.Background(), hatSql.QuerySubscriptionDefinition{
		Query:        "FROM CACHE('people') SELECT name",
		Dependencies: []string{"people"},
		AsOf:         1,
	}, resolver, hatSql.QueryOptions{}); err == nil {
		t.Fatal("Subscribe() error = nil for a non-historical AS OF resolver")
	}
	if _, err := registry.Subscribe(context.Background(), hatSql.QuerySubscriptionDefinition{
		Query:        "FROM CACHE('people') SELECT name",
		Dependencies: []string{"people"},
		AsOf:         3,
		UpTo:         2,
	}, resolver, hatSql.QueryOptions{}); err == nil {
		t.Fatal("Subscribe() error = nil for an inverted frontier")
	}
	if err := registry.NotifyChangedAt(context.Background(), 0, nil, resolver, hatSql.QueryOptions{}); err == nil {
		t.Fatal("NotifyChangedAt() error = nil for a zero frontier")
	}
}

func receiveSubscriptionUpdate(t *testing.T, subscription *hatSql.QuerySubscription) hatSql.QuerySubscriptionSnapshot {
	t.Helper()
	select {
	case update := <-subscription.Updates():
		return update
	case <-time.After(time.Second):
		t.Fatal("subscription update timeout")
		return hatSql.QuerySubscriptionSnapshot{}
	}
}

func BenchmarkQuerySubscriptionFrontier(b *testing.B) {
	resolver := hatSql.SourceResolverFunc(func(string, string) ([]hatSql.Row, error) {
		return []hatSql.Row{{"name": "Ada"}}, nil
	})
	query := "FROM CACHE('people') SELECT name"
	b.Run("legacy_noop", func(b *testing.B) {
		registry := hatSql.NewQuerySubscriptions(1)
		subscription, err := registry.Subscribe(context.Background(), hatSql.QuerySubscriptionDefinition{
			Query:        query,
			Dependencies: []string{"people"},
		}, resolver, hatSql.QueryOptions{})
		if err != nil {
			b.Fatal(err)
		}
		defer subscription.Close()
		b.ReportAllocs()
		for range b.N {
			if err := registry.NotifyChanged(context.Background(), []string{"teams"}, resolver, hatSql.QueryOptions{}); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("framed_frontier", func(b *testing.B) {
		registry := hatSql.NewQuerySubscriptions(1)
		subscription, err := registry.Subscribe(context.Background(), hatSql.QuerySubscriptionDefinition{
			Query:        query,
			Dependencies: []string{"people"},
		}, resolver, hatSql.QueryOptions{})
		if err != nil {
			b.Fatal(err)
		}
		defer subscription.Close()
		b.ReportAllocs()
		for sequence := uint64(1); sequence <= uint64(b.N); sequence++ {
			if err := registry.NotifyChangedAt(context.Background(), sequence, nil, resolver, hatSql.QueryOptions{}); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("framed_progress", func(b *testing.B) {
		registry := hatSql.NewQuerySubscriptions(1)
		subscription, err := registry.Subscribe(context.Background(), hatSql.QuerySubscriptionDefinition{
			Query:        query,
			Dependencies: []string{"people"},
			EmitProgress: true,
		}, resolver, hatSql.QueryOptions{})
		if err != nil {
			b.Fatal(err)
		}
		defer subscription.Close()
		b.ReportAllocs()
		for sequence := uint64(1); sequence <= uint64(b.N); sequence++ {
			if err := registry.NotifyChangedAt(context.Background(), sequence, nil, resolver, hatSql.QueryOptions{}); err != nil {
				b.Fatal(err)
			}
		}
	})
}
