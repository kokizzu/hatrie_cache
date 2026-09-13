package hatSql_test

import (
	"context"
	"fmt"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestMZ025HeartbeatEmitsProgressWithoutQueryEvaluation(t *testing.T) {
	queryCalls := 0
	resolver := hatSql.SourceResolverFunc(func(_ string, key string) ([]hatSql.Row, error) {
		queryCalls++
		return []hatSql.Row{{"name": key}}, nil
	})
	registry := hatSql.NewQuerySubscriptions(2)
	progress, err := registry.Subscribe(context.Background(), hatSql.QuerySubscriptionDefinition{
		Query:        "FROM CACHE('people') SELECT name",
		Dependencies: []string{"people"},
		EmitProgress: true,
	}, resolver, hatSql.QueryOptions{})
	if err != nil {
		t.Fatalf("Subscribe(progress) error = %v", err)
	}
	defer progress.Close()
	regular, err := registry.Subscribe(context.Background(), hatSql.QuerySubscriptionDefinition{
		Query:        "FROM CACHE('teams') SELECT name",
		Dependencies: []string{"teams"},
	}, resolver, hatSql.QueryOptions{})
	if err != nil {
		t.Fatalf("Subscribe(regular) error = %v", err)
	}
	defer regular.Close()
	if queryCalls != 2 {
		t.Fatalf("initial query calls = %d, want 2", queryCalls)
	}

	if err := registry.Heartbeat(10); err != nil {
		t.Fatalf("Heartbeat() error = %v", err)
	}
	update := <-progress.Updates()
	if !update.Progress || update.Frontier != 10 || update.Revision != 1 || update.Complete {
		t.Fatalf("heartbeat update = %#v, want progress frontier 10 revision 1", update)
	}
	if queryCalls != 2 {
		t.Fatalf("heartbeat query calls = %d, want unchanged at 2", queryCalls)
	}
	select {
	case update := <-regular.Updates():
		t.Fatalf("regular subscription received heartbeat = %#v", update)
	default:
	}

	if err := registry.Heartbeat(10); err != nil {
		t.Fatalf("Heartbeat(same frontier) error = %v", err)
	}
	repeated := <-progress.Updates()
	if !repeated.Progress || repeated.Frontier != 10 || repeated.Revision != 1 {
		t.Fatalf("repeated heartbeat = %#v, want same progress frontier and revision", repeated)
	}

	if err := registry.Heartbeat(0); err == nil {
		t.Fatal("Heartbeat(zero) error = nil")
	}
	var nilRegistry *hatSql.QuerySubscriptions
	if err := nilRegistry.Heartbeat(1); err == nil {
		t.Fatal("nil registry Heartbeat() error = nil")
	}
}

func TestMZ025HeartbeatEmitsDifferentialProgressAndCompletesAtUpTo(t *testing.T) {
	resolver := hatSql.SourceResolverFunc(func(_ string, key string) ([]hatSql.Row, error) {
		return []hatSql.Row{{"name": key}}, nil
	})
	registry := hatSql.NewQuerySubscriptions(2)
	differential, err := registry.SubscribeDifferential(context.Background(), hatSql.QuerySubscriptionDefinition{
		Query:        "FROM CACHE('people') SELECT name",
		Dependencies: []string{"people"},
		EmitProgress: true,
	}, resolver, hatSql.QueryOptions{})
	if err != nil {
		t.Fatalf("SubscribeDifferential() error = %v", err)
	}
	defer differential.Close()
	initial := <-differential.Updates()
	if initial.Progress || len(initial.Deltas) == 0 {
		t.Fatalf("initial differential batch = %#v, want data deltas", initial)
	}
	if err := registry.Heartbeat(10); err != nil {
		t.Fatalf("Heartbeat(differential) error = %v", err)
	}
	progress := <-differential.Updates()
	if !progress.Progress || progress.Frontier != 10 || len(progress.Deltas) != 0 {
		t.Fatalf("differential heartbeat = %#v, want empty progress batch", progress)
	}

	bounded, err := registry.Subscribe(context.Background(), hatSql.QuerySubscriptionDefinition{
		Query:        "FROM CACHE('people') SELECT name",
		Dependencies: []string{"people"},
		UpTo:         20,
		EmitProgress: true,
		StartLive:    true,
	}, resolver, hatSql.QueryOptions{})
	if err != nil {
		t.Fatalf("Subscribe(bounded) error = %v", err)
	}
	if err := registry.Heartbeat(25); err != nil {
		t.Fatalf("Heartbeat(complete) error = %v", err)
	}
	completed := <-bounded.Updates()
	if !completed.Progress || !completed.Complete || completed.Frontier != 20 {
		t.Fatalf("completed heartbeat = %#v, want complete frontier 20", completed)
	}
	if _, open := <-bounded.Updates(); open {
		t.Fatal("bounded Updates() remained open after heartbeat completion")
	}
	if _, ok := bounded.Snapshot(); ok {
		t.Fatal("bounded Snapshot() remained available after heartbeat completion")
	}
}

func ExampleQuerySubscriptions_Heartbeat() {
	resolver := hatSql.SourceResolverFunc(func(_ string, key string) ([]hatSql.Row, error) {
		return []hatSql.Row{{"name": key}}, nil
	})
	registry := hatSql.NewQuerySubscriptions(1)
	subscription, _ := registry.Subscribe(context.Background(), hatSql.QuerySubscriptionDefinition{
		Query:        "FROM CACHE('people') SELECT name",
		Dependencies: []string{"people"},
		EmitProgress: true,
	}, resolver, hatSql.QueryOptions{})
	defer subscription.Close()
	_ = registry.Heartbeat(42)
	update := <-subscription.Updates()
	fmt.Println(update.Progress, update.Frontier, update.Revision)
	// Output: true 42 1
}
