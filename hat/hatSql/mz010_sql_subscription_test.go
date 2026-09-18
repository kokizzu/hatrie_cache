//go:build !mz010baseline

package hatSql

import (
	"context"
	"reflect"
	"testing"
)

func TestMZ010SQLQueryDependencies(t *testing.T) {
	got, err := SQLQueryDependencies(`WITH active AS (FROM CACHE('people') SELECT id) FROM active JOIN CACHE('teams') ON active.id = teams.id SELECT active.id`, nil)
	if err != nil {
		t.Fatalf("SQLQueryDependencies() error = %v", err)
	}
	want := []string{"people", "teams"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("SQLQueryDependencies() = %#v, want %#v", got, want)
	}
	got, err = SQLQueryDependencies(`FROM CACHE($1) SELECT id`, []interface{}{"people"})
	if err != nil {
		t.Fatalf("SQLQueryDependencies(parameterized) error = %v", err)
	}
	if want := []string{"people"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("SQLQueryDependencies(parameterized) = %#v, want %#v", got, want)
	}
}

func TestMZ010SubscribeSQLDerivesDependencies(t *testing.T) {
	rows := map[string][]Row{
		"people": {{"id": 1, "name": "Ada"}},
		"teams":  {{"id": 1, "name": "Core"}},
	}
	resolver := SourceResolverFunc(func(_ string, key string) ([]Row, error) {
		return CloneRows(rows[key]), nil
	})
	registry := NewQuerySubscriptions(2)
	subscription, err := registry.SubscribeSQL(context.Background(), QuerySubscriptionDefinition{
		Query: `FROM CACHE('people') SELECT name`,
	}, resolver, QueryOptions{})
	if err != nil {
		t.Fatalf("SubscribeSQL() error = %v", err)
	}
	defer subscription.Close()
	snapshot, ok := subscription.Snapshot()
	if !ok || !reflect.DeepEqual(snapshot.Result.Rows, []Row{{"name": "Ada"}}) {
		t.Fatalf("initial snapshot = %#v, ok=%v", snapshot, ok)
	}
	rows["people"] = []Row{{"id": 2, "name": "Lin"}}
	if err := registry.NotifyChanged(context.Background(), []string{"people"}, resolver, QueryOptions{}); err != nil {
		t.Fatalf("NotifyChanged() error = %v", err)
	}
	update := <-subscription.Updates()
	if !reflect.DeepEqual(update.Result.Rows, []Row{{"name": "Lin"}}) || update.Revision != 2 {
		t.Fatalf("update = %#v", update)
	}
}

func TestMZ010SubscribeDifferentialSQLDerivesDependencies(t *testing.T) {
	rows := map[string][]Row{"people": {{"id": 1, "name": "Ada"}}}
	resolver := SourceResolverFunc(func(_ string, key string) ([]Row, error) {
		return CloneRows(rows[key]), nil
	})
	registry := NewQuerySubscriptions(2)
	subscription, err := registry.SubscribeDifferentialSQL(context.Background(), QuerySubscriptionDefinition{
		Query:              `FROM CACHE('people') SELECT id, name`,
		DeterministicOrder: true,
	}, resolver, QueryOptions{})
	if err != nil {
		t.Fatalf("SubscribeDifferentialSQL() error = %v", err)
	}
	defer subscription.Close()
	initial := <-subscription.Updates()
	if len(initial.Deltas) != 1 || initial.Deltas[0].Diff != 1 {
		t.Fatalf("initial differential batch = %#v", initial)
	}
	rows["people"] = []Row{{"id": 2, "name": "Lin"}}
	if err := registry.NotifyChanged(context.Background(), []string{"people"}, resolver, QueryOptions{}); err != nil {
		t.Fatalf("NotifyChanged() error = %v", err)
	}
	update := <-subscription.Updates()
	if len(update.Deltas) != 2 || update.Deltas[0].Diff != -1 || update.Deltas[1].Diff != 1 {
		t.Fatalf("update differential batch = %#v", update)
	}
}

func TestMZ010SQLQueryDependenciesRejectsInvalidQuery(t *testing.T) {
	if _, err := SQLQueryDependencies("SELECT", nil); err == nil {
		t.Fatal("SQLQueryDependencies() error = nil, want parse error")
	}
}
