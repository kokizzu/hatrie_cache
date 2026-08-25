package hatSql_test

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"hatrie_cache/hat/hatSql"
)

func TestQuerySubscriptionsPublishOnlyChangedResultSnapshots(t *testing.T) {
	rows := map[string][]hatSql.Row{"people": {{"name": "Ada"}}, "teams": {{"name": "Core"}}}
	resolver := hatSql.SourceResolverFunc(func(_ string, key string) ([]hatSql.Row, error) {
		return hatSql.CloneRows(rows[key]), nil
	})
	subscriptions := hatSql.NewQuerySubscriptions(1)
	subscription, err := subscriptions.Subscribe(context.Background(), hatSql.QuerySubscriptionDefinition{
		Query:        "FROM CACHE('people') SELECT name",
		Dependencies: []string{"people"},
	}, resolver, hatSql.QueryOptions{})
	if err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}
	snapshot, ok := subscription.Snapshot()
	if !ok || snapshot.Revision != 1 || !reflect.DeepEqual(snapshot.Result.Rows, []hatSql.Row{{"name": "Ada"}}) {
		t.Fatalf("initial snapshot = %#v, %v", snapshot, ok)
	}
	rows["people"] = []hatSql.Row{{"name": "Lin"}}
	if err := subscriptions.NotifyChanged(context.Background(), []string{"people"}, resolver, hatSql.QueryOptions{}); err != nil {
		t.Fatalf("NotifyChanged() error = %v", err)
	}
	select {
	case update := <-subscription.Updates():
		if update.Revision != 2 || !reflect.DeepEqual(update.Result.Rows, []hatSql.Row{{"name": "Lin"}}) {
			t.Fatalf("update = %#v", update)
		}
	case <-time.After(time.Second):
		t.Fatal("subscription did not receive changed result")
	}
	if err := subscriptions.NotifyChanged(context.Background(), []string{"teams"}, resolver, hatSql.QueryOptions{}); err != nil {
		t.Fatalf("unrelated NotifyChanged() error = %v", err)
	}
	select {
	case update := <-subscription.Updates():
		t.Fatalf("unrelated dependency update = %#v", update)
	default:
	}
	if err := subscriptions.NotifyChanged(context.Background(), []string{"people"}, resolver, hatSql.QueryOptions{}); err != nil {
		t.Fatalf("unchanged NotifyChanged() error = %v", err)
	}
	select {
	case update := <-subscription.Updates():
		t.Fatalf("unchanged result update = %#v", update)
	default:
	}
	subscription.Close()
	if _, open := <-subscription.Updates(); open {
		t.Fatal("closed subscription update channel remains open")
	}
}

func TestQuerySubscriptionsPreserveSnapshotsWhenRefreshFails(t *testing.T) {
	rows := map[string][]hatSql.Row{"people": {{"name": "Ada"}}, "teams": {{"name": "Core"}}}
	resolver := hatSql.SourceResolverFunc(func(_ string, key string) ([]hatSql.Row, error) {
		if _, ok := rows[key]; !ok {
			return nil, fmt.Errorf("source %q unavailable", key)
		}
		return hatSql.CloneRows(rows[key]), nil
	})
	subscriptions := hatSql.NewQuerySubscriptions(1)
	people, err := subscriptions.Subscribe(context.Background(), hatSql.QuerySubscriptionDefinition{
		Query: "FROM CACHE('people') SELECT name", Dependencies: []string{"people"},
	}, resolver, hatSql.QueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	teams, err := subscriptions.Subscribe(context.Background(), hatSql.QuerySubscriptionDefinition{
		Query: "FROM CACHE('teams') SELECT name", Dependencies: []string{"teams"},
	}, resolver, hatSql.QueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	rows["people"] = []hatSql.Row{{"name": "Lin"}}
	delete(rows, "teams")
	if err := subscriptions.NotifyChanged(context.Background(), []string{"people", "teams"}, resolver, hatSql.QueryOptions{}); err == nil {
		t.Fatal("NotifyChanged() error = nil, want unavailable source")
	}
	for _, subscription := range []*hatSql.QuerySubscription{people, teams} {
		snapshot, ok := subscription.Snapshot()
		if !ok || snapshot.Revision != 1 {
			t.Fatalf("snapshot after failed refresh = %#v, %v", snapshot, ok)
		}
		select {
		case update := <-subscription.Updates():
			t.Fatalf("update after failed refresh = %#v", update)
		default:
		}
		subscription.Close()
	}
}
