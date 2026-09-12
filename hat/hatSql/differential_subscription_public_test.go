package hatSql_test

import (
	"context"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestQueryDifferentialSubscriptionIsImportable(t *testing.T) {
	resolver := hatSql.SourceResolverFunc(func(string, string) ([]hatSql.Row, error) {
		return []hatSql.Row{{"value": int64(1)}}, nil
	})
	registry := hatSql.NewQuerySubscriptions(1)
	subscription, err := registry.SubscribeDifferential(context.Background(), hatSql.QuerySubscriptionDefinition{
		Query:        "FROM CACHE('events') SELECT value",
		Dependencies: []string{"events"},
	}, resolver, hatSql.QueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer subscription.Close()
	update := <-subscription.Updates()
	if len(update.Deltas) != 1 || update.Deltas[0].Diff != 1 {
		t.Fatalf("initial update = %#v", update)
	}
}
