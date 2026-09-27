package hatSql_test

import (
	"context"
	"strconv"
	"testing"

	"hatrie_cache/hat/hatSql"
)

var ch011SourceNotificationsBenchmarkSink int

// BenchmarkCH011DirectMaterializedViewRefresh is the pre-queue baseline:
// every source notification refreshes the dependency set synchronously.
func BenchmarkCH011DirectMaterializedViewRefresh(b *testing.B) {
	current := "Ada"
	resolver := hatSql.SourceResolverFunc(func(_ string, _ string) ([]hatSql.Row, error) {
		return []hatSql.Row{{"name": current}}, nil
	})
	views := hatSql.NewMaterializedViews()
	if _, err := views.Create(context.Background(), hatSql.MaterializedViewDefinition{
		Name:         "people_view",
		Query:        "FROM CACHE('people') SELECT name",
		Dependencies: []string{"people"},
	}, resolver, hatSql.QueryOptions{}); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		current = "person-" + strconv.Itoa(index)
		statuses, err := views.RefreshChanged(context.Background(), []string{"people"}, resolver, hatSql.QueryOptions{})
		if err != nil {
			b.Fatal(err)
		}
		ch011SourceNotificationsBenchmarkSink += len(statuses)
	}
}
