package hatSql_test

import (
	"context"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func BenchmarkC237ProjectionExplain(b *testing.B) {
	resolver := &projectionSelectionResolver{
		rows:    []hatSql.Row{{"name": "Ada"}, {"name": "Lin"}},
		version: "1",
	}
	views := hatSql.NewMaterializedViews()
	query := "FROM CACHE('events') SELECT name"
	if _, err := views.Create(context.Background(), hatSql.MaterializedViewDefinition{
		Name:         "event_names",
		Query:        query,
		Dependencies: []string{"events"},
	}, resolver, hatSql.QueryOptions{}); err != nil {
		b.Fatal(err)
	}
	options := hatSql.QueryOptions{ProjectionCatalog: views}
	b.ReportAllocs()
	for range b.N {
		if _, err := hatSql.ExecuteSQLQueryContext(context.Background(), "EXPLAIN "+query, resolver, options); err != nil {
			b.Fatal(err)
		}
	}
}
