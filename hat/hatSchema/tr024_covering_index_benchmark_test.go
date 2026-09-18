package hatSchema

import (
	"context"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func BenchmarkTR024CoveringIndexedProjection(b *testing.B) {
	source := newTR024BenchmarkSource(b)
	if _, err := source.BuildCoveringIndex("region", []string{"name"}); err != nil {
		b.Fatal(err)
	}
	resolver := SQLResolverAdapter{Sources: map[string]*MaterializedSource{"items": source}}
	const query = "FROM CACHE('items') AS i WHERE i.region = 'r-07' SELECT i.region, i.name"
	b.ReportAllocs()
	for b.Loop() {
		result, err := hatSql.ExecuteSQLQueryContext(context.Background(), query, resolver, hatSql.SQLQueryOptions{})
		if err != nil {
			b.Fatal(err)
		}
		if len(result.Rows) != 313 {
			b.Fatalf("rows = %d, want 313", len(result.Rows))
		}
	}
}
