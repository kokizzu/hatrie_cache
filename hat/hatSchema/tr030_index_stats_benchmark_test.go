package hatSchema

import (
	"context"
	"fmt"
	"hatrie_cache/hat/hatSql"
	"testing"
)

type tr030NoStatsResolver struct {
	source *MaterializedSource
}

func (resolver tr030NoStatsResolver) ResolveSQLSource(name, key string) ([]hatSql.Row, error) {
	if name != "CACHE" || key != "events" {
		return nil, nil
	}
	return sqlRows(resolver.source.Rows()), nil
}

func (resolver tr030NoStatsResolver) ResolveSQLIndexedSource(name, key, field string, value interface{}) ([]hatSql.Row, bool, error) {
	if name != "CACHE" || key != "events" || !resolver.source.HasIndex(field) {
		return nil, false, nil
	}
	return sqlRows(resolver.source.Lookup(field, value)), true, nil
}

func BenchmarkTR030BeforeMaterializedIndexStats(b *testing.B) {
	source := benchmarkTR030Source(b)
	resolver := tr030NoStatsResolver{source: source}
	const query = "FROM CACHE('events') AS event WHERE event.kind = 'common' AND event.id = 'id-7777' SELECT event.id"
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		result, err := hatSql.ExecuteSQLQueryContext(context.Background(), query, resolver, hatSql.SQLQueryOptions{})
		if err != nil {
			b.Fatal(err)
		}
		if len(result.Rows) != 1 {
			b.Fatalf("rows = %d, want 1", len(result.Rows))
		}
	}
}

func BenchmarkTR030AfterMaterializedIndexStats(b *testing.B) {
	source := benchmarkTR030Source(b)
	resolver := SQLResolverAdapter{Sources: map[string]*MaterializedSource{"events": source}}
	const query = "FROM CACHE('events') AS event WHERE event.kind = 'common' AND event.id = 'id-7777' SELECT event.id"
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		result, err := hatSql.ExecuteSQLQueryContext(context.Background(), query, resolver, hatSql.SQLQueryOptions{})
		if err != nil {
			b.Fatal(err)
		}
		if len(result.Rows) != 1 {
			b.Fatalf("rows = %d, want 1", len(result.Rows))
		}
	}
}

func BenchmarkTR030BeforeMaterializedIndexStatsSinglePredicate(b *testing.B) {
	source := benchmarkTR030Source(b)
	resolver := tr030NoStatsResolver{source: source}
	const query = "FROM CACHE('events') AS event WHERE event.id = 'id-7777' SELECT event.id"
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		result, err := hatSql.ExecuteSQLQueryContext(context.Background(), query, resolver, hatSql.SQLQueryOptions{})
		if err != nil {
			b.Fatal(err)
		}
		if len(result.Rows) != 1 {
			b.Fatalf("rows = %d, want 1", len(result.Rows))
		}
	}
}

func BenchmarkTR030AfterMaterializedIndexStatsSinglePredicate(b *testing.B) {
	source := benchmarkTR030Source(b)
	resolver := SQLResolverAdapter{Sources: map[string]*MaterializedSource{"events": source}}
	const query = "FROM CACHE('events') AS event WHERE event.id = 'id-7777' SELECT event.id"
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		result, err := hatSql.ExecuteSQLQueryContext(context.Background(), query, resolver, hatSql.SQLQueryOptions{})
		if err != nil {
			b.Fatal(err)
		}
		if len(result.Rows) != 1 {
			b.Fatalf("rows = %d, want 1", len(result.Rows))
		}
	}
}

func benchmarkTR030Source(b *testing.B) *MaterializedSource {
	b.Helper()
	source := NewMaterializedSource([]DerivedColumn{{Name: "id", Indexed: true}, {Name: "kind", Indexed: true}})
	for index := 0; index < 10_000; index++ {
		kind := "common"
		if index%1_000 == 0 {
			kind = "rare"
		}
		if _, err := source.Insert(Row{"id": fmt.Sprintf("id-%04d", index), "kind": kind}); err != nil {
			b.Fatal(err)
		}
	}
	return source
}
