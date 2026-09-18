package hatSchema

import (
	"context"
	"fmt"
	"hatrie_cache/hat/hatSql"
	"strings"
	"testing"
)

func BenchmarkTR023AfterFunctionalIndexSQLLookup(b *testing.B) {
	source := benchmarkTR023MaterializedSource(b)
	if _, err := source.BuildFunctionalIndex(hatSql.LowerIndexField("name"), []string{"name"}, func(row Row) (interface{}, error) {
		name, ok := row["name"].(string)
		if !ok {
			return nil, fmt.Errorf("name is not text")
		}
		return strings.ToLower(name), nil
	}); err != nil {
		b.Fatal(err)
	}
	adapter := SQLResolverAdapter{Sources: map[string]*MaterializedSource{"people": source}}
	const query = "FROM CACHE('people') AS person WHERE LOWER(person.name) = 'name-042' SELECT person.id"
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		result, err := hatSql.ExecuteSQLQueryContext(context.Background(), query, adapter, hatSql.SQLQueryOptions{})
		if err != nil {
			b.Fatal(err)
		}
		if len(result.Rows) != 100 {
			b.Fatalf("indexed rows = %d, want 100", len(result.Rows))
		}
	}
}

func BenchmarkTR023AfterFunctionalIndexBuild(b *testing.B) {
	source := benchmarkTR023MaterializedSource(b)
	evaluator := func(row Row) (interface{}, error) {
		name, ok := row["name"].(string)
		if !ok {
			return nil, fmt.Errorf("name is not text")
		}
		return strings.ToLower(name), nil
	}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		report, err := source.BuildFunctionalIndex(hatSql.LowerIndexField("name"), []string{"name"}, evaluator)
		if err != nil {
			b.Fatal(err)
		}
		if report.Rows != 10_000 {
			b.Fatalf("build rows = %d, want 10000", report.Rows)
		}
	}
}
