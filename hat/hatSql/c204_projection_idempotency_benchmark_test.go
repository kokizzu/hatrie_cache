package hatSql_test

import (
	"context"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func BenchmarkC204ProjectionApplyBaseline(b *testing.B) {
	runner, changes := newC204BenchmarkRunner(b)
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		changes[0].Sequence = uint64(index + 1)
		if _, err := runner.Apply(context.Background(), changes); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkC204ProjectionApplyWithIdempotencyKeys(b *testing.B) {
	runner, changes := newC204BenchmarkRunner(b)
	keys := []string{"async-key"}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		changes[0].Sequence = uint64(index + 1)
		if _, err := runner.ApplyWithIdempotencyKeys(context.Background(), changes, keys); err != nil {
			b.Fatal(err)
		}
	}
}

func newC204BenchmarkRunner(b *testing.B) (*hatSql.IncrementalProjectionRunner, []hatSql.ProjectionChange) {
	resolver := hatSql.SourceResolverFunc(func(_ string, key string) ([]hatSql.Row, error) {
		if key != "people" {
			return nil, nil
		}
		return []hatSql.Row{{"name": "Ada"}}, nil
	})
	views := hatSql.NewMaterializedViews()
	if _, err := views.Create(context.Background(), hatSql.MaterializedViewDefinition{
		Name:         "people_view",
		Query:        "FROM CACHE('people') SELECT name",
		Dependencies: []string{"people"},
	}, resolver, hatSql.QueryOptions{}); err != nil {
		b.Fatal(err)
	}
	runner, err := hatSql.NewIncrementalProjectionRunner(views, resolver, hatSql.QueryOptions{}, hatSql.IncrementalProjectionRunnerOptions{
		Name:    "people",
		Enabled: true,
	})
	if err != nil {
		b.Fatal(err)
	}
	changes := []hatSql.ProjectionChange{{Dependency: "people"}}
	return runner, changes
}
