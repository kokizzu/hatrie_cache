package hatSql

import (
	"context"
	"testing"
)

type sqlColumnarSelectiveBenchmarkResolver struct {
	batch ColumnarBatch
}

func (resolver sqlColumnarSelectiveBenchmarkResolver) ResolveSQLSource(string, string) ([]Row, error) {
	return nil, nil
}

func (resolver sqlColumnarSelectiveBenchmarkResolver) ResolveSQLColumnarSource(_ string, _ string, _ []string) (ColumnarBatch, bool, error) {
	return resolver.batch, true, nil
}

func (resolver sqlColumnarSelectiveBenchmarkResolver) SQLSourceVersion(string, string) (string, bool, error) {
	return "benchmark-v1", true, nil
}

func BenchmarkSQLColumnarSelectiveFilter(b *testing.B) {
	const rows = 20_000
	ids := make([]interface{}, rows)
	scores := make([]interface{}, rows)
	teams := make([]interface{}, rows)
	for index := 0; index < rows; index++ {
		ids[index] = int64(index)
		scores[index] = int64(index % 1_000)
		teams[index] = "core"
	}
	resolver := sqlColumnarSelectiveBenchmarkResolver{batch: ColumnarBatch{Columns: map[string][]interface{}{
		"id":    ids,
		"score": scores,
		"team":  teams,
	}, Rows: rows}}
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		if _, err := ExecuteSQLQueryParameters(ctx, "SELECT id, team FROM CACHE('items') WHERE score = 7", resolver, nil, SQLQueryOptions{}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSQLColumnarSelectiveFilterConditionCacheHit(b *testing.B) {
	const rows = 20_000
	ids := make([]interface{}, rows)
	scores := make([]interface{}, rows)
	teams := make([]interface{}, rows)
	for index := 0; index < rows; index++ {
		ids[index] = int64(index)
		scores[index] = int64(index % 1_000)
		teams[index] = "core"
	}
	resolver := sqlColumnarSelectiveBenchmarkResolver{batch: ColumnarBatch{Columns: map[string][]interface{}{
		"id":    ids,
		"score": scores,
		"team":  teams,
	}, Rows: rows}}
	ctx := context.Background()
	options := SQLQueryOptions{ConditionCache: NewSQLQueryConditionCache(1, 64)}
	if _, err := ExecuteSQLQueryParameters(ctx, "SELECT id, team FROM CACHE('items') WHERE score = 7", resolver, nil, options); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		if _, err := ExecuteSQLQueryParameters(ctx, "SELECT id, team FROM CACHE('items') WHERE score = 7", resolver, nil, options); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSQLColumnarSelectiveFilterConditionCacheCold(b *testing.B) {
	const rows = 20_000
	ids := make([]interface{}, rows)
	scores := make([]interface{}, rows)
	teams := make([]interface{}, rows)
	for index := 0; index < rows; index++ {
		ids[index] = int64(index)
		scores[index] = int64(index % 1_000)
		teams[index] = "core"
	}
	resolver := sqlColumnarSelectiveBenchmarkResolver{batch: ColumnarBatch{Columns: map[string][]interface{}{
		"id":    ids,
		"score": scores,
		"team":  teams,
	}, Rows: rows}}
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		options := SQLQueryOptions{ConditionCache: NewSQLQueryConditionCache(1, 64)}
		if _, err := ExecuteSQLQueryParameters(ctx, "SELECT id, team FROM CACHE('items') WHERE score = 7", resolver, nil, options); err != nil {
			b.Fatal(err)
		}
	}
}
