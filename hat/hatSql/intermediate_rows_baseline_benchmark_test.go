package hatSql

import (
	"context"
	"testing"
)

func BenchmarkSQLIntermediateRowsBaseline(b *testing.B) {
	resolver := SourceResolverFunc(func(_, _ string) ([]Row, error) {
		rows := make([]Row, 0, 64)
		for index := 0; index < 64; index++ {
			rows = append(rows, Row{"id": index, "key": index % 8})
		}
		return rows, nil
	})
	query := `FROM CACHE('left') AS l JOIN CACHE('right') AS r ON l.key = r.key SELECT l.id, r.id`
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		result, err := ExecuteSQLQueryContext(context.Background(), query, resolver, SQLQueryOptions{})
		if err != nil || len(result.Rows) != 512 {
			b.Fatalf("baseline query result=%d err=%v, want 512 rows", len(result.Rows), err)
		}
	}
}
