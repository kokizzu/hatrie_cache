package hatSql

import (
	"context"
	"fmt"
	"testing"
)

func BenchmarkC227ExternalGroupMergeBudgetDisabled(b *testing.B) {
	benchmarkC227ExternalGroupMerge(b, 0)
}

func BenchmarkC227ExternalGroupMergeBudgetEnabled(b *testing.B) {
	benchmarkC227ExternalGroupMerge(b, 1<<20)
}

func benchmarkC227ExternalGroupMerge(b *testing.B, mergeBytes int) {
	query := c227GroupQuery(2048)
	options := SQLQueryOptions{
		MaxGroupBytes:      16 << 10,
		MaxGroupMergeBytes: mergeBytes,
		SpillDirectory:     b.TempDir(),
		MaxSpillBytes:      64 << 20,
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		result, err := ExecuteSQLQueryContext(context.Background(), query, nil, options)
		if err != nil || len(result.Rows) != 2048 {
			b.Fatalf("external group merge: rows=%d err=%v", len(result.Rows), err)
		}
	}
}

func c227GroupQuery(groups int) string {
	query := "FROM VALUES "
	for index := 0; index < groups; index++ {
		if index > 0 {
			query += ", "
		}
		query += fmt.Sprintf("('group-%04d')", index)
	}
	return query + " AS src(region) SELECT src.region, COUNT(*) AS total GROUP BY src.region"
}
