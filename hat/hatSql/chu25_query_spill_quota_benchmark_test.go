package hatSql_test

import (
	"context"
	"strings"
	"testing"

	hatSql "hatrie_cache/hat/hatSql"
)

func BenchmarkCHU25QuerySpillQuota(b *testing.B) {
	rows := make([]hatSql.Row, 128)
	for index := range rows {
		rows[index] = hatSql.Row{
			"id":      int64(index),
			"payload": strings.Repeat("payload-", 8),
		}
	}
	for _, benchmark := range []struct {
		name  string
		quota int64
	}{
		{name: "quota-disabled"},
		{name: "quota-enabled", quota: 16 << 20},
	} {
		b.Run(benchmark.name, func(b *testing.B) {
			spillDirectory := b.TempDir()
			options := hatSql.SQLQueryOptions{
				MaxSortBytes:       128,
				SpillDirectory:     spillDirectory,
				MaxSpillBytes:      16 << 20,
				MaxQuerySpillBytes: benchmark.quota,
			}
			b.ReportAllocs()
			b.ResetTimer()
			for index := 0; index < b.N; index++ {
				if err := hatSql.ExecuteSQLQueryRows(context.Background(), `
FROM EXTERNAL('events') AS event
SELECT event.id, event.payload
ORDER BY event.id DESC`, &chu02ExternalStreamResolver{rows: rows}, nil, options, func([]string, hatSql.SQLRow) error {
					return nil
				}); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
