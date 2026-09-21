package hatSql_test

import (
	"context"
	"fmt"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func BenchmarkC228ExternalSortStableRuns(b *testing.B) {
	const rowCount = 192
	rows := make([]hatSql.Row, rowCount)
	for index := range rows {
		rows[index] = hatSql.Row{
			"id":    int64(index),
			"score": int64(index % 3),
			"tag":   fmt.Sprintf("row-%03d", index),
		}
	}
	query := `
FROM EXTERNAL('events') AS event
SELECT event.id, event.score, event.tag
ORDER BY event.score ASC`
	options := hatSql.QueryOptions{
		MaxSortBytes:   128,
		SpillDirectory: b.TempDir(),
		MaxSpillBytes:  1 << 20,
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		resolver := &chu02ExternalStreamResolver{rows: rows}
		emitted := 0
		err := hatSql.ExecuteSQLQueryRows(context.Background(), query, resolver, nil, options, func([]string, hatSql.SQLRow) error {
			emitted++
			return nil
		})
		if err != nil {
			b.Fatal(err)
		}
		if emitted != rowCount {
			b.Fatalf("emitted rows = %d, want %d", emitted, rowCount)
		}
	}
}
