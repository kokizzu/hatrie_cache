package hatSql_test

import (
	"context"
	"testing"

	"hatrie_cache/hat/hatSql"
)

const chu05ExternalPartitionedWindowBenchmarkQuery = `
FROM EXTERNAL('events') AS event
SELECT event.id, event.group,
       ROW_NUMBER() OVER (PARTITION BY event.group) AS row_number,
       SUM(event.value) OVER (PARTITION BY event.group) AS running_sum,
       LAG(event.value) OVER (PARTITION BY event.group) AS previous_value`

func chu05ExternalPartitionedWindowBenchmarkRows(count, groups int) []hatSql.Row {
	rows := make([]hatSql.Row, count)
	for index := range rows {
		rows[index] = hatSql.Row{
			"id":    int64(index),
			"group": "group-" + string(rune('a'+index%groups)),
			"value": int64(index%17 + 1),
		}
	}
	return rows
}

var chu05ExternalPartitionedWindowBenchmarkSink int

func BenchmarkCHU05ExternalPartitionedWindowMaterialized(b *testing.B) {
	rows := chu05ExternalPartitionedWindowBenchmarkRows(4096, 64)
	resolver := &chu02ExternalStreamResolver{rows: rows}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		result, err := hatSql.ExecuteSQLQueryContext(context.Background(), chu05ExternalPartitionedWindowBenchmarkQuery, resolver, hatSql.QueryOptions{})
		if err != nil {
			b.Fatal(err)
		}
		chu05ExternalPartitionedWindowBenchmarkSink = len(result.Rows)
	}
}

func BenchmarkCHU05ExternalPartitionedWindowStreaming(b *testing.B) {
	rows := chu05ExternalPartitionedWindowBenchmarkRows(4096, 64)
	resolver := &chu02ExternalStreamResolver{rows: rows}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		count := 0
		err := hatSql.ExecuteSQLQueryRows(context.Background(), chu05ExternalPartitionedWindowBenchmarkQuery, resolver, nil, hatSql.QueryOptions{}, func([]string, hatSql.SQLRow) error {
			count++
			return nil
		})
		if err != nil {
			b.Fatal(err)
		}
		chu05ExternalPartitionedWindowBenchmarkSink = count
	}
}
