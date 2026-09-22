package hatSql

import (
	"context"
	"testing"
)

func BenchmarkCH004FinalSchemaRegistry(b *testing.B) {
	rows := ch004SchemaBenchmarkRows()
	resolver := ch004SchemaBenchmarkSourceResolver(rows)
	registry := NewSQLFinalSchemaRegistry()
	if err := registry.Register("CACHE", "events", ch004SchemaBenchmarkOptions()); err != nil {
		b.Fatal(err)
	}
	options := SQLQueryOptions{FinalSchema: registry}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := ExecuteSQLQueryParameters(context.Background(),
			"FROM CACHE('events') AS event FINAL SELECT event.id, event.value ORDER BY event.id",
			resolver, nil, options); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCH004FinalExplicitResolverSameWorkload(b *testing.B) {
	rows := ch004SchemaBenchmarkRows()
	resolver := ch004SchemaBenchmarkSourceResolver(rows)
	options := SQLQueryOptions{
		FinalSourceOptions: ch004FinalSourceOptionsResolver(func(kind, key string) (SQLFinalOptions, bool, error) {
			if kind != "CACHE" || key != "events" {
				return SQLFinalOptions{}, false, nil
			}
			return ch004SchemaBenchmarkOptions(), true, nil
		}),
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := ExecuteSQLQueryParameters(context.Background(),
			"FROM CACHE('events') AS event FINAL SELECT event.id, event.value ORDER BY event.id",
			resolver, nil, options); err != nil {
			b.Fatal(err)
		}
	}
}

func ch004SchemaBenchmarkRows() []SQLRow {
	rows := make([]SQLRow, 0, 32)
	for index := 0; index < 32; index++ {
		rows = append(rows, SQLRow{
			"id":      string(rune('a' + index%8)),
			"version": uint64(index),
			"value":   index,
		})
	}
	return rows
}

func ch004SchemaBenchmarkSourceResolver(rows []SQLRow) SourceResolverFunc {
	return func(_, _ string) ([]Row, error) {
		return rows, nil
	}
}

func ch004SchemaBenchmarkOptions() SQLFinalOptions {
	return SQLFinalOptions{
		Mode: SQLFinalReplacing,
		Key: func(row SQLRow) string {
			return row["id"].(string)
		},
		Version: func(row SQLRow) (uint64, error) {
			return row["version"].(uint64), nil
		},
	}
}
