package hatSql

import (
	"context"
	"fmt"
	"testing"
)

func BenchmarkCH004Final(b *testing.B) {
	cases := []struct {
		name    string
		query   string
		options SQLQueryOptions
	}{
		{
			name:  "legacy",
			query: "FROM CACHE('events') AS event SELECT event.id, event.value",
		},
		{
			name:    "replacing",
			query:   "FROM CACHE('events') AS event FINAL SELECT event.id, event.value",
			options: SQLQueryOptions{FinalSourceOptions: ch004FinalSourceOptionsResolver(ch004BenchmarkReplacingOptions)},
		},
		{
			name:    "collapsing",
			query:   "FROM CACHE('events') AS event FINAL SELECT event.id, event.value",
			options: SQLQueryOptions{FinalSourceOptions: ch004FinalSourceOptionsResolver(ch004BenchmarkCollapsingOptions)},
		},
	}
	rows := ch004BenchmarkRows()
	resolver := SourceResolverFunc(func(_, _ string) ([]Row, error) { return rows, nil })
	for _, test := range cases {
		b.Run(test.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ReportMetric(float64(len(rows)), "input_rows")
			b.ResetTimer()
			for index := 0; index < b.N; index++ {
				if _, err := ExecuteSQLQueryContext(context.Background(), test.query, resolver, test.options); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkCH004FinalRows(b *testing.B) {
	rows := ch004BenchmarkRows()
	resolver := SourceResolverFunc(func(_, _ string) ([]Row, error) { return rows, nil })
	options := SQLQueryOptions{FinalSourceOptions: ch004FinalSourceOptionsResolver(ch004BenchmarkReplacingOptions)}
	b.ReportAllocs()
	b.ReportMetric(float64(len(rows)), "input_rows")
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		count := 0
		err := ExecuteSQLQueryRows(context.Background(),
			"FROM CACHE('events') AS event FINAL SELECT event.id, event.value",
			resolver, nil, options, func(_ []string, _ SQLRow) error {
				count++
				return nil
			})
		if err != nil {
			b.Fatal(err)
		}
		if count == 0 {
			b.Fatal("FINAL row stream returned no rows")
		}
	}
}

func ch004BenchmarkRows() []SQLRow {
	rows := make([]SQLRow, 0, 4096)
	for version := uint64(1); version <= 4; version++ {
		for key := 0; key < 1024; key++ {
			rows = append(rows, SQLRow{
				"id":      fmt.Sprintf("key-%04d", key),
				"version": version,
				"sign":    1,
				"value":   version,
			})
		}
	}
	return rows
}

func ch004BenchmarkReplacingOptions(kind, key string) (SQLFinalOptions, bool, error) {
	if kind != "CACHE" || key != "events" {
		return SQLFinalOptions{}, false, nil
	}
	return SQLFinalOptions{
		Mode: SQLFinalReplacing,
		Key:  func(row SQLRow) string { return row["id"].(string) },
		Version: func(row SQLRow) (uint64, error) {
			return row["version"].(uint64), nil
		},
	}, true, nil
}

func ch004BenchmarkCollapsingOptions(kind, key string) (SQLFinalOptions, bool, error) {
	if kind != "CACHE" || key != "events" {
		return SQLFinalOptions{}, false, nil
	}
	return SQLFinalOptions{
		Mode: SQLFinalCollapsing,
		Key:  func(row SQLRow) string { return row["id"].(string) },
		Sign: func(row SQLRow) (int, error) { return row["sign"].(int), nil },
	}, true, nil
}
