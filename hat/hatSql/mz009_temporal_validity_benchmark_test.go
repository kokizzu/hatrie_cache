package hatSql_test

import (
	"testing"
	"time"

	"hatrie_cache/hat/hatSql"
)

var sqlTemporalValidityBenchmarkSink int

func benchmarkSQLTemporalValidityResolver() hatSql.SourceResolver {
	rows := make([]hatSql.Row, 1000)
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for index := range rows {
		from := base.Add(time.Duration(index) * time.Hour)
		to := from.Add(2 * time.Hour)
		if index%3 == 0 {
			rows[index] = hatSql.Row{"id": int64(index), "valid_from": from, "valid_to": nil}
			continue
		}
		rows[index] = hatSql.Row{"id": int64(index), "valid_from": from, "valid_to": to}
	}
	return hatSql.SourceResolverFunc(func(name, key string) ([]hatSql.Row, error) {
		if name != "CACHE" || key != "events" {
			return nil, nil
		}
		return rows, nil
	})
}

func BenchmarkSQLTemporalValidityBaseline(b *testing.B) {
	benchmarkSQLTemporalValidity(b, "SELECT id FROM CACHE('events') WHERE valid_from <= TIMESTAMP '2026-01-22T12:00:00Z' AND (valid_to IS NULL OR TIMESTAMP '2026-01-22T12:00:00Z' < valid_to)")
}

func BenchmarkSQLTemporalValidityFunction(b *testing.B) {
	benchmarkSQLTemporalValidity(b, "SELECT id FROM CACHE('events') WHERE VALID_AT(TIMESTAMP '2026-01-22T12:00:00Z', valid_from, valid_to)")
}

func benchmarkSQLTemporalValidity(b *testing.B, query string) {
	resolver := benchmarkSQLTemporalValidityResolver()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		result, err := hatSql.ExecuteSQLQuery(query, resolver)
		if err != nil {
			b.Fatal(err)
		}
		sqlTemporalValidityBenchmarkSink = len(result.Rows)
	}
}
