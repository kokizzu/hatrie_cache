package hatSql_test

import (
	"testing"
	"time"

	"hatrie_cache/hat/hatSql"
)

var mz009ValidityIndexBenchmarkSink int

func BenchmarkMZ009ValidityIndexBaseline(b *testing.B) {
	rows, candidates := mz009ValidityIndexBenchmarkRows()
	resolver := &mz009ValidityIndexResolver{rows: rows, candidates: candidates}
	benchmarkMZ009ValidityIndex(b, resolver)
}

func BenchmarkMZ009ValidityIndexIndexed(b *testing.B) {
	rows, candidates := mz009ValidityIndexBenchmarkRows()
	resolver := &mz009ValidityIndexResolver{rows: rows, candidates: candidates, indexAvailable: true}
	benchmarkMZ009ValidityIndex(b, resolver)
}

func benchmarkMZ009ValidityIndex(b *testing.B, resolver *mz009ValidityIndexResolver) {
	b.Helper()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		result, err := hatSql.ExecuteSQLQuery(mz009ValidityIndexQuery, resolver)
		if err != nil {
			b.Fatal(err)
		}
		mz009ValidityIndexBenchmarkSink = len(result.Rows)
	}
}

func mz009ValidityIndexBenchmarkRows() ([]hatSql.Row, []hatSql.Row) {
	const rowCount = 10_000
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	at := base.Add(1 * time.Hour)
	rows := make([]hatSql.Row, rowCount)
	candidates := make([]hatSql.Row, 0, 2)
	for index := range rows {
		from := base.Add(time.Duration(index) * time.Hour)
		to := from.Add(2 * time.Hour)
		if index%3 == 0 {
			rows[index] = hatSql.Row{"id": int64(index), "valid_from": from, "valid_to": nil}
		} else {
			rows[index] = hatSql.Row{"id": int64(index), "valid_from": from, "valid_to": to}
		}
		if !from.After(at) {
			candidates = append(candidates, rows[index])
		}
	}
	return rows, candidates
}
