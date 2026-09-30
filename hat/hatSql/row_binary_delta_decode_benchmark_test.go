package hatSql_test

import (
	"testing"

	"hatrie_cache/hat/hatSql"
)

func BenchmarkSQLRowBinaryDeltaDecodeBaseline(b *testing.B) {
	columns, rows := deltaIntoBenchmarkFixture()
	encoded, err := hatSql.EncodeSQLRowBinaryDelta(columns, rows)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for range b.N {
		decoded, err := hatSql.DecodeSQLRowBinaryDelta(columns, encoded)
		if err != nil {
			b.Fatal(err)
		}
		if len(decoded) != len(rows) {
			b.Fatalf("decoded rows = %d, want %d", len(decoded), len(rows))
		}
		b.ReportMetric(float64(len(encoded)), "wire-B")
	}
}

func BenchmarkSQLRowBinaryDoubleDeltaDecodeBaseline(b *testing.B) {
	columns, rows := deltaIntoBenchmarkFixture()
	encoded, err := hatSql.EncodeSQLRowBinaryDoubleDelta(columns, rows)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for range b.N {
		decoded, err := hatSql.DecodeSQLRowBinaryDelta(columns, encoded)
		if err != nil {
			b.Fatal(err)
		}
		if len(decoded) != len(rows) {
			b.Fatalf("decoded rows = %d, want %d", len(decoded), len(rows))
		}
		b.ReportMetric(float64(len(encoded)), "wire-B")
	}
}
