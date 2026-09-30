package hatSql_test

import (
	"testing"

	"hatrie_cache/hat/hatSql"
)

func BenchmarkSQLRowBinaryDeltaDecodeIntoReuse(b *testing.B) {
	benchmarkSQLRowBinaryDeltaDecodeIntoReuse(b, false)
}

func BenchmarkSQLRowBinaryDoubleDeltaDecodeIntoReuse(b *testing.B) {
	benchmarkSQLRowBinaryDeltaDecodeIntoReuse(b, true)
}

func benchmarkSQLRowBinaryDeltaDecodeIntoReuse(b *testing.B, doubleDelta bool) {
	columns, rows := deltaIntoBenchmarkFixture()
	encoded, err := hatSql.EncodeSQLRowBinaryDelta(columns, rows)
	if doubleDelta {
		encoded, err = hatSql.EncodeSQLRowBinaryDoubleDelta(columns, rows)
	}
	if err != nil {
		b.Fatal(err)
	}
	decoded, err := hatSql.DecodeSQLRowBinaryDeltaInto(nil, columns, encoded)
	if err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		decoded, err = hatSql.DecodeSQLRowBinaryDeltaInto(decoded[:0], columns, encoded)
		if err != nil {
			b.Fatal(err)
		}
		if len(decoded) != len(rows) {
			b.Fatalf("decoded %d rows, want %d", len(decoded), len(rows))
		}
	}
	b.ReportMetric(float64(len(encoded)), "wire-B")
}
