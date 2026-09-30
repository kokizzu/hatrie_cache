package hatSql_test

import (
	"testing"

	"hatrie_cache/hat/hatSql"
)

func BenchmarkSQLRowBinaryBitmapDecodeBaseline(b *testing.B) {
	columns, rows := bitmapIntoBenchmarkFixture()
	encoded, err := hatSql.EncodeSQLRowBinaryBitmap(columns, rows)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for range b.N {
		decoded, err := hatSql.DecodeSQLRowBinaryBitmap(columns, encoded)
		if err != nil {
			b.Fatal(err)
		}
		if len(decoded) != len(rows) {
			b.Fatalf("decoded rows = %d, want %d", len(decoded), len(rows))
		}
		b.ReportMetric(float64(len(encoded)), "wire-B")
	}
}
