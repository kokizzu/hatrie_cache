package hatSql_test

import (
	"testing"

	"hatrie_cache/hat/hatSql"
)

func BenchmarkSQLRowBinaryBitmapDecodeIntoReuse(b *testing.B) {
	columns, rows := bitmapIntoBenchmarkFixture()
	encoded, err := hatSql.EncodeSQLRowBinaryBitmap(columns, rows)
	if err != nil {
		b.Fatal(err)
	}
	decoded, err := hatSql.DecodeSQLRowBinaryBitmapInto(nil, columns, encoded)
	if err != nil {
		b.Fatal(err)
	}
	destination := decoded[:0]
	b.ReportAllocs()
	for range b.N {
		decoded, err = hatSql.DecodeSQLRowBinaryBitmapInto(destination, columns, encoded)
		if err != nil {
			b.Fatal(err)
		}
		b.ReportMetric(float64(len(encoded)), "wire-B")
		destination = decoded[:0]
	}
}
