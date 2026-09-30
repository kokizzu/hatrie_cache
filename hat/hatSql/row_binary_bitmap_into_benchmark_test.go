package hatSql_test

import (
	"testing"

	"hatrie_cache/hat/hatSql"
)

func BenchmarkSQLRowBinaryBitmapEncodeIntoReuse(b *testing.B) {
	columns, rows := bitmapIntoBenchmarkFixture()
	encoded, err := hatSql.EncodeSQLRowBinaryBitmap(columns, rows)
	if err != nil {
		b.Fatal(err)
	}
	destination := encoded[:0]
	b.ReportAllocs()
	for range b.N {
		encoded, err = hatSql.EncodeSQLRowBinaryBitmapInto(destination, columns, rows)
		if err != nil {
			b.Fatal(err)
		}
		b.ReportMetric(float64(len(encoded)), "wire-B")
		destination = encoded[:0]
	}
}
