package hatSql_test

import (
	"testing"

	"hatrie_cache/hat/hatSql"
)

func BenchmarkSQLRowBinaryDeltaEncodeIntoReuse(b *testing.B) {
	columns, rows := deltaIntoBenchmarkFixture()
	benchmarkSQLRowBinaryDeltaEncodeInto(b, columns, rows, false)
}

func BenchmarkSQLRowBinaryDoubleDeltaEncodeIntoReuse(b *testing.B) {
	columns, rows := deltaIntoBenchmarkFixture()
	benchmarkSQLRowBinaryDeltaEncodeInto(b, columns, rows, true)
}

func benchmarkSQLRowBinaryDeltaEncodeInto(b *testing.B, columns []hatSql.SQLRowBinaryColumn, rows []hatSql.SQLRow, doubleDelta bool) {
	var encode func([]byte, []hatSql.SQLRowBinaryColumn, []hatSql.SQLRow) ([]byte, error)
	if doubleDelta {
		encode = hatSql.EncodeSQLRowBinaryDoubleDeltaInto
	} else {
		encode = hatSql.EncodeSQLRowBinaryDeltaInto
	}
	encoded, err := encode(nil, columns, rows)
	if err != nil {
		b.Fatal(err)
	}
	destination := encoded[:0]
	b.ReportAllocs()
	for range b.N {
		encoded, err = encode(destination, columns, rows)
		if err != nil {
			b.Fatal(err)
		}
		b.ReportMetric(float64(len(encoded)), "wire-B")
		destination = encoded[:0]
	}
}
