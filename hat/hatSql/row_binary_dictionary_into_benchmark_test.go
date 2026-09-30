package hatSql_test

import (
	"testing"

	"hatrie_cache/hat/hatSql"
)

func BenchmarkSQLRowBinaryDictionaryEncodeIntoReuse(b *testing.B) {
	columns := dictionaryTestColumns()
	rows := dictionaryBenchmarkRows(0, 256)
	encoder, err := hatSql.NewSQLRowBinaryDictionaryEncoder(columns, []string{"region", "payload", "metadata"})
	if err != nil {
		b.Fatal(err)
	}
	encoded, err := encoder.EncodeInto(nil, rows)
	if err != nil {
		b.Fatal(err)
	}
	destination := encoded[:0]
	b.ReportAllocs()
	for range b.N {
		encoded, err = encoder.EncodeInto(destination, rows)
		if err != nil {
			b.Fatal(err)
		}
		b.ReportMetric(float64(len(encoded)), "wire-B")
		destination = encoded[:0]
	}
}
