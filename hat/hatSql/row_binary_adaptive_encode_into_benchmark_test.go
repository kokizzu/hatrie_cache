package hatSql_test

import (
	"testing"
	"time"

	"hatrie_cache/hat/hatSql"
)

func BenchmarkSQLRowBinaryAdaptiveInto(b *testing.B) {
	columns, rows := rowBinaryAdaptiveIntoBenchmarkFixture()
	wire, err := hatSql.EncodeSQLRowBinaryAdaptive(columns, rows)
	if err != nil {
		b.Fatal(err)
	}
	b.Run("encode", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(len(wire)))
		b.ReportMetric(float64(len(wire)), "wire_bytes")
		for range b.N {
			if _, err := hatSql.EncodeSQLRowBinaryAdaptive(columns, rows); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("encode_into", func(b *testing.B) {
		dst := make([]byte, 0, len(wire))
		b.ReportAllocs()
		b.SetBytes(int64(len(wire)))
		b.ReportMetric(float64(len(wire)), "wire_bytes")
		for range b.N {
			dst, err = hatSql.EncodeSQLRowBinaryAdaptiveInto(dst, columns, rows)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func rowBinaryAdaptiveIntoBenchmarkFixture() ([]hatSql.SQLRowBinaryColumn, []hatSql.SQLRow) {
	columns := []hatSql.SQLRowBinaryColumn{
		{Name: "id", Type: hatSql.SQLRowBinaryInt64},
		{Name: "at", Type: hatSql.SQLRowBinaryDateTime},
		{Name: "label", Type: hatSql.SQLRowBinaryString},
	}
	rows := make([]hatSql.SQLRow, 128)
	start := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	for index := range rows {
		rows[index] = hatSql.SQLRow{
			"id":    int64(index + 1),
			"at":    start.Add(time.Duration(index) * time.Second),
			"label": "steady",
		}
	}
	return columns, rows
}
