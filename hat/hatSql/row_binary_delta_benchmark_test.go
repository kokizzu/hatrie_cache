package hatSql_test

import (
	"fmt"
	"testing"
	"time"

	"hatrie_cache/hat/hatSql"
)

func BenchmarkSQLRowBinaryDeltaEncodeBaseline(b *testing.B) {
	columns, rows := deltaIntoBenchmarkFixture()
	b.ReportAllocs()
	for range b.N {
		encoded, err := hatSql.EncodeSQLRowBinaryDelta(columns, rows)
		if err != nil {
			b.Fatal(err)
		}
		b.ReportMetric(float64(len(encoded)), "wire-B")
	}
}

func BenchmarkSQLRowBinaryDoubleDeltaEncodeBaseline(b *testing.B) {
	columns, rows := deltaIntoBenchmarkFixture()
	b.ReportAllocs()
	for range b.N {
		encoded, err := hatSql.EncodeSQLRowBinaryDoubleDelta(columns, rows)
		if err != nil {
			b.Fatal(err)
		}
		b.ReportMetric(float64(len(encoded)), "wire-B")
	}
}

func deltaIntoBenchmarkFixture() ([]hatSql.SQLRowBinaryColumn, []hatSql.SQLRow) {
	columns := []hatSql.SQLRowBinaryColumn{
		{Name: "id", Type: hatSql.SQLRowBinaryInt64},
		{Name: "at", Type: hatSql.SQLRowBinaryDateTime},
		{Name: "amount", Type: hatSql.SQLRowBinaryInt64, Nullable: true},
		{Name: "label", Type: hatSql.SQLRowBinaryString},
	}
	rows := make([]hatSql.SQLRow, 4096)
	for index := range rows {
		rows[index] = hatSql.SQLRow{
			"id":     int64(index + 1000),
			"at":     time.Unix(1700000000+int64(index)*60, 0).UTC(),
			"amount": int64(index*7 + 3),
			"label":  fmt.Sprintf("bucket-%02d", index%32),
		}
		if index%13 == 0 {
			rows[index]["amount"] = nil
		}
	}
	return columns, rows
}
