package hatSql_test

import (
	"fmt"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func BenchmarkSQLRowBinaryBitmapEncodeBaseline(b *testing.B) {
	columns, rows := bitmapIntoBenchmarkFixture()
	b.ReportAllocs()
	for range b.N {
		encoded, err := hatSql.EncodeSQLRowBinaryBitmap(columns, rows)
		if err != nil {
			b.Fatal(err)
		}
		b.ReportMetric(float64(len(encoded)), "wire-B")
	}
}

func bitmapIntoBenchmarkFixture() ([]hatSql.SQLRowBinaryColumn, []hatSql.SQLRow) {
	columns := []hatSql.SQLRowBinaryColumn{
		{Name: "id", Type: hatSql.SQLRowBinaryInt64, Nullable: true},
		{Name: "name", Type: hatSql.SQLRowBinaryString, Nullable: true},
		{Name: "payload", Type: hatSql.SQLRowBinaryBytes, Nullable: true},
		{Name: "flag", Type: hatSql.SQLRowBinaryBool, Nullable: true},
	}
	rows := make([]hatSql.SQLRow, 4096)
	for index := range rows {
		seed := bitmapIntoBenchmarkMix(uint64(index) + 0x243f6a8885a308d3)
		rows[index] = hatSql.SQLRow{
			"id":      int64(seed),
			"name":    fmt.Sprintf("name-%016x", seed),
			"payload": []byte{byte(seed), byte(seed >> 8), byte(seed >> 16), byte(seed >> 24)},
			"flag":    seed&1 == 1,
		}
		if index%4 == 0 {
			rows[index]["name"] = nil
		}
		if index%5 == 0 {
			rows[index]["payload"] = nil
		}
		if index%7 == 0 {
			rows[index]["flag"] = nil
		}
	}
	return columns, rows
}

func bitmapIntoBenchmarkMix(value uint64) uint64 {
	value = (value ^ (value >> 30)) * 0xbf58476d1ce4e5b9
	value = (value ^ (value >> 27)) * 0x94d049bb133111eb
	return value ^ (value >> 31)
}
