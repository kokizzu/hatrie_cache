package hatSql_test

import (
	"fmt"
	"testing"

	"hatrie_cache/hat/hatSql"
)

var sqlRowBinaryParallelDecodeSink []hatSql.SQLRow

func BenchmarkSQLRowBinaryParallelDecodeWorkload(b *testing.B) {
	columns := []hatSql.SQLRowBinaryColumn{
		{Name: "id", Type: hatSql.SQLRowBinaryInt64},
		{Name: "name", Type: hatSql.SQLRowBinaryString},
		{Name: "active", Type: hatSql.SQLRowBinaryBool},
		{Name: "note", Type: hatSql.SQLRowBinaryBytes, Nullable: true},
	}
	rows := make([]hatSql.SQLRow, 4096)
	for index := range rows {
		rows[index] = hatSql.SQLRow{
			"id":     int64(index),
			"name":   fmt.Sprintf("customer-%08d", index),
			"active": index%2 == 0,
		}
		if index%5 != 0 {
			rows[index]["note"] = []byte("repeated-payload-for-parallel-row-binary")
		}
	}
	wire, err := hatSql.EncodeSQLRowBinary(columns, rows)
	if err != nil {
		b.Fatal(err)
	}
	b.SetBytes(int64(len(wire)))
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		decoded, err := hatSql.DecodeSQLRowBinary(columns, wire)
		if err != nil {
			b.Fatal(err)
		}
		if len(decoded) != len(rows) {
			b.Fatalf("decoded rows = %d, want %d", len(decoded), len(rows))
		}
		sqlRowBinaryParallelDecodeSink = decoded
	}
}
