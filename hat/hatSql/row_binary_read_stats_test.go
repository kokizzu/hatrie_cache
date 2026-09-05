package hatSql_test

import (
	"testing"
	time "time"

	"hatrie_cache/hat/hatSql"
)

func TestAnalyzeSQLRowBinaryReadReportsColumnAmplification(t *testing.T) {
	columns := []hatSql.SQLRowBinaryColumn{
		{Name: "id", Type: hatSql.SQLRowBinaryInt64},
		{Name: "name", Type: hatSql.SQLRowBinaryString, Nullable: true},
		{Name: "at", Type: hatSql.SQLRowBinaryDateTime},
	}
	rows := []hatSql.SQLRow{
		{"id": int64(1), "name": "Ada", "at": time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)},
		{"id": int64(2), "name": nil, "at": time.Date(2026, time.January, 1, 0, 0, 1, 0, time.UTC)},
	}
	wire, err := hatSql.EncodeSQLRowBinary(columns, rows)
	if err != nil {
		t.Fatal(err)
	}
	stats, err := hatSql.AnalyzeSQLRowBinaryRead(columns, wire)
	if err != nil {
		t.Fatalf("AnalyzeSQLRowBinaryRead() error = %v", err)
	}
	if stats.Rows != 2 || stats.Bytes != len(wire) || len(stats.Columns) != len(columns) {
		t.Fatalf("read stats header = %#v", stats)
	}
	if stats.Columns[0].Name != "id" || stats.Columns[0].Bytes != 16 || stats.Columns[0].Values != 2 || stats.Columns[0].Nulls != 0 {
		t.Fatalf("id stats = %#v", stats.Columns[0])
	}
	if stats.Columns[1].Name != "name" || stats.Columns[1].Values != 1 || stats.Columns[1].Nulls != 1 || stats.Columns[1].Bytes != 6 {
		t.Fatalf("name stats = %#v", stats.Columns[1])
	}
	if stats.Columns[2].Name != "at" || stats.Columns[2].Bytes != 16 || stats.Columns[2].Values != 2 || stats.Columns[2].Nulls != 0 {
		t.Fatalf("at stats = %#v", stats.Columns[2])
	}
	columnBytes := 0
	for _, column := range stats.Columns {
		columnBytes += column.Bytes
	}
	if columnBytes != stats.Bytes {
		t.Fatalf("column bytes = %d, total = %d", columnBytes, stats.Bytes)
	}
}

func TestAnalyzeSQLRowBinaryReadRejectsMalformedInput(t *testing.T) {
	columns := []hatSql.SQLRowBinaryColumn{{Name: "id", Type: hatSql.SQLRowBinaryInt64}}
	for index, wire := range [][]byte{[]byte{1, 2, 3}, []byte("\x00\x01"), []byte("\x01\x02\x03\x04\x05\x06\x07")} {
		t.Run(string(rune('a'+index)), func(t *testing.T) {
			defer func() {
				if recovered := recover(); recovered != nil {
					t.Fatalf("AnalyzeSQLRowBinaryRead() panicked: %v", recovered)
				}
			}()
			if _, err := hatSql.AnalyzeSQLRowBinaryRead(columns, wire); err == nil {
				t.Fatal("AnalyzeSQLRowBinaryRead() error = nil")
			}
		})
	}
}

func TestAnalyzeSQLRowBinaryReadEmptyPayload(t *testing.T) {
	columns := []hatSql.SQLRowBinaryColumn{{Name: "id", Type: hatSql.SQLRowBinaryInt64}}
	stats, err := hatSql.AnalyzeSQLRowBinaryRead(columns, nil)
	if err != nil {
		t.Fatal(err)
	}
	if stats.Rows != 0 || stats.Bytes != 0 || stats.Columns[0].Name != "id" {
		t.Fatalf("empty read stats = %#v", stats)
	}
}

func BenchmarkAnalyzeSQLRowBinaryRead(b *testing.B) {
	columns := []hatSql.SQLRowBinaryColumn{
		{Name: "id", Type: hatSql.SQLRowBinaryInt64},
		{Name: "name", Type: hatSql.SQLRowBinaryString, Nullable: true},
		{Name: "at", Type: hatSql.SQLRowBinaryDateTime},
	}
	rows := make([]hatSql.SQLRow, 1024)
	for index := range rows {
		rows[index] = hatSql.SQLRow{
			"id":   int64(index),
			"name": "steady",
			"at":   time.Unix(int64(index), 0).UTC(),
		}
	}
	wire, err := hatSql.EncodeSQLRowBinary(columns, rows)
	if err != nil {
		b.Fatal(err)
	}
	b.Run("analyze", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			stats, err := hatSql.AnalyzeSQLRowBinaryRead(columns, wire)
			if err != nil || stats.Rows != len(rows) {
				b.Fatalf("read stats = %#v, %v", stats, err)
			}
		}
	})
	b.Run("legacy_decode", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			decoded, err := hatSql.DecodeSQLRowBinary(columns, wire)
			if err != nil || len(decoded) != len(rows) {
				b.Fatalf("decoded rows = %d, %v", len(decoded), err)
			}
		}
	})
}
