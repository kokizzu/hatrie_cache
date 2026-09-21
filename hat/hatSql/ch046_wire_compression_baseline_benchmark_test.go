package hatSql

import (
	"bytes"
	"testing"
)

var ch046BaselineWireSink int

func BenchmarkCH046CurrentColumnarStream(b *testing.B) {
	columns := []SQLRowBinaryColumn{
		{Name: "id", Type: SQLRowBinaryInt64},
		{Name: "state", Type: SQLRowBinaryString},
		{Name: "region", Type: SQLRowBinaryString},
	}
	rows := ch046BenchmarkRows()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		var encoded bytes.Buffer
		writer := NewSQLColumnarBlockStreamWriterWithColumns(&encoded, columns, 256)
		for _, row := range rows {
			if err := writer.WriteRow(row); err != nil {
				b.Fatal(err)
			}
		}
		if err := writer.Finish(); err != nil {
			b.Fatal(err)
		}
		ch046BaselineWireSink += encoded.Len()
		b.SetBytes(int64(encoded.Len()))
	}
}

func ch046BenchmarkRows() []Row {
	rows := make([]Row, 4096)
	for index := range rows {
		rows[index] = Row{
			"id":     int64(index),
			"state":  "ready",
			"region": "ap-southeast-1",
		}
	}
	return rows
}
