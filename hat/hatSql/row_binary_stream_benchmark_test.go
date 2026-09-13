package hatSql

import (
	"bytes"
	"testing"
)

func BenchmarkCH050RowBinaryStream(b *testing.B) {
	rows := ch050BenchmarkRows()
	columns := []string{"id", "name", "enabled", "payload"}
	b.ReportAllocs()
	bytesWritten := 0
	for iteration := 0; iteration < b.N; iteration++ {
		var encoded bytes.Buffer
		writer := NewSQLRowBinaryStreamWriter(&encoded, columns)
		for _, row := range rows {
			if err := writer.WriteRow(row); err != nil {
				b.Fatal(err)
			}
		}
		if err := writer.Finish(); err != nil {
			b.Fatal(err)
		}
		bytesWritten = encoded.Len()
	}
	b.SetBytes(int64(bytesWritten))
	b.ReportMetric(float64(bytesWritten), "bytes/result")
}
