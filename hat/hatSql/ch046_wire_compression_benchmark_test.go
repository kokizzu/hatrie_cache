package hatSql

import (
	"bytes"
	"compress/flate"
	"testing"
)

var ch046WireCompressionBenchmarkSink []byte

func BenchmarkCH046LegacyColumnarStream(b *testing.B) {
	benchmarkCH046ColumnarStream(b, SQLColumnarBlockStreamOptions{Compression: SQLColumnarBlockStreamCompressionNone})
}

func BenchmarkCH046AdaptiveColumnarStream(b *testing.B) {
	benchmarkCH046ColumnarStream(b, SQLColumnarBlockStreamOptions{Compression: SQLColumnarBlockStreamCompressionAuto})
}

func BenchmarkCH046HuffmanOnlyColumnarStream(b *testing.B) {
	benchmarkCH046ColumnarStream(b, SQLColumnarBlockStreamOptions{Compression: SQLColumnarBlockStreamCompressionFlate, CompressionLevel: flate.HuffmanOnly})
}

func benchmarkCH046ColumnarStream(b *testing.B, options SQLColumnarBlockStreamOptions) {
	columns := []SQLRowBinaryColumn{
		{Name: "id", Type: SQLRowBinaryInt64},
		{Name: "state", Type: SQLRowBinaryString},
		{Name: "region", Type: SQLRowBinaryString},
	}
	rows := make([]Row, 4096)
	for index := range rows {
		rows[index] = Row{
			"id":     int64(index),
			"state":  "ready",
			"region": "ap-southeast-1",
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		var encoded bytes.Buffer
		writer := NewSQLColumnarBlockStreamWriterWithColumnsAndOptions(&encoded, columns, 256, options)
		for _, row := range rows {
			if err := writer.WriteRow(row); err != nil {
				b.Fatal(err)
			}
		}
		if err := writer.Finish(); err != nil {
			b.Fatal(err)
		}
		ch046WireCompressionBenchmarkSink = encoded.Bytes()
		b.SetBytes(int64(encoded.Len()))
		b.ReportMetric(float64(encoded.Len()), "wire-bytes")
	}
}
