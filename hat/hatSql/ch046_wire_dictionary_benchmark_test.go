package hatSql

import (
	"bytes"
	"strconv"
	"testing"
)

var ch046DictionaryDecodeSink int

func BenchmarkCH046DictionaryColumnarStream(b *testing.B) {
	benchmarkCH046ColumnarStream(b, SQLColumnarBlockStreamOptions{Compression: SQLColumnarBlockStreamCompressionDictionary})
}

func BenchmarkCH046DictionaryHighCardinalityColumnarStream(b *testing.B) {
	columns := []SQLRowBinaryColumn{
		{Name: "id", Type: SQLRowBinaryInt64},
		{Name: "state", Type: SQLRowBinaryString},
		{Name: "region", Type: SQLRowBinaryString},
	}
	rows := make([]Row, 4096)
	for index := range rows {
		rows[index] = Row{
			"id":     int64(index),
			"state":  "state-" + strconv.Itoa(index),
			"region": "region-" + strconv.Itoa(index),
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		var encoded bytes.Buffer
		writer := NewSQLColumnarBlockStreamWriterWithColumnsAndOptions(
			&encoded,
			columns,
			1024,
			SQLColumnarBlockStreamOptions{Compression: SQLColumnarBlockStreamCompressionDictionary},
		)
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

func BenchmarkCH046LegacyColumnarStreamDecode(b *testing.B) {
	benchmarkCH046ColumnarStreamDecode(b, SQLColumnarBlockStreamOptions{})
}

func BenchmarkCH046DictionaryColumnarStreamDecode(b *testing.B) {
	benchmarkCH046ColumnarStreamDecode(b, SQLColumnarBlockStreamOptions{Compression: SQLColumnarBlockStreamCompressionDictionary})
}

func benchmarkCH046ColumnarStreamDecode(b *testing.B, options SQLColumnarBlockStreamOptions) {
	columns := []SQLRowBinaryColumn{
		{Name: "id", Type: SQLRowBinaryInt64},
		{Name: "state", Type: SQLRowBinaryString},
		{Name: "region", Type: SQLRowBinaryString},
	}
	rows := ch046BenchmarkRows()
	var encoded bytes.Buffer
	writer := NewSQLColumnarBlockStreamWriterWithColumnsAndOptions(&encoded, columns, 1024, options)
	for _, row := range rows {
		if err := writer.WriteRow(row); err != nil {
			b.Fatal(err)
		}
	}
	if err := writer.Finish(); err != nil {
		b.Fatal(err)
	}
	wire := append([]byte(nil), encoded.Bytes()...)
	b.ReportAllocs()
	b.SetBytes(int64(len(wire)))
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		reader, err := NewSQLColumnarBlockStreamReader(bytes.NewReader(wire))
		if err != nil {
			b.Fatal(err)
		}
		decodedRows := 0
		for reader.NextBlock() {
			decodedRows += len(reader.Block())
		}
		if err := reader.Err(); err != nil {
			b.Fatal(err)
		}
		ch046DictionaryDecodeSink = decodedRows
	}
}
