package hatSql

import (
	"bytes"
	"fmt"
	"testing"
)

var ch046WireDictionaryBenchmarkSink []byte

func BenchmarkCH046WireDictionaryLegacyEncode(b *testing.B) {
	benchmarkCH046WireDictionaryEncode(b, SQLColumnarBlockStreamOptions{})
}

func BenchmarkCH046WireDictionaryAutoEncode(b *testing.B) {
	benchmarkCH046WireDictionaryEncode(b, SQLColumnarBlockStreamOptions{
		Dictionary: SQLColumnarBlockStreamDictionaryAuto,
	})
}

func BenchmarkCH046WireDictionaryLegacyDecode(b *testing.B) {
	benchmarkCH046WireDictionaryDecode(b, SQLColumnarBlockStreamOptions{})
}

func BenchmarkCH046WireDictionaryAutoDecode(b *testing.B) {
	benchmarkCH046WireDictionaryDecode(b, SQLColumnarBlockStreamOptions{
		Dictionary: SQLColumnarBlockStreamDictionaryAuto,
	})
}

func BenchmarkCH046WireDictionaryHighCardinalityAutoEncode(b *testing.B) {
	columns, rows := ch046WireDictionaryHighCardinalityFixture()
	benchmarkCH046WireDictionaryEncodeRows(b, columns, rows, SQLColumnarBlockStreamOptions{
		Dictionary: SQLColumnarBlockStreamDictionaryAuto,
	})
}

func BenchmarkCH046WireDictionaryHighCardinalityLegacyEncode(b *testing.B) {
	columns, rows := ch046WireDictionaryHighCardinalityFixture()
	benchmarkCH046WireDictionaryEncodeRows(b, columns, rows, SQLColumnarBlockStreamOptions{})
}

func ch046WireDictionaryHighCardinalityFixture() ([]SQLRowBinaryColumn, []Row) {
	columns := []SQLRowBinaryColumn{{Name: "value", Type: SQLRowBinaryString}}
	rows := make([]Row, 4096)
	for index := range rows {
		rows[index] = Row{"value": fmt.Sprintf("unique-%06d-payload", index)}
	}
	return columns, rows
}

func benchmarkCH046WireDictionaryEncode(b *testing.B, options SQLColumnarBlockStreamOptions) {
	columns, rows := ch046WireDictionaryBenchmarkFixture()
	benchmarkCH046WireDictionaryEncodeRows(b, columns, rows, options)
}

func benchmarkCH046WireDictionaryEncodeRows(b *testing.B, columns []SQLRowBinaryColumn, rows []Row, options SQLColumnarBlockStreamOptions) {
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
		ch046WireDictionaryBenchmarkSink = encoded.Bytes()
		b.SetBytes(int64(encoded.Len()))
		b.ReportMetric(float64(encoded.Len()), "wire-bytes")
	}
}

func benchmarkCH046WireDictionaryDecode(b *testing.B, options SQLColumnarBlockStreamOptions) {
	columns, rows := ch046WireDictionaryBenchmarkFixture()
	encoded := encodeCH046DictionaryBenchmarkRows(b, columns, rows, options)
	b.SetBytes(int64(len(encoded)))
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		reader, err := NewSQLColumnarBlockStreamReader(bytes.NewReader(encoded))
		if err != nil {
			b.Fatal(err)
		}
		count := 0
		for reader.NextBlock() {
			count += len(reader.Block())
		}
		if err := reader.Err(); err != nil {
			b.Fatal(err)
		}
		ch046WireDictionaryBenchmarkSink = []byte{byte(count)}
	}
}

func encodeCH046DictionaryBenchmarkRows(b *testing.B, columns []SQLRowBinaryColumn, rows []Row, options SQLColumnarBlockStreamOptions) []byte {
	b.Helper()
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
	return append([]byte(nil), encoded.Bytes()...)
}

func ch046WireDictionaryBenchmarkFixture() ([]SQLRowBinaryColumn, []Row) {
	columns := []SQLRowBinaryColumn{
		{Name: "id", Type: SQLRowBinaryInt64},
		{Name: "state", Type: SQLRowBinaryString},
		{Name: "region", Type: SQLRowBinaryString, Nullable: true},
	}
	rows := make([]Row, 4096)
	for index := range rows {
		rows[index] = Row{
			"id":     int64(index),
			"state":  []string{"ready", "queued", "ready", "ready"}[index%4],
			"region": []string{"ap-southeast-1", "ap-southeast-1", "us-east-1", "ap-southeast-1"}[index%4],
		}
	}
	return columns, rows
}
