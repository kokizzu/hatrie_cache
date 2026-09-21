package hatCache

import (
	"bytes"
	"encoding/json"
	"testing"

	"hatrie_cache/hat/hatSql"
)

var ch046BaselineSink []byte

func TestCH046WireSizes(t *testing.T) {
	columns, rows := ch046BaselineFixture()
	rowBinary, err := hatSql.EncodeSQLRowBinary(columns, rows)
	if err != nil {
		t.Fatal(err)
	}
	columnar := ch046EncodeColumnar(columns, rows)
	jsonRows, err := json.Marshal(rows)
	if err != nil {
		t.Fatal(err)
	}
	projectionColumns, projectionRows := ch046ProjectionFixture()
	projectionRowBinary, err := hatSql.EncodeSQLRowBinary(projectionColumns, projectionRows)
	if err != nil {
		t.Fatal(err)
	}
	projectionColumnar := ch046EncodeColumnar(projectionColumns, projectionRows)
	t.Logf("rows=%d rowbinary_bytes=%d columnar_bytes=%d json_bytes=%d projection_rowbinary_bytes=%d projection_columnar_bytes=%d", len(rows), len(rowBinary), len(columnar), len(jsonRows), len(projectionRowBinary), len(projectionColumnar))
}

func ch046BaselineFixture() ([]hatSql.SQLRowBinaryColumn, []hatSql.Row) {
	columns := []hatSql.SQLRowBinaryColumn{
		{Name: "id", Type: hatSql.SQLRowBinaryInt64},
		{Name: "category", Type: hatSql.SQLRowBinaryString},
		{Name: "amount", Type: hatSql.SQLRowBinaryFloat64},
		{Name: "active", Type: hatSql.SQLRowBinaryBool},
	}
	rows := make([]hatSql.Row, 8192)
	for index := range rows {
		rows[index] = hatSql.Row{
			"id":       int64(index),
			"category": []string{"alpha", "beta", "gamma", "delta"}[index%4],
			"amount":   float64(index%997) * 1.25,
			"active":   index%3 != 0,
		}
	}
	return columns, rows
}

func BenchmarkCH046RowBinaryBaseline(b *testing.B) {
	columns, rows := ch046BaselineFixture()
	encoded, err := hatSql.EncodeSQLRowBinary(columns, rows)
	if err != nil {
		b.Fatal(err)
	}
	b.SetBytes(int64(len(encoded)))
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		encoded, err = hatSql.EncodeSQLRowBinary(columns, rows)
		if err != nil {
			b.Fatal(err)
		}
		ch046BaselineSink = encoded
	}
}

func BenchmarkCH046JSONBaseline(b *testing.B) {
	_, rows := ch046BaselineFixture()
	encoded, err := json.Marshal(rows)
	if err != nil {
		b.Fatal(err)
	}
	b.SetBytes(int64(len(encoded)))
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		encoded, err = json.Marshal(rows)
		if err != nil {
			b.Fatal(err)
		}
		ch046BaselineSink = encoded
	}
}

func BenchmarkCH046ColumnarBlockStream(b *testing.B) {
	columns, rows := ch046BaselineFixture()
	encoded := ch046EncodeColumnar(columns, rows)
	b.SetBytes(int64(len(encoded)))
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		encoded = ch046EncodeColumnar(columns, rows)
		ch046BaselineSink = encoded
	}
}

func BenchmarkCH046RowBinaryDecodeProjectionBaseline(b *testing.B) {
	columns, rows := ch046ProjectionFixture()
	encoded, err := hatSql.EncodeSQLRowBinary(columns, rows)
	if err != nil {
		b.Fatal(err)
	}
	b.SetBytes(int64(len(encoded)))
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		decoded, err := hatSql.DecodeSQLRowBinary(columns, encoded)
		if err != nil {
			b.Fatal(err)
		}
		ch046BaselineSink = []byte{byte(len(decoded))}
	}
}

func BenchmarkCH046ColumnarBlockStreamProjection(b *testing.B) {
	columns, rows := ch046ProjectionFixture()
	encoded := ch046EncodeColumnar(columns, rows)
	b.SetBytes(int64(len(encoded)))
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		reader, err := hatSql.NewSQLColumnarBlockStreamReader(bytes.NewReader(encoded))
		if err != nil {
			b.Fatal(err)
		}
		count := 0
		for reader.NextBlockFields([]string{"category"}) {
			count += len(reader.Block())
		}
		if err := reader.Err(); err != nil {
			b.Fatal(err)
		}
		ch046BaselineSink = []byte{byte(count)}
	}
}

func ch046EncodeColumnar(columns []hatSql.SQLRowBinaryColumn, rows []hatSql.Row) []byte {
	var encoded bytes.Buffer
	writer := hatSql.NewSQLColumnarBlockStreamWriterWithColumns(&encoded, columns, 1024)
	for _, row := range rows {
		if err := writer.WriteRow(row); err != nil {
			panic(err)
		}
	}
	if err := writer.Finish(); err != nil {
		panic(err)
	}
	return encoded.Bytes()
}

func ch046ProjectionFixture() ([]hatSql.SQLRowBinaryColumn, []hatSql.Row) {
	columns, rows := ch046BaselineFixture()
	columns = append(columns, hatSql.SQLRowBinaryColumn{Name: "payload", Type: hatSql.SQLRowBinaryBytes})
	for index := range rows {
		payload := make([]byte, 256)
		for payloadIndex := range payload {
			payload[payloadIndex] = byte((index + payloadIndex) % 251)
		}
		rows[index]["payload"] = payload
	}
	return columns, rows
}
