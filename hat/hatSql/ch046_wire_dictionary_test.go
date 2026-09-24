package hatSql

import (
	"bytes"
	"reflect"
	"testing"
)

func TestSQLColumnarBlockStreamDictionaryRoundTripsRepeatedStrings(t *testing.T) {
	columns := []SQLRowBinaryColumn{
		{Name: "id", Type: SQLRowBinaryInt64},
		{Name: "state", Type: SQLRowBinaryString, Nullable: true},
		{Name: "region", Type: SQLRowBinaryString},
	}
	rows := []Row{
		{"id": int64(1), "state": "ready", "region": "ap-southeast-1"},
		{"id": int64(2), "state": nil, "region": "ap-southeast-1"},
		{"id": int64(3), "state": "ready", "region": "ap-southeast-1"},
	}

	var encoded bytes.Buffer
	writer := NewSQLColumnarBlockStreamWriterWithColumnsAndOptions(
		&encoded,
		columns,
		3,
		SQLColumnarBlockStreamOptions{Compression: SQLColumnarBlockStreamCompressionDictionary},
	)
	for _, row := range rows {
		if err := writer.WriteRow(row); err != nil {
			t.Fatalf("WriteRow() error = %v", err)
		}
	}
	if err := writer.Finish(); err != nil {
		t.Fatalf("Finish() error = %v", err)
	}
	if !bytes.Contains(encoded.Bytes(), []byte{sqlColumnarBlockEncodingDictionary}) {
		t.Fatalf("encoded stream does not contain dictionary encoding: %x", encoded.Bytes())
	}

	reader, err := NewSQLColumnarBlockStreamReader(bytes.NewReader(encoded.Bytes()))
	if err != nil {
		t.Fatalf("NewSQLColumnarBlockStreamReader() error = %v", err)
	}
	var decoded []Row
	for reader.NextBlock() {
		decoded = append(decoded, reader.Block()...)
	}
	if err := reader.Err(); err != nil {
		t.Fatalf("reader Err() = %v", err)
	}
	if !reflect.DeepEqual(decoded, rows) {
		t.Fatalf("decoded rows = %#v, want %#v", decoded, rows)
	}
}

func TestSQLColumnarBlockStreamDictionaryRejectsInvalidID(t *testing.T) {
	column := SQLRowBinaryColumn{Name: "state", Type: SQLRowBinaryString}
	malformed := []byte{1, 1, 'x', 1}
	if _, err := decodeSQLColumnarBlockPayload(
		malformed,
		sqlColumnarBlockEncodingDictionary,
		sqlColumnarBlockStreamMaxBytes,
		column,
		1,
	); err == nil {
		t.Fatal("decodeSQLColumnarBlockPayload() accepted an out-of-range dictionary ID")
	}
}

func TestSQLColumnarBlockStreamDictionaryRejectsMalformedEntries(t *testing.T) {
	column := SQLRowBinaryColumn{Name: "state", Type: SQLRowBinaryString}
	tests := []struct {
		name    string
		payload []byte
		rows    int
		budget  uint64
	}{
		{name: "entry length exceeds payload", payload: []byte{1, 5, 'x'}, rows: 1, budget: sqlColumnarBlockStreamMaxBytes},
		{name: "entry count exceeds rows", payload: []byte{2}, rows: 1, budget: sqlColumnarBlockStreamMaxBytes},
		{name: "null marker exceeds decoded budget", payload: []byte{0, 1}, rows: 1, budget: 0},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := decodeSQLColumnarBlockPayload(
				test.payload,
				sqlColumnarBlockEncodingDictionary,
				test.budget,
				column,
				test.rows,
			); err == nil {
				t.Fatal("decodeSQLColumnarBlockPayload() accepted malformed dictionary input")
			}
		})
	}
}
