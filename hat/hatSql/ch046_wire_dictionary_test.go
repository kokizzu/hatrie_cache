package hatSql

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"reflect"
	"testing"
)

func TestSQLColumnarBlockStreamDictionaryRoundTripsAndProjects(t *testing.T) {
	columns := []SQLRowBinaryColumn{
		{Name: "id", Type: SQLRowBinaryInt64},
		{Name: "state", Type: SQLRowBinaryString},
		{Name: "note", Type: SQLRowBinaryString, Nullable: true},
	}
	rows := make([]Row, 96)
	for index := range rows {
		rows[index] = Row{
			"id":    int64(index),
			"state": []string{"ready", "ready", "queued", "ready"}[index%4],
		}
		if index%3 != 0 {
			rows[index]["note"] = []string{"hot", "cold", "hot"}[index%3]
		} else {
			rows[index]["note"] = nil
		}
	}

	dictionaryWire := encodeCH046DictionaryTestRows(t, columns, rows, SQLColumnarBlockStreamOptions{
		Dictionary: SQLColumnarBlockStreamDictionaryAuto,
	})
	if got := roundTripCH046Rows(t, dictionaryWire); !reflect.DeepEqual(got, rows) {
		t.Fatalf("dictionary round trip = %#v, want %#v", got, rows)
	}

	reader, err := NewSQLColumnarBlockStreamReader(bytes.NewReader(dictionaryWire))
	if err != nil {
		t.Fatalf("projection reader creation error = %v", err)
	}
	rowIndex := 0
	for reader.NextBlockFields([]string{"state"}) {
		for _, row := range reader.Block() {
			want := Row{"state": rows[rowIndex]["state"]}
			if !reflect.DeepEqual(row, want) {
				t.Fatalf("projected row %d = %#v, want %#v", rowIndex, row, want)
			}
			rowIndex++
		}
	}
	if err := reader.Err(); err != nil {
		t.Fatalf("projected reader Err() = %v", err)
	}
	if rowIndex != len(rows) {
		t.Fatalf("projected rows = %d, want %d", rowIndex, len(rows))
	}

	legacyWire := encodeCH046DictionaryTestRows(t, columns, rows, SQLColumnarBlockStreamOptions{})
	if len(dictionaryWire) >= len(legacyWire) {
		t.Fatalf("dictionary wire bytes = %d, legacy bytes = %d; expected dictionary reduction", len(dictionaryWire), len(legacyWire))
	}
}

func TestSQLColumnarBlockStreamDictionaryFallsBackForHighCardinality(t *testing.T) {
	columns := []SQLRowBinaryColumn{{Name: "value", Type: SQLRowBinaryString}}
	rows := make([]Row, 64)
	for index := range rows {
		rows[index] = Row{"value": fmt.Sprintf("value-%03d-unique-payload", index)}
	}
	wire := encodeCH046DictionaryTestRows(t, columns, rows, SQLColumnarBlockStreamOptions{
		Dictionary: SQLColumnarBlockStreamDictionaryAuto,
	})
	if got := roundTripCH046Rows(t, wire); !reflect.DeepEqual(got, rows) {
		t.Fatalf("high-cardinality dictionary round trip = %#v, want %#v", got, rows)
	}
}

func TestSQLColumnarBlockStreamDictionaryRejectsTruncatedFrames(t *testing.T) {
	columns := []SQLRowBinaryColumn{{Name: "state", Type: SQLRowBinaryString}}
	rows := []Row{{"state": "ready"}, {"state": "ready"}, {"state": "queued"}}
	wire := encodeCH046DictionaryTestRows(t, columns, rows, SQLColumnarBlockStreamOptions{
		Dictionary: SQLColumnarBlockStreamDictionaryAuto,
	})
	for cut := 1; cut <= 3; cut++ {
		reader, err := NewSQLColumnarBlockStreamReader(bytes.NewReader(wire[:len(wire)-cut]))
		if err != nil {
			t.Fatalf("truncated reader creation error for cut %d = %v", cut, err)
		}
		for reader.NextBlock() {
		}
		if reader.Err() == nil {
			t.Fatalf("truncated dictionary frame with cut %d was accepted", cut)
		}
	}
}

func TestSQLColumnarBlockStreamDictionaryRejectsInvalidValueID(t *testing.T) {
	columns := []SQLRowBinaryColumn{{Name: "state", Type: SQLRowBinaryString}}
	rows := []Row{{"state": "ready"}, {"state": "queued"}, {"state": "ready"}}
	wire := encodeCH046DictionaryTestRows(t, columns, rows, SQLColumnarBlockStreamOptions{
		Dictionary: SQLColumnarBlockStreamDictionaryAuto,
	})
	corrupt := append([]byte(nil), wire...)
	if !corruptCH046DictionaryFirstValueID(corrupt) {
		t.Fatal("dictionary test fixture did not use dictionary encoding")
	}
	reader, err := NewSQLColumnarBlockStreamReader(bytes.NewReader(corrupt))
	if err != nil {
		t.Fatalf("corrupt reader creation error = %v", err)
	}
	for reader.NextBlock() {
	}
	if reader.Err() == nil {
		t.Fatal("dictionary value ID outside the dictionary was accepted")
	}
}

func TestSQLColumnarBlockStreamDictionaryRejectsInvalidOptions(t *testing.T) {
	writer := NewSQLColumnarBlockStreamWriterWithColumnsAndOptions(
		&bytes.Buffer{},
		[]SQLRowBinaryColumn{{Name: "state", Type: SQLRowBinaryString}},
		2,
		SQLColumnarBlockStreamOptions{Dictionary: SQLColumnarBlockStreamDictionary(99)},
	)
	if err := writer.WriteRow(Row{"state": "ready"}); err == nil {
		t.Fatal("invalid dictionary mode unexpectedly succeeded")
	}
}

func encodeCH046DictionaryTestRows(t *testing.T, columns []SQLRowBinaryColumn, rows []Row, options SQLColumnarBlockStreamOptions) []byte {
	t.Helper()
	var encoded bytes.Buffer
	writer := NewSQLColumnarBlockStreamWriterWithColumnsAndOptions(&encoded, columns, 32, options)
	for _, row := range rows {
		if err := writer.WriteRow(row); err != nil {
			t.Fatalf("WriteRow() error = %v", err)
		}
	}
	if err := writer.Finish(); err != nil {
		t.Fatalf("Finish() error = %v", err)
	}
	return encoded.Bytes()
}

func corruptCH046DictionaryFirstValueID(wire []byte) bool {
	offset := len(sqlColumnarBlockStreamMagic)
	headerLength, size := binary.Uvarint(wire[offset:])
	if size <= 0 {
		return false
	}
	offset += size + int(headerLength)
	if offset >= len(wire) || wire[offset] != sqlColumnarBlockFrameData {
		return false
	}
	offset++
	for index := 0; index < 5; index++ {
		_, size = binary.Uvarint(wire[offset:])
		if size <= 0 {
			return false
		}
		offset += size
	}
	if offset >= len(wire) {
		return false
	}
	encoding := wire[offset]
	offset++
	if encoding != sqlColumnarBlockEncodingDictionary {
		return false
	}
	payloadLength, size := binary.Uvarint(wire[offset:])
	if size <= 0 {
		return false
	}
	offset += size
	if payloadLength > uint64(len(wire)-offset) {
		return false
	}
	payloadEnd := offset + int(payloadLength)
	if _, size = binary.Uvarint(wire[offset:payloadEnd]); size <= 0 {
		return false
	}
	dictionaryCount, size := binary.Uvarint(wire[offset:payloadEnd])
	if size <= 0 || dictionaryCount == 0 {
		return false
	}
	offset += size
	for index := uint64(0); index < dictionaryCount; index++ {
		_, next, err := decodeSQLRowBinaryDictionaryString(wire[:payloadEnd], offset)
		if err != nil {
			return false
		}
		offset = next
	}
	if offset >= payloadEnd {
		return false
	}
	wire[offset] = 0x7f
	return true
}
