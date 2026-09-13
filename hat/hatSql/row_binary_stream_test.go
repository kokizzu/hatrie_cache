package hatSql

import (
	"bytes"
	"encoding/json"
	"reflect"
	"testing"
	"time"
)

func TestSQLRowBinaryStreamRoundTripsTypedRows(t *testing.T) {
	columns := []string{"id", "name", "enabled", "missing", "at", "date", "uuid", "decimal", "duration_text", "payload", "doc", "duration"}
	rows := []Row{
		{
			"id":            int64(7),
			"name":          "first",
			"enabled":       true,
			"missing":       nil,
			"at":            time.Date(2026, time.January, 2, 3, 4, 5, 0, time.UTC),
			"date":          SQLDate("2026-01-02"),
			"uuid":          SQLUUID("00112233-4455-6677-8899-aabbccddeeff"),
			"decimal":       SQLDecimal("12.30"),
			"duration_text": SQLDuration("2h3m"),
			"payload":       []byte{1, 2, 3},
			"doc":           nil,
			"duration":      time.Second,
		},
		{
			"id":            int64(8),
			"name":          "second",
			"enabled":       false,
			"missing":       "present",
			"at":            time.Date(2026, time.January, 3, 3, 4, 5, 0, time.UTC),
			"date":          SQLDate("2026-01-03"),
			"uuid":          SQLUUID("ffeeddcc-bbaa-9988-7766-554433221100"),
			"decimal":       SQLDecimal("13.40"),
			"duration_text": SQLDuration("3h4m"),
			"payload":       []byte{4, 5},
			"doc":           map[string]interface{}{"ok": true},
			"duration":      2 * time.Second,
		},
	}
	var encoded bytes.Buffer
	writer := NewSQLRowBinaryStreamWriter(&encoded, columns)
	for _, row := range rows {
		if err := writer.WriteRow(row); err != nil {
			t.Fatalf("WriteRow() error = %v", err)
		}
	}
	if err := writer.Finish(); err != nil {
		t.Fatalf("Finish() error = %v", err)
	}

	gotColumns, gotRows, err := DecodeSQLRowBinaryStream(encoded.Bytes())
	if err != nil {
		t.Fatalf("DecodeSQLRowBinaryStream() error = %v", err)
	}
	if !reflect.DeepEqual(gotColumns, []SQLRowBinaryColumn{
		{Name: "id", Type: SQLRowBinaryInt64, Nullable: true},
		{Name: "name", Type: SQLRowBinaryString, Nullable: true},
		{Name: "enabled", Type: SQLRowBinaryBool, Nullable: true},
		{Name: "missing", Type: SQLRowBinaryJSON, Nullable: true},
		{Name: "at", Type: SQLRowBinaryDateTime, Nullable: true},
		{Name: "date", Type: SQLRowBinaryDate, Nullable: true},
		{Name: "uuid", Type: SQLRowBinaryUUID, Nullable: true},
		{Name: "decimal", Type: SQLRowBinaryString, Nullable: true},
		{Name: "duration_text", Type: SQLRowBinaryString, Nullable: true},
		{Name: "payload", Type: SQLRowBinaryBytes, Nullable: true},
		{Name: "doc", Type: SQLRowBinaryJSON, Nullable: true},
		{Name: "duration", Type: SQLRowBinaryDuration, Nullable: true},
	}) {
		t.Fatalf("columns = %#v", gotColumns)
	}
	expectedRows := []Row{
		{
			"id":            int64(7),
			"name":          "first",
			"enabled":       true,
			"missing":       nil,
			"at":            time.Date(2026, time.January, 2, 3, 4, 5, 0, time.UTC),
			"date":          time.Date(2026, time.January, 2, 0, 0, 0, 0, time.UTC),
			"uuid":          [16]byte{0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff},
			"decimal":       "12.30",
			"duration_text": "2h3m",
			"payload":       []byte{1, 2, 3},
			"doc":           nil,
			"duration":      time.Second,
		},
		{
			"id":            int64(8),
			"name":          "second",
			"enabled":       false,
			"missing":       json.RawMessage(`"present"`),
			"at":            time.Date(2026, time.January, 3, 3, 4, 5, 0, time.UTC),
			"date":          time.Date(2026, time.January, 3, 0, 0, 0, 0, time.UTC),
			"uuid":          [16]byte{0xff, 0xee, 0xdd, 0xcc, 0xbb, 0xaa, 0x99, 0x88, 0x77, 0x66, 0x55, 0x44, 0x33, 0x22, 0x11, 0x00},
			"decimal":       "13.40",
			"duration_text": "3h4m",
			"payload":       []byte{4, 5},
			"doc":           json.RawMessage(`{"ok":true}`),
			"duration":      2 * time.Second,
		},
	}
	if !reflect.DeepEqual(gotRows, expectedRows) {
		t.Fatalf("rows = %#v, want %#v", gotRows, expectedRows)
	}
}

func TestSQLRowBinaryStreamFinishesEmptyWithColumnMetadata(t *testing.T) {
	var encoded bytes.Buffer
	writer := NewSQLRowBinaryStreamWriter(&encoded, []string{"id", "name"})
	if err := writer.Finish(); err != nil {
		t.Fatalf("Finish() error = %v", err)
	}
	columns, rows, err := DecodeSQLRowBinaryStream(encoded.Bytes())
	if err != nil {
		t.Fatalf("DecodeSQLRowBinaryStream() error = %v", err)
	}
	if len(rows) != 0 || !reflect.DeepEqual(columns, []SQLRowBinaryColumn{
		{Name: "id", Type: SQLRowBinaryJSON, Nullable: true},
		{Name: "name", Type: SQLRowBinaryJSON, Nullable: true},
	}) {
		t.Fatalf("empty stream = columns %#v rows %#v", columns, rows)
	}
}

func TestSQLRowBinaryStreamReaderDecodesRowsIncrementally(t *testing.T) {
	columns := []string{"id", "name"}
	var encoded bytes.Buffer
	writer := NewSQLRowBinaryStreamWriter(&encoded, columns)
	for _, row := range []Row{{"id": int64(1), "name": "one"}, {"id": int64(2), "name": "two"}} {
		if err := writer.WriteRow(row); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Finish(); err != nil {
		t.Fatal(err)
	}

	reader, err := NewSQLRowBinaryStreamReader(bytes.NewReader(encoded.Bytes()))
	if err != nil {
		t.Fatalf("NewSQLRowBinaryStreamReader() error = %v", err)
	}
	if !reflect.DeepEqual(reader.Columns(), []SQLRowBinaryColumn{
		{Name: "id", Type: SQLRowBinaryInt64, Nullable: true},
		{Name: "name", Type: SQLRowBinaryString, Nullable: true},
	}) {
		t.Fatalf("columns = %#v", reader.Columns())
	}
	if !reader.Next() || !reflect.DeepEqual(reader.Row(), Row{"id": int64(1), "name": "one"}) {
		t.Fatalf("first row = %#v", reader.Row())
	}
	if !reader.Next() || !reflect.DeepEqual(reader.Row(), Row{"id": int64(2), "name": "two"}) {
		t.Fatalf("second row = %#v", reader.Row())
	}
	if reader.Next() || reader.Err() != nil {
		t.Fatalf("reader completion = next %v err %v", reader.Next(), reader.Err())
	}
}
