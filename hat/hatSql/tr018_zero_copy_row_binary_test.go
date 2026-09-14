package hatSql

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestTR018BorrowedRowBinaryFieldsBorrowInput(t *testing.T) {
	columns := tr018RowBinaryColumns()
	rows := []SQLRow{{
		"id":       int64(42),
		"name":     "borrowed-name",
		"payload":  []byte("borrowed-payload"),
		"document": json.RawMessage(`{"kind":"borrowed","active":true}`),
		"optional": nil,
	}}
	encoded, err := EncodeSQLRowBinary(columns, rows)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	reader, err := NewSQLRowBinaryBorrowedReader(columns, encoded)
	if err != nil {
		t.Fatalf("new reader: %v", err)
	}
	if !reader.Next() {
		t.Fatalf("Next returned false: %v", reader.Err())
	}
	row := reader.Row()
	if len(row.Fields) != len(columns) {
		t.Fatalf("field count = %d, want %d", len(row.Fields), len(columns))
	}
	for index, field := range row.Fields {
		if field.Type != columns[index].Type {
			t.Errorf("field %d type = %d, want %d", index, field.Type, columns[index].Type)
		}
	}
	if !row.Fields[4].Null || row.Fields[4].Data != nil {
		t.Fatalf("optional NULL field = %#v, want Null=true and nil Data", row.Fields[4])
	}
	if got := int64(binary.LittleEndian.Uint64(row.Fields[0].Data)); got != 42 {
		t.Fatalf("id = %d, want 42", got)
	}
	name := []byte("borrowed-name")
	nameOffset := bytes.Index(encoded, name)
	if nameOffset < 0 {
		t.Fatal("encoded name not found")
	}
	if !bytes.Equal(row.Fields[1].Data, name) {
		t.Fatalf("name = %q, want %q", row.Fields[1].Data, name)
	}
	if len(row.Fields[1].Data) == 0 || &row.Fields[1].Data[0] != &encoded[nameOffset] {
		t.Fatal("name field does not borrow the encoded input")
	}
	payload := []byte("borrowed-payload")
	payloadOffset := bytes.Index(encoded, payload)
	if payloadOffset < 0 {
		t.Fatal("encoded payload not found")
	}
	if len(row.Fields[2].Data) == 0 || &row.Fields[2].Data[0] != &encoded[payloadOffset] {
		t.Fatal("payload field does not borrow the encoded input")
	}
	if !json.Valid(row.Fields[3].Data) {
		t.Fatal("borrowed JSON field is not valid JSON")
	}
	if reader.Next() {
		t.Fatal("Next returned an unexpected second row")
	}
	if err := reader.Err(); err != nil {
		t.Fatalf("read: %v", err)
	}
}

func TestTR018BorrowedRowBinaryMatchesTypedBoundaries(t *testing.T) {
	columns := tr018RowBinaryColumns()
	rows := tr018RowBinaryRows(3)
	encoded, err := EncodeSQLRowBinary(columns, rows)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	reader, err := NewSQLRowBinaryBorrowedReader(columns, encoded)
	if err != nil {
		t.Fatalf("new reader: %v", err)
	}
	readRows := 0
	for reader.Next() {
		row := reader.Row()
		expected := rows[readRows]
		if got := int64(binary.LittleEndian.Uint64(row.Fields[0].Data)); got != expected["id"].(int64) {
			t.Errorf("row %d id = %d, want %d", readRows, got, expected["id"])
		}
		if got := string(row.Fields[1].Data); got != expected["name"].(string) {
			t.Errorf("row %d name = %q, want %q", readRows, got, expected["name"])
		}
		if !bytes.Equal(row.Fields[2].Data, expected["payload"].([]byte)) {
			t.Errorf("row %d payload = %q, want %q", readRows, row.Fields[2].Data, expected["payload"])
		}
		if !bytes.Equal(row.Fields[3].Data, expected["document"].(json.RawMessage)) {
			t.Errorf("row %d document = %q, want %q", readRows, row.Fields[3].Data, expected["document"])
		}
		if expected["optional"] == nil {
			if !row.Fields[4].Null || row.Fields[4].Data != nil {
				t.Errorf("row %d optional = %#v, want NULL", readRows, row.Fields[4])
			}
		} else if row.Fields[4].Null || string(row.Fields[4].Data) != expected["optional"].(string) {
			t.Errorf("row %d optional = %#v, want %q", readRows, row.Fields[4], expected["optional"])
		}
		readRows++
	}
	if err := reader.Err(); err != nil {
		t.Fatalf("read: %v", err)
	}
	if readRows != len(rows) {
		t.Fatalf("read rows = %d, want %d", readRows, len(rows))
	}
}

func TestTR018BorrowedRowBinarySupportsAllTypes(t *testing.T) {
	ipv4, err := ParseSQLIPv4("192.0.2.42")
	if err != nil {
		t.Fatalf("parse IPv4: %v", err)
	}
	ipv6, err := ParseSQLIPv6("2001:db8::42")
	if err != nil {
		t.Fatalf("parse IPv6: %v", err)
	}
	var uuid [16]byte
	for index := range uuid {
		uuid[index] = byte(index + 1)
	}
	columns := []SQLRowBinaryColumn{
		{Name: "int64", Type: SQLRowBinaryInt64},
		{Name: "uint64", Type: SQLRowBinaryUint64},
		{Name: "float64", Type: SQLRowBinaryFloat64},
		{Name: "bool", Type: SQLRowBinaryBool},
		{Name: "string", Type: SQLRowBinaryString},
		{Name: "bytes", Type: SQLRowBinaryBytes},
		{Name: "date", Type: SQLRowBinaryDate},
		{Name: "datetime", Type: SQLRowBinaryDateTime},
		{Name: "duration", Type: SQLRowBinaryDuration},
		{Name: "uuid", Type: SQLRowBinaryUUID},
		{Name: "json", Type: SQLRowBinaryJSON},
		{Name: "ipv4", Type: SQLRowBinaryIPv4},
		{Name: "ipv6", Type: SQLRowBinaryIPv6},
		{Name: "enum8", Type: SQLRowBinaryEnum8, EnumValues: []string{"queued", "running"}},
		{Name: "enum16", Type: SQLRowBinaryEnum16, EnumValues: []string{"queued", "running"}},
		{Name: "decimal128", Type: SQLRowBinaryDecimal128, DecimalScale: 2, DecimalPrecision: 38},
		{Name: "decimal256", Type: SQLRowBinaryDecimal256, DecimalScale: 4, DecimalPrecision: 76},
	}
	rows := []SQLRow{{
		"int64":      int64(-42),
		"uint64":     uint64(99),
		"float64":    float64(1.25),
		"bool":       true,
		"string":     "all-types",
		"bytes":      []byte{1, 2, 3, 4},
		"date":       time.Date(2026, time.September, 14, 0, 0, 0, 0, time.UTC),
		"datetime":   time.Date(2026, time.September, 14, 12, 34, 56, 789000000, time.UTC),
		"duration":   1500 * time.Millisecond,
		"uuid":       uuid,
		"json":       json.RawMessage(`{"kind":"all-types"}`),
		"ipv4":       ipv4,
		"ipv6":       ipv6,
		"enum8":      "running",
		"enum16":     SQLEnum16(1),
		"decimal128": "123.45",
		"decimal256": "-9876.5432",
	}}
	encoded, err := EncodeSQLRowBinary(columns, rows)
	if err != nil {
		t.Fatalf("encode all types: %v", err)
	}
	reader, err := NewSQLRowBinaryBorrowedReader(columns, encoded)
	if err != nil {
		t.Fatalf("new reader: %v", err)
	}
	if !reader.Next() {
		t.Fatalf("Next returned false: %v", reader.Err())
	}
	wantLengths := []int{8, 8, 8, 1, len("all-types"), 4, 4, 8, 8, 16, len(`{"kind":"all-types"}`), 4, 16, 1, 2, 16, 32}
	row := reader.Row()
	if len(row.Fields) != len(wantLengths) {
		t.Fatalf("field count = %d, want %d", len(row.Fields), len(wantLengths))
	}
	for index, field := range row.Fields {
		if field.Null {
			t.Errorf("field %d unexpectedly NULL", index)
		}
		if got := len(field.Data); got != wantLengths[index] {
			t.Errorf("field %d length = %d, want %d", index, got, wantLengths[index])
		}
	}
	if reader.Next() {
		t.Fatal("Next returned an unexpected second row")
	}
	if err := reader.Err(); err != nil {
		t.Fatalf("read all types: %v", err)
	}
}

func TestTR018BorrowedRowBinaryResetReusesReader(t *testing.T) {
	columns := tr018RowBinaryColumns()
	encoded, err := EncodeSQLRowBinary(columns, tr018RowBinaryRows(1))
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	reader, err := NewSQLRowBinaryBorrowedReader(columns, encoded)
	if err != nil {
		t.Fatalf("new reader: %v", err)
	}
	if !reader.Next() {
		t.Fatalf("first Next returned false: %v", reader.Err())
	}
	firstFields := reader.Row().Fields
	firstFieldAddress := &firstFields[0]
	if err := reader.Reset(encoded); err != nil {
		t.Fatalf("reset: %v", err)
	}
	if !reader.Next() {
		t.Fatalf("second Next returned false: %v", reader.Err())
	}
	secondFields := reader.Row().Fields
	if &secondFields[0] != firstFieldAddress {
		t.Fatal("Reset did not reuse the field storage")
	}
	if err := reader.Err(); err != nil {
		t.Fatalf("read after reset: %v", err)
	}
}

func TestTR018BorrowedRowBinaryRejectsMalformedInput(t *testing.T) {
	columns := tr018RowBinaryColumns()
	encoded, err := EncodeSQLRowBinary(columns, tr018RowBinaryRows(2))
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	t.Run("truncated", func(t *testing.T) {
		assertTR018BorrowedReaderError(t, columns, encoded[:len(encoded)-1], "RowBinary")
	})
	t.Run("invalid nullable marker", func(t *testing.T) {
		invalid := append([]byte(nil), encoded...)
		invalid[0] = 2
		assertTR018BorrowedReaderError(t, columns, invalid, "invalid NULL marker")
	})
	t.Run("invalid JSON", func(t *testing.T) {
		invalid := append([]byte(nil), encoded...)
		needle := []byte(`{"kind":"row","active":true}`)
		offset := bytes.Index(invalid, needle)
		if offset < 0 {
			t.Fatal("encoded JSON not found")
		}
		copy(invalid[offset:], strings.Repeat("x", len(needle)))
		assertTR018BorrowedReaderError(t, columns, invalid, "invalid JSON")
	})
}

func assertTR018BorrowedReaderError(t *testing.T, columns []SQLRowBinaryColumn, encoded []byte, want string) {
	t.Helper()
	reader, err := NewSQLRowBinaryBorrowedReader(columns, encoded)
	if err != nil {
		t.Fatalf("new reader: %v", err)
	}
	for reader.Next() {
	}
	if err := reader.Err(); err == nil || !strings.Contains(err.Error(), want) {
		t.Fatalf("Err = %v, want substring %q", err, want)
	}
}
