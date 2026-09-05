package hatSql_test

import (
	"bytes"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"hatrie_cache/hat/hatSql"
)

var rowBinaryColumns = []hatSql.SQLRowBinaryColumn{
	{Name: "id", Type: hatSql.SQLRowBinaryInt64},
	{Name: "score", Type: hatSql.SQLRowBinaryFloat64},
	{Name: "active", Type: hatSql.SQLRowBinaryBool},
	{Name: "name", Type: hatSql.SQLRowBinaryString, Nullable: true},
	{Name: "payload", Type: hatSql.SQLRowBinaryBytes, Nullable: true},
	{Name: "created_at", Type: hatSql.SQLRowBinaryDateTime},
	{Name: "duration", Type: hatSql.SQLRowBinaryDuration, Nullable: true},
}

func TestSQLRowBinaryRoundTrip(t *testing.T) {
	createdAt := time.Date(2026, time.September, 5, 12, 34, 56, 123000000, time.UTC)
	rows := []hatSql.SQLRow{
		{
			"id":         int64(7),
			"score":      float64(1.25),
			"active":     true,
			"name":       "alpha",
			"payload":    []byte{1, 2, 3},
			"created_at": createdAt,
			"duration":   1500 * time.Millisecond,
		},
		{
			"id":         int64(8),
			"score":      float64(-2.5),
			"active":     false,
			"name":       nil,
			"payload":    nil,
			"created_at": createdAt.Add(time.Minute),
			"duration":   nil,
		},
	}

	encoded, err := hatSql.EncodeSQLRowBinary(rowBinaryColumns, rows)
	if err != nil {
		t.Fatalf("encode rows: %v", err)
	}
	decoded, err := hatSql.DecodeSQLRowBinary(rowBinaryColumns, encoded)
	if err != nil {
		t.Fatalf("decode rows: %v", err)
	}
	if !reflect.DeepEqual(decoded, rows) {
		t.Fatalf("decoded rows = %#v, want %#v", decoded, rows)
	}
	rows[0]["payload"].([]byte)[0] = 9
	if decoded[0]["payload"].([]byte)[0] != 1 {
		t.Fatal("decoded bytes alias the source row")
	}
}

func TestSQLRowBinaryUsesCompactSchemaAwareEncoding(t *testing.T) {
	columns := []hatSql.SQLRowBinaryColumn{{Name: "id", Type: hatSql.SQLRowBinaryInt64}}
	encoded, err := hatSql.EncodeSQLRowBinary(columns, []hatSql.SQLRow{{"id": int64(1)}})
	if err != nil {
		t.Fatalf("encode fixed-width row: %v", err)
	}
	if want := 8; len(encoded) != want {
		t.Fatalf("encoded fixed-width row length = %d, want %d", len(encoded), want)
	}
}

func TestSQLRowBinaryRoundTripAdditionalTypes(t *testing.T) {
	columns := []hatSql.SQLRowBinaryColumn{
		{Name: "count", Type: hatSql.SQLRowBinaryUint64},
		{Name: "day", Type: hatSql.SQLRowBinaryDate},
		{Name: "uuid", Type: hatSql.SQLRowBinaryUUID},
		{Name: "document", Type: hatSql.SQLRowBinaryJSON},
	}
	var uuid [16]byte
	for index := range uuid {
		uuid[index] = byte(index + 1)
	}
	rows := []hatSql.SQLRow{{
		"count":    uint64(99),
		"day":      time.Date(2026, time.September, 5, 0, 0, 0, 0, time.UTC),
		"uuid":     uuid,
		"document": json.RawMessage(`{"ok":true}`),
	}}
	encoded, err := hatSql.EncodeSQLRowBinary(columns, rows)
	if err != nil {
		t.Fatalf("encode additional types: %v", err)
	}
	decoded, err := hatSql.DecodeSQLRowBinary(columns, encoded)
	if err != nil {
		t.Fatalf("decode additional types: %v", err)
	}
	if !reflect.DeepEqual(decoded, rows) {
		t.Fatalf("decoded additional types = %#v, want %#v", decoded, rows)
	}
}

func TestSQLRowBinaryPayloadIsSmallerThanJSON(t *testing.T) {
	rows, columns := benchmarkSQLRowBinaryRows()
	encoded, err := hatSql.EncodeSQLRowBinary(columns, rows)
	if err != nil {
		t.Fatalf("encode payload: %v", err)
	}
	jsonPayload, err := json.Marshal(rows)
	if err != nil {
		t.Fatalf("encode JSON payload: %v", err)
	}
	if len(encoded) >= len(jsonPayload) {
		t.Fatalf("RowBinary payload = %d bytes, JSON payload = %d bytes", len(encoded), len(jsonPayload))
	}
	t.Logf("RowBinary payload = %d bytes, JSON payload = %d bytes", len(encoded), len(jsonPayload))
}

func TestSQLRowBinaryRejectsInvalidInput(t *testing.T) {
	columns := []hatSql.SQLRowBinaryColumn{{Name: "id", Type: hatSql.SQLRowBinaryInt64}}
	if _, err := hatSql.EncodeSQLRowBinary(nil, []hatSql.SQLRow{{"id": int64(1)}}); err == nil {
		t.Fatal("expected missing schema error")
	}
	if _, err := hatSql.EncodeSQLRowBinary(columns, []hatSql.SQLRow{{"id": nil}}); err == nil {
		t.Fatal("expected null non-nullable value error")
	}
	if _, err := hatSql.DecodeSQLRowBinary(columns, []byte{1, 2, 3}); err == nil {
		t.Fatal("expected truncated fixed-width value error")
	}
	nullable := []hatSql.SQLRowBinaryColumn{{Name: "name", Type: hatSql.SQLRowBinaryString, Nullable: true}}
	if _, err := hatSql.DecodeSQLRowBinary(nullable, []byte{2}); err == nil {
		t.Fatal("expected invalid nullable marker error")
	}
	if _, err := hatSql.EncodeSQLRowBinary([]hatSql.SQLRowBinaryColumn{{Name: "id", Type: 255}}, []hatSql.SQLRow{{"id": int64(1)}}); err == nil {
		t.Fatal("expected unsupported type error")
	}
}

func TestSQLRowBinaryEmptyStream(t *testing.T) {
	columns := []hatSql.SQLRowBinaryColumn{{Name: "id", Type: hatSql.SQLRowBinaryInt64}}
	decoded, err := hatSql.DecodeSQLRowBinary(columns, nil)
	if err != nil {
		t.Fatalf("decode empty stream: %v", err)
	}
	if decoded != nil {
		t.Fatalf("decoded empty stream = %#v, want nil", decoded)
	}
}

func benchmarkSQLRowBinaryRows() ([]hatSql.SQLRow, []hatSql.SQLRowBinaryColumn) {
	rows := make([]hatSql.SQLRow, 128)
	for index := range rows {
		rows[index] = hatSql.SQLRow{
			"id":      int64(index),
			"score":   float64(index) * 1.25,
			"active":  index%2 == 0,
			"name":    "name",
			"payload": []byte("payload"),
		}
	}
	columns := []hatSql.SQLRowBinaryColumn{
		{Name: "id", Type: hatSql.SQLRowBinaryInt64},
		{Name: "score", Type: hatSql.SQLRowBinaryFloat64},
		{Name: "active", Type: hatSql.SQLRowBinaryBool},
		{Name: "name", Type: hatSql.SQLRowBinaryString},
		{Name: "payload", Type: hatSql.SQLRowBinaryBytes},
	}
	return rows, columns
}

func BenchmarkSQLRowBinaryEncode(b *testing.B) {
	rows, columns := benchmarkSQLRowBinaryRows()
	payload, err := hatSql.EncodeSQLRowBinary(columns, rows)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ReportMetric(float64(len(payload)), "wire-B")
	for range b.N {
		if _, err := hatSql.EncodeSQLRowBinary(columns, rows); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSQLJSONEncodeBaseline(b *testing.B) {
	rows, _ := benchmarkSQLRowBinaryRows()
	payload, err := json.Marshal(rows)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ReportMetric(float64(len(payload)), "wire-B")
	for range b.N {
		if _, err := json.Marshal(rows); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSQLRowBinaryDecode(b *testing.B) {
	rows, columns := benchmarkSQLRowBinaryRows()
	encoded, err := hatSql.EncodeSQLRowBinary(columns, rows)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ReportMetric(float64(len(encoded)), "wire-B")
	for range b.N {
		if _, err := hatSql.DecodeSQLRowBinary(columns, encoded); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSQLJSONDecodeBaseline(b *testing.B) {
	rows, _ := benchmarkSQLRowBinaryRows()
	encoded, err := json.Marshal(rows)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ReportMetric(float64(len(encoded)), "wire-B")
	for range b.N {
		var decoded []hatSql.SQLRow
		if err := json.Unmarshal(encoded, &decoded); err != nil {
			b.Fatal(err)
		}
	}
	if bytes.Equal(encoded, nil) {
		b.Fatal("unexpected empty benchmark payload")
	}
}
