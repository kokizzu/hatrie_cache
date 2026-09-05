package hatSql_test

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"hatrie_cache/hat/hatSql"
)

func TestEncodeSQLRowBinaryBitmapRoundTrip(t *testing.T) {
	columns := []hatSql.SQLRowBinaryColumn{
		{Name: "id", Type: hatSql.SQLRowBinaryInt64},
		{Name: "count", Type: hatSql.SQLRowBinaryUint64, Nullable: true},
		{Name: "score", Type: hatSql.SQLRowBinaryFloat64, Nullable: true},
		{Name: "active", Type: hatSql.SQLRowBinaryBool, Nullable: true},
		{Name: "name", Type: hatSql.SQLRowBinaryString, Nullable: true},
		{Name: "payload", Type: hatSql.SQLRowBinaryBytes, Nullable: true},
		{Name: "day", Type: hatSql.SQLRowBinaryDate, Nullable: true},
		{Name: "at", Type: hatSql.SQLRowBinaryDateTime, Nullable: true},
		{Name: "elapsed", Type: hatSql.SQLRowBinaryDuration, Nullable: true},
		{Name: "uuid", Type: hatSql.SQLRowBinaryUUID, Nullable: true},
		{Name: "metadata", Type: hatSql.SQLRowBinaryJSON, Nullable: true},
	}
	rows := []hatSql.SQLRow{
		{
			"id":       int64(7),
			"count":    uint64(20),
			"score":    2.5,
			"active":   false,
			"name":     "Ada",
			"payload":  []byte{1, 2},
			"day":      time.Date(2026, time.January, 3, 0, 0, 0, 0, time.UTC),
			"at":       time.Date(2026, time.January, 3, 12, 0, 0, 0, time.UTC),
			"elapsed":  5 * time.Second,
			"uuid":     [16]byte{0x01, 0xff},
			"metadata": json.RawMessage(`{"tier":"warm"}`),
		},
		{
			"id":       int64(8),
			"count":    nil,
			"score":    nil,
			"active":   nil,
			"name":     nil,
			"payload":  nil,
			"day":      nil,
			"at":       nil,
			"elapsed":  nil,
			"uuid":     nil,
			"metadata": nil,
		},
	}
	wire, err := hatSql.EncodeSQLRowBinaryBitmap(columns, rows)
	if err != nil {
		t.Fatalf("EncodeSQLRowBinaryBitmap() error = %v", err)
	}
	got, err := hatSql.DecodeSQLRowBinaryBitmap(columns, wire)
	if err != nil {
		t.Fatalf("DecodeSQLRowBinaryBitmap() error = %v", err)
	}
	if !reflect.DeepEqual(got, rows) {
		t.Fatalf("round trip mismatch: got %#v want %#v", got, rows)
	}
	if empty, err := hatSql.EncodeSQLRowBinaryBitmap(columns, nil); err != nil || empty != nil {
		t.Fatalf("empty encode = %x, %v", empty, err)
	}
}

func TestSQLRowBinaryBitmapRejectsMalformedInput(t *testing.T) {
	columns := []hatSql.SQLRowBinaryColumn{
		{Name: "id", Type: hatSql.SQLRowBinaryInt64},
		{Name: "active", Type: hatSql.SQLRowBinaryBool, Nullable: true},
	}
	wire, err := hatSql.EncodeSQLRowBinaryBitmap(columns, []hatSql.SQLRow{{"id": int64(1), "active": true}})
	if err != nil {
		t.Fatal(err)
	}
	tests := [][]byte{
		[]byte("BAD!"),
		wire[:3],
		wire[:len(wire)-1],
		append(append([]byte(nil), wire...), 1),
	}
	for index, encoded := range tests {
		t.Run(string(rune('a'+index)), func(t *testing.T) {
			defer func() {
				if recovered := recover(); recovered != nil {
					t.Fatalf("DecodeSQLRowBinaryBitmap() panicked: %v", recovered)
				}
			}()
			if _, err := hatSql.DecodeSQLRowBinaryBitmap(columns, encoded); err == nil {
				t.Fatal("DecodeSQLRowBinaryBitmap() error = nil")
			}
		})
	}
	unusedBitColumns := []hatSql.SQLRowBinaryColumn{{Name: "value", Type: hatSql.SQLRowBinaryInt64, Nullable: true}}
	unusedBitWire, err := hatSql.EncodeSQLRowBinaryBitmap(unusedBitColumns, []hatSql.SQLRow{{"value": nil}})
	if err != nil {
		t.Fatal(err)
	}
	unusedBitWire[len(unusedBitWire)-1] = 0x02
	if _, err := hatSql.DecodeSQLRowBinaryBitmap(unusedBitColumns, unusedBitWire); err == nil {
		t.Fatal("DecodeSQLRowBinaryBitmap() error = nil for nonzero unused bitmap bit")
	}
}

func TestSQLRowBinaryBitmapReducesNullableMarkerOverhead(t *testing.T) {
	columns := make([]hatSql.SQLRowBinaryColumn, 16)
	rows := make([]hatSql.SQLRow, 256)
	for columnIndex := range columns {
		columns[columnIndex] = hatSql.SQLRowBinaryColumn{
			Name:     "nullable_" + string(rune('a'+columnIndex)),
			Type:     hatSql.SQLRowBinaryInt64,
			Nullable: true,
		}
	}
	for rowIndex := range rows {
		rows[rowIndex] = make(hatSql.SQLRow, len(columns))
		for _, column := range columns {
			rows[rowIndex][column.Name] = nil
		}
	}
	legacy, err := hatSql.EncodeSQLRowBinary(columns, rows)
	if err != nil {
		t.Fatal(err)
	}
	bitmap, err := hatSql.EncodeSQLRowBinaryBitmap(columns, rows)
	if err != nil {
		t.Fatal(err)
	}
	if len(bitmap) >= len(legacy) {
		t.Fatalf("bitmap wire size = %d, legacy = %d", len(bitmap), len(legacy))
	}
	if len(bitmap)*4 > len(legacy) {
		t.Fatalf("bitmap wire size = %d, legacy = %d, expected at least 4x reduction", len(bitmap), len(legacy))
	}
}

func BenchmarkSQLRowBinaryBitmap(b *testing.B) {
	columns := make([]hatSql.SQLRowBinaryColumn, 16)
	rows := make([]hatSql.SQLRow, 1024)
	for columnIndex := range columns {
		columns[columnIndex] = hatSql.SQLRowBinaryColumn{
			Name:     "nullable_" + string(rune('a'+columnIndex)),
			Type:     hatSql.SQLRowBinaryInt64,
			Nullable: true,
		}
	}
	for rowIndex := range rows {
		rows[rowIndex] = make(hatSql.SQLRow, len(columns))
		for columnIndex, column := range columns {
			if (rowIndex+columnIndex)%4 == 0 {
				rows[rowIndex][column.Name] = nil
			} else {
				rows[rowIndex][column.Name] = int64(rowIndex + columnIndex)
			}
		}
	}
	legacy, err := hatSql.EncodeSQLRowBinary(columns, rows)
	if err != nil {
		b.Fatal(err)
	}
	bitmap, err := hatSql.EncodeSQLRowBinaryBitmap(columns, rows)
	if err != nil {
		b.Fatal(err)
	}
	b.Run("legacy_encode", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			if _, err := hatSql.EncodeSQLRowBinary(columns, rows); err != nil {
				b.Fatal(err)
			}
		}
		b.ReportMetric(float64(len(legacy)), "wire_bytes")
	})
	b.Run("bitmap_encode", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			if _, err := hatSql.EncodeSQLRowBinaryBitmap(columns, rows); err != nil {
				b.Fatal(err)
			}
		}
		b.ReportMetric(float64(len(bitmap)), "wire_bytes")
	})
	b.Run("legacy_decode", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			decoded, err := hatSql.DecodeSQLRowBinary(columns, legacy)
			if err != nil || len(decoded) != len(rows) {
				b.Fatalf("legacy rows = %d, %v", len(decoded), err)
			}
		}
	})
	b.Run("bitmap_decode", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			decoded, err := hatSql.DecodeSQLRowBinaryBitmap(columns, bitmap)
			if err != nil || len(decoded) != len(rows) {
				b.Fatalf("bitmap rows = %d, %v", len(decoded), err)
			}
		}
	})
}
