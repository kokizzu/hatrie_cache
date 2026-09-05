package hatSql

import (
	"reflect"
	"testing"
	"time"
)

func TestSQLRowBinaryDateAndDateTimeUseFixedWidthEncoding(t *testing.T) {
	columns := []SQLRowBinaryColumn{
		{Name: "day", Type: SQLRowBinaryDate},
		{Name: "at", Type: SQLRowBinaryDateTime},
	}
	rows := []SQLRow{{
		"day": time.Date(2026, time.January, 3, 21, 45, 0, 0, time.FixedZone("UTC+8", 8*60*60)),
		"at":  time.Date(2026, time.January, 3, 21, 45, 12, 345_000_000, time.FixedZone("UTC+8", 8*60*60)),
	}}
	wire, err := EncodeSQLRowBinary(columns, rows)
	if err != nil {
		t.Fatal(err)
	}
	if len(wire) != 4+8 {
		t.Fatalf("fixed-width date/time payload length = %d, want 12", len(wire))
	}
	decoded, err := DecodeSQLRowBinary(columns, wire)
	if err != nil {
		t.Fatal(err)
	}
	want := []SQLRow{{
		"day": time.Date(2026, time.January, 3, 0, 0, 0, 0, time.UTC),
		"at":  rows[0]["at"].(time.Time).UTC(),
	}}
	if !reflect.DeepEqual(decoded, want) {
		t.Fatalf("decoded rows = %#v, want %#v", decoded, want)
	}
}
