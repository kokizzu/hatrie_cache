package hatSql_test

import (
	"bytes"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"hatrie_cache/hat/hatSql"
)

func TestSQLRowBinaryDeltaRoundTrip(t *testing.T) {
	columns := []hatSql.SQLRowBinaryColumn{
		{Name: "id", Type: hatSql.SQLRowBinaryInt64},
		{Name: "sequence", Type: hatSql.SQLRowBinaryUint64},
		{Name: "day", Type: hatSql.SQLRowBinaryDate},
		{Name: "at", Type: hatSql.SQLRowBinaryDateTime},
		{Name: "elapsed", Type: hatSql.SQLRowBinaryDuration},
		{Name: "active", Type: hatSql.SQLRowBinaryBool},
		{Name: "name", Type: hatSql.SQLRowBinaryString},
		{Name: "payload", Type: hatSql.SQLRowBinaryBytes, Nullable: true},
		{Name: "metadata", Type: hatSql.SQLRowBinaryJSON},
		{Name: "optional_id", Type: hatSql.SQLRowBinaryInt64, Nullable: true},
	}
	rows := []hatSql.SQLRow{
		{
			"id":          int64(100),
			"sequence":    uint64(900),
			"day":         time.Date(2026, time.January, 3, 0, 0, 0, 0, time.UTC),
			"at":          time.Date(2026, time.January, 3, 12, 0, 0, 123, time.UTC),
			"elapsed":     2 * time.Second,
			"active":      true,
			"name":        "alpha",
			"payload":     []byte{1, 2, 3},
			"metadata":    json.RawMessage(`{"tier":"warm"}`),
			"optional_id": nil,
		},
		{
			"id":          int64(101),
			"sequence":    uint64(901),
			"day":         time.Date(2026, time.January, 4, 0, 0, 0, 0, time.UTC),
			"at":          time.Date(2026, time.January, 3, 12, 0, 1, 123, time.UTC),
			"elapsed":     2*time.Second + 10*time.Millisecond,
			"active":      false,
			"name":        "beta",
			"payload":     nil,
			"metadata":    json.RawMessage(`{"tier":"warm"}`),
			"optional_id": int64(7),
		},
		{
			"id":          int64(99),
			"sequence":    uint64(899),
			"day":         time.Date(2026, time.January, 2, 0, 0, 0, 0, time.UTC),
			"at":          time.Date(2026, time.January, 3, 11, 59, 59, 123, time.UTC),
			"elapsed":     time.Second,
			"active":      true,
			"name":        "gamma",
			"payload":     []byte{},
			"metadata":    json.RawMessage(`{"tier":"cold"}`),
			"optional_id": int64(6),
		},
	}

	wire, err := hatSql.EncodeSQLRowBinaryDelta(columns, rows)
	if err != nil {
		t.Fatalf("EncodeSQLRowBinaryDelta() error = %v", err)
	}
	got, err := hatSql.DecodeSQLRowBinaryDelta(columns, wire)
	if err != nil {
		t.Fatalf("DecodeSQLRowBinaryDelta() error = %v", err)
	}
	if !reflect.DeepEqual(got, rows) {
		t.Fatalf("round trip mismatch: got %#v want %#v", got, rows)
	}
	doubleWire, err := hatSql.EncodeSQLRowBinaryDoubleDelta(columns, rows)
	if err != nil {
		t.Fatalf("EncodeSQLRowBinaryDoubleDelta() error = %v", err)
	}
	doubleGot, err := hatSql.DecodeSQLRowBinaryDelta(columns, doubleWire)
	if err != nil {
		t.Fatalf("DecodeSQLRowBinaryDelta() double-delta error = %v", err)
	}
	if !reflect.DeepEqual(doubleGot, rows) {
		t.Fatalf("double-delta round trip mismatch: got %#v want %#v", doubleGot, rows)
	}

	empty, err := hatSql.EncodeSQLRowBinaryDelta(columns, nil)
	if err != nil {
		t.Fatalf("empty EncodeSQLRowBinaryDelta() error = %v", err)
	}
	if empty != nil {
		t.Fatalf("empty encoding = %x, want nil", empty)
	}
	decodedEmpty, err := hatSql.DecodeSQLRowBinaryDelta(columns, nil)
	if err != nil || decodedEmpty != nil {
		t.Fatalf("empty decode = %#v, %v", decodedEmpty, err)
	}
}

func TestSQLRowBinaryDeltaRejectsMalformedInput(t *testing.T) {
	columns := []hatSql.SQLRowBinaryColumn{{Name: "id", Type: hatSql.SQLRowBinaryInt64}}
	rows := []hatSql.SQLRow{{"id": int64(1)}}
	wire, err := hatSql.EncodeSQLRowBinaryDelta(columns, rows)
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name string
		wire []byte
	}{
		{name: "bad magic", wire: []byte("BAD!\x01\x01")},
		{name: "truncated header", wire: wire[:3]},
		{name: "truncated value", wire: wire[:len(wire)-1]},
		{name: "trailing byte", wire: append(append([]byte(nil), wire...), 0xff)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			defer func() {
				if recovered := recover(); recovered != nil {
					t.Fatalf("DecodeSQLRowBinaryDelta() panicked: %v", recovered)
				}
			}()
			if _, err := hatSql.DecodeSQLRowBinaryDelta(columns, test.wire); err == nil {
				t.Fatal("DecodeSQLRowBinaryDelta() error = nil")
			}
		})
	}
}

func TestSQLRowBinaryDeltaReducesMonotonicNumericWireSize(t *testing.T) {
	columns := []hatSql.SQLRowBinaryColumn{
		{Name: "id", Type: hatSql.SQLRowBinaryInt64},
		{Name: "sequence", Type: hatSql.SQLRowBinaryUint64},
		{Name: "at", Type: hatSql.SQLRowBinaryDateTime},
		{Name: "label", Type: hatSql.SQLRowBinaryString},
	}
	rows := make([]hatSql.SQLRow, 1024)
	start := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	for index := range rows {
		rows[index] = hatSql.SQLRow{
			"id":       int64(index + 1),
			"sequence": uint64(100_000 + index),
			"at":       start.Add(time.Duration(index) * time.Second),
			"label":    "steady",
		}
	}
	standard, err := hatSql.EncodeSQLRowBinary(columns, rows)
	if err != nil {
		t.Fatal(err)
	}
	delta, err := hatSql.EncodeSQLRowBinaryDelta(columns, rows)
	if err != nil {
		t.Fatal(err)
	}
	if len(delta) >= len(standard) {
		t.Fatalf("delta wire size = %d, standard = %d", len(delta), len(standard))
	}
	doubleDelta, err := hatSql.EncodeSQLRowBinaryDoubleDelta(columns, rows)
	if err != nil {
		t.Fatal(err)
	}
	if len(doubleDelta) >= len(delta) {
		t.Fatalf("double-delta wire size = %d, delta = %d", len(doubleDelta), len(delta))
	}
	if !bytes.Contains(delta, []byte("HSD1")) {
		t.Fatalf("delta wire is missing format marker: %x", delta[:minInt(len(delta), 8)])
	}
	if !bytes.Contains(doubleDelta, []byte("HSD2")) {
		t.Fatalf("double-delta wire is missing format marker: %x", doubleDelta[:minInt(len(doubleDelta), 8)])
	}
}

func BenchmarkSQLRowBinaryDelta(b *testing.B) {
	columns := []hatSql.SQLRowBinaryColumn{
		{Name: "id", Type: hatSql.SQLRowBinaryInt64},
		{Name: "sequence", Type: hatSql.SQLRowBinaryUint64},
		{Name: "at", Type: hatSql.SQLRowBinaryDateTime},
		{Name: "label", Type: hatSql.SQLRowBinaryString},
	}
	rows := make([]hatSql.SQLRow, 1024)
	start := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	for index := range rows {
		rows[index] = hatSql.SQLRow{
			"id":       int64(index + 1),
			"sequence": uint64(100_000 + index),
			"at":       start.Add(time.Duration(index) * time.Second),
			"label":    "steady",
		}
	}
	standard, err := hatSql.EncodeSQLRowBinary(columns, rows)
	if err != nil {
		b.Fatal(err)
	}
	delta, err := hatSql.EncodeSQLRowBinaryDelta(columns, rows)
	if err != nil {
		b.Fatal(err)
	}
	doubleDelta, err := hatSql.EncodeSQLRowBinaryDoubleDelta(columns, rows)
	if err != nil {
		b.Fatal(err)
	}
	b.Run("standard_encode", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			if _, err := hatSql.EncodeSQLRowBinary(columns, rows); err != nil {
				b.Fatal(err)
			}
		}
		b.ReportMetric(float64(len(standard)), "wire_bytes")
	})
	b.Run("delta_encode", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			if _, err := hatSql.EncodeSQLRowBinaryDelta(columns, rows); err != nil {
				b.Fatal(err)
			}
		}
		b.ReportMetric(float64(len(delta)), "wire_bytes")
	})
	b.Run("double_delta_encode", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			if _, err := hatSql.EncodeSQLRowBinaryDoubleDelta(columns, rows); err != nil {
				b.Fatal(err)
			}
		}
		b.ReportMetric(float64(len(doubleDelta)), "wire_bytes")
	})
	b.Run("standard_decode", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			if _, err := hatSql.DecodeSQLRowBinary(columns, standard); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("delta_decode", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			if _, err := hatSql.DecodeSQLRowBinaryDelta(columns, delta); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("double_delta_decode", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			if _, err := hatSql.DecodeSQLRowBinaryDelta(columns, doubleDelta); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func minInt(left, right int) int {
	if left < right {
		return left
	}
	return right
}
