package hatSql_test

import (
	"bytes"
	"reflect"
	"testing"
	"time"

	"hatrie_cache/hat/hatSql"
)

func TestSQLRowBinaryAdaptiveSelectsAndRoundTripsBestCodec(t *testing.T) {
	columns := []hatSql.SQLRowBinaryColumn{
		{Name: "id", Type: hatSql.SQLRowBinaryInt64},
		{Name: "at", Type: hatSql.SQLRowBinaryDateTime},
		{Name: "label", Type: hatSql.SQLRowBinaryString},
	}
	rows := make([]hatSql.SQLRow, 128)
	start := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	for index := range rows {
		rows[index] = hatSql.SQLRow{
			"id":    int64(index + 1),
			"at":    start.Add(time.Duration(index) * time.Second),
			"label": "steady",
		}
	}
	adaptive, err := hatSql.EncodeSQLRowBinaryAdaptive(columns, rows)
	if err != nil {
		t.Fatal(err)
	}
	if len(adaptive) < 5 || !bytes.Equal(adaptive[:4], []byte("HSA1")) {
		t.Fatalf("adaptive header = %x", adaptive[:minAdaptiveInt(len(adaptive), 8)])
	}
	if adaptive[4] != byte(hatSql.SQLRowBinaryAdaptiveCodecDoubleDelta) {
		t.Fatalf("adaptive codec = %d, want double-delta", adaptive[4])
	}
	got, err := hatSql.DecodeSQLRowBinaryAdaptive(columns, adaptive)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, rows) {
		t.Fatalf("adaptive round trip mismatch: got %#v want %#v", got, rows)
	}

	legacy, err := hatSql.EncodeSQLRowBinary(columns, rows)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := hatSql.DecodeSQLRowBinary(columns, legacy); err != nil {
		t.Fatal(err)
	}
}

func TestSQLRowBinaryAdaptiveKeepsIrregularPayloadsLegacy(t *testing.T) {
	columns := []hatSql.SQLRowBinaryColumn{{Name: "value", Type: hatSql.SQLRowBinaryInt64}}
	rows := []hatSql.SQLRow{
		{"value": int64(0)},
		{"value": int64(1 << 62)},
		{"value": int64(-1 << 62)},
		{"value": int64(7)},
	}
	adaptive, err := hatSql.EncodeSQLRowBinaryAdaptive(columns, rows)
	if err != nil {
		t.Fatal(err)
	}
	if adaptive[4] != byte(hatSql.SQLRowBinaryAdaptiveCodecLegacy) {
		t.Fatalf("adaptive codec = %d, want legacy", adaptive[4])
	}
	got, err := hatSql.DecodeSQLRowBinaryAdaptive(columns, adaptive)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, rows) {
		t.Fatalf("legacy adaptive round trip mismatch: got %#v want %#v", got, rows)
	}
}

func TestSQLRowBinaryAdaptiveRejectsMalformedEnvelope(t *testing.T) {
	columns := []hatSql.SQLRowBinaryColumn{{Name: "id", Type: hatSql.SQLRowBinaryInt64}}
	tests := [][]byte{
		[]byte("BAD!"),
		[]byte("HSA1"),
		[]byte("HSA1\x00\x01"),
		[]byte("HSA1\x7f\x00"),
	}
	for index, encoded := range tests {
		t.Run(string(rune('a'+index)), func(t *testing.T) {
			defer func() {
				if recovered := recover(); recovered != nil {
					t.Fatalf("DecodeSQLRowBinaryAdaptive() panicked: %v", recovered)
				}
			}()
			if _, err := hatSql.DecodeSQLRowBinaryAdaptive(columns, encoded); err == nil {
				t.Fatal("DecodeSQLRowBinaryAdaptive() error = nil")
			}
		})
	}
}

func BenchmarkSQLRowBinaryAdaptive(b *testing.B) {
	columns := []hatSql.SQLRowBinaryColumn{
		{Name: "id", Type: hatSql.SQLRowBinaryInt64},
		{Name: "at", Type: hatSql.SQLRowBinaryDateTime},
		{Name: "label", Type: hatSql.SQLRowBinaryString},
	}
	rows := make([]hatSql.SQLRow, 128)
	start := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	for index := range rows {
		rows[index] = hatSql.SQLRow{
			"id":    int64(index + 1),
			"at":    start.Add(time.Duration(index) * time.Second),
			"label": "steady",
		}
	}
	legacy, err := hatSql.EncodeSQLRowBinary(columns, rows)
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
	adaptive, err := hatSql.EncodeSQLRowBinaryAdaptive(columns, rows)
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
	b.Run("adaptive_encode", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			if _, err := hatSql.EncodeSQLRowBinaryAdaptive(columns, rows); err != nil {
				b.Fatal(err)
			}
		}
		b.ReportMetric(float64(len(adaptive)), "wire_bytes")
	})
	b.Run("legacy_decode", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			if _, err := hatSql.DecodeSQLRowBinary(columns, legacy); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("adaptive_decode", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			if _, err := hatSql.DecodeSQLRowBinaryAdaptive(columns, adaptive); err != nil {
				b.Fatal(err)
			}
		}
	})
	_ = delta
	_ = doubleDelta
}

func minAdaptiveInt(left, right int) int {
	if left < right {
		return left
	}
	return right
}
