package hatSql

import (
	"bytes"
	"reflect"
	"testing"
	"time"
)

func TestSQLRowBinaryAdaptiveSampledRoundTripsAndSelectsFromPrefix(t *testing.T) {
	columns := []SQLRowBinaryColumn{
		{Name: "id", Type: SQLRowBinaryInt64},
		{Name: "at", Type: SQLRowBinaryDateTime},
		{Name: "label", Type: SQLRowBinaryString},
	}
	rows := make([]SQLRow, 256)
	start := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	for index := range rows {
		rows[index] = SQLRow{
			"id":    int64(index + 1),
			"at":    start.Add(time.Duration(index) * time.Second),
			"label": "steady",
		}
	}

	encoded, err := EncodeSQLRowBinaryAdaptiveSampled(columns, rows, 32)
	if err != nil {
		t.Fatalf("EncodeSQLRowBinaryAdaptiveSampled() error = %v", err)
	}
	if len(encoded) < len(sqlRowBinaryAdaptiveMagic)+1 || !bytes.Equal(encoded[:len(sqlRowBinaryAdaptiveMagic)], sqlRowBinaryAdaptiveMagic[:]) {
		t.Fatalf("sampled adaptive header = %x", encoded[:minAdaptiveSampledInt(len(encoded), 8)])
	}
	if encoded[len(sqlRowBinaryAdaptiveMagic)] != byte(SQLRowBinaryAdaptiveCodecDoubleDelta) {
		t.Fatalf("sampled adaptive codec = %d, want double-delta", encoded[len(sqlRowBinaryAdaptiveMagic)])
	}

	got, err := DecodeSQLRowBinaryAdaptive(columns, encoded)
	if err != nil {
		t.Fatalf("DecodeSQLRowBinaryAdaptive() error = %v", err)
	}
	if !reflect.DeepEqual(got, rows) {
		t.Fatalf("sampled adaptive round trip mismatch: got %#v want %#v", got, rows)
	}
}

func TestSQLRowBinaryAdaptiveSampledClampsSampleAndRejectsInvalidSize(t *testing.T) {
	columns := []SQLRowBinaryColumn{{Name: "id", Type: SQLRowBinaryInt64}}
	rows := []SQLRow{{"id": int64(1)}, {"id": int64(2)}}

	if _, err := EncodeSQLRowBinaryAdaptiveSampled(columns, rows, len(rows)+1); err != nil {
		t.Fatalf("sample larger than batch error = %v", err)
	}
	for _, sampleRows := range []int{0, -1} {
		if _, err := EncodeSQLRowBinaryAdaptiveSampled(columns, rows, sampleRows); err == nil {
			t.Errorf("sampleRows=%d error = nil", sampleRows)
		}
	}
}

func TestSQLRowBinaryAdaptiveSampledPreservesExistingAdaptiveFormats(t *testing.T) {
	columns := []SQLRowBinaryColumn{{Name: "value", Type: SQLRowBinaryInt64}}
	rows := []SQLRow{{"value": int64(0)}, {"value": int64(1 << 62)}, {"value": int64(-1 << 62)}, {"value": int64(7)}}

	sampled, err := EncodeSQLRowBinaryAdaptiveSampled(columns, rows, 2)
	if err != nil {
		t.Fatalf("EncodeSQLRowBinaryAdaptiveSampled() error = %v", err)
	}
	legacy, err := EncodeSQLRowBinary(columns, rows)
	if err != nil {
		t.Fatalf("EncodeSQLRowBinary() error = %v", err)
	}
	decodedSampled, err := DecodeSQLRowBinaryAdaptive(columns, sampled)
	if err != nil {
		t.Fatalf("DecodeSQLRowBinaryAdaptive() error = %v", err)
	}
	decodedLegacy, err := DecodeSQLRowBinary(columns, legacy)
	if err != nil {
		t.Fatalf("DecodeSQLRowBinary() error = %v", err)
	}
	if !reflect.DeepEqual(decodedSampled, rows) || !reflect.DeepEqual(decodedLegacy, rows) {
		t.Fatalf("sampled or legacy format failed to preserve rows")
	}
}

func BenchmarkSQLRowBinaryAdaptiveSampled(b *testing.B) {
	columns := []SQLRowBinaryColumn{
		{Name: "id", Type: SQLRowBinaryInt64},
		{Name: "at", Type: SQLRowBinaryDateTime},
		{Name: "label", Type: SQLRowBinaryString},
	}
	rows := make([]SQLRow, 1024)
	start := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	for index := range rows {
		rows[index] = SQLRow{
			"id":    int64(index + 1),
			"at":    start.Add(time.Duration(index) * time.Second),
			"label": "steady",
		}
	}
	fullAdaptive, err := EncodeSQLRowBinaryAdaptive(columns, rows)
	if err != nil {
		b.Fatal(err)
	}
	sampled, err := EncodeSQLRowBinaryAdaptiveSampled(columns, rows, 32)
	if err != nil {
		b.Fatal(err)
	}
	shiftedRows := make([]SQLRow, len(rows))
	copy(shiftedRows, rows)
	for index := 32; index < len(shiftedRows); index++ {
		value := int64((uint64(index) * 11400714819323198485) & (uint64(1<<60) - 1))
		shiftedRows[index] = SQLRow{
			"id":    value,
			"at":    start.Add(time.Duration(value)),
			"label": "steady",
		}
	}
	shiftedFullAdaptive, err := EncodeSQLRowBinaryAdaptive(columns, shiftedRows)
	if err != nil {
		b.Fatal(err)
	}
	shiftedSampled, err := EncodeSQLRowBinaryAdaptiveSampled(columns, shiftedRows, 32)
	if err != nil {
		b.Fatal(err)
	}
	singleColumns := []SQLRowBinaryColumn{{Name: "value", Type: SQLRowBinaryInt64}}
	singleRows := make([]SQLRow, len(rows))
	for index := range singleRows {
		value := int64(index)
		if index >= 32 {
			value = int64((uint64(index) * 11400714819323198485) & (uint64(1<<60) - 1))
		}
		singleRows[index] = SQLRow{"value": value}
	}
	singleLegacy, err := EncodeSQLRowBinary(singleColumns, singleRows)
	if err != nil {
		b.Fatal(err)
	}
	singleFullAdaptive, err := EncodeSQLRowBinaryAdaptive(singleColumns, singleRows)
	if err != nil {
		b.Fatal(err)
	}
	singleSampled, err := EncodeSQLRowBinaryAdaptiveSampled(singleColumns, singleRows, 32)
	if err != nil {
		b.Fatal(err)
	}
	b.Run("full_adaptive", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			if _, err := EncodeSQLRowBinaryAdaptive(columns, rows); err != nil {
				b.Fatal(err)
			}
		}
		b.ReportMetric(float64(len(fullAdaptive)), "wire_bytes")
	})
	b.Run("sampled_32", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			if _, err := EncodeSQLRowBinaryAdaptiveSampled(columns, rows, 32); err != nil {
				b.Fatal(err)
			}
		}
		b.ReportMetric(float64(len(sampled)), "wire_bytes")
	})
	b.Run("shifted_full_adaptive", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			if _, err := EncodeSQLRowBinaryAdaptive(columns, shiftedRows); err != nil {
				b.Fatal(err)
			}
		}
		b.ReportMetric(float64(len(shiftedFullAdaptive)), "wire_bytes")
	})
	b.Run("shifted_sampled_32", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			if _, err := EncodeSQLRowBinaryAdaptiveSampled(columns, shiftedRows, 32); err != nil {
				b.Fatal(err)
			}
		}
		b.ReportMetric(float64(len(shiftedSampled)), "wire_bytes")
	})
	b.Run("single_shifted_legacy", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			if _, err := EncodeSQLRowBinary(singleColumns, singleRows); err != nil {
				b.Fatal(err)
			}
		}
		b.ReportMetric(float64(len(singleLegacy)), "wire_bytes")
	})
	b.Run("single_shifted_full_adaptive", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			if _, err := EncodeSQLRowBinaryAdaptive(singleColumns, singleRows); err != nil {
				b.Fatal(err)
			}
		}
		b.ReportMetric(float64(len(singleFullAdaptive)), "wire_bytes")
	})
	b.Run("single_shifted_sampled_32", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			if _, err := EncodeSQLRowBinaryAdaptiveSampled(singleColumns, singleRows, 32); err != nil {
				b.Fatal(err)
			}
		}
		b.ReportMetric(float64(len(singleSampled)), "wire_bytes")
	})
}

func minAdaptiveSampledInt(left, right int) int {
	if left < right {
		return left
	}
	return right
}
