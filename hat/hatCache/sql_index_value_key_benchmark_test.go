package hatCache

import (
	"encoding/json"
	"math"
	"testing"
)

var sqlIndexValueKeyBenchmarkResult string

func sqlIndexJSONFloatValueKey(value float64) (string, bool) {
	encoded, err := json.Marshal(value)
	return string(encoded), err == nil
}

func TestSQLIndexFloatValueKeyMatchesJSONEncoding(t *testing.T) {
	t.Parallel()
	for _, value := range []float64{0, math.Copysign(0, -1), 1, -1, 1.25, 1e-7, 1e-8, 1e20, 1e21, math.MaxFloat64, math.SmallestNonzeroFloat64} {
		want, err := json.Marshal(value)
		if err != nil {
			t.Fatalf("json.Marshal(%v): %v", value, err)
		}
		got, ok := sqlIndexFloatValueKey(value)
		if !ok || got != string(want) {
			t.Fatalf("sqlIndexFloatValueKey(%v) = %q/%t, want %q/true", value, got, ok, want)
		}
	}
	for _, value := range []float64{math.Inf(1), math.Inf(-1), math.NaN()} {
		if got, ok := sqlIndexFloatValueKey(value); ok || got != "" {
			t.Fatalf("sqlIndexFloatValueKey(%v) = %q/%t, want empty/false", value, got, ok)
		}
	}
}

func TestSQLIndexIntegerValueKeyMatchesJSONEncoding(t *testing.T) {
	t.Parallel()
	for _, value := range []interface{}{int(-1), int8(-128), int16(-32768), int32(-2147483648), int64(math.MinInt64), uint(1), uint8(255), uint16(65535), uint32(math.MaxUint32), uint64(math.MaxUint64)} {
		want, err := json.Marshal(value)
		if err != nil {
			t.Fatalf("json.Marshal(%T): %v", value, err)
		}
		got, ok := sqlIndexIntegerValueKey(value)
		if !ok || got != string(want) {
			t.Fatalf("sqlIndexIntegerValueKey(%T(%v)) = %q/%t, want %q/true", value, value, got, ok, want)
		}
	}
}

func BenchmarkSQLIndexFloatValueKey(b *testing.B) {
	value := float64(99_999)
	for _, benchmark := range []struct {
		name string
		run  func() (string, bool)
	}{
		{"json_marshal", func() (string, bool) { return sqlIndexJSONFloatValueKey(value) }},
		{"format_float", func() (string, bool) { return sqlIndexFloatValueKey(value) }},
	} {
		b.Run(benchmark.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				key, ok := benchmark.run()
				if !ok {
					b.Fatal("numeric key was rejected")
				}
				sqlIndexValueKeyBenchmarkResult = key
			}
		})
	}
}

func BenchmarkSQLIndexIntegerValueKey(b *testing.B) {
	value := int64(99_999)
	for _, benchmark := range []struct {
		name string
		run  func() (string, bool)
	}{
		{"json_marshal", func() (string, bool) { encoded, err := json.Marshal(value); return string(encoded), err == nil }},
		{"format_int", func() (string, bool) { return sqlIndexIntegerValueKey(value) }},
	} {
		b.Run(benchmark.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				key, ok := benchmark.run()
				if !ok {
					b.Fatal("numeric key was rejected")
				}
				sqlIndexValueKeyBenchmarkResult = key
			}
		})
	}
}
