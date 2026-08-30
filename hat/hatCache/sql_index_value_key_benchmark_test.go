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
