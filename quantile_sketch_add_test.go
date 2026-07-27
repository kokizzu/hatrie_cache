package hatriecache

import (
	"math"
	"reflect"
	"testing"
)

var benchmarkQuantileEstimateSink QuantileEstimate

func TestQuantileSketchAddPreflightRejectsNonFiniteWithoutMutation(t *testing.T) {
	sketch := newDefaultQuantileSketchData()
	sketch.Add(1, 2, 3)
	before := sketch.Snapshot()

	for _, values := range [][]float64{
		{4, math.NaN(), 5},
		{math.Inf(1), 4, 5},
		{4, 5, math.Inf(-1)},
	} {
		if got := sketch.Add(values[0], values[1:]...); got != (QuantileEstimate{}) {
			t.Fatalf("Add(%v) = %#v, want zero", values, got)
		}
		if after := sketch.Snapshot(); !reflect.DeepEqual(after, before) {
			t.Fatalf("Add(%v) mutated sketch: after=%#v before=%#v", values, after, before)
		}
	}
}

func BenchmarkQuantileSketchAddValidation(b *testing.B) {
	b.Run("Scalar", func(b *testing.B) {
		sketch := newDefaultQuantileSketchData()
		for value := 0; value < 1024; value++ {
			sketch.Add(float64(value))
		}
		b.ReportAllocs()
		b.ResetTimer()
		for iteration := 0; iteration < b.N; iteration++ {
			benchmarkQuantileEstimateSink = sketch.Add(float64(iteration & 1023))
		}
	})

	b.Run("Batch16", func(b *testing.B) {
		sketch := newDefaultQuantileSketchData()
		values := make([]float64, 16)
		for index := range values {
			values[index] = float64(index)
			sketch.Add(values[index])
		}
		b.ReportAllocs()
		b.ResetTimer()
		for iteration := 0; iteration < b.N; iteration++ {
			benchmarkQuantileEstimateSink = sketch.Add(values[0], values[1:]...)
		}
	})
}
