package hatDataStructure

import "testing"

var c223QuantileSink QuantileEstimate

func BenchmarkC223QuantileReplayBaseline(b *testing.B) {
	left := c223QuantileValues(0, 2048)
	right := c223QuantileValues(2048, 2048)
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		sketch, err := NewQuantileSketch(0.05)
		if err != nil {
			b.Fatal(err)
		}
		sketch.AddValidBatch(left)
		sketch.AddValidBatch(right)
		c223QuantileSink, _ = sketch.Estimate(0.95)
	}
}

func BenchmarkC223QuantileMerge(b *testing.B) {
	left, err := c223BuildQuantile(0, 2048)
	if err != nil {
		b.Fatal(err)
	}
	right, err := c223BuildQuantile(2048, 2048)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		merged, err := NewQuantileSketch(0.05)
		if err != nil {
			b.Fatal(err)
		}
		if err := merged.Merge(left); err != nil {
			b.Fatal(err)
		}
		if err := merged.Merge(right); err != nil {
			b.Fatal(err)
		}
		c223QuantileSink, _ = merged.Estimate(0.95)
	}
}

func c223BuildQuantile(offset, count int) (QuantileSketch, error) {
	sketch, err := NewQuantileSketch(0.05)
	if err != nil {
		return QuantileSketch{}, err
	}
	sketch.AddValidBatch(c223QuantileValues(offset, count))
	return sketch, nil
}

func c223QuantileValues(offset, count int) []float64 {
	values := make([]float64, count)
	for index := range values {
		values[index] = float64(offset+index) + float64((index*17)%10)/10
	}
	return values
}
