package hatSql

import "testing"

var c210TypedTableHistogramSink TypedTableHistogram

func BenchmarkTypedTableHistogram(b *testing.B) {
	table := newC209TypedTableStatsBenchmarkTable(b)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		histogram, err := table.Histogram("score", TypedTableHistogramOptions{Bins: 32})
		if err != nil {
			b.Fatal(err)
		}
		c210TypedTableHistogramSink = histogram
	}
}
