package hatMetrics

// HistogramSnapshot is a deep-copyable cumulative histogram state.
type HistogramSnapshot struct {
	Bounds  []float64
	Buckets []uint64
	Count   uint64
	Sum     float64
}

// Histogram tracks observations against ordered inclusive bounds. It is not
// synchronized; callers that share it concurrently must provide locking.
type Histogram struct {
	bounds  []float64
	buckets []uint64
	count   uint64
	sum     float64
}

func NewHistogram(bounds []float64) *Histogram {
	copied := append([]float64(nil), bounds...)
	return &Histogram{bounds: copied, buckets: make([]uint64, len(copied))}
}

func (histogram *Histogram) Observe(value float64) {
	if histogram == nil {
		return
	}
	if value < 0 {
		value = 0
	}
	histogram.count++
	histogram.sum += value
	for index, bound := range histogram.bounds {
		if value <= bound {
			histogram.buckets[index]++
			return
		}
	}
}

func (histogram *Histogram) Snapshot() HistogramSnapshot {
	if histogram == nil {
		return HistogramSnapshot{}
	}
	return HistogramSnapshot{Bounds: append([]float64(nil), histogram.bounds...), Buckets: append([]uint64(nil), histogram.buckets...), Count: histogram.count, Sum: histogram.sum}
}
