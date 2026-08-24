package hatMetrics_test

import (
	"testing"

	"hatrie_cache/hat/hatMetrics"
)

func TestHistogramTracksBoundsAndSnapshots(t *testing.T) {
	histogram := hatMetrics.NewHistogram([]float64{1, 5})
	histogram.Observe(-1)
	histogram.Observe(3)
	histogram.Observe(8)
	snapshot := histogram.Snapshot()
	if snapshot.Count != 3 || snapshot.Sum != 11 || len(snapshot.Buckets) != 2 || snapshot.Buckets[0] != 1 || snapshot.Buckets[1] != 1 {
		t.Fatalf("Snapshot() = %#v", snapshot)
	}
}
