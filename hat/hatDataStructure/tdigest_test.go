package hatDataStructure

import (
	"math"
	"testing"
)

func TestTDigestTracksTailAndRoundTrips(t *testing.T) {
	digest, err := NewTDigest(100)
	if err != nil {
		t.Fatalf("NewTDigest() error = %v", err)
	}
	for value := 1; value <= 10000; value++ {
		digest.Add(float64(value))
	}
	estimate, ok := digest.Estimate(0.99)
	if !ok || estimate.Value < 9700 || estimate.Value > 10000 {
		t.Fatalf("Estimate(0.99) = %#v/%v, want a value in [9700, 10000]", estimate, ok)
	}
	if estimate.Count != 10000 {
		t.Fatalf("Estimate(0.99).Count = %d, want 10000", estimate.Count)
	}

	restored, err := NewTDigestFromSnapshot(digest.Snapshot())
	if err != nil {
		t.Fatalf("NewTDigestFromSnapshot() error = %v", err)
	}
	restoredEstimate, ok := restored.Estimate(0.99)
	if !ok || restoredEstimate != estimate {
		t.Fatalf("restored Estimate(0.99) = %#v/%v, want %#v/true", restoredEstimate, ok, estimate)
	}
}

func TestTDigestRejectsInvalidConfigurationAndSnapshots(t *testing.T) {
	if _, err := NewTDigest(0); err == nil {
		t.Fatal("NewTDigest(0) error = nil, want validation error")
	}
	if _, err := NewTDigest(MaxTDigestCompression + 1); err == nil {
		t.Fatal("NewTDigest(max+1) error = nil, want validation error")
	}
	if _, err := NewTDigestFromSnapshot(TDigestSnapshot{Compression: 100, Count: 1}); err == nil {
		t.Fatal("NewTDigestFromSnapshot(missing centroids) error = nil, want validation error")
	}
}

func TestTDigestMergePreservesCountAndTail(t *testing.T) {
	left := NewDefaultTDigest()
	right := NewDefaultTDigest()
	for value := 1; value <= 5000; value++ {
		left.Add(float64(value))
	}
	for value := 5001; value <= 10000; value++ {
		right.Add(float64(value))
	}
	if err := left.Merge(right); err != nil {
		t.Fatalf("Merge() error = %v", err)
	}
	estimate, ok := left.Estimate(0.99)
	if !ok || estimate.Value < 9700 || estimate.Value > 10000 || estimate.Count != 10000 {
		t.Fatalf("merged Estimate(0.99) = %#v/%v, want count 10000 and value in [9700, 10000]", estimate, ok)
	}
}

func TestTDigestRejectsNonFiniteBatchWithoutMutation(t *testing.T) {
	digest := NewDefaultTDigest()
	if got := digest.Add(1, math.NaN()); got != (QuantileEstimate{}) {
		t.Fatalf("Add(non-finite) = %#v, want zero estimate", got)
	}
	if info := digest.Info(); info.Count != 0 || info.CentroidSize != 0 {
		t.Fatalf("digest after rejected Add = %#v, want empty", info)
	}
	if got := digest.AddValidBatch([]float64{1, math.Inf(1)}); got != (QuantileEstimate{}) {
		t.Fatalf("AddValidBatch(non-finite) = %#v, want zero estimate", got)
	}
	if info := digest.Info(); info.Count != 0 || info.CentroidSize != 0 {
		t.Fatalf("digest after rejected batch = %#v, want empty", info)
	}
}

func TestTDigestTailAccuracyComparedWithGK(t *testing.T) {
	gk, err := NewQuantileSketch(0.01)
	if err != nil {
		t.Fatalf("NewQuantileSketch() error = %v", err)
	}
	tdigest := NewDefaultTDigest()
	for index := 0; index < 99900; index++ {
		value := float64(index % 100)
		gk.Add(value)
		tdigest.Add(value)
	}
	for index := 0; index < 100; index++ {
		value := float64(1000000 + index*100000)
		gk.Add(value)
		tdigest.Add(value)
	}
	gkEstimate, ok := gk.Estimate(0.999)
	if !ok {
		t.Fatal("GK Estimate(0.999) unavailable")
	}
	tdigestEstimate, ok := tdigest.Estimate(0.999)
	if !ok {
		t.Fatal("TDigest Estimate(0.999) unavailable")
	}
	t.Logf("p99.9 estimates: GK=%v TDigest=%v", gkEstimate.Value, tdigestEstimate.Value)
	exact := float64(1000000)
	if math.Abs(tdigestEstimate.Value-exact) >= math.Abs(gkEstimate.Value-exact) {
		t.Fatalf("TDigest p99.9 = %v, GK = %v, want t-digest closer to exact %v", tdigestEstimate.Value, gkEstimate.Value, exact)
	}
}
