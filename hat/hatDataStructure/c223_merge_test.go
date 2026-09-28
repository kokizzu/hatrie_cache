package hatDataStructure

import "testing"

func TestQuantileSketchMergePreservesBoundedRankError(t *testing.T) {
	left, err := NewQuantileSketch(0.05)
	if err != nil {
		t.Fatal(err)
	}
	right, err := NewQuantileSketch(0.05)
	if err != nil {
		t.Fatal(err)
	}
	valuesLeft := c223TestQuantileValues(0, 2048)
	valuesRight := c223TestQuantileValues(2048, 2048)
	left.AddValidBatch(valuesLeft)
	right.AddValidBatch(valuesRight)
	if err := left.Merge(right); err != nil {
		t.Fatal(err)
	}
	if left.Snapshot().Count != uint64(len(valuesLeft)+len(valuesRight)) {
		t.Fatalf("merged count = %d, want %d", left.Snapshot().Count, len(valuesLeft)+len(valuesRight))
	}
	if err := ValidateQuantileSketchSnapshot(left.Snapshot()); err != nil {
		t.Fatalf("merged snapshot invalid: %v", err)
	}
	estimate, ok := left.Estimate(0.5)
	if !ok || estimate.Value < 1500 || estimate.Value > 2600 {
		t.Fatalf("merged median = %#v, want value in [1500, 2600]", estimate)
	}

	mismatched, err := NewQuantileSketch(0.1)
	if err != nil {
		t.Fatal(err)
	}
	if err := left.Merge(mismatched); err == nil {
		t.Fatal("expected epsilon mismatch")
	}
}

func c223TestQuantileValues(offset, count int) []float64 {
	values := make([]float64, count)
	for index := range values {
		values[index] = float64(offset+index) + float64((index*17)%10)/10
	}
	return values
}
