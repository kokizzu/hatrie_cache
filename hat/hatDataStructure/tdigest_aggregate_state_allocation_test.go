package hatDataStructure

import "testing"

var tdigestAggregateStateAllocationSink []byte

func TestTDigestMarshalAggregateStateAllocationBudget(t *testing.T) {
	digest := NewDefaultTDigest()
	for value := 0; value < 4096; value++ {
		digest.Add(float64(value))
	}

	allocations := testing.AllocsPerRun(20, func() {
		encoded, err := digest.MarshalAggregateState()
		if err != nil {
			t.Fatalf("MarshalAggregateState() error = %v", err)
		}
		tdigestAggregateStateAllocationSink = encoded
	})
	if allocations > 1 {
		t.Fatalf("MarshalAggregateState() allocations = %.0f, want at most 1", allocations)
	}
}
