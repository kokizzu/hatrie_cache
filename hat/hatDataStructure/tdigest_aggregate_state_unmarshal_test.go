package hatDataStructure

import "testing"

var tdigestAggregateStateUnmarshalAllocationSink TDigest

func TestTDigestUnmarshalAggregateStateAllocationBudget(t *testing.T) {
	digest := NewDefaultTDigest()
	for value := 0; value < 4096; value++ {
		digest.Add(float64(value))
	}
	encoded, err := digest.MarshalAggregateState()
	if err != nil {
		t.Fatalf("MarshalAggregateState() error = %v", err)
	}

	allocations := testing.AllocsPerRun(20, func() {
		decoded, err := NewTDigestFromAggregateState(encoded)
		if err != nil {
			t.Fatalf("NewTDigestFromAggregateState() error = %v", err)
		}
		tdigestAggregateStateUnmarshalAllocationSink = decoded
	})
	if allocations > 3 {
		t.Fatalf("NewTDigestFromAggregateState() allocations = %.0f, want at most 3", allocations)
	}
}
