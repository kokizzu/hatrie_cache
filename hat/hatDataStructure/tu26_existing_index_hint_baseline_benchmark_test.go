package hatDataStructure

import "testing"

type tu26BaselineIndexCandidate struct {
	name             string
	estimatedCost    uint64
	supportsEquality bool
}

var tu26BaselineSelectionSink string

func BenchmarkTU26BaselineLinearIndexSelection(b *testing.B) {
	candidates := []tu26BaselineIndexCandidate{
		{name: "ordered-created", estimatedCost: 90, supportsEquality: true},
		{name: "hash-account", estimatedCost: 12, supportsEquality: true},
		{name: "text-search", estimatedCost: 30, supportsEquality: false},
		{name: "fallback", estimatedCost: 300, supportsEquality: true},
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bestCost := ^uint64(0)
		bestName := ""
		for _, candidate := range candidates {
			if candidate.supportsEquality && candidate.estimatedCost < bestCost {
				bestCost = candidate.estimatedCost
				bestName = candidate.name
			}
		}
		tu26BaselineSelectionSink = bestName
	}
}
