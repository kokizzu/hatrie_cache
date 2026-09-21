package hatSql_test

import "testing"

func BenchmarkCH014BReadyQueueNoReady(b *testing.B) {
	graph, err := newCH014BBlockedGraph()
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for range b.N {
		if claimed := graph.ClaimReady(1); len(claimed) != 0 {
			b.Fatalf("ClaimReady returned %d blocked tasks", len(claimed))
		}
	}
}
