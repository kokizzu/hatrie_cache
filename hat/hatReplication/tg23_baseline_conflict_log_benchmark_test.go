package hatReplication

import "testing"

var tg23BaselineConflictSequence uint64

func BenchmarkTG23BaselineConflictResolution(b *testing.B) {
	left := ConflictVersion{Timestamp: 1, NodeID: "node-a", Sequence: 1}
	right := ConflictVersion{Timestamp: 2, NodeID: "node-b", Sequence: 1}
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		winner, err := ResolveConflictVersion(left, right)
		if err != nil {
			b.Fatal(err)
		}
		tg23BaselineConflictSequence += winner.Sequence
	}
}
