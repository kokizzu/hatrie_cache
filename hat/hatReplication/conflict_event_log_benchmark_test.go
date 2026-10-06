package hatReplication

import "testing"

var conflictEventLogBenchmarkSink uint64

func BenchmarkConflictEventLog(b *testing.B) {
	left := ConflictVersion{Timestamp: 1, NodeID: "node-a", Sequence: 1}
	right := ConflictVersion{Timestamp: 2, NodeID: "node-b", Sequence: 1}
	event := ConflictEvent{
		Space:     "orders",
		KeyDigest: [16]byte{1},
		Left:      left,
		Right:     right,
		Winner:    right,
		Decision:  ConflictEventDecisionRight,
		Policy:    ConflictPolicyLastWriteWins,
	}
	b.Run("direct-resolution-control", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			winner, err := ResolveConflictVersion(left, right)
			if err != nil {
				b.Fatal(err)
			}
			conflictEventLogBenchmarkSink += winner.Sequence
		}
	})
	b.Run("opt-in-append", func(b *testing.B) {
		log, err := NewConflictEventLog(1024)
		if err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		for range b.N {
			sequence, err := log.Append(event)
			if err != nil {
				b.Fatal(err)
			}
			conflictEventLogBenchmarkSink += sequence
		}
	})
}
