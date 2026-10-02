package hatDataStructure_test

import "testing"

func BenchmarkMZ028SpillableCompactIfNeededWithoutStale(b *testing.B) {
	arrangement := newRound60SpillableCompactionArrangement(b)
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		compacted, err := arrangement.CompactIfNeeded(1)
		if err != nil {
			b.Fatal(err)
		}
		if compacted {
			b.Fatal("clean segment was compacted")
		}
	}
}
