package hatDataStructure_test

import (
	"testing"

	"hatrie_cache/hat/hatDataStructure"
)

var m209MonotoneTimestampAdvanceSink uint64

func BenchmarkM209MonotoneLogicalTimestampAdvanceIfNewer(b *testing.B) {
	timestamp := hatDataStructure.NewMonotoneLogicalTimestamp(0)
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		if !timestamp.AdvanceIfNewer(uint64(index + 1)) {
			b.Fatal("AdvanceIfNewer() did not advance")
		}
	}
	m209MonotoneTimestampAdvanceSink = timestamp.Current()
}

func BenchmarkM209MonotoneLogicalTimestampAdvance(b *testing.B) {
	timestamp := hatDataStructure.NewMonotoneLogicalTimestamp(0)
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		if _, err := timestamp.Advance(uint64(index + 1)); err != nil {
			b.Fatal(err)
		}
	}
	m209MonotoneTimestampAdvanceSink = timestamp.Current()
}
