package hatDataStructure

import (
	"fmt"
	"testing"
)

var benchmarkTU53FrontierSink uint64

func BenchmarkTU53DirectMinimum64(b *testing.B) {
	values := make([]uint64, 64)
	for index := range values {
		values[index] = uint64(index)
	}

	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		minimum := values[0]
		for _, value := range values[1:] {
			if value < minimum {
				minimum = value
			}
		}
		benchmarkTU53FrontierSink = minimum
	}
}

func BenchmarkTU53AdvanceNonMinimumSource(b *testing.B) {
	frontier, err := NewLogicalFrontier(LogicalFrontierConfig{MaxSources: 64})
	if err != nil {
		b.Fatal(err)
	}
	for index := 0; index < 64; index++ {
		if err := frontier.Register(fmt.Sprintf("source-%02d", index), 0); err != nil {
			b.Fatal(err)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		advanced, advanceErr := frontier.Advance("source-63", uint64(iteration+1))
		if advanceErr != nil || !advanced {
			b.Fatalf("advance = %v, %v", advanced, advanceErr)
		}
	}
}

func BenchmarkTU53Snapshot64(b *testing.B) {
	frontier, err := NewLogicalFrontier(LogicalFrontierConfig{MaxSources: 64})
	if err != nil {
		b.Fatal(err)
	}
	for index := 0; index < 64; index++ {
		if err := frontier.Register(fmt.Sprintf("source-%02d", index), uint64(index)); err != nil {
			b.Fatal(err)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		snapshot := frontier.Snapshot()
		benchmarkTU53FrontierSink = snapshot.Frontier
	}
}
