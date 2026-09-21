package hatReplication_test

import (
	"fmt"
	"sync/atomic"
	"testing"

	"hatrie_cache/hat/hatReplication"
)

var m209RawChangefeedFrontierAdvanceSink uint64

func BenchmarkM209ChangefeedFrontierAdvanceBaseline(b *testing.B) {
	var sequence uint64
	for index := 0; index < b.N; index++ {
		progress, err := m209BaselineChangefeedAdvance(&sequence, uint64(index+1))
		if err != nil {
			b.Fatal(err)
		}
		sequence = progress.Sequence
	}
	m209RawChangefeedFrontierAdvanceSink = sequence
}

func m209BaselineChangefeedAdvance(sequence *uint64, next uint64) (hatReplication.ChangefeedProgress, error) {
	for {
		current := atomic.LoadUint64(sequence)
		if next < current {
			return hatReplication.ChangefeedProgress{Sequence: current, Progressed: true}, fmt.Errorf("unexpected regression: current=%d next=%d", current, next)
		}
		if next == current || atomic.CompareAndSwapUint64(sequence, current, next) {
			return hatReplication.ChangefeedProgress{Sequence: next, Progressed: true}, nil
		}
	}
}
