//go:build mu37baseline

package hatStorage

import (
	"runtime"
	"testing"
)

type mu37BaselineCompactionCounters struct {
	logicalBytes  uint64
	physicalBytes uint64
	debtBytes     uint64
	observations  uint64
}

//go:noinline
func (counters *mu37BaselineCompactionCounters) record(index uint64) {
	counters.logicalBytes = index + 1
	counters.physicalBytes = index + 2
	counters.debtBytes = index + 3
	counters.observations++
}

func BenchmarkMU37CompactionCountersBaseline(b *testing.B) {
	var counters mu37BaselineCompactionCounters
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		counters.record(uint64(index))
	}
	b.StopTimer()
	runtime.KeepAlive(counters)
	if counters.observations == 0 {
		b.Fatal("baseline did not record any observations")
	}
}
