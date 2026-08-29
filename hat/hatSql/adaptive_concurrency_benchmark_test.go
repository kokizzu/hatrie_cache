package hatSql

import (
	"sync/atomic"
	"testing"
)

func BenchmarkAdaptivePlannerConcurrentFeedback(b *testing.B) {
	keys := [32]string{}
	for index := range keys {
		keys[index] = "CACHE(events).kind=" + string(rune('a'+index))
	}
	planner := NewAdaptivePlanner(AdaptivePlannerOptions{MinSamples: 8, UnderestimateFactor: 2})
	var next atomic.Uint32
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(worker *testing.PB) {
		key := keys[next.Add(1)%uint32(len(keys))]
		for worker.Next() {
			planner.ObserveIndex(key, 1, 3)
			_ = planner.ShouldUseIndex(key)
		}
	})
}
