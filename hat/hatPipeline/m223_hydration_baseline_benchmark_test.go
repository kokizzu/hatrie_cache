package hatPipeline_test

import (
	"sync"
	"testing"
)

type m223BaselineHydrationSnapshot struct {
	state      uint8
	generation uint64
	completed  uint64
	total      uint64
	remaining  uint64
}

type m223BaselineHydrationState struct {
	mu         sync.Mutex
	state      uint8
	generation uint64
	completed  uint64
	total      uint64
}

func (state *m223BaselineHydrationState) snapshot() m223BaselineHydrationSnapshot {
	state.mu.Lock()
	defer state.mu.Unlock()
	snapshot := m223BaselineHydrationSnapshot{
		state:      state.state,
		generation: state.generation,
		completed:  state.completed,
		total:      state.total,
	}
	if snapshot.total > snapshot.completed {
		snapshot.remaining = snapshot.total - snapshot.completed
	}
	return snapshot
}

func BenchmarkM223BaselineSnapshot(b *testing.B) {
	state := &m223BaselineHydrationState{state: 2, generation: 1, completed: 1, total: 1}
	var sink m223BaselineHydrationSnapshot
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		sink = state.snapshot()
	}
	_ = sink
}
