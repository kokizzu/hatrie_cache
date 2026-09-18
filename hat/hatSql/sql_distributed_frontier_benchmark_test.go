package hatSql

import "testing"

func newM032BenchmarkBarriers(b *testing.B) []*SQLSourceFrontierBarrier {
	b.Helper()
	barriers := make([]*SQLSourceFrontierBarrier, 0, 8)
	for sourceIndex := 0; sourceIndex < 8; sourceIndex++ {
		barrier, err := NewSQLSourceFrontierBarrierFromPartitions([]SQLSourceFrontierPartition{
			{Source: "source", Partition: string(rune('a' + sourceIndex*2))},
			{Source: "source", Partition: string(rune('b' + sourceIndex*2))},
		})
		if err != nil {
			b.Fatal(err)
		}
		if _, err := barrier.ObserveBatch([]SQLSourceFrontier{
			{Source: "source", Partition: string(rune('a' + sourceIndex*2)), Frontier: 100},
			{Source: "source", Partition: string(rune('b' + sourceIndex*2)), Frontier: 100},
		}); err != nil {
			b.Fatal(err)
		}
		barriers = append(barriers, barrier)
	}
	return barriers
}

func BenchmarkM032IndependentFrontierBaseline(b *testing.B) {
	barriers := newM032BenchmarkBarriers(b)
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		ready := true
		for _, barrier := range barriers {
			ready = barrier.ReadyAt(100) && ready
		}
		if !ready {
			b.Fatal("frontier unexpectedly not ready")
		}
	}
}

func BenchmarkM032DistributedFrontier(b *testing.B) {
	coordinator, err := NewSQLDistributedFrontierCoordinator(newM032BenchmarkBarriers(b))
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if !coordinator.ReadyAt(100) {
			b.Fatal("frontier unexpectedly not ready")
		}
	}
}

func BenchmarkM032DistributedFrontierConstruction(b *testing.B) {
	barriers := newM032BenchmarkBarriers(b)
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		coordinator, err := NewSQLDistributedFrontierCoordinator(barriers)
		if err != nil || coordinator == nil {
			b.Fatal("coordinator construction failed")
		}
	}
}
