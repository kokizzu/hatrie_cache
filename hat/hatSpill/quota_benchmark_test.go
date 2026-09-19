package hatSpill

import (
	"context"
	"runtime"
	"sync/atomic"
	"testing"
)

func BenchmarkDirectAtomicSpillCounter(b *testing.B) {
	var used uint64
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		atomic.AddUint64(&used, 64)
		atomic.AddUint64(&used, ^uint64(63))
	}
	b.StopTimer()
	runtime.KeepAlive(used)
}

func BenchmarkBudgetReserveRelease(b *testing.B) {
	budget := NewBudget(1 << 30)
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lease, err := budget.Reserve(ctx, 64)
		if err != nil {
			b.Fatal(err)
		}
		if !lease.Release() {
			b.Fatal("Release returned false")
		}
	}
}

func BenchmarkBudgetTryReserveRelease(b *testing.B) {
	budget := NewBudget(1 << 30)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lease, ok := budget.TryReserve(64)
		if !ok {
			b.Fatal("TryReserve returned false")
		}
		if !lease.Release() {
			b.Fatal("Release returned false")
		}
	}
}

func BenchmarkBudgetSnapshot(b *testing.B) {
	budget := NewBudget(1 << 30)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if budget.Snapshot().Limit == 0 {
			b.Fatal("unexpected unlimited budget")
		}
	}
}
