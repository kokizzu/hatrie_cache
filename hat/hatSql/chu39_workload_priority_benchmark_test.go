package hatSql

import (
	"context"
	"fmt"
	"testing"
)

func BenchmarkCHU39GateAcquireRelease(b *testing.B) {
	b.Run("default", func(b *testing.B) {
		gate := newNamespaceQueryGate(1)
		ctx := context.Background()
		b.ReportAllocs()
		b.ResetTimer()
		for index := 0; index < b.N; index++ {
			if err := gate.acquire(ctx); err != nil {
				b.Fatal(err)
			}
			gate.release()
		}
	})
	b.Run("high", func(b *testing.B) {
		gate := newNamespaceQueryGate(1)
		ctx := context.Background()
		b.ReportAllocs()
		b.ResetTimer()
		for index := 0; index < b.N; index++ {
			if err := gate.acquireWithPriority(ctx, 10); err != nil {
				b.Fatal(err)
			}
			gate.release()
		}
	})
}

func BenchmarkCHU39PrioritySelection(b *testing.B) {
	for _, count := range []int{2, 8, 64} {
		count := count
		b.Run(fmt.Sprintf("waiters_%d", count), func(b *testing.B) {
			templates := make([]*namespaceQueryWaiter, count)
			waiters := make([]*namespaceQueryWaiter, count)
			for index := range templates {
				templates[index] = &namespaceQueryWaiter{priority: index % 3, sequence: uint64(index)}
			}
			gate := &namespaceQueryGate{waiters: waiters}
			b.ReportAllocs()
			b.ResetTimer()
			for index := 0; index < b.N; index++ {
				copy(waiters, templates)
				gate.waiters = waiters[:count]
				gate.mu.Lock()
				_ = gate.nextWaiterLocked()
				gate.mu.Unlock()
			}
		})
	}
}
