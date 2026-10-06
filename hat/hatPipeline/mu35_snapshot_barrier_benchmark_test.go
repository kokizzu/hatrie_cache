package hatPipeline

import (
	"context"
	"testing"
)

func BenchmarkMU35SnapshotBarrierDirectReadyCheck(b *testing.B) {
	ready := true
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if !ready {
			b.Fatal("direct readiness unexpectedly false")
		}
	}
}

func BenchmarkMU35SnapshotBarrierReadyWait(b *testing.B) {
	barrier, err := NewSnapshotBarrier(SnapshotBarrierOptions{Prerequisites: []string{"source"}})
	if err != nil {
		b.Fatal(err)
	}
	if err := barrier.MarkReady("source"); err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := barrier.Wait(ctx); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMU35SnapshotBarrierStatus(b *testing.B) {
	barrier, err := NewSnapshotBarrier(SnapshotBarrierOptions{Prerequisites: []string{"index", "source"}})
	if err != nil {
		b.Fatal(err)
	}
	if err := barrier.MarkReady("index"); err != nil {
		b.Fatal(err)
	}
	if err := barrier.MarkReady("source"); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		status := barrier.Status()
		if status.State != SnapshotBarrierReady {
			b.Fatal("barrier unexpectedly not ready")
		}
	}
}
