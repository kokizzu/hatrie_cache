package hatReplication

import "testing"

func newTR003BenchmarkBarrier(b *testing.B) *ReplicaPromotionBarrier {
	b.Helper()
	barrier, err := NewReplicaPromotionBarrier(ReplicaPromotionBarrierOptions{MaxReplicas: 64})
	if err != nil {
		b.Fatal(err)
	}
	if err := barrier.ObserveSource(100); err != nil {
		b.Fatal(err)
	}
	if err := barrier.ObserveReplica("standby-a", 100); err != nil {
		b.Fatal(err)
	}
	return barrier
}

func BenchmarkTR003ReplicaPromotionBarrierCapture(b *testing.B) {
	barrier := newTR003BenchmarkBarrier(b)
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := barrier.Capture("standby-a"); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTR003ReplicaPromotionBarrierPromote(b *testing.B) {
	barrier := newTR003BenchmarkBarrier(b)
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		token, err := barrier.Capture("standby-a")
		if err != nil {
			b.Fatal(err)
		}
		if _, err := barrier.Promote(token); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTR003ReplicaPromotionBarrierObserveReplica(b *testing.B) {
	barrier := newTR003BenchmarkBarrier(b)
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if err := barrier.ObserveReplica("standby-a", uint64(101+index)); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTR003ReplicaPromotionBarrierSnapshot(b *testing.B) {
	barrier := newTR003BenchmarkBarrier(b)
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if len(barrier.Snapshot().Replicas) != 1 {
			b.Fatal("unexpected replica count")
		}
	}
}
