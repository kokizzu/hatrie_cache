package hatDataStructure

import (
	"testing"
	"time"
)

func BenchmarkVisibilityQueueLeaseWithTokenAckToken(b *testing.B) {
	now := time.Unix(700, 0).UTC()
	queue := NewVisibilityQueueWithEpoch[string](1, time.Minute, 7)
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if !queue.Enqueue("value") {
			b.Fatal("Enqueue returned false")
		}
		lease, ok := queue.LeaseWithToken(now)
		if !ok || !queue.AckToken(lease.Token) {
			b.Fatalf("token lease/ack = %+v/%t, want success", lease, ok)
		}
	}
}

func BenchmarkVisibilityQueueLeaseWithTokenAckTokenResident256(b *testing.B) {
	now := time.Unix(800, 0).UTC()
	queue := NewVisibilityQueueWithEpoch[int](512, time.Minute, 8)
	leases := make([]VisibilityQueueLease[int], 256)
	for value := 0; value < 256; value++ {
		if !queue.Enqueue(value) {
			b.Fatalf("initial Enqueue(%d) returned false", value)
		}
	}
	for index := range leases {
		lease, ok := queue.LeaseWithToken(now)
		if !ok {
			b.Fatal("initial token Lease returned no item")
		}
		leases[index] = lease
	}
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		leaseIndex := index & (len(leases) - 1)
		if !queue.AckToken(leases[leaseIndex].Token) {
			b.Fatal("resident token Ack returned false")
		}
		if !queue.Enqueue(index) {
			b.Fatal("resident Enqueue returned false")
		}
		lease, ok := queue.LeaseWithToken(now)
		if !ok {
			b.Fatal("resident token Lease returned no item")
		}
		leases[leaseIndex] = lease
	}
}
