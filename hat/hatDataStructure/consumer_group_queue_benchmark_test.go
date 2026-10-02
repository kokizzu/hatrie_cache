package hatDataStructure

import (
	"testing"
	"time"
)

func BenchmarkConsumerGroupQueueT43LeaseAck(b *testing.B) {
	queue := NewConsumerGroupQueue[int](1, time.Minute)
	if !queue.Register("workers", "worker-1") {
		b.Fatal("Register returned false")
	}
	now := time.Unix(1000, 0).UTC()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if !queue.Enqueue("workers", index) {
			b.Fatal("Enqueue returned false")
		}
		lease, ok := queue.Lease("workers", "worker-1", now)
		if !ok || !queue.Ack(lease.Token) {
			b.Fatal("Lease/Ack failed")
		}
	}
}

func BenchmarkConsumerGroupQueueT43LeaseAckResident256(b *testing.B) {
	queue := NewConsumerGroupQueue[int](512, time.Minute)
	if !queue.Register("workers", "worker-1") {
		b.Fatal("Register returned false")
	}
	leases := make([]ConsumerGroupLease[int], 256)
	now := time.Unix(1000, 0).UTC()
	for index := range leases {
		if !queue.Enqueue("workers", index) {
			b.Fatalf("initial Enqueue(%d) returned false", index)
		}
		lease, ok := queue.Lease("workers", "worker-1", now)
		if !ok {
			b.Fatalf("initial Lease(%d) returned no item", index)
		}
		leases[index] = lease
	}
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		leaseIndex := index % len(leases)
		if !queue.Ack(leases[leaseIndex].Token) {
			b.Fatal("Ack returned false")
		}
		if !queue.Enqueue("workers", index) {
			b.Fatal("Enqueue returned false")
		}
		lease, ok := queue.Lease("workers", "worker-1", now)
		if !ok {
			b.Fatal("Lease returned no item")
		}
		leases[leaseIndex] = lease
	}
}
