package hatDataStructure

import (
	"testing"
	"time"
)

func BenchmarkConsumerGroupQueueT43Setup(b *testing.B) {
	for index := 0; index < b.N; index++ {
		queue := NewConsumerGroupQueue[int](256, time.Minute)
		if !queue.Register("workers", "worker-1") {
			b.Fatal("Register returned false")
		}
	}
}
