package hatPipeline

import (
	"sync"
	"testing"
	"time"
)

var mz009TimestampDomainBenchmarkSink uint64

func BenchmarkMZ009DirectCounter(b *testing.B) {
	var timestamp uint64
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		timestamp++
	}
	mz009TimestampDomainBenchmarkSink = timestamp
}

func BenchmarkMZ009DirectMutexCounter(b *testing.B) {
	var mu sync.Mutex
	var timestamp uint64
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		mu.Lock()
		timestamp++
		mu.Unlock()
	}
	mz009TimestampDomainBenchmarkSink = timestamp
}

func BenchmarkMZ009LeaseNext(b *testing.B) {
	registry, err := NewTimestampDomainLeaseRegistry(TimestampDomainLeaseOptions{
		Clock: func() time.Time { return time.Unix(100, 0).UTC() },
	})
	if err != nil {
		b.Fatal(err)
	}
	lease, err := registry.Acquire("orders", "reader", time.Minute)
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		timestamp, err := registry.Next(lease)
		if err != nil {
			b.Fatal(err)
		}
		mz009TimestampDomainBenchmarkSink = timestamp
	}
}
