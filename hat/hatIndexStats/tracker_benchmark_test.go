package hatIndexStats

import (
	"runtime"
	"sync/atomic"
	"testing"
)

func BenchmarkDirectIndexCounter(b *testing.B) {
	var operations uint64
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		atomic.AddUint64(&operations, 1)
	}
	b.StopTimer()
	runtime.KeepAlive(operations)
}

func BenchmarkTrackerObserveSampled(b *testing.B) {
	tracker, err := New(Config{TopK: 8, SampleEvery: 16, MaxKeyBytes: 128})
	if err != nil {
		b.Fatal(err)
	}
	key := []byte("hot-key")
	tracker.Observe(key, 4, true)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tracker.Observe(key, 4, true)
	}
}

func BenchmarkTrackerObserveEveryKey(b *testing.B) {
	tracker, err := New(Config{TopK: 8, SampleEvery: 1, MaxKeyBytes: 128})
	if err != nil {
		b.Fatal(err)
	}
	key := []byte("hot-key")
	tracker.Observe(key, 4, true)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tracker.Observe(key, 4, true)
	}
}

func BenchmarkTrackerSnapshot(b *testing.B) {
	tracker, err := New(Config{TopK: 8, SampleEvery: 1, MaxKeyBytes: 128})
	if err != nil {
		b.Fatal(err)
	}
	for i := 0; i < 8; i++ {
		tracker.Observe([]byte{byte(i)}, uint64(i), true)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		snapshot := tracker.Snapshot()
		if len(snapshot.TopKeys) != 8 {
			b.Fatal("unexpected top-key count")
		}
	}
}
