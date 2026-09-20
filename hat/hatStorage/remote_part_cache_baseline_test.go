package hatStorage_test

import (
	"context"
	"testing"
	"time"

	"hatrie_cache/hat/hatStorage"
)

func remotePartCacheBenchmarkInput(b testing.TB) (hatStorage.RemotePartReference, []byte) {
	b.Helper()
	reference, err := hatStorage.NewRemotePartReference("s3://bucket/parts/hot", "parts/hot.json", "sha256:hot", 64<<10)
	if err != nil {
		b.Fatal(err)
	}
	payload := make([]byte, 64<<10)
	for index := range payload {
		payload[index] = byte(index)
	}
	return reference, payload
}

func BenchmarkRemotePartCacheBaseline(b *testing.B) {
	_, payload := remotePartCacheBenchmarkInput(b)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		loaded := append([]byte(nil), payload...)
		if len(loaded) != len(payload) {
			b.Fatal("direct loader returned an unexpected size")
		}
	}
}

var remotePartCacheBenchmarkSink byte

func BenchmarkRemotePartCache(b *testing.B) {
	reference, payload := remotePartCacheBenchmarkInput(b)
	cache, err := hatStorage.NewRemotePartCache(hatStorage.RemotePartCacheOptions{MaxBytes: uint64(len(payload))})
	if err != nil {
		b.Fatal(err)
	}
	loader := func(_ context.Context, _ hatStorage.RemotePartReference) ([]byte, error) {
		return append([]byte(nil), payload...), nil
	}
	if _, err := cache.Get(context.Background(), reference, 1, loader); err != nil {
		b.Fatal(err)
	}
	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	b.Run("cached-get", func(b *testing.B) {
		for range b.N {
			loaded, err := cache.Get(context.Background(), reference, 1, loader)
			if err != nil {
				b.Fatal(err)
			}
			remotePartCacheBenchmarkSink ^= loaded[0]
		}
	})
	b.Run("pinned-acquire-release", func(b *testing.B) {
		for range b.N {
			lease, err := cache.Acquire(context.Background(), reference, 1, loader)
			if err != nil {
				b.Fatal(err)
			}
			remotePartCacheBenchmarkSink ^= lease.Bytes()[0]
			lease.Release()
		}
	})
}

func remotePartCachePrefetchBenchmarkInput(b testing.TB) ([]hatStorage.RemotePartReference, []byte) {
	b.Helper()
	payload := make([]byte, 4096)
	for index := range payload {
		payload[index] = byte(index)
	}
	references := make([]hatStorage.RemotePartReference, 16)
	for index := range references {
		name := string(rune('a' + index))
		reference, err := hatStorage.NewRemotePartReference(
			"s3://bucket/parts/prefetch-"+name,
			"parts/prefetch-"+name+".json",
			"sha256:prefetch-"+name,
			uint64(len(payload)),
		)
		if err != nil {
			b.Fatal(err)
		}
		references[index] = reference
	}
	return references, payload
}

func benchmarkRemotePartCachePrefetch(b *testing.B, concurrency int, latency time.Duration) {
	references, payload := remotePartCachePrefetchBenchmarkInput(b)
	loader := func(_ context.Context, _ hatStorage.RemotePartReference) ([]byte, error) {
		if latency > 0 {
			time.Sleep(latency)
		}
		return append([]byte(nil), payload...), nil
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		b.StopTimer()
		cache, err := hatStorage.NewRemotePartCache(hatStorage.RemotePartCacheOptions{
			MaxBytes:   uint64(len(payload) * len(references)),
			MaxEntries: len(references),
		})
		if err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
		if concurrency == 1 {
			for _, reference := range references {
				if _, err := cache.Get(context.Background(), reference, 1, loader); err != nil {
					b.Fatal(err)
				}
			}
		} else if err := cache.Prefetch(context.Background(), references, hatStorage.RemotePartPrefetchOptions{
			MaxConcurrent: concurrency,
			Priority:      1,
		}, loader); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
}

func BenchmarkRemotePartCachePrefetch(b *testing.B) {
	b.Run("zero-latency/sequential", func(b *testing.B) {
		benchmarkRemotePartCachePrefetch(b, 1, 0)
	})
	b.Run("zero-latency/bounded-2", func(b *testing.B) {
		benchmarkRemotePartCachePrefetch(b, 2, 0)
	})
	b.Run("remote-latency/sequential", func(b *testing.B) {
		benchmarkRemotePartCachePrefetch(b, 1, 100*time.Microsecond)
	})
	b.Run("remote-latency/bounded-2", func(b *testing.B) {
		benchmarkRemotePartCachePrefetch(b, 2, 100*time.Microsecond)
	})
}

func BenchmarkRemotePartCacheColumnAware(b *testing.B) {
	part, err := hatStorage.NewRemotePartReference("s3://bucket/parts/column-benchmark", "parts/column-benchmark.json", "sha256:part", 64<<10)
	if err != nil {
		b.Fatal(err)
	}
	column, err := hatStorage.NewRemotePartColumnReference(part, "payload", "sha256:payload", 12<<10, 4<<10)
	if err != nil {
		b.Fatal(err)
	}
	wholePayload := make([]byte, 64<<10)
	columnPayload := make([]byte, 4<<10)

	b.Run("whole-part", func(b *testing.B) {
		b.ReportAllocs()
		var remoteBytes int64
		for range b.N {
			cache, err := hatStorage.NewRemotePartCache(hatStorage.RemotePartCacheOptions{MaxBytes: uint64(len(wholePayload)), MaxEntries: 1})
			if err != nil {
				b.Fatal(err)
			}
			if _, err := cache.Get(context.Background(), part, 1, func(context.Context, hatStorage.RemotePartReference) ([]byte, error) {
				remoteBytes += int64(len(wholePayload))
				return wholePayload, nil
			}); err != nil {
				b.Fatal(err)
			}
		}
		b.ReportMetric(float64(remoteBytes)/float64(b.N), "remote-B/op")
		b.ReportMetric(float64(len(wholePayload)), "retained-B/op")
	})

	b.Run("projected-column", func(b *testing.B) {
		b.ReportAllocs()
		var remoteBytes int64
		for range b.N {
			cache, err := hatStorage.NewRemotePartCache(hatStorage.RemotePartCacheOptions{MaxBytes: uint64(len(columnPayload)), MaxEntries: 1})
			if err != nil {
				b.Fatal(err)
			}
			if _, err := cache.GetColumn(context.Background(), column, 1, func(context.Context, hatStorage.RemotePartColumnReference) ([]byte, error) {
				remoteBytes += int64(len(columnPayload))
				return columnPayload, nil
			}); err != nil {
				b.Fatal(err)
			}
		}
		b.ReportMetric(float64(remoteBytes)/float64(b.N), "remote-B/op")
		b.ReportMetric(float64(len(columnPayload)), "retained-B/op")
	})
}
