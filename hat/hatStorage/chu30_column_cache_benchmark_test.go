package hatStorage_test

import (
	"context"
	"strconv"
	"testing"
	"time"

	"hatrie_cache/hat/hatStorage"
)

func BenchmarkCHU30RemotePartCacheColumnCachedGet(b *testing.B) {
	reference, err := hatStorage.NewRemotePartReference("s3://bucket/parts/column", "parts/column", "sha256:column", 1)
	if err != nil {
		b.Fatal(err)
	}
	cache, err := hatStorage.NewRemotePartCache(hatStorage.RemotePartCacheOptions{MaxBytes: 1})
	if err != nil {
		b.Fatal(err)
	}
	loader := func(context.Context, hatStorage.RemotePartReference, string) ([]byte, error) {
		return []byte("x"), nil
	}
	if _, err := cache.GetColumn(context.Background(), reference, "id", 1, loader); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for range b.N {
		if _, err := cache.GetColumn(context.Background(), reference, "id", 1, loader); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCHU30RemotePartCachePrefetchColumns(b *testing.B) {
	b.Run("zero-latency/bounded-2", func(b *testing.B) {
		benchmarkCHU30ColumnPrefetch(b, 0)
	})
	b.Run("remote-latency/bounded-2", func(b *testing.B) {
		benchmarkCHU30ColumnPrefetch(b, 100*time.Microsecond)
	})
}

func benchmarkCHU30ColumnPrefetch(b *testing.B, latency time.Duration) {
	b.Helper()
	requests := make([]hatStorage.RemotePartColumnRequest, 8)
	for index := range requests {
		name := strconv.Itoa(index)
		reference, err := hatStorage.NewRemotePartReference("s3://bucket/parts/"+name, "parts/"+name, "sha256:"+name, 1)
		if err != nil {
			b.Fatal(err)
		}
		requests[index] = hatStorage.RemotePartColumnRequest{Reference: reference, Column: "value"}
	}
	loader := func(_ context.Context, _ hatStorage.RemotePartReference, _ string) ([]byte, error) {
		if latency > 0 {
			time.Sleep(latency)
		}
		return []byte("x"), nil
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		b.StopTimer()
		cache, err := hatStorage.NewRemotePartCache(hatStorage.RemotePartCacheOptions{MaxBytes: uint64(len(requests)), MaxEntries: len(requests)})
		if err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
		if err := cache.PrefetchColumns(context.Background(), requests, hatStorage.RemotePartPrefetchOptions{MaxConcurrent: 2, Priority: 1}, loader); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
}
