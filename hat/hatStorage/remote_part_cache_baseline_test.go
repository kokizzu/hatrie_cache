package hatStorage_test

import (
	"context"
	"testing"

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
