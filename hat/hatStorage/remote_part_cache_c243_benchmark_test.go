package hatStorage

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"testing"
)

func benchmarkC243Reference(b *testing.B, data []byte) RemotePartReference {
	b.Helper()
	digest := sha256.Sum256(data)
	reference, err := NewRemotePartReference(
		"s3://bucket/parts/benchmark",
		"parts/benchmark.json",
		"sha256:"+hex.EncodeToString(digest[:]),
		uint64(len(data)),
	)
	if err != nil {
		b.Fatal(err)
	}
	return reference
}

func BenchmarkC243RemotePartCacheHit(b *testing.B) {
	data := make([]byte, 64<<10)
	reference := benchmarkC243Reference(b, data)
	cache, err := NewRemotePartCache(RemotePartCacheOptions{MaxBytes: uint64(len(data))})
	if err != nil {
		b.Fatal(err)
	}
	loader := func(context.Context, RemotePartReference) ([]byte, error) {
		return data, nil
	}
	if _, err := cache.Get(context.Background(), reference, 0, loader); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.SetBytes(int64(len(data)))
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := cache.Get(context.Background(), reference, 0, loader); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkC243RemotePartCacheMiss(b *testing.B) {
	data := make([]byte, 64<<10)
	reference := benchmarkC243Reference(b, data)
	loader := func(context.Context, RemotePartReference) ([]byte, error) {
		return data, nil
	}
	b.ReportAllocs()
	b.SetBytes(int64(len(data)))
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		cache, err := NewRemotePartCache(RemotePartCacheOptions{MaxBytes: uint64(len(data))})
		if err != nil {
			b.Fatal(err)
		}
		if _, err := cache.Get(context.Background(), reference, 0, loader); err != nil {
			b.Fatal(err)
		}
	}
}
