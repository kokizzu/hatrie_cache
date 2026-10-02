package hatStorage

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"testing"
)

func ch020BenchmarkFixture(b *testing.B, verifyChecksums bool) (*RemotePartCache, RemotePartReference, []byte, RemotePartCacheLoader, RemotePartCacheOwnedLoader) {
	b.Helper()
	payload := make([]byte, 64*1024)
	for i := range payload {
		payload[i] = byte(i * 29)
	}
	digest := sha256.Sum256(payload)
	reference, err := NewRemotePartReference(
		"https://example.invalid/ch020-benchmark",
		"ch020-benchmark.meta",
		"sha256:"+hex.EncodeToString(digest[:]),
		uint64(len(payload)),
	)
	if err != nil {
		b.Fatal(err)
	}
	cache, err := NewRemotePartCache(RemotePartCacheOptions{
		MaxBytes:        uint64(len(payload) * 2),
		MaxEntries:      2,
		VerifyChecksums: verifyChecksums,
	})
	if err != nil {
		b.Fatal(err)
	}
	loader := func(context.Context, RemotePartReference) ([]byte, error) {
		return payload, nil
	}
	return cache, reference, payload, loader, RemotePartCacheOwnedLoader(loader)
}

func BenchmarkCH020RemotePartCacheCopyColdMissAfter(b *testing.B) {
	cache, reference, payload, loader, _ := ch020BenchmarkFixture(b, false)
	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.Invalidate(reference)
		if _, err := cache.Get(context.Background(), reference, 0, loader); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCH020RemotePartCacheOwnedColdMiss(b *testing.B) {
	cache, reference, payload, _, loader := ch020BenchmarkFixture(b, false)
	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.Invalidate(reference)
		if _, err := cache.GetOwned(context.Background(), reference, 0, loader); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCH020RemotePartCacheOwnedAcquireColdMissNoVerification(b *testing.B) {
	cache, reference, payload, _, loader := ch020BenchmarkFixture(b, false)
	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.Invalidate(reference)
		handle, err := cache.AcquireOwned(context.Background(), reference, 0, loader)
		if err != nil {
			b.Fatal(err)
		}
		handle.Release()
	}
}

func BenchmarkCH020RemotePartCacheOwnedAcquireColdMissWithVerification(b *testing.B) {
	cache, reference, payload, _, loader := ch020BenchmarkFixture(b, true)
	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.Invalidate(reference)
		handle, err := cache.AcquireOwned(context.Background(), reference, 0, loader)
		if err != nil {
			b.Fatal(err)
		}
		handle.Release()
	}
}
