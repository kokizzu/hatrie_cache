package hatStorage

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"testing"
)

func ch019BenchmarkFixture(b *testing.B, verifyChecksums bool) (*RemotePartCache, RemotePartReference, []byte, RemotePartCacheLoader) {
	b.Helper()
	payload := make([]byte, 64*1024)
	for i := range payload {
		payload[i] = byte(i * 31)
	}
	digest := sha256.Sum256(payload)
	reference, err := NewRemotePartReference(
		"https://example.invalid/ch019-benchmark",
		"ch019-benchmark.meta",
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
	return cache, reference, payload, loader
}

func BenchmarkCH019RemotePartCacheColdMissNoVerification(b *testing.B) {
	cache, reference, payload, loader := ch019BenchmarkFixture(b, false)
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

func BenchmarkCH019RemotePartCacheColdMissWithVerification(b *testing.B) {
	cache, reference, payload, loader := ch019BenchmarkFixture(b, true)
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

func BenchmarkCH019RemotePartCacheHitWithVerification(b *testing.B) {
	cache, reference, payload, loader := ch019BenchmarkFixture(b, true)
	if _, err := cache.Get(context.Background(), reference, 0, loader); err != nil {
		b.Fatal(err)
	}
	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := cache.Get(context.Background(), reference, 0, loader); err != nil {
			b.Fatal(err)
		}
	}
}
