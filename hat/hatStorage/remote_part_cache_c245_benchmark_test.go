package hatStorage

import (
	"context"
	"testing"
)

func BenchmarkC245RemotePartCacheAdmissionWarmHit(b *testing.B) {
	data := make([]byte, 64<<10)
	reference, err := NewRemotePartReference("s3://bucket/parts/c245", "parts/c245.json", "sha256:c245", uint64(len(data)))
	if err != nil {
		b.Fatal(err)
	}
	cache, err := NewRemotePartCache(RemotePartCacheOptions{MaxBytes: uint64(len(data)), MaxEntries: 1, MinAccesses: 2})
	if err != nil {
		b.Fatal(err)
	}
	loader := func(context.Context, RemotePartReference) ([]byte, error) {
		return data, nil
	}
	if _, err := cache.Get(context.Background(), reference, 0, loader); err != nil {
		b.Fatal(err)
	}
	if _, err := cache.Get(context.Background(), reference, 0, loader); err != nil {
		b.Fatal(err)
	}
	b.SetBytes(int64(len(data)))
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		value, err := cache.Get(context.Background(), reference, 0, loader)
		if err != nil {
			b.Fatal(err)
		}
		if len(value) == 0 {
			b.Fatal("empty cached part")
		}
	}
}

func BenchmarkC245RemotePartCacheCold(b *testing.B) {
	data := make([]byte, 64<<10)
	reference, err := NewRemotePartReference("s3://bucket/parts/c245-cold", "parts/c245-cold.json", "sha256:c245-cold", uint64(len(data)))
	if err != nil {
		b.Fatal(err)
	}
	loader := func(context.Context, RemotePartReference) ([]byte, error) {
		return data, nil
	}
	for _, admission := range []uint64{0, 2} {
		b.Run(map[uint64]string{0: "eager", 2: "frequency"}[admission], func(b *testing.B) {
			cache, err := NewRemotePartCache(RemotePartCacheOptions{MaxBytes: 1, MaxEntries: 1, MinAccesses: admission})
			if err != nil {
				b.Fatal(err)
			}
			b.SetBytes(int64(len(data)))
			b.ReportAllocs()
			b.ResetTimer()
			for index := 0; index < b.N; index++ {
				value, getErr := cache.Get(context.Background(), reference, 0, loader)
				if getErr != nil {
					b.Fatal(getErr)
				}
				if len(value) == 0 {
					b.Fatal("empty uncached part")
				}
			}
		})
	}
}
