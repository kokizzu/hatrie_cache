package hatStorage

import (
	"context"
	"crypto/sha256"
	"os"
	"path/filepath"
	"testing"
)

var ch021BaselineSink []byte

func BenchmarkCH021BaselineLocalRead(b *testing.B) {
	path := filepath.Join(b.TempDir(), "part.bin")
	data := make([]byte, 64<<10)
	for index := range data {
		data[index] = byte(index)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		b.Fatal(err)
	}
	b.SetBytes(int64(len(data)))
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		read, err := os.ReadFile(path)
		if err != nil {
			b.Fatal(err)
		}
		ch021BaselineSink = read
	}
}

func BenchmarkCH021BaselineRemoteCacheHit(b *testing.B) {
	data := make([]byte, 64<<10)
	for index := range data {
		data[index] = byte(index)
	}
	digest := sha256.Sum256(data)
	reference, err := NewRemotePartReference(
		"https://objects.example.test/parts/ch021.bin",
		"parts/ch021.bin",
		stringDigestCH021(digest[:]),
		uint64(len(data)),
	)
	if err != nil {
		b.Fatal(err)
	}
	cache, err := NewRemotePartCache(RemotePartCacheOptions{MaxBytes: uint64(len(data) * 2), MaxEntries: 1})
	if err != nil {
		b.Fatal(err)
	}
	loader := func(context.Context, RemotePartReference) ([]byte, error) {
		return append([]byte(nil), data...), nil
	}
	if _, err := cache.Get(context.Background(), reference, 0, loader); err != nil {
		b.Fatal(err)
	}
	b.SetBytes(int64(len(data)))
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		read, err := cache.Get(context.Background(), reference, 0, loader)
		if err != nil {
			b.Fatal(err)
		}
		ch021BaselineSink = read
	}
}

func stringDigestCH021(data []byte) string {
	const hexDigits = "0123456789abcdef"
	out := make([]byte, len(data)*2)
	for index, value := range data {
		out[index*2] = hexDigits[value>>4]
		out[index*2+1] = hexDigits[value&0x0f]
	}
	return string(out)
}

func BenchmarkCH021TieredLocalRead(b *testing.B) {
	root := b.TempDir()
	path := filepath.Join(root, "parts", "part.bin")
	data := make([]byte, 64<<10)
	for index := range data {
		data[index] = byte(index)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		b.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		b.Fatal(err)
	}
	reader, err := NewStorageTierReader(StorageTierReaderOptions{LocalRoot: root})
	if err != nil {
		b.Fatal(err)
	}
	part := StorageTierReadPart{Key: "part-1", Tier: StorageTierLocal, LocalPath: "parts/part.bin"}
	b.SetBytes(int64(len(data)))
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		read, readErr := reader.Read(context.Background(), part)
		if readErr != nil {
			b.Fatal(readErr)
		}
		ch021BaselineSink = read
	}
}

func BenchmarkCH021TieredObjectCacheHit(b *testing.B) {
	data := make([]byte, 64<<10)
	for index := range data {
		data[index] = byte(index)
	}
	digest := sha256.Sum256(data)
	reference, err := NewRemotePartReference(
		"https://objects.example.test/parts/ch021-tiered.bin",
		"parts/ch021-tiered.bin",
		stringDigestCH021(digest[:]),
		uint64(len(data)),
	)
	if err != nil {
		b.Fatal(err)
	}
	cache, err := NewRemotePartCache(RemotePartCacheOptions{MaxBytes: uint64(len(data) * 2), MaxEntries: 1})
	if err != nil {
		b.Fatal(err)
	}
	loader := func(context.Context, RemotePartReference) ([]byte, error) {
		return append([]byte(nil), data...), nil
	}
	reader, err := NewStorageTierReader(StorageTierReaderOptions{RemoteCache: cache, RemoteLoader: loader})
	if err != nil {
		b.Fatal(err)
	}
	part := StorageTierReadPart{Key: "part-1", Tier: StorageTierObject, Remote: reference}
	if _, err := reader.Read(context.Background(), part); err != nil {
		b.Fatal(err)
	}
	b.SetBytes(int64(len(data)))
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		read, readErr := reader.Read(context.Background(), part)
		if readErr != nil {
			b.Fatal(readErr)
		}
		ch021BaselineSink = read
	}
}
