package hatMerkle

import (
	"bytes"
	"testing"
)

var ch020SharedPartBenchmarkSink []byte
var ch020SharedLeaseBenchmarkSink uint64

func BenchmarkCH020PayloadCloneBaseline(b *testing.B) {
	payload := bytes.Repeat([]byte("x"), 1<<20)
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		ch020SharedPartBenchmarkSink = bytes.Clone(payload)
	}
}

func BenchmarkCH020SharedLease(b *testing.B) {
	registry, err := NewSharedPartRegistry(SharedPartRegistryOptions{MaxEntries: 1, MaxLeases: 1})
	if err != nil {
		b.Fatal(err)
	}
	if _, err := registry.Register(SharedPartDescriptor{
		ShareID:       "share-1",
		SourceReplica: "node-a",
		Entry: PartCatalogEntry{
			Name:     "part-0001",
			Location: "shared://node-a/part-0001",
			Manifest: PartManifest{Checksum: ChecksumPart([]byte("payload"))},
		},
	}, func(SharedPartDescriptor) error { return nil }); err != nil {
		b.Fatal(err)
	}
	warmup, err := registry.Acquire("share-1")
	if err != nil {
		b.Fatal(err)
	}
	if err := registry.Release(warmup.ID); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		lease, err := registry.Acquire("share-1")
		if err != nil {
			b.Fatal(err)
		}
		ch020SharedLeaseBenchmarkSink = lease.ID
		if err := registry.Release(lease.ID); err != nil {
			b.Fatal(err)
		}
	}
}
