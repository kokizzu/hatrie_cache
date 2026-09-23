package hatSql

import (
	"strconv"
	"testing"
)

var (
	m226BenchmarkMetadata SQLShardConsensusMetadata
	m226BenchmarkBytes    []byte
	m226BenchmarkSink     uint64
)

func m226BenchmarkRegistry(b *testing.B, maxShards int) *SQLShardConsensusMetadataRegistry {
	b.Helper()
	registry, err := NewSQLShardConsensusMetadataRegistry(SQLShardConsensusMetadataRegistryOptions{
		ShardCount:    16,
		MaxShards:     maxShards,
		MaxOwnerBytes: 64,
	})
	if err != nil {
		b.Fatal(err)
	}
	return registry
}

func BenchmarkM226DisabledControl(b *testing.B) {
	for index := 0; index < b.N; index++ {
		m226BenchmarkSink += uint64(index)
	}
}

func BenchmarkM226FrontierOnlyBaseline(b *testing.B) {
	tracker, err := NewSQLSourceFrontierTracker([]SQLSourceFrontierPartition{{Source: "events", Partition: "0"}})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := tracker.Observe(SQLSourceFrontier{Source: "events", Partition: "0", Frontier: uint64(index + 1)}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkM226AdvanceFrontier(b *testing.B) {
	registry := m226BenchmarkRegistry(b, 1)
	if _, err := registry.Claim("region-a/0", "worker-a", 1); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		metadata, err := registry.AdvanceFrontier("region-a/0", "worker-a", 1, uint64(index+1))
		if err != nil {
			b.Fatal(err)
		}
		m226BenchmarkMetadata = metadata
	}
}

func BenchmarkM226ClaimTakeover(b *testing.B) {
	registry := m226BenchmarkRegistry(b, 1)
	if _, err := registry.Claim("region-a/0", "worker-a", 1); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		metadata, err := registry.Claim("region-a/0", "worker-a", uint64(index+2))
		if err != nil {
			b.Fatal(err)
		}
		m226BenchmarkMetadata = metadata
	}
}

func BenchmarkM226Get(b *testing.B) {
	registry := m226BenchmarkRegistry(b, 1)
	if _, err := registry.Claim("region-a/0", "worker-a", 1); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		metadata, ok := registry.Get("region-a/0")
		if !ok {
			b.Fatal("Get() returned no metadata")
		}
		m226BenchmarkMetadata = metadata
	}
}

func BenchmarkM226MarshalBinary(b *testing.B) {
	registry := m226BenchmarkRegistry(b, 128)
	for index := 0; index < 128; index++ {
		if _, err := registry.Claim("region-a/"+strconv.Itoa(index), "worker-a", uint64(index+1)); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		encoded, err := registry.MarshalBinary()
		if err != nil {
			b.Fatal(err)
		}
		m226BenchmarkBytes = encoded
	}
}

func BenchmarkM226UnmarshalBinary(b *testing.B) {
	registry := m226BenchmarkRegistry(b, 128)
	for index := 0; index < 128; index++ {
		if _, err := registry.Claim("region-a/"+strconv.Itoa(index), "worker-a", uint64(index+1)); err != nil {
			b.Fatal(err)
		}
	}
	encoded, err := registry.MarshalBinary()
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		snapshot, err := UnmarshalSQLShardConsensusMetadataSnapshot(encoded)
		if err != nil {
			b.Fatal(err)
		}
		m226BenchmarkBytes = encoded
		m226BenchmarkSink = snapshot.Generation
	}
}
