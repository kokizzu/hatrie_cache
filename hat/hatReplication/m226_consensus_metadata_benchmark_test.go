package hatReplication

import (
	"encoding/json"
	"testing"
)

func BenchmarkShardConsensusMetadataCommit(b *testing.B) {
	store, err := NewShardConsensusMetadataStore("region-a")
	if err != nil {
		b.Fatal(err)
	}
	metadata := ShardConsensusMetadata{
		Shard:                   "region-a",
		Owner:                   "node-a",
		FencingToken:            1,
		Term:                    1,
		VotedFor:                "node-a",
		CommitIndex:             1,
		AppliedIndex:            1,
		Frontier:                1,
		ConfigurationGeneration: 1,
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		sequence := uint64(index + 1)
		metadata.FencingToken = sequence
		metadata.Term = sequence
		metadata.CommitIndex = sequence
		metadata.AppliedIndex = sequence
		metadata.Frontier = sequence
		metadata.ConfigurationGeneration = sequence
		if _, err := store.Commit(metadata); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkShardConsensusMetadataMarshalBinary(b *testing.B) {
	metadata := benchmarkShardConsensusMetadata()
	b.ReportAllocs()
	b.SetBytes(int64(len(mustMarshalShardConsensusBinary(b, metadata))))
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := metadata.MarshalBinary(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkShardConsensusMetadataMarshalJSONControl(b *testing.B) {
	metadata := benchmarkShardConsensusMetadata()
	encoded := mustMarshalShardConsensusJSON(b, metadata)
	b.ReportAllocs()
	b.SetBytes(int64(len(encoded)))
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := json.Marshal(metadata); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkShardConsensusMetadataUnmarshalBinary(b *testing.B) {
	metadata := benchmarkShardConsensusMetadata()
	encoded := mustMarshalShardConsensusBinary(b, metadata)
	b.ReportAllocs()
	b.SetBytes(int64(len(encoded)))
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := UnmarshalShardConsensusMetadata(encoded); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkShardConsensusMetadataUnmarshalJSONControl(b *testing.B) {
	metadata := benchmarkShardConsensusMetadata()
	encoded := mustMarshalShardConsensusJSON(b, metadata)
	b.ReportAllocs()
	b.SetBytes(int64(len(encoded)))
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		var decoded ShardConsensusMetadata
		if err := json.Unmarshal(encoded, &decoded); err != nil {
			b.Fatal(err)
		}
	}
}

func TestShardConsensusMetadataBinaryIsSmallerThanJSON(t *testing.T) {
	metadata := benchmarkShardConsensusMetadata()
	binaryMetadata := mustMarshalShardConsensusBinary(t, metadata)
	jsonMetadata := mustMarshalShardConsensusJSON(t, metadata)
	if len(binaryMetadata) >= len(jsonMetadata) {
		t.Fatalf("binary metadata should be smaller: binary=%d json=%d", len(binaryMetadata), len(jsonMetadata))
	}
	t.Logf("metadata bytes: binary=%d json=%d ratio=%.2fx", len(binaryMetadata), len(jsonMetadata), float64(len(jsonMetadata))/float64(len(binaryMetadata)))
}

func benchmarkShardConsensusMetadata() ShardConsensusMetadata {
	return ShardConsensusMetadata{
		Shard:                   "region-a",
		Owner:                   "node-a",
		FencingToken:            7,
		Term:                    42,
		VotedFor:                "node-a",
		CommitIndex:             100,
		AppliedIndex:            95,
		Frontier:                90,
		ConfigurationGeneration: 3,
	}
}

func mustMarshalShardConsensusBinary(tb testing.TB, metadata ShardConsensusMetadata) []byte {
	tb.Helper()
	encoded, err := metadata.MarshalBinary()
	if err != nil {
		tb.Fatal(err)
	}
	return encoded
}

func mustMarshalShardConsensusJSON(tb testing.TB, metadata ShardConsensusMetadata) []byte {
	tb.Helper()
	encoded, err := json.Marshal(metadata)
	if err != nil {
		tb.Fatal(err)
	}
	return encoded
}
