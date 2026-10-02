package hatStorage_test

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"hatrie_cache/hat/hatStorage"
)

var mu38BenchmarkBytes []byte
var mu38BenchmarkManifest hatStorage.ImmutablePartManifest

func BenchmarkMU38MarshalBinary(b *testing.B) {
	manifest := mu38BenchmarkInput(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		payload, err := manifest.MarshalBinary()
		if err != nil {
			b.Fatal(err)
		}
		mu38BenchmarkBytes = payload
	}
	b.StopTimer()
	b.ReportMetric(float64(len(mu38BenchmarkBytes)), "bytes/manifest")
}

func BenchmarkMU38MarshalJSONFallback(b *testing.B) {
	manifest := mu38BenchmarkInput(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		payload, err := json.Marshal(manifest)
		if err != nil {
			b.Fatal(err)
		}
		mu38BenchmarkBytes = payload
	}
	b.StopTimer()
	b.ReportMetric(float64(len(mu38BenchmarkBytes)), "bytes/manifest")
}

func BenchmarkMU38DecodeBinary(b *testing.B) {
	manifest := mu38BenchmarkInput(b)
	payload, err := manifest.MarshalBinary()
	if err != nil {
		b.Fatal(err)
	}
	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		decoded, err := hatStorage.DecodeImmutablePartManifest(payload)
		if err != nil {
			b.Fatal(err)
		}
		mu38BenchmarkManifest = decoded
	}
}

func BenchmarkMU38DecodeJSONFallback(b *testing.B) {
	manifest := mu38BenchmarkInput(b)
	payload, err := json.Marshal(manifest)
	if err != nil {
		b.Fatal(err)
	}
	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var decoded hatStorage.ImmutablePartManifest
		if err := json.Unmarshal(payload, &decoded); err != nil {
			b.Fatal(err)
		}
		mu38BenchmarkManifest = decoded
	}
}

func mu38BenchmarkInput(b *testing.B) hatStorage.ImmutablePartManifest {
	b.Helper()
	parts := make([]hatStorage.ImmutableDataPart, 512)
	for i := range parts {
		id := fmt.Sprintf("part-%03d", i)
		reference, err := hatStorage.NewRemotePartReference(
			"s3://bucket/parts/"+id,
			"parts/"+id+".json",
			"sha256:"+id,
			uint64(4096+i),
		)
		if err != nil {
			b.Fatal(err)
		}
		parts[i] = hatStorage.ImmutableDataPart{
			PartID:      id,
			PartitionID: fmt.Sprintf("region-%d", i%8),
			Generation:  3,
			RowCount:    uint64(1000 + i),
			LowerBound:  "key-" + id,
			UpperBound:  "key-" + id + "-end",
			Reference:   reference.Metadata(),
		}
	}
	manifest := hatStorage.ImmutablePartManifest{
		Version:    hatStorage.ImmutablePartManifestVersion,
		ManifestID: "manifest-benchmark",
		Generation: 3,
		CreatedAt:  time.Date(2026, 10, 2, 0, 0, 0, 0, time.UTC),
		Parts:      parts,
	}
	normalized, err := manifest.Normalize()
	if err != nil {
		b.Fatal(err)
	}
	return normalized
}
