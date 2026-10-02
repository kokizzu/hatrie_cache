package hatStorage

import (
	"context"
	"testing"
	"time"
)

func newCH021TierReaderBenchmarkPolicy(b testing.TB) StorageTierPolicy {
	b.Helper()
	hot, err := NewDiskPlacementPolicy("hot", []DiskPlacementRule{{Path: "/data/hot", Weight: 1}})
	if err != nil {
		b.Fatal(err)
	}
	cold, err := NewDiskPlacementPolicy("cold", []DiskPlacementRule{{Path: "/data/cold", Weight: 1}})
	if err != nil {
		b.Fatal(err)
	}
	policy, err := NewStorageTierPolicy([]StorageTierRule{
		{Name: "hot", MinAge: 0, Placement: hot},
		{Name: "cold", MinAge: 24 * time.Hour, Placement: cold},
	})
	if err != nil {
		b.Fatal(err)
	}
	return policy
}

func BenchmarkCH021TieredStorageReaderCurrentHit(b *testing.B) {
	policy := newCH021TierReaderBenchmarkPolicy(b)
	payload := []byte("part-payload")
	reader, err := NewStorageTierReader(policy, func(context.Context, StorageTierSelection) ([]byte, error) {
		return payload, nil
	})
	if err != nil {
		b.Fatal(err)
	}
	part := StorageTierReadPart{Key: "part-1", CurrentTier: "hot", Age: time.Hour}
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		result, err := reader.Read(ctx, part)
		if err != nil || result.Fallback || result.Selection.Tier != "hot" || len(result.Data) != len(payload) {
			b.Fatalf("read = %#v, err = %v", result, err)
		}
	}
}

func BenchmarkCH021TieredStorageReaderFallback(b *testing.B) {
	policy := newCH021TierReaderBenchmarkPolicy(b)
	payload := []byte("part-payload")
	reader, err := NewStorageTierReader(policy, func(_ context.Context, selection StorageTierSelection) ([]byte, error) {
		if selection.Tier == "hot" {
			return nil, ErrStorageTierPartNotFound
		}
		return payload, nil
	})
	if err != nil {
		b.Fatal(err)
	}
	part := StorageTierReadPart{Key: "part-1", CurrentTier: "hot", Age: 48 * time.Hour}
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		result, err := reader.Read(ctx, part)
		if err != nil || !result.Fallback || result.Selection.Tier != "cold" || len(result.Data) != len(payload) {
			b.Fatalf("read = %#v, err = %v", result, err)
		}
	}
}
