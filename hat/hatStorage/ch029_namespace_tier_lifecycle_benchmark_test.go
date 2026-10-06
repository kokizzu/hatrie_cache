package hatStorage_test

import (
	"testing"
	"time"

	"hatrie_cache/hat/hatStorage"
)

func BenchmarkCHU29NamespaceTierPlan(b *testing.B) {
	policy := newCHU29TestPolicy(b)
	registry, err := hatStorage.NewStorageTierNamespaceRegistry(
		hatStorage.StorageTierNamespacePolicy{Namespace: "eu", Policy: policy},
	)
	if err != nil {
		b.Fatal(err)
	}
	now := time.Date(2026, time.January, 2, 0, 0, 0, 0, time.UTC)
	parts := make([]hatStorage.StorageTierLifecyclePart, 64)
	for index := range parts {
		parts[index] = hatStorage.StorageTierLifecyclePart{
			Key:           "part-" + time.Duration(index).String(),
			CurrentTier:   "hot",
			LifecycleTime: now.Add(-time.Duration(index%48) * time.Hour),
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		moves, err := registry.Plan("eu", now, parts)
		if err != nil {
			b.Fatal(err)
		}
		b.ReportMetric(float64(len(moves)), "moves/op")
	}
}

func BenchmarkCHU29DirectTierPlan(b *testing.B) {
	policy := newCHU29TestPolicy(b)
	parts := make([]hatStorage.StorageTierPart, 64)
	for index := range parts {
		parts[index] = hatStorage.StorageTierPart{
			Key:         "part-" + time.Duration(index).String(),
			CurrentTier: "hot",
			Age:         time.Duration(index%48) * time.Hour,
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		moves, err := policy.PlanStorageTierMoves(parts)
		if err != nil {
			b.Fatal(err)
		}
		b.ReportMetric(float64(len(moves)), "moves/op")
	}
}
