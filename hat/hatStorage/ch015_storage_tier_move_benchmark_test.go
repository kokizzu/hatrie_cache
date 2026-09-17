package hatStorage_test

import (
	"testing"
	"time"

	"hatrie_cache/hat/hatStorage"
)

func newCH015StorageTierMoveBenchmarkInput(b testing.TB) (hatStorage.StorageTierPolicy, []hatStorage.StorageTierPart) {
	b.Helper()
	policy := newCH015StorageTierMovePolicyForBenchmark(b)
	parts := make([]hatStorage.StorageTierPart, 256)
	for index := range parts {
		current := "hot"
		if index%3 == 1 {
			current = "warm"
		} else if index%3 == 2 {
			current = "cold"
		}
		parts[index] = hatStorage.StorageTierPart{
			Key:         "part-" + benchmarkIndexString(index),
			CurrentTier: current,
			Age:         time.Duration(index%48) * time.Hour,
		}
	}
	return policy, parts
}

func newCH015StorageTierMovePolicyForBenchmark(b testing.TB) hatStorage.StorageTierPolicy {
	b.Helper()
	rules := []struct {
		name string
		path string
	}{
		{name: "hot", path: "/data/hot"},
		{name: "warm", path: "/data/warm"},
		{name: "cold", path: "/data/cold"},
	}
	tierRules := make([]hatStorage.StorageTierRule, 0, len(rules))
	for index, rule := range rules {
		placement, err := hatStorage.NewDiskPlacementPolicy(rule.name, []hatStorage.DiskPlacementRule{{Path: rule.path, Weight: 1}})
		if err != nil {
			b.Fatal(err)
		}
		tierRules = append(tierRules, hatStorage.StorageTierRule{
			Name:      rule.name,
			MinAge:    time.Duration(index) * time.Hour,
			Placement: placement,
		})
	}
	policy, err := hatStorage.NewStorageTierPolicy(tierRules)
	if err != nil {
		b.Fatal(err)
	}
	return policy
}

func benchmarkIndexString(index int) string {
	digits := [3]byte{'0', '0', '0'}
	for position := len(digits) - 1; position >= 0; position-- {
		digits[position] = byte('0' + index%10)
		index /= 10
	}
	return string(digits[:])
}

func BenchmarkCH015StorageTierSelectBaseline(b *testing.B) {
	policy, parts := newCH015StorageTierMoveBenchmarkInput(b)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		for _, part := range parts {
			if _, err := policy.Select(part.Age, part.Key); err != nil {
				b.Fatal(err)
			}
		}
	}
}

func BenchmarkCH015StorageTierMovePlan(b *testing.B) {
	policy, parts := newCH015StorageTierMoveBenchmarkInput(b)
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := policy.PlanStorageTierMoves(parts); err != nil {
			b.Fatal(err)
		}
	}
}
