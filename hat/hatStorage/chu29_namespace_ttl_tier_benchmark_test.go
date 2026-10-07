package hatStorage_test

import (
	"context"
	"testing"
	"time"

	"hatrie_cache/hat/hatSql"
	"hatrie_cache/hat/hatStorage"
)

var chu29NamespaceTierMoveBenchmarkSink int

func newCHU29NamespaceControllerForBenchmark(b testing.TB) (*hatStorage.NamespaceLifecycleController, hatStorage.StorageTierPolicy, []hatStorage.StorageTierPart) {
	b.Helper()
	registry, err := hatStorage.NewSQLAdapterRegistry(nil, hatStorage.SQLNamespaceAdapter{
		NamespaceName: "bench",
		Store:         testEngine{},
		Resolver:      hatSql.SourceResolverFunc(func(string, string) ([]hatSql.Row, error) { return nil, nil }),
	})
	if err != nil {
		b.Fatal(err)
	}
	policy := newCHU29TierPolicyForBenchmark(b)
	controller, err := hatStorage.NewNamespaceLifecycleController(registry, map[string]hatStorage.NamespaceLifecyclePolicy{
		"bench": {TierPolicy: &policy},
	})
	if err != nil {
		b.Fatal(err)
	}
	parts := make([]hatStorage.StorageTierPart, 256)
	for index := range parts {
		current := "hot"
		if index%2 == 0 {
			current = "warm"
		}
		parts[index] = hatStorage.StorageTierPart{
			Key:         "part-" + string(rune(index)),
			CurrentTier: current,
			Age:         time.Duration(index%3) * time.Hour,
		}
	}
	return controller, policy, parts
}

func newCHU29TierPolicyForBenchmark(b testing.TB) hatStorage.StorageTierPolicy {
	b.Helper()
	rules := make([]hatStorage.StorageTierRule, 0, 2)
	for _, rule := range []struct {
		name string
		path string
		age  time.Duration
	}{
		{name: "hot", path: "/data/hot", age: 0},
		{name: "warm", path: "/data/warm", age: time.Hour},
	} {
		placement, err := hatStorage.NewDiskPlacementPolicy(rule.name, []hatStorage.DiskPlacementRule{{Path: rule.path, Weight: 1}})
		if err != nil {
			b.Fatal(err)
		}
		rules = append(rules, hatStorage.StorageTierRule{Name: rule.name, MinAge: rule.age, Placement: placement})
	}
	policy, err := hatStorage.NewStorageTierPolicy(rules)
	if err != nil {
		b.Fatal(err)
	}
	return policy
}

func BenchmarkCHU29NamespaceTierMovePlanning(b *testing.B) {
	controller, policy, parts := newCHU29NamespaceControllerForBenchmark(b)
	b.Run("direct", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for index := 0; index < b.N; index++ {
			moves, err := policy.PlanStorageTierMoves(parts)
			if err != nil {
				b.Fatal(err)
			}
			chu29NamespaceTierMoveBenchmarkSink = len(moves)
		}
	})
	b.Run("namespace_lifecycle", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for index := 0; index < b.N; index++ {
			moves, err := controller.PlanNamespaceStorageTierMoves(context.Background(), "bench", parts)
			if err != nil {
				b.Fatal(err)
			}
			chu29NamespaceTierMoveBenchmarkSink = len(moves)
		}
	})
}
