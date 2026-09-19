package hatJournal

import "testing"

var spaceSyncPolicyBenchmarkSink SpaceSyncPolicy

func BenchmarkSpaceSyncPolicyResolve(b *testing.B) {
	registry, err := NewSpaceSyncPolicyRegistry(SpaceSyncPolicyOptions{
		Capacity:      8,
		DefaultPolicy: SpaceSyncPolicyPeriodic,
	})
	if err != nil {
		b.Fatal(err)
	}
	if err := registry.Register("orders", SpaceSyncPolicyImmediate); err != nil {
		b.Fatal(err)
	}
	if err := registry.Register("users", SpaceSyncPolicyDisabled); err != nil {
		b.Fatal(err)
	}

	b.Run("registry", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			spaceSyncPolicyBenchmarkSink = registry.Resolve("orders")
		}
	})

	b.Run("direct-map-control", func(b *testing.B) {
		policies := map[string]SpaceSyncPolicy{
			"orders": SpaceSyncPolicyImmediate,
			"users":  SpaceSyncPolicyDisabled,
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			spaceSyncPolicyBenchmarkSink = policies["orders"]
		}
	})
}
