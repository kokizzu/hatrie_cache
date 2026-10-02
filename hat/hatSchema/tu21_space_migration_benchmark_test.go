package hatSchema

import (
	"context"
	"testing"
)

func benchmarkTU21Manager(b *testing.B) *SpaceMigrationManager {
	b.Helper()
	manager, err := NewSpaceMigrationManager(SpaceMigrationManagerOptions{MaxPlans: 4, MaxSteps: 4})
	if err != nil {
		b.Fatal(err)
	}
	_, err = manager.Prepare(SpaceMigrationPlan{
		ID:                 "orders-v3",
		Space:              "orders",
		FromVersion:        1,
		CompatibleVersions: []uint64{1, 2},
		Steps: []SpaceMigrationStep{
			{ID: "add-status", TargetVersion: 2},
			{ID: "add-region", TargetVersion: 3},
		},
	})
	if err != nil {
		b.Fatal(err)
	}
	return manager
}

func BenchmarkTU21SpaceMigrationStatus(b *testing.B) {
	manager := benchmarkTU21Manager(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := manager.Status("orders-v3"); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTU21SpaceMigrationAllowsVersion(b *testing.B) {
	manager := benchmarkTU21Manager(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		allowed, err := manager.AllowsVersion("orders-v3", 2)
		if err != nil || !allowed {
			b.Fatalf("allowed=%v err=%v", allowed, err)
		}
	}
}

func BenchmarkTU21SpaceMigrationSnapshot(b *testing.B) {
	manager := benchmarkTU21Manager(b)
	snapshot, err := manager.MarshalSnapshot()
	if err != nil {
		b.Fatal(err)
	}
	b.SetBytes(int64(len(snapshot)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := manager.MarshalSnapshot(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTU21SpaceMigrationRun(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		manager := benchmarkTU21Manager(b)
		if err := manager.Run(context.Background(), "orders-v3", SpaceMigrationCallbacks{
			Apply: func(context.Context, SpaceMigrationStep) error { return nil },
		}); err != nil {
			b.Fatal(err)
		}
	}
}
