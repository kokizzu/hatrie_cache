package hatSql

import (
	"context"
	"testing"
)

func BenchmarkM221ExistingNamedClusterDispatch(b *testing.B) {
	manager := NewSQLQueryManagerWithOptions(SQLQueryManagerOptions{
		ComputeClusters: map[string]SQLComputeClusterOptions{
			"analytics": {Workers: 1, QueueCapacity: 8},
		},
	})
	b.Cleanup(func() { _ = manager.Close() })
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		compute, err := manager.computeFor("analytics")
		if err != nil || compute == nil {
			b.Fatalf("computeFor() = %#v, %v", compute, err)
		}
	}
}

func BenchmarkM221BudgetedClusterMemoryStats(b *testing.B) {
	manager := NewSQLQueryManagerWithOptions(SQLQueryManagerOptions{
		ComputeClusters: map[string]SQLComputeClusterOptions{
			"analytics": {Workers: 1, MemoryBudgetBytes: 1 << 30},
		},
	})
	b.Cleanup(func() { _ = manager.Close() })
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := manager.ComputeClusterMemoryStats("analytics"); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkM221BudgetedClusterAcquireRelease(b *testing.B) {
	manager := NewSQLQueryManagerWithOptions(SQLQueryManagerOptions{
		ComputeClusters: map[string]SQLComputeClusterOptions{
			"analytics": {Workers: 1, MemoryBudgetBytes: 1 << 30},
		},
	})
	b.Cleanup(func() { _ = manager.Close() })
	compute, err := manager.computeFor("analytics")
	if err != nil || compute == nil || compute.memoryAdmission == nil {
		b.Fatalf("budgeted compute = %#v, %v", compute, err)
	}
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		release, err := compute.memoryAdmission.Acquire(context.Background(), 1024)
		if err != nil {
			b.Fatal(err)
		}
		release()
	}
}
