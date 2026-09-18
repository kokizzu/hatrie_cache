package hatSql

import (
	"context"
	"testing"
)

func BenchmarkTR037RowLockDefaultAcquireRelease(b *testing.B) {
	manager := NewSQLRowLockManager(SQLRowLockManagerOptions{})
	ctx := context.Background()
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		lease, err := manager.Acquire(ctx, "row")
		if err != nil {
			b.Fatal(err)
		}
		lease.Release()
	}
}

func BenchmarkTR037OwnedDeadlockDetectionAcquireRelease(b *testing.B) {
	manager := NewSQLRowLockManager(SQLRowLockManagerOptions{EnableDeadlockDetection: true})
	ctx := context.Background()
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		lease, err := manager.AcquireOwned(ctx, "transaction", "row")
		if err != nil {
			b.Fatal(err)
		}
		lease.Release()
	}
}

func BenchmarkTR037OwnedDeadlockDetectionHeldKey(b *testing.B) {
	manager := NewSQLRowLockManager(SQLRowLockManagerOptions{EnableDeadlockDetection: true})
	ctx := context.Background()
	holder, err := manager.AcquireOwned(ctx, "holder", "row")
	if err != nil {
		b.Fatal(err)
	}
	defer holder.Release()
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		if _, err := manager.TryAcquireOwned("transaction", "row"); err != nil {
			b.Fatal(err)
		}
	}
}
