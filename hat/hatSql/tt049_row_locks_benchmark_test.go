package hatSql

import (
	"sync"
	"testing"
)

var tt049RowLockLeaseSink *SQLRowLockLease

func BenchmarkTT049RowLockAcquireRelease(b *testing.B) {
	manager := NewSQLRowLockManager(SQLRowLockManagerOptions{ShardCount: 16, MaxKeys: 1024})
	b.ReportAllocs()
	for b.Loop() {
		lease, err := manager.TryAcquire("row:1")
		if err != nil || lease == nil {
			b.Fatalf("TryAcquire() = %v, %v", lease, err)
		}
		tt049RowLockLeaseSink = lease
		if !lease.Release() {
			b.Fatal("Release() = false")
		}
	}
}

func BenchmarkTT049SyncMutexAcquireRelease(b *testing.B) {
	var mutex sync.Mutex
	b.ReportAllocs()
	for b.Loop() {
		mutex.Lock()
		mutex.Unlock()
	}
}
