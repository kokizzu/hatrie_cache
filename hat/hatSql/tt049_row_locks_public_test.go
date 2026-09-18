package hatSql_test

import (
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestTT049RowLocksAreImportable(t *testing.T) {
	manager := hatSql.NewSQLRowLockManager(hatSql.SQLRowLockManagerOptions{MaxKeys: 2})
	lease, err := manager.TryAcquire("row:1")
	if err != nil || lease == nil {
		t.Fatalf("TryAcquire() = %v, %v", lease, err)
	}
	if !lease.Release() {
		t.Fatal("Release() = false")
	}
}
