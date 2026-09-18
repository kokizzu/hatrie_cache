package hatSql_test

import (
	"context"
	"fmt"

	"hatrie_cache/hat/hatSql"
)

func ExampleSQLRowLockManager_AcquireOwned_deadlockDetection() {
	manager := hatSql.NewSQLRowLockManager(hatSql.SQLRowLockManagerOptions{EnableDeadlockDetection: true})
	lease, err := manager.AcquireOwned(context.Background(), "transaction-1", "orders/42")
	if err != nil {
		panic(err)
	}
	defer lease.Release()
	if manager.Stats().WaitEdges != 0 {
		panic("unexpected wait edge")
	}
	fmt.Println(lease.Key())
	// Output: orders/42
}
