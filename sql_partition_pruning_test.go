package hatriecache

import (
	"context"
	"testing"
)

func TestSQLPartitionPruningUsesOnlyCacheKeyPartition(t *testing.T) {
	trie := newTestTrie(t)
	if err := trie.ConfigureLocalPartitions(2); err != nil {
		t.Fatalf("ConfigureLocalPartitions() error = %v", err)
	}
	if err := trie.UpsertStringChecked("people", `[{"id":7}]`); err != nil {
		t.Fatal(err)
	}
	plan, err := trie.SQLPartitionPruningPlan("CACHE", "people")
	if err != nil || !plan.Pruned || plan.Partition < 0 || plan.Partitions != 2 {
		t.Fatalf("SQLPartitionPruningPlan() = %#v, %v", plan, err)
	}
	result, err := ExecuteSQLQueryParameters(context.Background(), "FROM CACHE('people') AS person SELECT person.id", trie, nil, SQLQueryOptions{})
	if err != nil || len(result.Rows) != 1 || result.Rows[0]["id"] != float64(7) {
		t.Fatalf("partitioned query = %#v, %v", result, err)
	}
}
