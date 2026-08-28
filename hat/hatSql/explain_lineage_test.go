package hatSql

import (
	"context"
	"testing"
)

func TestExplainProjectIncludesColumnLineage(t *testing.T) {
	result, err := ExecuteSQLQueryParameters(context.Background(), "EXPLAIN SELECT id AS user_id, total + tax AS grand_total FROM CACHE('orders')", SQLSourceResolverFunc(func(string, string) ([]SQLRow, error) { return nil, nil }), nil, SQLQueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for _, step := range result.Plan {
		if step.Node != "PROJECT" {
			continue
		}
		if len(step.Lineage) != 2 || step.Lineage[0].Output != "user_id" || step.Lineage[0].SourceFields[0] != "id" || step.Lineage[1].Output != "grand_total" || len(step.Lineage[1].SourceFields) != 2 {
			t.Fatalf("PROJECT lineage = %#v", step.Lineage)
		}
		return
	}
	t.Fatal("PROJECT step missing")
}
