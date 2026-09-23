package hatSql

import (
	"reflect"
	"testing"
)

func TestCH045ExplainEstimateUsesCostModel(t *testing.T) {
	result, err := ExecuteSQLQuery("EXPLAIN ESTIMATE FROM VALUES (1), (2), (3) AS values(id) SELECT id", nil)
	if err != nil {
		t.Fatalf("EXPLAIN ESTIMATE returned error: %v", err)
	}
	if got, want := result.Columns, []string{"node", "detail", "estimated_rows", "estimated_cost", "estimated_memory_bytes"}; !equalStrings(got, want) {
		t.Fatalf("EXPLAIN ESTIMATE columns = %#v, want %#v", got, want)
	}
	if len(result.Plan) == 0 || result.Plan[0].EstimatedCost == nil || result.Plan[0].EstimatedMemoryBytes == nil {
		t.Fatalf("EXPLAIN ESTIMATE plan = %#v, want cost and memory estimates", result.Plan)
	}
	if len(result.Rows) == 0 || result.Rows[0]["estimated_cost"] == nil || result.Rows[0]["estimated_memory_bytes"] == nil {
		t.Fatalf("EXPLAIN ESTIMATE rows = %#v, want cost and memory estimates", result.Rows)
	}
}

func TestCH045EstimateRemainsUsableAsIdentifier(t *testing.T) {
	result, err := ExecuteSQLQuery("FROM VALUES (1) AS values(estimate) SELECT estimate", nil)
	if err != nil {
		t.Fatalf("identifier named estimate returned error: %v", err)
	}
	if len(result.Rows) != 1 || result.Rows[0]["estimate"] != int64(1) {
		t.Fatalf("identifier named estimate result = %#v, want one value", result.Rows)
	}
}

func TestCH045ExplainEstimateMatchesExplainCost(t *testing.T) {
	const query = "FROM VALUES (1), (2), (3) AS values(id) WHERE id > 0 SELECT id"
	cost, err := ExecuteSQLQuery("EXPLAIN COST "+query, nil)
	if err != nil {
		t.Fatalf("EXPLAIN COST returned error: %v", err)
	}
	estimate, err := ExecuteSQLQuery("EXPLAIN ESTIMATE "+query, nil)
	if err != nil {
		t.Fatalf("EXPLAIN ESTIMATE returned error: %v", err)
	}
	if !reflect.DeepEqual(estimate.Columns, cost.Columns) || !reflect.DeepEqual(estimate.Rows, cost.Rows) || !reflect.DeepEqual(estimate.Plan, cost.Plan) {
		t.Fatalf("EXPLAIN ESTIMATE differs from EXPLAIN COST:\nestimate=%#v\ncost=%#v", estimate, cost)
	}
}

func TestCH045ExplainEstimateAnalyzePreservesAnalyzeSemantics(t *testing.T) {
	result, err := ExecuteSQLQuery("EXPLAIN ESTIMATE ANALYZE FROM VALUES (1), (2) AS values(id) SELECT id", nil)
	if err != nil {
		t.Fatalf("EXPLAIN ESTIMATE ANALYZE returned error: %v", err)
	}
	if result.Stats == nil || result.Stats.OutputRows != 2 {
		t.Fatalf("EXPLAIN ESTIMATE ANALYZE stats = %#v, want two output rows", result.Stats)
	}
	if len(result.Plan) == 0 || result.Plan[0].ActualOutputRows == nil {
		t.Fatalf("EXPLAIN ESTIMATE ANALYZE plan = %#v, want analyzed metrics", result.Plan)
	}
}

func equalStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}
