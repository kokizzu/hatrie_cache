package hatSql

import (
	"reflect"
	"testing"
)

func TestExplainPipelineReportsStageAndWorkerMetadata(t *testing.T) {
	t.Parallel()
	result, err := ExecuteSQLQuery("EXPLAIN PIPELINE FROM VALUES (1), (2) AS values(id) SELECT id", nil)
	if err != nil {
		t.Fatalf("ExecuteSQLQuery() error = %v", err)
	}
	wantColumns := []string{"node", "detail", "stage", "worker", "workers", "estimated_rows"}
	if !reflect.DeepEqual(result.Columns, wantColumns) {
		t.Fatalf("pipeline columns = %#v, want %#v", result.Columns, wantColumns)
	}
	if len(result.Rows) == 0 || len(result.Plan) != len(result.Rows) {
		t.Fatalf("pipeline rows/plan = %d/%d, want matching non-empty output", len(result.Rows), len(result.Plan))
	}
	for index, row := range result.Rows {
		if row["stage"] != 1 || row["worker"] != 1 || row["workers"] != 1 {
			t.Fatalf("pipeline row %d = %#v, want stage 1 worker 1/1", index, row)
		}
		if result.Plan[index].Stage != 1 || result.Plan[index].Worker != 1 || result.Plan[index].Workers != 1 {
			t.Fatalf("pipeline plan step %d = %#v, want stage 1 worker 1/1", index, result.Plan[index])
		}
	}
}

func TestExplainPipelineLeavesRegularExplainShapeUnchanged(t *testing.T) {
	t.Parallel()
	result, err := ExecuteSQLQuery("EXPLAIN FROM VALUES (1) AS values(id) SELECT id", nil)
	if err != nil {
		t.Fatalf("ExecuteSQLQuery() error = %v", err)
	}
	if reflect.DeepEqual(result.Columns, []string{"node", "detail", "stage", "worker", "workers", "estimated_rows"}) {
		t.Fatalf("regular EXPLAIN unexpectedly has pipeline columns: %#v", result.Columns)
	}
	for _, row := range result.Rows {
		if _, found := row["stage"]; found {
			t.Fatalf("regular EXPLAIN row unexpectedly has stage metadata: %#v", row)
		}
	}
}

func TestExplainPipelineAdvancesStageAtBlockingOperators(t *testing.T) {
	t.Parallel()
	result, err := ExecuteSQLQuery("EXPLAIN PIPELINE FROM VALUES (1), (1) AS values(id) SELECT id, COUNT(*) AS total GROUP BY id", nil)
	if err != nil {
		t.Fatalf("ExecuteSQLQuery() error = %v", err)
	}
	stages := make(map[string]int)
	for _, row := range result.Rows {
		stages[row["node"].(string)] = row["stage"].(int)
	}
	if stages["SCAN"] != 1 || stages["AGGREGATE"] != 2 || stages["PROJECT"] != 2 {
		t.Fatalf("pipeline stages = %#v, want scan=1 aggregate/project=2", stages)
	}
}
