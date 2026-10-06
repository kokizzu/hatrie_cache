package hatSql

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestC237CostedExplainIncludesEstimatedIOCost(t *testing.T) {
	scanRows := 12
	filterRows := 12
	steps := []ExplainStep{
		{Node: "SCAN", EstimatedRows: &scanRows},
		{Node: "FILTER", EstimatedRows: &filterRows},
	}

	costed := CostSQLExplainSteps(steps, SQLExplainCostOptions{
		CPUCostPerRow:     2,
		MemoryBytesPerRow: 8,
		IOCostPerRow:      3,
	})
	if len(costed) != len(steps) {
		t.Fatalf("CostSQLExplainSteps() returned %d steps, want %d", len(costed), len(steps))
	}
	if costed[0].EstimatedIOCost == nil || *costed[0].EstimatedIOCost != 36 {
		t.Fatalf("scan estimated I/O cost = %#v, want 36", costed[0].EstimatedIOCost)
	}
	if costed[1].EstimatedIOCost != nil {
		t.Fatalf("filter estimated I/O cost = %#v, want not applicable", costed[1].EstimatedIOCost)
	}
	if costed[0].EstimatedCost == nil || *costed[0].EstimatedCost != 24 {
		t.Fatalf("scan estimated CPU cost = %#v, want 24", costed[0].EstimatedCost)
	}
	encoded, err := json.Marshal(costed[0])
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(encoded), `"estimated_io_cost":36`) {
		t.Fatalf("costed explain JSON = %s, want estimated_io_cost", encoded)
	}
}

func TestC237EstimatedIOCostCloneDoesNotAlias(t *testing.T) {
	original := 7
	dataflowClone := cloneExplainDataflowStep(ExplainStep{EstimatedIOCost: &original})
	cacheClone := cloneResultCachePlanStep(ExplainStep{EstimatedIOCost: &original})
	if dataflowClone.EstimatedIOCost == nil || cacheClone.EstimatedIOCost == nil {
		t.Fatal("estimated IO cost clone is nil")
	}
	*dataflowClone.EstimatedIOCost = 8
	*cacheClone.EstimatedIOCost = 9
	if original != 7 {
		t.Fatalf("original estimated IO cost = %d, want 7", original)
	}
}

func TestC237PipelineExplainIncludesEstimatedIOCost(t *testing.T) {
	result, err := ExecuteSQLQuery("EXPLAIN PIPELINE COST FROM VALUES (1), (2) AS values(id) SELECT id", nil)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(strings.Join(result.Columns, ","), "estimated_io_cost") {
		t.Fatalf("pipeline columns = %#v, want estimated_io_cost", result.Columns)
	}
	for index, step := range result.Plan {
		if step.Node == "SCAN" && (step.EstimatedIOCost == nil || *step.EstimatedIOCost <= 0) {
			t.Fatalf("pipeline scan step %d = %#v, want positive estimated IO cost", index, step)
		}
	}
}
