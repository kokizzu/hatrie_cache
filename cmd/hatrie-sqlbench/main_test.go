package main

import (
	"context"
	"strings"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestRunReportsThroughputAllocationSpillAndPlan(t *testing.T) {
	report, err := run(context.Background(), 16, 1)
	if err != nil {
		t.Fatalf("run() error = %v", err)
	}
	if report.Schema != "hatrie-cache-sql-benchmark/v1" || len(report.Results) != 3 {
		t.Fatalf("report = %#v", report)
	}
	for _, result := range report.Results {
		if result.Rows <= 0 || result.RowsPerSecond <= 0 || result.Allocations == 0 || result.BytesAllocated == 0 || result.PlanSignature == "" {
			t.Fatalf("scenario %q = %#v, want measured throughput, allocation, and plan", result.Name, result)
		}
	}
	if result := report.Results[1]; result.Name != "external_sort" || result.SpillBytes <= 0 || !strings.Contains(result.PlanSignature, "EXTERNAL SORT") {
		t.Fatalf("sort result = %#v, want external spill plan", result)
	}
}

func TestSummarizePlanExtractsSpillBytes(t *testing.T) {
	plan, spill := summarizePlan([]hatSql.ExplainStep{{Node: "EXTERNAL SORT", Detail: "id spill_bytes=42 runs=2"}})
	if spill != 42 || len(plan) != 1 || plan[0] != "EXTERNAL SORT: id spill_bytes=42 runs=2" {
		t.Fatalf("summarizePlan() = %#v, %d", plan, spill)
	}
}
