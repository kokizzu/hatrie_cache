package hatSql_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestCHU14SpillRuntimeBloomFilterPreservesRowsAndReportsSkips(t *testing.T) {
	left := make([]hatSql.Row, 0, 8)
	for index := 0; index < 8; index++ {
		left = append(left, hatSql.Row{
			"id": index,
			"k":  fmt.Sprintf("key-%03d", index),
		})
	}
	right := make([]hatSql.Row, 0, 192)
	for index := 0; index < 192; index++ {
		right = append(right, hatSql.Row{
			"id": 1000 + index,
			"k":  fmt.Sprintf("key-%03d", index),
		})
	}
	query := "FROM CACHE('left') AS l JOIN CACHE('right') AS r ON l.k = r.k SELECT l.id, r.id AS right_id"
	baseOptions := hatSql.QueryOptions{
		JoinOverflowPolicy: hatSql.SQLJoinOverflowSpill,
		MaxJoinBytes:       128,
		MaxSpillBytes:      1 << 20,
	}

	baselineOptions := baseOptions
	baselineOptions.SpillDirectory = t.TempDir()
	baselineResolver := &exactJoinPlanResolver{sources: map[string][]hatSql.Row{
		"left":  left,
		"right": right,
	}}
	baseline, err := hatSql.ExecuteSQLQueryContext(context.Background(), query, baselineResolver, baselineOptions)
	if err != nil {
		t.Fatal(err)
	}

	filteredOptions := baseOptions
	filteredOptions.SpillBloom = true
	filteredOptions.SpillDirectory = t.TempDir()
	filteredResolver := &exactJoinPlanResolver{sources: map[string][]hatSql.Row{
		"left":  left,
		"right": right,
	}}
	filtered, err := hatSql.ExecuteSQLQueryContext(context.Background(), query, filteredResolver, filteredOptions)
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprintf("%#v", filtered.Rows) != fmt.Sprintf("%#v", baseline.Rows) {
		t.Fatalf("filtered rows = %#v, baseline = %#v", filtered.Rows, baseline.Rows)
	}
	if len(filtered.Rows) != 8 {
		t.Fatalf("filtered rows = %d, want 8", len(filtered.Rows))
	}

	planOptions := filteredOptions
	planOptions.SpillDirectory = t.TempDir()
	plan, err := hatSql.ExecuteSQLQueryContext(context.Background(), "EXPLAIN ANALYZE "+query, filteredResolver, planOptions)
	if err != nil {
		t.Fatal(err)
	}
	planText := fmt.Sprintf("%#v", plan.Plan)
	if !strings.Contains(planText, "RUNTIME JOIN FILTER") {
		t.Fatalf("plan = %s, want runtime join filter", planText)
	}
	if !strings.Contains(planText, "skipped=") {
		t.Fatalf("plan = %s, want skipped probe count", planText)
	}
}
