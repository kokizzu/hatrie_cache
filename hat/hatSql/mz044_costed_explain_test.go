//go:build !mz044baseline

package hatSql_test

import (
	"reflect"
	"strings"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestMZ044CostedExplainAnnotatesOperatorsWithoutReadingSources(t *testing.T) {
	resolver := hatSql.SourceResolverFunc(func(string, string) ([]hatSql.Row, error) {
		t.Fatal("EXPLAIN COST unexpectedly read a source")
		return nil, nil
	})
	result, err := hatSql.ExecuteSQLQuery(
		"EXPLAIN COST FROM VALUES (1), (2), (3) AS values(id) WHERE id > 0 SELECT id",
		resolver,
	)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(result.Columns, []string{"node", "detail", "estimated_rows", "estimated_cost", "estimated_memory_bytes"}) {
		t.Fatalf("EXPLAIN COST columns = %#v", result.Columns)
	}
	var foundScan bool
	for _, step := range result.Plan {
		if step.Node != "SCAN" {
			continue
		}
		foundScan = true
		if step.EstimatedRows == nil || *step.EstimatedRows != 3 {
			t.Fatalf("costed scan rows = %#v, want 3", step.EstimatedRows)
		}
		if step.EstimatedCost == nil || *step.EstimatedCost <= 0 {
			t.Fatalf("costed scan cost = %#v, want positive", step.EstimatedCost)
		}
		if step.EstimatedMemoryBytes == nil || *step.EstimatedMemoryBytes <= 0 {
			t.Fatalf("costed scan memory = %#v, want positive", step.EstimatedMemoryBytes)
		}
	}
	if !foundScan {
		t.Fatalf("EXPLAIN COST plan = %#v, want SCAN", result.Plan)
	}
	var foundCostRow bool
	for _, row := range result.Rows {
		if _, ok := row["estimated_cost"]; ok {
			foundCostRow = true
		}
	}
	if !foundCostRow {
		t.Fatalf("EXPLAIN COST rows = %#v, want at least one estimated_cost", result.Rows)
	}
}

func TestMZ044RegularExplainOmitsCostFields(t *testing.T) {
	result, err := hatSql.ExecuteSQLQuery("EXPLAIN FROM VALUES (1) AS values(id) SELECT id", nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, step := range result.Plan {
		if step.EstimatedCost != nil || step.EstimatedMemoryBytes != nil {
			t.Fatalf("regular EXPLAIN step = %#v, want no cost fields", step)
		}
	}
	if strings.Contains(strings.Join(result.Columns, ","), "estimated_cost") {
		t.Fatalf("regular EXPLAIN columns = %#v, want no cost column", result.Columns)
	}
}
