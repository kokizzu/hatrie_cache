package hatSql

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
)

func TestCH022ExplainAnalyzeIncludesStructuredPruningTelemetry(t *testing.T) {
	probe := ch022ExplainProbe()
	result, err := ExecuteSQLQueryParameters(context.Background(), "EXPLAIN ANALYZE SELECT id FROM CACHE('items') ORDER BY score ASC LIMIT 2", probe, nil, SQLQueryOptions{})
	if err != nil {
		t.Fatalf("EXPLAIN ANALYZE error = %v", err)
	}
	var pruning *ExplainPruning
	for _, step := range result.Plan {
		if step.Node == "COLUMNAR TOP-N SEGMENT SKIP" {
			pruning = step.Pruning
			break
		}
	}
	if pruning == nil {
		t.Fatalf("EXPLAIN ANALYZE plan = %#v, want structured pruning telemetry", result.Plan)
	}
	if pruning.TotalRows <= 0 || pruning.SkippedRows <= 0 || pruning.ScannedRows <= 0 {
		t.Fatalf("pruning = %#v, want positive total, skipped, and scanned rows", pruning)
	}
	if pruning.SkippedRows+pruning.ScannedRows != pruning.TotalRows {
		t.Fatalf("pruning = %#v, want skipped + scanned = total", pruning)
	}
	if pruning.MatchedRows > pruning.ScannedRows || pruning.ResidualRows != pruning.ScannedRows-pruning.MatchedRows {
		t.Fatalf("pruning = %#v, want matched/residual rows bounded by scanned rows", pruning)
	}
	if pruning.ResidualFalsePositiveRate < 0 || pruning.ResidualFalsePositiveRate > 100 {
		t.Fatalf("pruning = %#v, want false-positive rate in [0, 100]", pruning)
	}

	encoded, err := json.Marshal(result.Plan)
	if err != nil {
		t.Fatalf("json.Marshal(plan) error = %v", err)
	}
	if !strings.Contains(string(encoded), `"pruning"`) || !strings.Contains(string(encoded), `"skipped_rows"`) {
		t.Fatalf("plan JSON = %s, want machine-readable pruning fields", encoded)
	}

	var row SQLRow
	for _, candidate := range result.Rows {
		if candidate["node"] == "COLUMNAR TOP-N SEGMENT SKIP" {
			row = candidate
			break
		}
	}
	if row == nil {
		t.Fatalf("EXPLAIN ANALYZE rows = %#v, want pruning row", result.Rows)
	}
	for _, name := range []string{"total_rows", "skipped_rows", "scanned_rows", "matched_rows", "residual_rows", "residual_false_positive_rate"} {
		if _, ok := row[name]; !ok {
			t.Fatalf("pruning row = %#v, want %q", row, name)
		}
	}
}
