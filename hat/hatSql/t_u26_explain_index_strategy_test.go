package hatSql_test

import (
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestRegularExplainPublishesIndexStrategyAlternatives(t *testing.T) {
	result, err := hatSql.ExecuteSQLQuery("EXPLAIN FROM CACHE('orders') AS o WHERE o.status = 'open' AND o.region = 'us' SELECT o.id", explainOptimizerResolver{})
	if err != nil {
		t.Fatal(err)
	}
	for _, step := range result.Plan {
		if step.Node != "SCAN" {
			continue
		}
		if len(step.Alternatives) != 2 {
			t.Fatalf("scan alternatives = %#v, want two candidates", step.Alternatives)
		}
		if !step.Alternatives[0].Selected || step.Alternatives[0].RejectedReason != "" {
			t.Fatalf("selected alternative = %#v, want selected", step.Alternatives[0])
		}
		if step.Alternatives[1].Selected || step.Alternatives[1].RejectedReason == "" {
			t.Fatalf("fallback alternative = %#v, want rejection reason", step.Alternatives[1])
		}
		return
	}
	t.Fatalf("EXPLAIN plan = %#v, want SCAN strategy diagnostics", result.Plan)
}

func TestRegularExplainMaterializesIndexStrategyColumns(t *testing.T) {
	result, err := hatSql.ExecuteSQLQuery("EXPLAIN FROM CACHE('orders') AS o WHERE o.status = 'open' AND o.region = 'us' SELECT o.id", explainOptimizerResolver{})
	if err != nil {
		t.Fatal(err)
	}
	wantColumns := []string{"node", "detail", "estimated_rows", "alternatives", "notices"}
	if len(result.Columns) != len(wantColumns) {
		t.Fatalf("EXPLAIN columns = %#v, want %#v", result.Columns, wantColumns)
	}
	for index, column := range wantColumns {
		if result.Columns[index] != column {
			t.Fatalf("EXPLAIN column %d = %q, want %q", index, result.Columns[index], column)
		}
	}
	for _, row := range result.Rows {
		if row["node"] != "SCAN" {
			continue
		}
		alternatives, ok := row["alternatives"].([]hatSql.ExplainAlternative)
		if !ok || len(alternatives) != 2 {
			t.Fatalf("materialized alternatives = %#v, want two structured alternatives", row["alternatives"])
		}
		notices, ok := row["notices"].([]hatSql.ExplainNotice)
		if !ok || len(notices) != 1 {
			t.Fatalf("materialized notices = %#v, want one structured notice", row["notices"])
		}
		return
	}
	t.Fatalf("EXPLAIN rows = %#v, want SCAN row", result.Rows)
}

func BenchmarkRegularExplainIndexStrategy(b *testing.B) {
	query := "EXPLAIN FROM CACHE('orders') AS o WHERE o.status = 'open' AND o.region = 'us' SELECT o.id"
	for i := 0; i < b.N; i++ {
		if _, err := hatSql.ExecuteSQLQuery(query, explainOptimizerResolver{}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRegularExplainWithoutIndexStrategy(b *testing.B) {
	query := "EXPLAIN FROM VALUES (1) AS values(id) SELECT id"
	for i := 0; i < b.N; i++ {
		if _, err := hatSql.ExecuteSQLQuery(query, nil); err != nil {
			b.Fatal(err)
		}
	}
}
