package hatSql_test

import (
	"fmt"
	"strings"
	"testing"

	"hatrie_cache/hat/hatSql"
)

const mu013CardinalityQuery = "FROM CACHE('orders') AS o INNER JOIN CACHE('customers') AS c ON o.customer_id = c.id WHERE o.status = 'open' GROUP BY o.status SELECT o.status, COUNT(*) AS total"

type mu013CardinalityResolver struct{}

func (mu013CardinalityResolver) ResolveSQLSource(name, key string) ([]hatSql.Row, error) {
	if name != "CACHE" {
		return nil, fmt.Errorf("unexpected source %s(%q)", name, key)
	}
	switch key {
	case "orders":
		return []hatSql.Row{
			{"id": int64(1), "status": "open", "customer_id": int64(1)},
			{"id": int64(2), "status": "closed", "customer_id": int64(2)},
			{"id": int64(3), "status": "closed", "customer_id": int64(4)},
		}, nil
	case "customers":
		return []hatSql.Row{
			{"id": int64(1), "name": "one"},
			{"id": int64(1), "name": "one-copy"},
			{"id": int64(2), "name": "two"},
		}, nil
	default:
		return nil, fmt.Errorf("unexpected cache %q", key)
	}
}

func (mu013CardinalityResolver) SQLSourceCardinality(name, key string) (int, bool, bool, error) {
	if name != "CACHE" {
		return 0, false, false, nil
	}
	switch key {
	case "orders":
		return 3, true, true, nil
	case "customers":
		return 3, true, true, nil
	default:
		return 0, false, false, nil
	}
}

func (mu013CardinalityResolver) SQLJSONIndexStats(key string, fields ...string) (hatSql.JSONIndexStats, bool, error) {
	if key == "orders" && len(fields) == 1 && fields[0] == "status" {
		return hatSql.JSONIndexStats{Key: key, Fields: fields, Rows: 3, DistinctKeys: 2}, true, nil
	}
	if key == "customers" && len(fields) == 1 && fields[0] == "id" {
		return hatSql.JSONIndexStats{Key: key, Fields: fields, Rows: 3, DistinctKeys: 2}, true, nil
	}
	return hatSql.JSONIndexStats{}, false, nil
}

func mu013PlanStep(plan []hatSql.ExplainStep, node string) *hatSql.ExplainStep {
	for index := range plan {
		if strings.TrimSpace(plan[index].Node) == node {
			return &plan[index]
		}
	}
	return nil
}

func mu013PlanStepWithSuffix(plan []hatSql.ExplainStep, suffix string) *hatSql.ExplainStep {
	for index := range plan {
		if strings.HasSuffix(strings.TrimSpace(plan[index].Node), suffix) {
			return &plan[index]
		}
	}
	return nil
}

func mu013RequireEstimate(t *testing.T, step *hatSql.ExplainStep, want int) {
	t.Helper()
	if step == nil {
		t.Fatalf("missing plan step, want estimated_rows=%d", want)
	}
	if step.EstimatedRows == nil || *step.EstimatedRows != want {
		t.Fatalf("plan step = %#v, want estimated_rows=%d", *step, want)
	}
}

func TestMU013RegularExplainEstimatesScanFilterJoinAndAggregate(t *testing.T) {
	resolver := mu013CardinalityResolver{}
	tests := []struct {
		name  string
		query string
		node  string
		want  int
	}{
		{
			name:  "filter",
			query: "EXPLAIN FROM CACHE('orders') AS o WHERE o.status = 'open' SELECT o.id",
			node:  "FILTER",
			want:  2,
		},
		{
			name:  "join",
			query: "EXPLAIN FROM CACHE('orders') AS o INNER JOIN CACHE('customers') AS c ON o.customer_id = c.id SELECT o.id",
			node:  "EQUALITY JOIN",
			want:  6,
		},
		{
			name:  "aggregate",
			query: "EXPLAIN FROM CACHE('orders') AS o GROUP BY o.status SELECT o.status, COUNT(*) AS total",
			node:  "AGGREGATE",
			want:  2,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result, err := hatSql.ExecuteSQLQuery(test.query, resolver)
			if err != nil {
				t.Fatal(err)
			}
			mu013RequireEstimate(t, mu013PlanStep(result.Plan, test.node), test.want)
		})
	}
}

func TestMU013ExplainAnalyzeComparesEstimateWithObservedRows(t *testing.T) {
	result, err := hatSql.ExecuteSQLQuery("EXPLAIN ANALYZE FROM CACHE('orders') AS o WHERE o.status = 'open' SELECT o.id", mu013CardinalityResolver{})
	if err != nil {
		t.Fatal(err)
	}
	scan := mu013PlanStep(result.Plan, "SCAN")
	mu013RequireEstimate(t, scan, 3)
	if scan.ActualOutputRows == nil || *scan.ActualOutputRows != 3 {
		t.Fatalf("scan step = %#v, want actual_output_rows=3", *scan)
	}
	filter := mu013PlanStep(result.Plan, "FILTER")
	mu013RequireEstimate(t, filter, 2)
	if filter.ActualOutputRows == nil || *filter.ActualOutputRows != 1 {
		t.Fatalf("filter step = %#v, want actual_output_rows=1", *filter)
	}
	if filter.EstimateErrorRows == nil || *filter.EstimateErrorRows != -1 {
		t.Fatalf("filter estimate error = %#v, want -1", filter.EstimateErrorRows)
	}
	if filter.EstimateErrorPercent == nil || *filter.EstimateErrorPercent != -50 {
		t.Fatalf("filter estimate error percent = %#v, want -50", filter.EstimateErrorPercent)
	}
}

func TestMU013ExplainAnalyzeIncludesJoinAndAggregateEstimates(t *testing.T) {
	resolver := mu013CardinalityResolver{}
	joinResult, err := hatSql.ExecuteSQLQuery("EXPLAIN ANALYZE FROM CACHE('orders') AS o INNER JOIN CACHE('customers') AS c ON o.customer_id = c.id SELECT o.id", resolver)
	if err != nil {
		t.Fatal(err)
	}
	join := mu013PlanStepWithSuffix(joinResult.Plan, "JOIN")
	mu013RequireEstimate(t, join, 6)
	if join.ActualOutputRows == nil || *join.ActualOutputRows != 3 {
		t.Fatalf("join step = %#v, want actual_output_rows=3", *join)
	}

	aggregateResult, err := hatSql.ExecuteSQLQuery("EXPLAIN ANALYZE FROM CACHE('orders') AS o GROUP BY o.status SELECT o.status, COUNT(*) AS total", resolver)
	if err != nil {
		t.Fatal(err)
	}
	aggregate := mu013PlanStepWithSuffix(aggregateResult.Plan, "AGGREGATE")
	mu013RequireEstimate(t, aggregate, 2)
	if aggregate.ActualOutputRows == nil || *aggregate.ActualOutputRows != 2 {
		t.Fatalf("aggregate step = %#v, want actual_output_rows=2", *aggregate)
	}
}

func TestMU013UnavailableMetadataLeavesEstimateOmitted(t *testing.T) {
	result, err := hatSql.ExecuteSQLQuery("EXPLAIN FROM CACHE('orders') AS o WHERE o.status = 'open' SELECT o.id", nil)
	if err != nil {
		t.Fatal(err)
	}
	if step := mu013PlanStep(result.Plan, "SCAN"); step == nil || step.EstimatedRows != nil {
		t.Fatalf("scan step = %#v, want no estimate without metadata", step)
	}
	if step := mu013PlanStep(result.Plan, "FILTER"); step == nil || step.EstimatedRows != nil {
		t.Fatalf("filter step = %#v, want no estimate without metadata", step)
	}
}

func BenchmarkMU013ExplainCardinality(b *testing.B) {
	resolver := mu013CardinalityResolver{}
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		if _, err := hatSql.ExecuteSQLQuery("EXPLAIN "+mu013CardinalityQuery, resolver); err != nil {
			b.Fatal(err)
		}
	}
}
