package hatSql

import (
	"context"
	"strings"
	"testing"
)

var m239ExplainBenchmarkSink QueryResult

func TestM239ExplainReportsTemporalRequirements(t *testing.T) {
	required, asOf := uint64(5), uint64(42)
	options := SQLQueryOptions{
		RequireSourceFrontier:  true,
		RequiredSourceFrontier: required,
		AsOfFrontier:           &asOf,
	}
	for _, query := range []string{
		"EXPLAIN SELECT id FROM CACHE('users')",
		"EXPLAIN ANALYZE SELECT id FROM CACHE('users')",
	} {
		t.Run(strings.TrimPrefix(query, "EXPLAIN "), func(t *testing.T) {
			result, err := ExecuteSQLQueryContext(context.Background(), query, mz050PlanSnapshotResolver{}, options)
			if err != nil {
				t.Fatalf("ExecuteSQLQueryContext() error = %v", err)
			}
			var scan *ExplainStep
			for index := range result.Plan {
				if strings.TrimSpace(result.Plan[index].Node) == "SCAN" {
					scan = &result.Plan[index]
					break
				}
			}
			if scan == nil {
				t.Fatalf("plan = %#v, want SCAN step", result.Plan)
			}
			findNotice := func(code string) (ExplainNotice, bool) {
				for _, notice := range scan.Notices {
					if notice.Code == code {
						return notice, true
					}
				}
				return ExplainNotice{}, false
			}
			logicalTimestamp, ok := findNotice("LOGICAL_TIMESTAMP")
			if !ok || logicalTimestamp.Detail != "as_of_frontier=42" {
				t.Fatalf("logical timestamp notice = %#v, want as_of_frontier=42", logicalTimestamp)
			}
			frontierRequirement, ok := findNotice("FRONTIER_REQUIREMENT")
			if !ok || frontierRequirement.Detail != "required_source_frontier=5" {
				t.Fatalf("frontier requirement notice = %#v, want required_source_frontier=5", frontierRequirement)
			}
		})
	}
}

func TestM239ExplainOmitsTemporalRequirementsByDefault(t *testing.T) {
	result, err := ExecuteSQLQuery("EXPLAIN SELECT id FROM CACHE('users')", mz050PlanSnapshotResolver{})
	if err != nil {
		t.Fatalf("ExecuteSQLQuery() error = %v", err)
	}
	for _, step := range result.Plan {
		if len(step.Notices) != 0 {
			t.Fatalf("default explain step = %#v, want no notices", step)
		}
	}
}

func BenchmarkM239ExplainTemporalQuery(b *testing.B) {
	required, asOf := uint64(5), uint64(42)
	options := SQLQueryOptions{
		RequireSourceFrontier:  true,
		RequiredSourceFrontier: required,
		AsOfFrontier:           &asOf,
	}
	resolver := mz050PlanSnapshotResolver{}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		result, err := ExecuteSQLQueryContext(context.Background(), "EXPLAIN SELECT id FROM CACHE('users')", resolver, options)
		if err != nil {
			b.Fatal(err)
		}
		m239ExplainBenchmarkSink = result
	}
}
