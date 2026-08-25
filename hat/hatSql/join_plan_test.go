package hatSql_test

import (
	"fmt"
	"strings"
	"testing"

	"hatrie_cache/hat/hatSql"
)

type exactJoinPlanResolver struct {
	sources       map[string][]hatSql.Row
	indexCalls    int
	estimateCalls int
	rangeCalls    int
}

func (resolver *exactJoinPlanResolver) ResolveSQLIndexedRangeSource(name, key, field, operator string, value interface{}) ([]hatSql.Row, bool, error) {
	if name != "CACHE" || key != "right" || field != "score" || operator != ">=" {
		return nil, false, nil
	}
	minimum, ok := hatSql.Number(value)
	if !ok {
		return nil, false, fmt.Errorf("invalid range value %T", value)
	}
	resolver.rangeCalls++
	rows := make([]hatSql.Row, 0)
	for _, row := range resolver.sources[key] {
		score, ok := hatSql.Number(row["score"])
		if ok && score >= minimum {
			rows = append(rows, row)
		}
	}
	return hatSql.CloneRows(rows), true, nil
}

func (resolver *exactJoinPlanResolver) ResolveSQLSource(name, key string) ([]hatSql.Row, error) {
	rows, ok := resolver.sources[key]
	if !ok || name != "CACHE" {
		return nil, fmt.Errorf("unexpected source %s(%q)", name, key)
	}
	return hatSql.CloneRows(rows), nil
}

func (resolver *exactJoinPlanResolver) ResolveSQLIndexedSource(name, key, field string, value interface{}) ([]hatSql.Row, bool, error) {
	if name != "CACHE" || key != "right" || field != "k" {
		return nil, false, fmt.Errorf("unexpected index %s(%q).%s", name, key, field)
	}
	resolver.indexCalls++
	if value == nil {
		return nil, true, nil
	}
	rows := make([]hatSql.Row, 0)
	for _, row := range resolver.sources[key] {
		if row["k"] == value {
			rows = append(rows, row)
		}
	}
	return hatSql.CloneRows(rows), true, nil
}

func (resolver *exactJoinPlanResolver) SQLJSONIndexStats(key string, fields ...string) (hatSql.JSONIndexStats, bool, error) {
	if key != "right" || len(fields) != 1 || fields[0] != "k" {
		return hatSql.JSONIndexStats{}, false, nil
	}
	return hatSql.JSONIndexStats{Key: key, Fields: []string{"k"}, Rows: len(resolver.sources[key])}, true, nil
}

func (resolver *exactJoinPlanResolver) SQLJSONIndexValueEstimate(key, field string, value interface{}) (int, bool, bool, error) {
	if key != "right" || field != "k" {
		return 0, false, false, nil
	}
	resolver.estimateCalls++
	count := 0
	for _, row := range resolver.sources[key] {
		if row["k"] == value {
			count++
		}
	}
	return count, true, true, nil
}

func TestExactIndexJoinPlanChoosesHashForHotPostingLists(t *testing.T) {
	resolver := &exactJoinPlanResolver{sources: map[string][]hatSql.Row{
		"left":  {{"id": 1, "k": "hot"}, {"id": 2, "k": "hot"}, {"id": 3, "k": "hot"}},
		"right": {{"id": 10, "k": "hot"}, {"id": 11, "k": "hot"}, {"id": 12, "k": "hot"}},
	}}
	query := "EXPLAIN ANALYZE FROM CACHE('left') AS l JOIN CACHE('right') AS r ON l.k = r.k SELECT l.id, r.id AS right_id"
	result, err := hatSql.ExecuteSQLQuery(query, resolver)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) == 0 || resolver.indexCalls != 0 || resolver.estimateCalls != 1 {
		t.Fatalf("hot join calls/rows = index=%d estimates=%d rows=%#v, want hash plan without index probes", resolver.indexCalls, resolver.estimateCalls, result.Rows)
	}
	if !hasJoinPlan(result.Plan, "selected HASH JOIN") {
		t.Fatalf("hot join plan = %#v, want exact HASH JOIN selection", result.Plan)
	}
}

func TestExactIndexJoinPlanKeepsSparsePostingListProbes(t *testing.T) {
	right := make([]hatSql.Row, 0, 50)
	for _, key := range []string{"a", "b", "c"} {
		right = append(right, hatSql.Row{"k": key})
	}
	for index := 0; index < 47; index++ {
		right = append(right, hatSql.Row{"k": fmt.Sprintf("other-%d", index)})
	}
	resolver := &exactJoinPlanResolver{sources: map[string][]hatSql.Row{
		"left":  {{"id": 1, "k": "a"}, {"id": 2, "k": "b"}, {"id": 3, "k": "c"}},
		"right": right,
	}}
	query := "EXPLAIN ANALYZE FROM CACHE('left') AS l JOIN CACHE('right') AS r ON l.k = r.k SELECT l.id"
	result, err := hatSql.ExecuteSQLQuery(query, resolver)
	if err != nil {
		t.Fatal(err)
	}
	if resolver.indexCalls != 4 || resolver.estimateCalls != 3 {
		t.Fatalf("sparse join calls = index=%d estimates=%d, want 4 index calls and 3 exact estimates", resolver.indexCalls, resolver.estimateCalls)
	}
	if !hasJoinPlan(result.Plan, "selected INDEX JOIN") {
		t.Fatalf("sparse join plan = %#v, want exact INDEX JOIN selection", result.Plan)
	}
}

func TestInnerJoinPushesRightOnlyRangePredicateIntoIndex(t *testing.T) {
	resolver := &exactJoinPlanResolver{sources: map[string][]hatSql.Row{
		"left": {
			{"id": 1, "k": "team"},
			{"id": 2, "k": "team"},
		},
		"right": {
			{"k": "team", "score": 10},
			{"k": "team", "score": 90},
			{"k": "team", "score": 95},
		},
	}}
	query := "EXPLAIN ANALYZE FROM CACHE('left') AS l JOIN CACHE('right') AS r ON l.k = r.k WHERE r.score >= 90 SELECT l.id, r.score"
	result, err := hatSql.ExecuteSQLQuery(query, resolver)
	if err != nil {
		t.Fatal(err)
	}
	if resolver.rangeCalls != 1 {
		t.Fatalf("range index calls = %d, want one pushed range probe", resolver.rangeCalls)
	}
	if len(result.Rows) == 0 || !hasPlanDetail(result.Plan, "JOIN FILTER PUSH DOWN", "right predicate pushed") {
		t.Fatalf("pushed range rows/plan = %#v/%#v, want right predicate pushdown", result.Rows, result.Plan)
	}
}

func TestOuterJoinDoesNotPushRightPredicate(t *testing.T) {
	resolver := &exactJoinPlanResolver{sources: map[string][]hatSql.Row{
		"left":  {{"id": 1, "k": "team"}},
		"right": {{"k": "team", "score": 10}},
	}}
	query := "FROM CACHE('left') AS l LEFT JOIN CACHE('right') AS r ON l.k = r.k WHERE r.score >= 90 SELECT l.id"
	result, err := hatSql.ExecuteSQLQuery(query, resolver)
	if err != nil {
		t.Fatal(err)
	}
	if resolver.rangeCalls != 0 {
		t.Fatalf("outer join range calls = %d, want no right predicate pushdown", resolver.rangeCalls)
	}
	if len(result.Rows) != 0 {
		t.Fatalf("outer join rows = %#v, want empty result", result.Rows)
	}
}

func hasJoinPlan(steps []hatSql.ExplainStep, detail string) bool {
	return hasPlanDetail(steps, "JOIN PLAN", detail)
}

func hasPlanDetail(steps []hatSql.ExplainStep, node, detail string) bool {
	for _, step := range steps {
		if step.Node == node && strings.Contains(step.Detail, detail) {
			return true
		}
	}
	return false
}
