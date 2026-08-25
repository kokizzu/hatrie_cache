package hatSql_test

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"

	"hatrie_cache/hat/hatSql"
)

type exactJoinPlanResolver struct {
	sources       map[string][]hatSql.Row
	indexCalls    int
	estimateCalls int
	rangeCalls    int
	streamCalls   int
}

func (resolver *exactJoinPlanResolver) StreamSQLSource(ctx context.Context, name, key string, visit func(hatSql.Row) error) error {
	rows, ok := resolver.sources[key]
	if !ok || name != "CACHE" {
		return fmt.Errorf("unexpected stream source %s(%q)", name, key)
	}
	resolver.streamCalls++
	for _, row := range rows {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := visit(row); err != nil {
			return err
		}
	}
	return nil
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

func TestLargeEqualityJoinSpillsBoundedHashPartitions(t *testing.T) {
	resolver := &exactJoinPlanResolver{sources: map[string][]hatSql.Row{
		"left": {
			{"id": 1, "k": "a"},
			{"id": 2, "k": "b"},
			{"id": 3, "k": "a"},
		},
		"right": {
			{"k": "a", "name": "Ada"},
			{"k": "b", "name": "Bea"},
			{"k": "a", "name": "Cia"},
		},
	}}
	directory := t.TempDir()
	options := hatSql.QueryOptions{MaxJoinBytes: 128, SpillDirectory: directory, MaxSpillBytes: 1 << 20}
	query := "FROM CACHE('left') AS l JOIN CACHE('right') AS r ON l.k = r.k SELECT l.id, r.name"
	result, err := hatSql.ExecuteSQLQueryContext(context.Background(), query, resolver, options)
	if err != nil {
		t.Fatal(err)
	}
	want := []hatSql.Row{
		{"id": 1, "name": "Ada"},
		{"id": 1, "name": "Cia"},
		{"id": 2, "name": "Bea"},
		{"id": 3, "name": "Ada"},
		{"id": 3, "name": "Cia"},
	}
	if fmt.Sprintf("%#v", result.Rows) != fmt.Sprintf("%#v", want) {
		t.Fatalf("spilled join rows = %#v, want %#v", result.Rows, want)
	}
	if resolver.streamCalls != 2 {
		t.Fatalf("stream calls = %d, want two streamed join inputs", resolver.streamCalls)
	}
	entries, err := os.ReadDir(directory)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("spill directory entries = %#v, want cleanup", entries)
	}

	analysis, err := hatSql.ExecuteSQLQueryContext(context.Background(), "EXPLAIN ANALYZE "+query, resolver, options)
	if err != nil {
		t.Fatal(err)
	}
	if !hasPlanDetail(analysis.Plan, "SPILL HASH JOIN", "partitioned") {
		t.Fatalf("spilled join plan = %#v, want partitioned spill hash join", analysis.Plan)
	}
}

func TestLargeEqualityJoinSpillBudgetCleansTemporaryFiles(t *testing.T) {
	resolver := &exactJoinPlanResolver{sources: map[string][]hatSql.Row{
		"left":  {{"id": 1, "k": "team"}},
		"right": {{"k": "team", "name": "Ada"}},
	}}
	directory := t.TempDir()
	query := "FROM CACHE('left') AS l JOIN CACHE('right') AS r ON l.k = r.k SELECT l.id, r.name"
	_, err := hatSql.ExecuteSQLQueryContext(context.Background(), query, resolver, hatSql.QueryOptions{MaxJoinBytes: 128, SpillDirectory: directory, MaxSpillBytes: 1})
	if err == nil || !strings.Contains(err.Error(), "spill disk budget") {
		t.Fatalf("spilled join error = %v, want disk-budget failure", err)
	}
	entries, readErr := os.ReadDir(directory)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if len(entries) != 0 {
		t.Fatalf("failed spill directory entries = %#v, want cleanup", entries)
	}
}

func TestWorkersPreserveGroupedResultOrder(t *testing.T) {
	resolver := &exactJoinPlanResolver{sources: map[string][]hatSql.Row{
		"events": {
			{"kind": "a", "value": 1},
			{"kind": "b", "value": 2},
			{"kind": "a", "value": 3},
			{"kind": "c", "value": 4},
		},
	}}
	query := "FROM CACHE('events') AS e SELECT e.kind, COUNT(*) AS total, SUM(e.value) AS sum GROUP BY e.kind"
	sequential, err := hatSql.ExecuteSQLQueryContext(context.Background(), query, resolver, hatSql.QueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	parallel, err := hatSql.ExecuteSQLQueryContext(context.Background(), query, resolver, hatSql.QueryOptions{Workers: 2})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(parallel, sequential) {
		t.Fatalf("parallel grouped result = %#v, want %#v", parallel, sequential)
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
