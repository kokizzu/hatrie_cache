package hatSql_test

import (
	"context"
	"fmt"
	"testing"

	"hatrie_cache/hat/hatSql"
)

type runtimeJoinFilterResolver struct {
	sources     map[string][]hatSql.SQLRow
	streamCalls int
}

type nonStreamingRuntimeJoinFilterResolver struct {
	sources map[string][]hatSql.SQLRow
}

func (resolver *runtimeJoinFilterResolver) ResolveSQLSource(name, key string) ([]hatSql.SQLRow, error) {
	if name != "CACHE" {
		return nil, fmt.Errorf("unexpected source type %q", name)
	}
	rows, ok := resolver.sources[key]
	if !ok {
		return nil, fmt.Errorf("unknown source %q", key)
	}
	return rows, nil
}

func (resolver *runtimeJoinFilterResolver) StreamSQLSource(ctx context.Context, name, key string, visit func(hatSql.SQLRow) error) error {
	if name != "CACHE" {
		return fmt.Errorf("unexpected streamed source type %q", name)
	}
	rows, ok := resolver.sources[key]
	if !ok {
		return fmt.Errorf("unknown streamed source %q", key)
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

func (resolver *nonStreamingRuntimeJoinFilterResolver) ResolveSQLSource(name, key string) ([]hatSql.SQLRow, error) {
	if name != "CACHE" {
		return nil, fmt.Errorf("unexpected source type %q", name)
	}
	rows, ok := resolver.sources[key]
	if !ok {
		return nil, fmt.Errorf("unknown source %q", key)
	}
	return rows, nil
}

func TestRuntimeJoinBloomFilterPreservesSelectiveInnerJoinResults(t *testing.T) {
	left := make([]hatSql.SQLRow, 0, 128)
	for index := 0; index < 128; index++ {
		left = append(left, hatSql.SQLRow{
			"id": index,
			"k":  fmt.Sprintf("key-%03d", index),
		})
	}
	right := make([]hatSql.SQLRow, 0, 16)
	for index := 0; index < 16; index++ {
		right = append(right, hatSql.SQLRow{
			"id": 1000 + index,
			"k":  fmt.Sprintf("key-%03d", index),
		})
	}
	resolver := &runtimeJoinFilterResolver{sources: map[string][]hatSql.SQLRow{
		"left":  left,
		"right": right,
	}}
	query := "FROM CACHE('left') AS l JOIN CACHE('right') AS r ON l.k = r.k SELECT l.id, r.id AS right_id"

	baseline, err := hatSql.ExecuteSQLQuery(query, resolver)
	if err != nil {
		t.Fatal(err)
	}
	filtered, err := hatSql.ExecuteSQLQueryContext(context.Background(), query, resolver, hatSql.QueryOptions{RuntimeJoinBloomFilter: true})
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprintf("%#v", filtered.Rows) != fmt.Sprintf("%#v", baseline.Rows) {
		t.Fatalf("filtered rows = %#v, baseline = %#v", filtered.Rows, baseline.Rows)
	}
	if resolver.streamCalls == 0 {
		t.Fatal("filtered execution did not use the streaming source path")
	}
	if len(filtered.Rows) != 16 {
		t.Fatalf("filtered rows = %d, want 16", len(filtered.Rows))
	}
	baselinePlan, err := hatSql.ExecuteSQLQuery("EXPLAIN ANALYZE "+query, resolver)
	if err != nil {
		t.Fatal(err)
	}
	filteredPlan, err := hatSql.ExecuteSQLQueryContext(context.Background(), "EXPLAIN ANALYZE "+query, resolver, hatSql.QueryOptions{RuntimeJoinBloomFilter: true})
	if err != nil {
		t.Fatal(err)
	}
	if !hasRuntimeJoinFilterStep(filteredPlan.Plan) {
		t.Fatalf("filtered plan = %#v, want runtime join filter", filteredPlan.Plan)
	}
	if hasRuntimeJoinFilterStep(baselinePlan.Plan) {
		t.Fatalf("default plan = %#v, runtime filter must be opt-in", baselinePlan.Plan)
	}
}

func TestRuntimeJoinBloomFilterDoesNotChangeLeftJoinNullExtension(t *testing.T) {
	resolver := &runtimeJoinFilterResolver{sources: map[string][]hatSql.SQLRow{
		"left": {
			{"id": 1, "k": "found"},
			{"id": 2, "k": "missing"},
			{"id": 3, "k": nil},
		},
		"right": {
			{"id": 11, "k": "found"},
		},
	}}
	query := "FROM CACHE('left') AS l LEFT JOIN CACHE('right') AS r ON l.k = r.k SELECT l.id, r.id AS right_id"
	baseline, err := hatSql.ExecuteSQLQuery(query, resolver)
	if err != nil {
		t.Fatal(err)
	}
	filtered, err := hatSql.ExecuteSQLQueryContext(context.Background(), query, resolver, hatSql.QueryOptions{RuntimeJoinBloomFilter: true})
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprintf("%#v", filtered.Rows) != fmt.Sprintf("%#v", baseline.Rows) {
		t.Fatalf("filtered left join = %#v, baseline = %#v", filtered.Rows, baseline.Rows)
	}
	if hasRuntimeJoinFilterStep(filtered.Plan) {
		t.Fatalf("left join plan = %#v, runtime filter must not skip null-extended rows", filtered.Plan)
	}
}

func TestRuntimeJoinBloomFilterPreservesDuplicateKeysAndNullSemantics(t *testing.T) {
	resolver := &runtimeJoinFilterResolver{sources: map[string][]hatSql.SQLRow{
		"left": {
			{"id": 1, "k": "same"},
			{"id": 2, "k": nil},
			{"id": 3, "k": "same"},
		},
		"right": {
			{"id": 11, "k": "same"},
			{"id": 12, "k": nil},
			{"id": 13, "k": "same"},
		},
	}}
	query := "FROM CACHE('left') AS l JOIN CACHE('right') AS r ON l.k = r.k SELECT l.id, r.id AS right_id"
	baseline, err := hatSql.ExecuteSQLQuery(query, resolver)
	if err != nil {
		t.Fatal(err)
	}
	filtered, err := hatSql.ExecuteSQLQueryContext(context.Background(), query, resolver, hatSql.QueryOptions{RuntimeJoinBloomFilter: true})
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprintf("%#v", filtered.Rows) != fmt.Sprintf("%#v", baseline.Rows) {
		t.Fatalf("filtered duplicate-key rows = %#v, baseline = %#v", filtered.Rows, baseline.Rows)
	}
	if len(filtered.Rows) != 4 {
		t.Fatalf("filtered duplicate-key rows = %d, want 4", len(filtered.Rows))
	}
}

func TestRuntimeJoinBloomFilterFallsBackWithoutStreamingResolver(t *testing.T) {
	resolver := &nonStreamingRuntimeJoinFilterResolver{sources: map[string][]hatSql.SQLRow{
		"left":  {{"id": 1, "k": "found"}, {"id": 2, "k": "missing"}},
		"right": {{"id": 11, "k": "found"}},
	}}
	query := "FROM CACHE('left') AS l JOIN CACHE('right') AS r ON l.k = r.k SELECT l.id, r.id AS right_id"
	baseline, err := hatSql.ExecuteSQLQuery(query, resolver)
	if err != nil {
		t.Fatal(err)
	}
	filtered, err := hatSql.ExecuteSQLQueryContext(context.Background(), query, resolver, hatSql.QueryOptions{RuntimeJoinBloomFilter: true})
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprintf("%#v", filtered.Rows) != fmt.Sprintf("%#v", baseline.Rows) {
		t.Fatalf("fallback rows = %#v, baseline = %#v", filtered.Rows, baseline.Rows)
	}
	if hasRuntimeJoinFilterStep(filtered.Plan) {
		t.Fatalf("fallback plan = %#v, runtime filter must require streaming", filtered.Plan)
	}
}

func hasRuntimeJoinFilterStep(steps []hatSql.ExplainStep) bool {
	for _, step := range steps {
		if step.Node == "RUNTIME JOIN FILTER" {
			return true
		}
	}
	return false
}
