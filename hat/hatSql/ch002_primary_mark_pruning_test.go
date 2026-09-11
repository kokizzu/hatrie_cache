package hatSql

import (
	"context"
	"fmt"
	"testing"
)

type ch002LegacyOrderedResolver struct {
	rows    []SQLRow
	visited int
}

func (resolver *ch002LegacyOrderedResolver) ResolveSQLSource(string, string) ([]SQLRow, error) {
	resolver.visited += len(resolver.rows)
	return resolver.rows, nil
}

func (resolver *ch002LegacyOrderedResolver) StreamSQLOrderedSource(ctx context.Context, _ string, _ string, _ string, _ bool, _ bool, _ bool, visit func(SQLRow) error) (bool, error) {
	for _, row := range resolver.rows {
		if err := ctx.Err(); err != nil {
			return true, err
		}
		resolver.visited++
		if err := visit(row); err != nil {
			return true, err
		}
	}
	return true, nil
}

type ch002SparseMarkResolver struct {
	ch002LegacyOrderedResolver
	rangeCalls int
}

func (resolver *ch002SparseMarkResolver) ResolveSQLOrderedSourceRange(_ string, _ string, _ string, _ bool, _ bool, _ bool, operator string, value interface{}) ([]SQLRow, bool, error) {
	threshold, ok := value.(int64)
	if !ok {
		return nil, false, nil
	}
	resolver.rangeCalls++
	rows := make([]SQLRow, 0)
	for _, row := range resolver.rows {
		score, ok := row["score"].(int64)
		if !ok || !ch002RangeMatch(score, operator, threshold) {
			continue
		}
		resolver.visited++
		rows = append(rows, row)
	}
	return rows, true, nil
}

func (resolver *ch002SparseMarkResolver) StreamSQLOrderedSourceRange(ctx context.Context, _ string, _ string, _ string, _ bool, _ bool, _ bool, operator string, value interface{}, visit func(SQLRow) error) (bool, error) {
	threshold, ok := value.(int64)
	if !ok {
		return false, nil
	}
	resolver.rangeCalls++
	for _, row := range resolver.rows {
		score, ok := row["score"].(int64)
		if !ok || !ch002RangeMatch(score, operator, threshold) {
			continue
		}
		if err := ctx.Err(); err != nil {
			return true, err
		}
		resolver.visited++
		if err := visit(row); err != nil {
			return true, err
		}
	}
	return true, nil
}

func ch002RangeMatch(value int64, operator string, threshold int64) bool {
	switch operator {
	case "=":
		return value == threshold
	case "<":
		return value < threshold
	case "<=":
		return value <= threshold
	case ">":
		return value > threshold
	case ">=":
		return value >= threshold
	default:
		return false
	}
}

func ch002PrimaryMarkRows(count int) []SQLRow {
	rows := make([]SQLRow, count)
	for index := range rows {
		rows[index] = SQLRow{
			"id":    int64(index),
			"score": int64(index),
		}
	}
	return rows
}

func ch002PrimaryMarkQuery() string {
	return "FROM CACHE('items') AS item WHERE item.score >= 90000 ORDER BY item.score LIMIT 10 SELECT item.id, item.score"
}

func TestSQLOrderedRangePruningPreservesResults(t *testing.T) {
	rows := ch002PrimaryMarkRows(100_000)
	parsed, err := parseSQLQuery(ch002PrimaryMarkQuery())
	if err != nil {
		t.Fatalf("parse query: %v", err)
	}
	operator, value, matched := sqlOrderedRangePredicate(parsed)
	if !matched {
		t.Fatalf("ordered range predicate not matched: from=%#v order=%#v where=%#v", parsed.from, parsed.orderBy, parsed.where)
	}
	if operator != ">=" || value != int64(90000) {
		t.Fatalf("ordered range predicate = %q/%#v, want >=/90000", operator, value)
	}
	legacy := &ch002LegacyOrderedResolver{rows: rows}
	baseline, err := ExecuteSQLQueryParameters(context.Background(), ch002PrimaryMarkQuery(), legacy, nil, SQLQueryOptions{})
	if err != nil {
		t.Fatalf("execute baseline query: %v", err)
	}
	sparse := &ch002SparseMarkResolver{ch002LegacyOrderedResolver: ch002LegacyOrderedResolver{rows: rows}}
	optimized, err := ExecuteSQLQueryParameters(context.Background(), ch002PrimaryMarkQuery(), sparse, nil, SQLQueryOptions{})
	if err != nil {
		t.Fatalf("execute sparse-mark query: %v", err)
	}
	if fmt.Sprint(optimized.Rows) != fmt.Sprint(baseline.Rows) {
		t.Fatalf("optimized rows = %#v, baseline rows = %#v", optimized.Rows, baseline.Rows)
	}
	if sparse.rangeCalls != 1 {
		t.Fatalf("range calls = %d, want 1", sparse.rangeCalls)
	}
	if sparse.visited >= legacy.visited {
		t.Fatalf("sparse visited %d rows, baseline visited %d; want fewer", sparse.visited, legacy.visited)
	}
	if sparse.visited != 10_000 {
		t.Fatalf("sparse visited %d rows, want the 10,000-row selected range", sparse.visited)
	}
	legacyRows := &ch002LegacyOrderedResolver{rows: rows}
	sparseRows := &ch002SparseMarkResolver{ch002LegacyOrderedResolver: ch002LegacyOrderedResolver{rows: rows}}
	var baselineStreamed, optimizedStreamed []SQLRow
	if err := ExecuteSQLQueryRows(context.Background(), ch002PrimaryMarkQuery(), legacyRows, nil, SQLQueryOptions{}, func(_ []string, row SQLRow) error {
		baselineStreamed = append(baselineStreamed, row)
		return nil
	}); err != nil {
		t.Fatalf("execute baseline streamed query: %v", err)
	}
	if err := ExecuteSQLQueryRows(context.Background(), ch002PrimaryMarkQuery(), sparseRows, nil, SQLQueryOptions{}, func(_ []string, row SQLRow) error {
		optimizedStreamed = append(optimizedStreamed, row)
		return nil
	}); err != nil {
		t.Fatalf("execute sparse streamed query: %v", err)
	}
	if fmt.Sprint(optimizedStreamed) != fmt.Sprint(baselineStreamed) {
		t.Fatalf("optimized streamed rows = %#v, baseline streamed rows = %#v", optimizedStreamed, baselineStreamed)
	}
	if sparseRows.rangeCalls != 1 {
		t.Fatalf("stream range calls = %d, want 1", sparseRows.rangeCalls)
	}
	if sparseRows.visited >= legacyRows.visited {
		t.Fatalf("stream sparse visited %d rows, baseline visited %d; want fewer", sparseRows.visited, legacyRows.visited)
	}
	if sparseRows.visited != len(optimizedStreamed) {
		t.Fatalf("stream sparse visited %d rows, returned %d; want direct range scan", sparseRows.visited, len(optimizedStreamed))
	}
}
