package hatSql

import (
	"context"
	"reflect"
	"testing"
)

func BenchmarkCH001HashJoinBaseline(b *testing.B) {
	resolver := newCH001FastOrderedJoinBenchmarkResolver(4096)
	query := "FROM CACHE('left') AS l JOIN CACHE('right') AS r ON l.k = r.k SELECT l.id, r.id"
	options := SQLQueryOptions{MaxRows: 100000}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		result, err := ExecuteSQLQueryContext(context.Background(), query, resolver, options)
		if err != nil || len(result.Rows) != 4096 {
			b.Fatalf("hash join result rows=%d error=%v", len(result.Rows), err)
		}
	}
}

func BenchmarkCH001PartialMergeJoin(b *testing.B) {
	resolver := newCH001FastOrderedJoinBenchmarkResolver(4096)
	query := "FROM CACHE('left') AS l JOIN CACHE('right') AS r ON l.k = r.k SELECT l.id, r.id"
	options := SQLQueryOptions{MaxRows: 100000, JoinAlgorithm: SQLJoinAlgorithmPartialMerge}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		result, err := ExecuteSQLQueryContext(context.Background(), query, resolver, options)
		if err != nil || len(result.Rows) != 4096 {
			b.Fatalf("partial merge result rows = %d, err = %v", len(result.Rows), err)
		}
	}
}

func ch001JoinBenchmarkResolver(count int) SQLSourceResolver {
	left := make([]SQLRow, count)
	right := make([]SQLRow, count)
	for index := range count {
		key := int64(index)
		left[index] = SQLRow{"k": key, "id": key}
		right[index] = SQLRow{"k": key, "id": key}
	}
	return SourceResolverFunc(func(_ string, key string) ([]SQLRow, error) {
		if key == "left" {
			return left, nil
		}
		return right, nil
	})
}

func TestCH001PartialMergeJoinPreservesDuplicatesAndNullSemantics(t *testing.T) {
	resolver := ch001OrderedJoinResolver{
		left: []SQLRow{
			{"k": int64(1), "id": "left-1a"},
			{"k": int64(1), "id": "left-1b"},
			{"k": int64(2), "id": "left-2"},
			{"k": nil, "id": "left-null"},
		},
		right: []SQLRow{
			{"k": int64(1), "id": "right-1a"},
			{"k": int64(1), "id": "right-1b"},
			{"k": int64(3), "id": "right-3"},
			{"k": nil, "id": "right-null"},
		},
	}
	result, err := ExecuteSQLQueryContext(context.Background(), "FROM CACHE('left') AS l JOIN CACHE('right') AS r ON l.k = r.k SELECT l.id AS left_id, r.id AS right_id", resolver, SQLQueryOptions{
		MaxRows:       100,
		JoinAlgorithm: SQLJoinAlgorithmPartialMerge,
	})
	if err != nil {
		t.Fatalf("partial merge join: %v", err)
	}
	want := []SQLRow{
		{"left_id": "left-1a", "right_id": "right-1a"},
		{"left_id": "left-1a", "right_id": "right-1b"},
		{"left_id": "left-1b", "right_id": "right-1a"},
		{"left_id": "left-1b", "right_id": "right-1b"},
	}
	if !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("rows = %#v, want %#v", result.Rows, want)
	}
}

func TestCH001PartialMergeJoinFallsBackWithoutOrderedResolver(t *testing.T) {
	resolver := ch001JoinBenchmarkResolver(4)
	result, err := ExecuteSQLQueryContext(context.Background(), "FROM CACHE('left') AS l JOIN CACHE('right') AS r ON l.k = r.k SELECT l.id, r.id", resolver, SQLQueryOptions{
		MaxRows:       100,
		JoinAlgorithm: SQLJoinAlgorithmPartialMerge,
	})
	if err != nil {
		t.Fatalf("fallback join: %v", err)
	}
	if len(result.Rows) != 4 {
		t.Fatalf("fallback rows = %d, want 4", len(result.Rows))
	}
}

func TestCH001DefaultJoinAlgorithmDoesNotProbeOrderedResolver(t *testing.T) {
	orderedCalls := 0
	resolver := ch001OrderedJoinResolver{
		left:         []SQLRow{{"k": int64(1), "id": "left"}},
		right:        []SQLRow{{"k": int64(1), "id": "right"}},
		orderedCalls: &orderedCalls,
	}
	if _, err := ExecuteSQLQueryContext(context.Background(), "FROM CACHE('left') AS l JOIN CACHE('right') AS r ON l.k = r.k SELECT l.id, r.id", resolver, SQLQueryOptions{MaxRows: 100}); err != nil {
		t.Fatalf("default hash join: %v", err)
	}
	if orderedCalls != 0 {
		t.Fatalf("ordered resolver calls = %d, want 0", orderedCalls)
	}
}

func TestCH001PartialMergeJoinSortsUnorderedLeftRows(t *testing.T) {
	resolver := ch001OrderedJoinResolver{
		left:  []SQLRow{{"k": int64(2), "id": "left-2"}, {"k": int64(1), "id": "left-1"}},
		right: []SQLRow{{"k": int64(1), "id": "right-1"}, {"k": int64(2), "id": "right-2"}},
	}
	result, err := ExecuteSQLQueryContext(context.Background(), "FROM CACHE('left') AS l JOIN CACHE('right') AS r ON l.k = r.k SELECT l.id AS left_id, r.id AS right_id", resolver, SQLQueryOptions{
		MaxRows:       100,
		JoinAlgorithm: SQLJoinAlgorithmPartialMerge,
	})
	if err != nil {
		t.Fatalf("partial merge join: %v", err)
	}
	want := []SQLRow{
		{"left_id": "left-2", "right_id": "right-2"},
		{"left_id": "left-1", "right_id": "right-1"},
	}
	if !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("rows = %#v, want %#v", result.Rows, want)
	}
}

type ch001OrderedJoinResolver struct {
	left         []SQLRow
	right        []SQLRow
	orderedCalls *int
}

func (r ch001OrderedJoinResolver) ResolveSQLSource(_ string, key string) ([]SQLRow, error) {
	if key == "left" {
		return append([]SQLRow(nil), r.left...), nil
	}
	return append([]SQLRow(nil), r.right...), nil
}

func (r ch001OrderedJoinResolver) ResolveSQLOrderedSource(_ string, key, _ string, _, _, _ bool) ([]SQLRow, bool, error) {
	if r.orderedCalls != nil {
		*r.orderedCalls++
	}
	rows, err := r.ResolveSQLSource("CACHE", key)
	return rows, true, err
}

type ch001FastOrderedJoinResolver struct {
	left  []SQLRow
	right []SQLRow
}

func newCH001FastOrderedJoinBenchmarkResolver(count int) SQLSourceResolver {
	left := make([]SQLRow, count)
	right := make([]SQLRow, count)
	for index := range count {
		key := int64(index)
		left[index] = SQLRow{"k": key, "id": key}
		right[index] = SQLRow{"k": key, "id": key}
	}
	return ch001FastOrderedJoinResolver{left: left, right: right}
}

func (r ch001FastOrderedJoinResolver) ResolveSQLSource(_ string, key string) ([]SQLRow, error) {
	if key == "left" {
		return r.left, nil
	}
	return r.right, nil
}

func (r ch001FastOrderedJoinResolver) ResolveSQLOrderedSource(_ string, key, _ string, _, _, _ bool) ([]SQLRow, bool, error) {
	rows, err := r.ResolveSQLSource("CACHE", key)
	return rows, true, err
}
