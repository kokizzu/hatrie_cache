package hatSql

import (
	"context"
	"reflect"
	"strings"
	"testing"
)

func BenchmarkCH003HashJoinBaseline(b *testing.B) {
	resolver := newCH003ParallelHashJoinResolver(16384)
	query := "FROM CACHE('left') AS l JOIN CACHE('right') AS r ON l.k = r.k SELECT l.id, r.id"
	options := SQLQueryOptions{MaxRows: 100000}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		result, err := ExecuteSQLQueryContext(context.Background(), query, resolver, options)
		if err != nil || len(result.Rows) != 16384 {
			b.Fatalf("hash join result rows=%d error=%v", len(result.Rows), err)
		}
	}
}

func BenchmarkCH003ParallelHashJoin(b *testing.B) {
	resolver := newCH003ParallelHashJoinResolver(16384)
	query := "FROM CACHE('left') AS l JOIN CACHE('right') AS r ON l.k = r.k SELECT l.id, r.id"
	options := SQLQueryOptions{MaxRows: 100000, JoinWorkers: 4}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		result, err := ExecuteSQLQueryContext(context.Background(), query, resolver, options)
		if err != nil || len(result.Rows) != 16384 {
			b.Fatalf("parallel hash join result rows=%d error=%v", len(result.Rows), err)
		}
	}
}

func TestCH003ParallelHashJoinMatchesDeterministicSerialOutput(t *testing.T) {
	resolver := ch003ParallelHashJoinResolver{
		left: []SQLRow{
			{"k": int64(2), "id": "left-2"},
			{"k": int64(1), "id": "left-1a"},
			{"k": int64(1), "id": "left-1b"},
			{"k": nil, "id": "left-null"},
			{"k": true, "id": "left-true"},
			{"k": "x", "id": "left-string"},
			{"k": []int64{1}, "id": "left-unsupported"},
		},
		right: []SQLRow{
			{"k": int64(1), "id": "right-1a"},
			{"k": int64(2), "id": "right-2"},
			{"k": int64(1), "id": "right-1b"},
			{"k": nil, "id": "right-null"},
			{"k": true, "id": "right-true"},
			{"k": "x", "id": "right-string"},
			{"k": []int64{1}, "id": "right-unsupported"},
		},
	}
	query := "FROM CACHE('left') AS l JOIN CACHE('right') AS r ON l.k = r.k SELECT l.id AS left_id, r.id AS right_id"
	serial, err := ExecuteSQLQueryContext(context.Background(), query, resolver, SQLQueryOptions{MaxRows: 100})
	if err != nil {
		t.Fatalf("serial hash join: %v", err)
	}
	for iteration := 0; iteration < 4; iteration++ {
		parallel, err := ExecuteSQLQueryContext(context.Background(), query, resolver, SQLQueryOptions{
			MaxRows:     100,
			JoinWorkers: 4,
		})
		if err != nil {
			t.Fatalf("parallel hash join iteration %d: %v", iteration, err)
		}
		if !reflect.DeepEqual(parallel, serial) {
			t.Fatalf("parallel hash join iteration %d = %#v, want %#v", iteration, parallel, serial)
		}
	}
	analysis, err := ExecuteSQLQueryContext(context.Background(), "EXPLAIN ANALYZE "+query, resolver, SQLQueryOptions{
		MaxRows:     100,
		JoinWorkers: 4,
	})
	if err != nil {
		t.Fatalf("parallel hash join explain: %v", err)
	}
	foundParallelPlan := false
	for _, step := range analysis.Plan {
		if strings.Contains(step.Node, "PARALLEL TYPED HASH JOIN") {
			foundParallelPlan = true
			break
		}
	}
	if !foundParallelPlan {
		t.Fatalf("parallel hash join plan = %#v, want parallel typed hash join", analysis.Plan)
	}
}

func TestCH003ParallelHashJoinSmallInputsPreserveMatch(t *testing.T) {
	resolver := ch003ParallelHashJoinResolver{
		left:  []SQLRow{{"k": int64(1), "id": "left"}},
		right: []SQLRow{{"k": int64(1), "id": "right"}},
	}
	query := "FROM CACHE('left') AS l JOIN CACHE('right') AS r ON l.k = r.k SELECT l.id, r.id"
	serial, err := ExecuteSQLQueryContext(context.Background(), query, resolver, SQLQueryOptions{MaxRows: 10})
	if err != nil {
		t.Fatalf("serial small join: %v", err)
	}
	parallel, err := ExecuteSQLQueryContext(context.Background(), query, resolver, SQLQueryOptions{MaxRows: 10, JoinWorkers: 4})
	if err != nil {
		t.Fatalf("parallel small join: %v", err)
	}
	if !reflect.DeepEqual(parallel, serial) {
		t.Fatalf("parallel small join = %#v, want %#v", parallel, serial)
	}
}

func TestCH003ParallelHashJoinEnforcesMaxRows(t *testing.T) {
	left := make([]SQLRow, 16)
	right := make([]SQLRow, 4)
	for index := range left {
		left[index] = SQLRow{"k": int64(1), "id": index}
	}
	for index := range right {
		right[index] = SQLRow{"k": int64(1), "id": index}
	}
	_, err := ExecuteSQLQueryContext(context.Background(), "FROM CACHE('left') AS l JOIN CACHE('right') AS r ON l.k = r.k SELECT l.id, r.id", ch003ParallelHashJoinResolver{
		left:  left,
		right: right,
	}, SQLQueryOptions{MaxRows: 20, JoinWorkers: 4})
	if err == nil || !strings.Contains(err.Error(), "SQL join exceeds the 20 row limit") {
		t.Fatalf("parallel row limit error = %v", err)
	}
}

type ch003ParallelHashJoinResolver struct {
	left  []SQLRow
	right []SQLRow
}

func (resolver ch003ParallelHashJoinResolver) ResolveSQLSource(_ string, key string) ([]SQLRow, error) {
	if key == "left" {
		return resolver.left, nil
	}
	return resolver.right, nil
}

func newCH003ParallelHashJoinResolver(count int) ch003ParallelHashJoinResolver {
	left := make([]SQLRow, count)
	right := make([]SQLRow, count)
	for index := range count {
		key := int64(index)
		left[index] = SQLRow{"k": key, "id": key}
		right[index] = SQLRow{"k": key, "id": key}
	}
	return ch003ParallelHashJoinResolver{left: left, right: right}
}
