package hatSql

import (
	"reflect"
	"testing"
)

func TestC213SQLHashJoinUsesTypedIndex(t *testing.T) {
	resolver := c212TypedJoinResolver{sources: map[string][]Row{
		"left": {
			{"id": int64(1), "key": "a"},
			{"id": int64(2), "key": true},
			{"id": int64(3), "key": nil},
			{"id": int64(4), "key": int64(7)},
		},
		"right": {
			{"id": int64(10), "key": "a"},
			{"id": int64(11), "key": "a"},
			{"id": int64(12), "key": true},
			{"id": int64(13), "key": int64(7)},
			{"id": int64(14), "key": nil},
		},
	}}
	query := "EXPLAIN ANALYZE FROM CACHE('left') AS l INNER JOIN CACHE('right') AS r ON l.key = r.key SELECT l.id, r.id AS right_id"
	result, err := ExecuteSQLQuery(query, resolver)
	if err != nil {
		t.Fatal(err)
	}
	foundTypedHashJoin := false
	for _, step := range result.Plan {
		if step.Node == "TYPED HASH JOIN" {
			foundTypedHashJoin = true
			break
		}
	}
	if !foundTypedHashJoin {
		t.Fatalf("plan = %#v, want TYPED HASH JOIN", result.Plan)
	}

	actual, err := ExecuteSQLQuery(query[len("EXPLAIN ANALYZE "):], resolver)
	if err != nil {
		t.Fatal(err)
	}
	want := []Row{
		{"id": int64(1), "right_id": int64(10)},
		{"id": int64(1), "right_id": int64(11)},
		{"id": int64(2), "right_id": int64(12)},
		{"id": int64(4), "right_id": int64(13)},
	}
	if !reflect.DeepEqual(actual.Rows, want) {
		t.Fatalf("rows = %#v, want %#v", actual.Rows, want)
	}
}

func TestC213SQLHashJoinPreservesLeftNullExtension(t *testing.T) {
	resolver := c212TypedJoinResolver{sources: map[string][]Row{
		"left": {
			{"id": int64(1), "key": "present"},
			{"id": int64(2), "key": "missing"},
			{"id": int64(3), "key": nil},
		},
		"right": {
			{"id": int64(10), "key": "present"},
		},
	}}
	result, err := ExecuteSQLQuery("FROM CACHE('left') AS l LEFT JOIN CACHE('right') AS r ON l.key = r.key SELECT l.id, r.id AS right_id", resolver)
	if err != nil {
		t.Fatal(err)
	}
	want := []Row{
		{"id": int64(1), "right_id": int64(10)},
		{"id": int64(2), "right_id": nil},
		{"id": int64(3), "right_id": nil},
	}
	if !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("rows = %#v, want %#v", result.Rows, want)
	}
}

var c213TypedHashJoinBenchmarkSink SQLQueryResult

func BenchmarkC213SQLHashJoin(b *testing.B) {
	left := make([]Row, 2048)
	for index := range left {
		left[index] = Row{"id": int64(index), "key": int64(index)}
	}
	right := make([]Row, 2048)
	for index := range right {
		right[index] = Row{"id": int64(index), "key": int64(index % 1024)}
	}
	resolver := c212TypedJoinResolver{sources: map[string][]Row{"left": left, "right": right}}
	query := "FROM CACHE('left') AS l INNER JOIN CACHE('right') AS r ON l.key = r.key SELECT l.id, r.id AS right_id"
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		result, err := ExecuteSQLQuery(query, resolver)
		if err != nil {
			b.Fatal(err)
		}
		c213TypedHashJoinBenchmarkSink = result
	}
}
