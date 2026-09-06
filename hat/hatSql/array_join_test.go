package hatSql

import "reflect"

import "testing"

func TestSQLArrayJoinExpandsRowsAndPreservesOrder(t *testing.T) {
	rows := []Row{
		{"id": int64(1), "tags": []interface{}{"a", "b"}},
		{"id": int64(2), "tags": nil},
		{"id": int64(3), "tags": []string{"c"}},
		{"id": int64(4), "tags": []interface{}{}},
	}
	result, err := ExecuteSQLQuery(`
		FROM CACHE('items')
		ARRAY JOIN tags AS tag
		SELECT id, tag
		ORDER BY id, tag`, SourceResolverFunc(func(string, string) ([]Row, error) { return rows, nil }))
	if err != nil {
		t.Fatal(err)
	}
	want := []Row{
		{"id": int64(1), "tag": "a"},
		{"id": int64(1), "tag": "b"},
		{"id": int64(3), "tag": "c"},
	}
	if !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("array join rows = %#v, want %#v", result.Rows, want)
	}
}

func TestSQLArrayJoinRejectsScalarValues(t *testing.T) {
	rows := []Row{{"id": int64(1), "tags": "not-an-array"}}
	_, err := ExecuteSQLQuery(`FROM CACHE('items') ARRAY JOIN tags AS tag SELECT id, tag`, SourceResolverFunc(func(string, string) ([]Row, error) { return rows, nil }))
	if err == nil {
		t.Fatal("ARRAY JOIN accepted a scalar value")
	}
}

func TestSQLArrayJoinUsesFieldNameWithoutAlias(t *testing.T) {
	rows := []Row{{"id": int64(1), "tags": []int64{7, 8}}}
	result, err := ExecuteSQLQuery(`FROM CACHE('items') ARRAY JOIN tags SELECT id, tags ORDER BY tags`, SourceResolverFunc(func(string, string) ([]Row, error) { return rows, nil }))
	if err != nil {
		t.Fatal(err)
	}
	want := []Row{{"id": int64(1), "tags": int64(7)}, {"id": int64(1), "tags": int64(8)}}
	if !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("array join rows without alias = %#v, want %#v", result.Rows, want)
	}
}

func TestSQLArrayJoinExplainPlan(t *testing.T) {
	result, err := ExecuteSQLQuery(`EXPLAIN FROM CACHE('items') ARRAY JOIN tags AS tag SELECT id, tag`, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, step := range result.Plan {
		if step.Node == "ARRAY JOIN" && step.Detail == "tags AS tag" {
			return
		}
	}
	t.Fatalf("plan = %#v, want ARRAY JOIN tags AS tag", result.Plan)
}

func BenchmarkSQLArrayJoin(b *testing.B) {
	rows := make([]Row, 1024)
	for index := range rows {
		rows[index] = Row{
			"id":   int64(index),
			"tags": []string{"a", "b", "c", "d"},
		}
	}
	resolver := SourceResolverFunc(func(string, string) ([]Row, error) { return rows, nil })
	query := `FROM CACHE('items') ARRAY JOIN tags AS tag SELECT id, tag`
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		result, err := ExecuteSQLQuery(query, resolver)
		if err != nil {
			b.Fatal(err)
		}
		if len(result.Rows) != len(rows)*4 {
			b.Fatalf("array join rows = %d, want %d", len(result.Rows), len(rows)*4)
		}
	}
}
