package hatSql

import (
	"reflect"
	"testing"
)

func TestCH037ArrayJoinTraversesNestedRowElements(t *testing.T) {
	rows := []Row{{
		"id": int64(1),
		"groups": []Row{
			{"name": "alpha", "tags": []string{"x", "y"}},
			{"name": "beta", "tags": []string{"z"}},
		},
	}}
	result, err := ExecuteSQLQuery(`
		FROM CACHE('items')
		ARRAY JOIN groups AS group
		ARRAY JOIN group.tags AS tag
		SELECT id, group.name AS group_name, tag
		ORDER BY id, group_name, tag`, SourceResolverFunc(func(string, string) ([]Row, error) {
		return rows, nil
	}))
	if err != nil {
		t.Fatalf("nested ARRAY JOIN: %v", err)
	}
	want := []Row{
		{"id": int64(1), "group_name": "alpha", "tag": "x"},
		{"id": int64(1), "group_name": "alpha", "tag": "y"},
		{"id": int64(1), "group_name": "beta", "tag": "z"},
	}
	if !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("nested ARRAY JOIN rows = %#v, want %#v", result.Rows, want)
	}
}

func TestCH037ArrayJoinTraversesStringKeyedMapElements(t *testing.T) {
	rows := []Row{{
		"groups": []map[string]interface{}{{"name": "alpha"}, {"name": "beta"}},
	}}
	result, err := ExecuteSQLQuery(`
		FROM CACHE('items')
		ARRAY JOIN groups AS group
		SELECT group.name AS group_name
		ORDER BY group_name`, SourceResolverFunc(func(string, string) ([]Row, error) {
		return rows, nil
	}))
	if err != nil {
		t.Fatalf("map-element ARRAY JOIN: %v", err)
	}
	want := []Row{{"group_name": "alpha"}, {"group_name": "beta"}}
	if !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("map-element ARRAY JOIN rows = %#v, want %#v", result.Rows, want)
	}
}

func BenchmarkCH037NestedArrayJoin(b *testing.B) {
	rows := make([]Row, 1024)
	for index := range rows {
		rows[index] = Row{
			"id": int64(index),
			"groups": []Row{
				{"name": "alpha", "tags": []string{"x", "y"}},
				{"name": "beta", "tags": []string{"z", "w"}},
			},
		}
	}
	resolver := SourceResolverFunc(func(string, string) ([]Row, error) { return rows, nil })
	query := `FROM CACHE('items') ARRAY JOIN groups AS group ARRAY JOIN group.tags AS tag SELECT id, group.name AS group_name, tag`
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		result, err := ExecuteSQLQuery(query, resolver)
		if err != nil {
			b.Fatal(err)
		}
		if len(result.Rows) != len(rows)*4 {
			b.Fatalf("nested ARRAY JOIN rows = %d, want %d", len(result.Rows), len(rows)*4)
		}
	}
}
