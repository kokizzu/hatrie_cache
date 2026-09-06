package hatSql

import (
	"reflect"
	"testing"
)

func TestSQLArrayAndMapAggregatesPreserveGroupedValues(t *testing.T) {
	rows := []Row{
		{"group": "a", "value": int64(1), "name": "first"},
		{"group": "a", "value": int64(2), "name": "second"},
		{"group": "a", "value": int64(2), "name": "replacement"},
		{"group": "b", "value": int64(3), "name": "third"},
	}
	result, err := ExecuteSQLQuery(`
		SELECT group, ARRAY_AGG(value) AS values,
		       GROUP_ARRAY(value) AS grouped_values,
		       GROUP_UNIQ_ARRAY(value) AS unique_values,
		       MAP_AGG(CAST(value AS TEXT), name) AS names
		FROM CACHE('items')
		GROUP BY group
		ORDER BY group`, SourceResolverFunc(func(string, string) ([]Row, error) { return rows, nil }))
	if err != nil {
		t.Fatal(err)
	}
	want := []Row{
		{
			"group":          "a",
			"values":         []interface{}{int64(1), int64(2), int64(2)},
			"grouped_values": []interface{}{int64(1), int64(2), int64(2)},
			"unique_values":  []interface{}{int64(1), int64(2)},
			"names":          map[interface{}]interface{}{"1": "first", "2": "replacement"},
		},
		{
			"group":          "b",
			"values":         []interface{}{int64(3)},
			"grouped_values": []interface{}{int64(3)},
			"unique_values":  []interface{}{int64(3)},
			"names":          map[interface{}]interface{}{"3": "third"},
		},
	}
	if !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("aggregate rows = %#v, want %#v", result.Rows, want)
	}
}

func TestSQLArrayAggregateFilterAndNullSemantics(t *testing.T) {
	rows := []Row{{"value": nil}, {"value": int64(1)}, {"value": int64(2)}}
	result, err := ExecuteSQLQuery(`
		SELECT ARRAY_AGG(value) FILTER (WHERE value IS NOT NULL) AS filtered,
		       GROUP_UNIQ_ARRAY(value) AS unique_values
		FROM CACHE('items')`, SourceResolverFunc(func(string, string) ([]Row, error) { return rows, nil }))
	if err != nil {
		t.Fatal(err)
	}
	want := Row{
		"filtered":      []interface{}{int64(1), int64(2)},
		"unique_values": []interface{}{nil, int64(1), int64(2)},
	}
	if len(result.Rows) != 1 || !reflect.DeepEqual(result.Rows[0], want) {
		t.Fatalf("aggregate row = %#v, want %#v", result.Rows, want)
	}
}

func TestSQLMapAggregateRejectsNullKeys(t *testing.T) {
	rows := []Row{{"key": nil, "value": "bad"}}
	if _, err := ExecuteSQLQuery(`SELECT MAP_AGG(key, value) FROM CACHE('items')`, SourceResolverFunc(func(string, string) ([]Row, error) { return rows, nil })); err == nil {
		t.Fatal("MAP_AGG accepted a NULL key")
	}
}

func TestSQLCollectionAggregateArity(t *testing.T) {
	rows := []Row{{"value": int64(1), "key": "a"}}
	resolver := SourceResolverFunc(func(string, string) ([]Row, error) { return rows, nil })
	for _, query := range []string{
		`SELECT ARRAY_AGG() FROM CACHE('items')`,
		`SELECT MAP_AGG(key) FROM CACHE('items')`,
	} {
		if _, err := ExecuteSQLQuery(query, resolver); err == nil {
			t.Fatalf("query %q unexpectedly succeeded", query)
		}
	}
}

func BenchmarkSQLAggregateCollections(b *testing.B) {
	rows := make([]Row, 1024)
	for index := range rows {
		rows[index] = Row{
			"group": int64(index % 64),
			"value": int64(index),
			"name":  "value",
		}
	}
	resolver := SourceResolverFunc(func(string, string) ([]Row, error) { return rows, nil })
	query := `
		SELECT group, ARRAY_AGG(value) AS values,
		       MAP_AGG(CAST(value AS TEXT), name) AS names
		FROM CACHE('items')
		GROUP BY group`
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := ExecuteSQLQuery(query, resolver); err != nil {
			b.Fatal(err)
		}
	}
}
