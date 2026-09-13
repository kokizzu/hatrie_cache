package hatSql

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestSQLBitmapAggregateAndSetFunctions(t *testing.T) {
	rows := []Row{
		{"group": "a", "value": int64(1)},
		{"group": "a", "value": int64(2)},
		{"group": "a", "value": int64(2)},
		{"group": "a", "value": int64(3)},
		{"group": "b", "value": int64(2)},
		{"group": "b", "value": int64(3)},
		{"group": "b", "value": int64(4)},
	}
	result, err := ExecuteSQLQuery(`
		SELECT BITMAP_COUNT(BITMAP_AGG(value)) AS cardinality,
		       BITMAP_CONTAINS(BITMAP_AGG(value), 2) AS has_two,
		       BITMAP_CONTAINS(BITMAP_AGG(value), 9) AS has_nine,
		       BITMAP_COUNT(BITMAP_AGG(value) FILTER (WHERE value >= 3)) AS filtered_cardinality,
		       BITMAP_COUNT(BITMAP_OR(
			       BITMAP_AGG(CASE WHEN group = 'a' THEN value END),
			       BITMAP_AGG(CASE WHEN group = 'b' THEN value END))) AS union_cardinality,
		       BITMAP_COUNT(BITMAP_AND(
			       BITMAP_AGG(CASE WHEN group = 'a' THEN value END),
			       BITMAP_AGG(CASE WHEN group = 'b' THEN value END))) AS intersection_cardinality,
		       BITMAP_COUNT(BITMAP_XOR(
			       BITMAP_AGG(CASE WHEN group = 'a' THEN value END),
			       BITMAP_AGG(CASE WHEN group = 'b' THEN value END))) AS xor_cardinality,
		       BITMAP_AGG(value) AS bitmap
		FROM CACHE('items')`, SourceResolverFunc(func(string, string) ([]Row, error) { return rows, nil }))
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 1 {
		t.Fatalf("bitmap result rows = %#v, want one row", result.Rows)
	}
	row := result.Rows[0]
	want := Row{
		"cardinality":              int64(4),
		"has_two":                  true,
		"has_nine":                 false,
		"filtered_cardinality":     int64(2),
		"union_cardinality":        int64(4),
		"intersection_cardinality": int64(2),
		"xor_cardinality":          int64(2),
	}
	for key, expected := range want {
		if !reflect.DeepEqual(row[key], expected) {
			t.Fatalf("%s = %#v, want %#v", key, row[key], expected)
		}
	}
	encoded, err := json.Marshal(row["bitmap"])
	if err != nil {
		t.Fatal(err)
	}
	if string(encoded) != "[1,2,3,4]" {
		t.Fatalf("bitmap JSON = %s, want [1,2,3,4]", encoded)
	}
}

func TestSQLBitmapFunctionsRejectInvalidValues(t *testing.T) {
	validRows := []Row{{"value": int64(1)}}
	for _, fixture := range []struct {
		name  string
		rows  []Row
		query string
	}{
		{
			name:  "negative aggregate value",
			rows:  []Row{{"value": int64(-1)}},
			query: `SELECT BITMAP_AGG(value) FROM CACHE('items')`,
		},
		{
			name:  "wrong bitmap input",
			rows:  validRows,
			query: `SELECT BITMAP_COUNT(value) FROM CACHE('items')`,
		},
		{
			name:  "negative membership value",
			rows:  validRows,
			query: `SELECT BITMAP_CONTAINS(BITMAP_AGG(value), -1) FROM CACHE('items')`,
		},
	} {
		t.Run(fixture.name, func(t *testing.T) {
			_, err := ExecuteSQLQuery(fixture.query, SourceResolverFunc(func(string, string) ([]Row, error) { return fixture.rows, nil }))
			if err == nil {
				t.Fatalf("query %q unexpectedly succeeded", fixture.query)
			}
		})
	}
}

func TestSQLBitmapIntersectionUsesTheSmallerOperand(t *testing.T) {
	left := NewSQLBitmap()
	left.Add(1, 2, 3)
	right := NewSQLBitmap()
	right.Add(2, 4)
	intersection := sqlCombineBitmaps("BITMAP_AND", left, right)
	if !reflect.DeepEqual(intersection.Values(), []uint32{2}) {
		t.Fatalf("intersection = %#v, want [2]", intersection.Values())
	}
}

func BenchmarkSQLBitmapCardinality(b *testing.B) {
	rows := make([]Row, 16384)
	for index := range rows {
		rows[index] = Row{
			"group": int64(index % 64),
			"value": int64(index % 4096),
		}
	}
	resolver := SourceResolverFunc(func(string, string) ([]Row, error) { return rows, nil })
	queries := []struct {
		name  string
		query string
	}{
		{
			name: "group-uniq-array",
			query: `
				SELECT group, GROUP_UNIQ_ARRAY(value) AS values
				FROM CACHE('items')
				GROUP BY group`,
		},
		{
			name: "bitmap-cardinality",
			query: `
				SELECT group, BITMAP_COUNT(BITMAP_AGG(value)) AS cardinality
				FROM CACHE('items')
				GROUP BY group`,
		},
	}
	for _, item := range queries {
		b.Run(item.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if _, err := ExecuteSQLQuery(item.query, resolver); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
