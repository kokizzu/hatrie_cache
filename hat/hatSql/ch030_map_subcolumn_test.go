package hatSql

import (
	"reflect"
	"strings"
	"testing"
)

type ch030MapSubcolumnResolver struct {
	rows              []Row
	columnarCalls     int
	mapSubcolumnCalls int
	requestedFields   []string
	requestedPaths    []ColumnarMapSubcolumn
}

func (resolver *ch030MapSubcolumnResolver) ResolveSQLSource(string, string) ([]Row, error) {
	return resolver.rows, nil
}

func (resolver *ch030MapSubcolumnResolver) ResolveSQLColumnarSource(_, _ string, fields []string) (ColumnarBatch, bool, error) {
	resolver.columnarCalls++
	resolver.requestedFields = append([]string(nil), fields...)
	columns := make(map[string][]interface{}, len(fields))
	for _, field := range fields {
		values := make([]interface{}, len(resolver.rows))
		for index, row := range resolver.rows {
			values[index] = row[field]
		}
		columns[field] = values
	}
	return ColumnarBatch{Columns: columns, Rows: len(resolver.rows)}, true, nil
}

func (resolver *ch030MapSubcolumnResolver) ResolveSQLColumnarMapSubcolumns(_, _ string, fields []string, paths []ColumnarMapSubcolumn) (ColumnarBatch, *ColumnarNumericSegments, bool, error) {
	resolver.mapSubcolumnCalls++
	resolver.requestedFields = append([]string(nil), fields...)
	resolver.requestedPaths = append([]ColumnarMapSubcolumn(nil), paths...)
	columns := make(map[string][]interface{}, len(fields))
	for _, field := range fields {
		if field == "doc" {
			continue
		}
		values := make([]interface{}, len(resolver.rows))
		for index, row := range resolver.rows {
			values[index] = row[field]
		}
		columns[field] = values
	}
	mapRows := make([]map[string]interface{}, len(resolver.rows))
	for rowIndex, row := range resolver.rows {
		mapRows[rowIndex] = make(map[string]interface{}, len(paths))
		for _, path := range paths {
			value, exists, err := JSONPathValue(row[path.Field], path.Path)
			if err != nil {
				return ColumnarBatch{}, nil, false, err
			}
			if exists {
				key := strings.TrimPrefix(path.Path, "$.")
				mapRows[rowIndex][key] = value
			}
		}
	}
	column, err := NewColumnarMapColumn(mapRows)
	if err != nil {
		return ColumnarBatch{}, nil, false, err
	}
	return ColumnarBatch{
		Columns:    columns,
		MapColumns: map[string]ColumnarMapColumn{"doc": column},
		Rows:       len(resolver.rows),
	}, nil, true, nil
}

type ch030FullMapResolver struct {
	rows []Row
}

func (resolver ch030FullMapResolver) ResolveSQLSource(string, string) ([]Row, error) {
	return resolver.rows, nil
}

func TestCH030MapColumnLookupPreservesMissingAndNull(t *testing.T) {
	column, err := NewColumnarMapColumn([]map[string]interface{}{
		{"present": "value", "null_value": nil},
		{},
	})
	if err != nil {
		t.Fatal(err)
	}
	if value, exists := column.Lookup(0, "present"); !exists || value != "value" {
		t.Fatalf("present lookup = %#v, %v", value, exists)
	}
	if value, exists := column.Lookup(0, "null_value"); !exists || value != nil {
		t.Fatalf("null lookup = %#v, %v", value, exists)
	}
	if value, exists := column.Lookup(0, "missing"); exists || value != nil {
		t.Fatalf("missing lookup = %#v, %v", value, exists)
	}
	if value, exists := column.Lookup(1, "present"); exists || value != nil {
		t.Fatalf("empty-row lookup = %#v, %v", value, exists)
	}
	batch := ColumnarBatch{MapColumns: map[string]ColumnarMapColumn{"doc": column}, Rows: 2}
	if got, ok := batch.Value("doc", 0); !ok || !reflect.DeepEqual(got, map[string]interface{}{"present": "value", "null_value": nil}) {
		t.Fatalf("batch map value = %#v/%v", got, ok)
	}
	if got := batch.FieldRows("doc"); got != 2 {
		t.Fatalf("FieldRows(doc) = %d, want 2", got)
	}
}

func TestCH030MapColumnRejectsMalformedLayout(t *testing.T) {
	if err := (ColumnarMapColumn{Offsets: []uint32{0, 1}, Keys: []string{"a"}}).Validate(1); err == nil {
		t.Fatal("malformed map column was accepted")
	}
	if err := (ColumnarMapColumn{Offsets: []uint32{0, 2}, Keys: []string{"a", "a"}, Values: []interface{}{int64(1), int64(2)}}).Validate(1); err == nil {
		t.Fatal("duplicate map keys were accepted")
	}
}

func TestCH030MapSubcolumnQueryUsesSelectedPathsAndMatchesRowExecution(t *testing.T) {
	rows := []Row{
		{"id": int64(1), "doc": map[string]interface{}{"country": "SG", "large": strings.Repeat("x", 2048)}},
		{"id": int64(2), "doc": map[string]interface{}{"country": "US", "large": strings.Repeat("y", 2048)}},
		{"id": int64(3), "doc": nil},
		{"id": int64(4), "doc": map[string]interface{}{"country": nil, "large": strings.Repeat("z", 2048)}},
		{"id": int64(5), "doc": map[string]interface{}{"large": strings.Repeat("w", 2048)}},
	}
	query := "FROM CACHE('docs') AS src WHERE JSON_VALUE(src.doc, '$.country') = 'SG' SELECT id, JSON_VALUE(src.doc, '$.country') AS country, JSON_EXISTS(src.doc, '$.country') AS country_exists"
	optimizedResolver := &ch030MapSubcolumnResolver{rows: rows}
	optimized, err := ExecuteSQLQuery(query, optimizedResolver)
	if err != nil {
		t.Fatal(err)
	}
	baseline, err := ExecuteSQLQuery(query, ch030FullMapResolver{rows: rows})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(optimized.Rows, baseline.Rows) {
		t.Fatalf("optimized rows = %#v, baseline rows = %#v", optimized.Rows, baseline.Rows)
	}
	if want := []Row{{"id": int64(1), "country": "SG", "country_exists": true}}; !reflect.DeepEqual(optimized.Rows, want) {
		t.Fatalf("rows = %#v, want %#v", optimized.Rows, want)
	}
	if optimizedResolver.mapSubcolumnCalls != 1 || optimizedResolver.columnarCalls != 0 {
		t.Fatalf("resolver calls = map:%d columnar:%d, want map:1 columnar:0", optimizedResolver.mapSubcolumnCalls, optimizedResolver.columnarCalls)
	}
	if !reflect.DeepEqual(optimizedResolver.requestedFields, []string{"doc", "id"}) {
		t.Fatalf("requested fields = %#v, want [doc id]", optimizedResolver.requestedFields)
	}
	if !reflect.DeepEqual(optimizedResolver.requestedPaths, []ColumnarMapSubcolumn{{Field: "doc", Path: "$.country"}}) {
		t.Fatalf("requested paths = %#v", optimizedResolver.requestedPaths)
	}
}

func TestCH030MapSubcolumnQueryPreservesNullAndMissingResults(t *testing.T) {
	rows := []Row{
		{"id": int64(1), "doc": map[string]interface{}{"country": "SG"}},
		{"id": int64(2), "doc": map[string]interface{}{"country": nil}},
		{"id": int64(3), "doc": nil},
		{"id": int64(4), "doc": map[string]interface{}{}},
	}
	resolver := &ch030MapSubcolumnResolver{rows: rows}
	result, err := ExecuteSQLQuery("FROM CACHE('docs') AS src SELECT id, JSON_VALUE(src.doc, '$.country') AS country, JSON_EXISTS(src.doc, '$.country') AS country_exists", resolver)
	if err != nil {
		t.Fatal(err)
	}
	want := []Row{
		{"id": int64(1), "country": "SG", "country_exists": true},
		{"id": int64(2), "country": nil, "country_exists": true},
		{"id": int64(3), "country": nil, "country_exists": false},
		{"id": int64(4), "country": nil, "country_exists": false},
	}
	if !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("rows = %#v, want %#v", result.Rows, want)
	}
}

func TestCH030UnsupportedMapPathFallsBackToRowExecution(t *testing.T) {
	rows := []Row{{"doc": map[string]interface{}{"profile": map[string]interface{}{"country": "SG"}}}}
	resolver := &ch030MapSubcolumnResolver{rows: rows}
	result, err := ExecuteSQLQuery("FROM CACHE('docs') AS src SELECT JSON_VALUE(src.doc, '$.profile.country') AS country", resolver)
	if err != nil {
		t.Fatal(err)
	}
	if want := []Row{{"country": "SG"}}; !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("rows = %#v, want %#v", result.Rows, want)
	}
	if resolver.mapSubcolumnCalls != 0 {
		t.Fatalf("map subcolumn calls = %d, want fallback", resolver.mapSubcolumnCalls)
	}
}
