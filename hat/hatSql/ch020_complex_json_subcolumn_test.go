package hatSql_test

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"

	"hatrie_cache/hat/hatSql"
)

type chu20JSONResolver struct {
	rows      []hatSql.Row
	batch     hatSql.ColumnarBatch
	available bool
}

func (resolver *chu20JSONResolver) ResolveSQLSource(string, string) ([]hatSql.Row, error) {
	return resolver.rows, nil
}

func (*chu20JSONResolver) ResolveSQLColumnarSource(string, string, []string) (hatSql.ColumnarBatch, bool, error) {
	return hatSql.ColumnarBatch{}, false, nil
}

func (resolver *chu20JSONResolver) ResolveSQLColumnarJSONSubcolumns(string, string, []string, []hatSql.ColumnarJSONSubcolumnRequest) (hatSql.ColumnarBatch, *hatSql.ColumnarNumericSegments, bool, error) {
	return resolver.batch, nil, resolver.available, nil
}

func TestCHU20ComplexJSONSubcolumnPreservesJSONQuerySemantics(t *testing.T) {
	documents := []interface{}{
		`{"user":{"name":"Ada","roles":["admin","ops"]}}`,
		`{"user":{"name":"Lin","roles":[]}}`,
		`{"other":1}`,
		`{"user":null}`,
	}
	column, err := hatSql.MaterializeJSONSubcolumn("$.user", documents)
	if err != nil {
		t.Fatalf("MaterializeJSONSubcolumn() error = %v", err)
	}
	if column.Kind != hatSql.ColumnarJSONSubcolumnJSON {
		t.Fatalf("column kind = %v, want JSON", column.Kind)
	}
	if len(column.JSONOffsets) != len(documents)+1 {
		t.Fatalf("JSON offsets = %d, want %d", len(column.JSONOffsets), len(documents)+1)
	}
	roles, err := hatSql.MaterializeJSONSubcolumn("$.user.roles", documents)
	if err != nil {
		t.Fatalf("MaterializeJSONSubcolumn(roles) error = %v", err)
	}
	if roles.Kind != hatSql.ColumnarJSONSubcolumnJSON {
		t.Fatalf("roles column kind = %v, want JSON", roles.Kind)
	}

	rows := make([]hatSql.Row, len(documents))
	for row, document := range documents {
		rows[row] = hatSql.Row{"doc": document}
	}
	query := "SELECT JSON_QUERY(doc, '$.user') AS user, JSON_QUERY(doc, '$.user.roles') AS roles, JSON_EXISTS(doc, '$.user') AS present FROM CACHE('items')"
	baseline, err := hatSql.ExecuteSQLQueryParameters(context.Background(), query, &chu20JSONResolver{rows: rows}, nil, hatSql.SQLQueryOptions{})
	if err != nil {
		t.Fatalf("baseline query error = %v", err)
	}
	optimized, err := hatSql.ExecuteSQLQueryParameters(context.Background(), query, &chu20JSONResolver{
		rows: rows,
		batch: hatSql.ColumnarBatch{
			Rows: len(rows),
			JSONSubcolumns: map[hatSql.ColumnarJSONSubcolumnKey]hatSql.ColumnarJSONSubcolumn{
				{Field: "doc", Path: "$.user"}:       column,
				{Field: "doc", Path: "$.user.roles"}: roles,
			},
		},
		available: true,
	}, nil, hatSql.SQLQueryOptions{})
	if err != nil {
		t.Fatalf("optimized query error = %v", err)
	}
	if !reflect.DeepEqual(optimized.Rows, baseline.Rows) {
		t.Fatalf("optimized rows = %#v, baseline rows = %#v", optimized.Rows, baseline.Rows)
	}

	var decoded map[string]interface{}
	if value, present := column.Value(0); !present {
		t.Fatal("complex row 0 is missing")
	} else if raw, ok := value.(json.RawMessage); !ok || json.Unmarshal(raw, &decoded) != nil || decoded["name"] != "Ada" {
		t.Fatalf("complex row 0 value = %#v", value)
	}
}

func TestCHU20ComplexJSONSubcolumnKeepsJSONValueError(t *testing.T) {
	documents := []interface{}{`{"user":{"name":"Ada"}}`}
	column, err := hatSql.MaterializeJSONSubcolumn("$.user", documents)
	if err != nil {
		t.Fatalf("MaterializeJSONSubcolumn() error = %v", err)
	}
	resolver := &chu20JSONResolver{
		rows: []hatSql.Row{{"doc": documents[0]}},
		batch: hatSql.ColumnarBatch{
			Rows: 1,
			JSONSubcolumns: map[hatSql.ColumnarJSONSubcolumnKey]hatSql.ColumnarJSONSubcolumn{
				{Field: "doc", Path: "$.user"}: column,
			},
		},
		available: true,
	}
	query := "SELECT JSON_VALUE(doc, '$.user') FROM CACHE('items')"
	_, baselineErr := hatSql.ExecuteSQLQueryParameters(context.Background(), query, &chu20JSONResolver{rows: resolver.rows}, nil, hatSql.SQLQueryOptions{})
	_, err = hatSql.ExecuteSQLQueryParameters(context.Background(), query, resolver, nil, hatSql.SQLQueryOptions{})
	if baselineErr == nil || err == nil {
		t.Fatal("JSON_VALUE unexpectedly accepted a complex subcolumn")
	}
	if err.Error() != baselineErr.Error() {
		t.Fatalf("optimized JSON_VALUE error = %q, baseline = %q", err, baselineErr)
	}
}

func TestCHU20ComplexJSONSubcolumnHonorsAutomaticRetentionLimit(t *testing.T) {
	materializer, err := hatSql.NewJSONSubcolumnAutoMaterializer(hatSql.JSONSubcolumnAutoMaterializerOptions{
		MinObservations: 1,
		MaxBytes:        1,
	})
	if err != nil {
		t.Fatal(err)
	}
	key := hatSql.JSONSubcolumnAutoKey{Field: "doc", Path: "$.user", Generation: 1}
	if _, ready, err := materializer.Observe(key, []interface{}{`{"user":{"name":"Ada"}}`}); err != nil || ready {
		t.Fatalf("complex retention-limited Observe() = ready %v, err %v", ready, err)
	}
	if stats := materializer.Stats(); stats.Rejections != 1 || stats.RetainedBytes != 0 {
		t.Fatalf("complex retention stats = %#v", stats)
	}
}

func TestCHU20ComplexJSONSubcolumnRejectsExplicitScalarPayload(t *testing.T) {
	if _, err := hatSql.NewColumnarJSONSubcolumnOfKind(hatSql.ColumnarJSONSubcolumnJSON, []hatSql.ColumnarJSONSubcolumnValue{{
		Present: true,
		Value:   int64(1),
	}}); err == nil {
		t.Fatal("complex JSON kind accepted a scalar payload")
	}
}
