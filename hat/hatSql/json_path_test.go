package hatSql_test

import (
	"strings"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestJSONPathExpressionsReadNestedObjectsAndArrays(t *testing.T) {
	result, err := hatSql.ExecuteSQLQuery(
		`FROM CACHE('docs') AS src SELECT JSON_VALUE(src.doc, '$.profile.city') AS city, JSON_VALUE(src.doc, '$.profile.tags[1]') AS tag, JSON_EXISTS(src.doc, '$.profile.missing') AS missing, JSON_QUERY(src.doc, '$.profile') AS profile`,
		testJSONPathResolver{},
	)
	if err != nil {
		t.Fatalf("ExecuteSQLQuery() error = %v", err)
	}
	if len(result.Rows) != 1 {
		t.Fatalf("len(result.Rows) = %d, want 1", len(result.Rows))
	}
	row := result.Rows[0]
	if row["city"] != "Singapore" || row["tag"] != "small" || row["missing"] != false {
		t.Fatalf("result row = %#v, want nested JSON values", row)
	}
	profile, ok := row["profile"].(map[string]interface{})
	if !ok || profile["city"] != "Singapore" {
		t.Fatalf("profile = %#v, want nested profile object", row["profile"])
	}
}

func TestJSONPathExpressionsRejectInvalidPath(t *testing.T) {
	_, err := hatSql.ExecuteSQLQuery(`FROM CACHE('docs') AS src SELECT JSON_VALUE(src.doc, 'profile.city')`, testJSONPathResolver{})
	if err == nil || !strings.Contains(err.Error(), "JSON path must start with $") {
		t.Fatalf("ExecuteSQLQuery() error = %v, want JSON path diagnostic", err)
	}
}

type testJSONPathResolver struct{}

func (testJSONPathResolver) ResolveSQLSource(name, key string) ([]hatSql.SQLRow, error) {
	if name != "CACHE" || key != "docs" {
		return nil, nil
	}
	return []hatSql.SQLRow{{"doc": map[string]interface{}{"profile": map[string]interface{}{"city": "Singapore", "tags": []interface{}{"fast", "small"}}}}}, nil
}
