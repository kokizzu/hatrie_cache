package hatSql_test

import (
	"strings"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestSQLRegexPredicatesAndExtraction(t *testing.T) {
	result, err := hatSql.ExecuteSQLQuery(
		`FROM CACHE('logs') AS src WHERE src.message REGEXP '^error' SELECT src.message, REGEXP_LIKE(src.message, '^error') AS matches, REGEXP_EXTRACT(src.detail, 'id=([0-9]+)', 1) AS id`,
		testRegexResolver{},
	)
	if err != nil {
		t.Fatalf("ExecuteSQLQuery() error = %v", err)
	}
	if len(result.Rows) != 1 {
		t.Fatalf("rows = %#v, want one matching row", result.Rows)
	}
	row := result.Rows[0]
	if row["message"] != "error opening socket" || row["matches"] != true || row["id"] != "42" {
		t.Fatalf("row = %#v, want predicate and extracted capture", row)
	}

	notResult, err := hatSql.ExecuteSQLQuery(`FROM CACHE('logs') AS src WHERE src.message NOT REGEXP '^error' SELECT src.message`, testRegexResolver{})
	if err != nil {
		t.Fatalf("NOT REGEXP query error = %v", err)
	}
	if len(notResult.Rows) != 1 || notResult.Rows[0]["message"] != "healthy" {
		t.Fatalf("NOT REGEXP rows = %#v, want healthy only", notResult.Rows)
	}
}

func TestSQLRegexReportsInvalidPatternsAndReturnsNullForNoMatch(t *testing.T) {
	result, err := hatSql.ExecuteSQLQuery(`FROM CACHE('logs') AS src WHERE src.message = 'healthy' SELECT REGEXP_EXTRACT(src.detail, 'id=([0-9]+)', 1) AS id`, testRegexResolver{})
	if err != nil {
		t.Fatalf("no-match query error = %v", err)
	}
	if len(result.Rows) != 1 || result.Rows[0]["id"] != nil {
		t.Fatalf("no-match rows = %#v, want NULL extraction", result.Rows)
	}

	_, err = hatSql.ExecuteSQLQuery(`FROM CACHE('logs') AS src WHERE src.message REGEXP '(' SELECT src.message`, testRegexResolver{})
	if err == nil || !strings.Contains(err.Error(), "invalid regular expression") {
		t.Fatalf("invalid regex error = %v, want regular expression diagnostic", err)
	}
}

type testRegexResolver struct{}

func (testRegexResolver) ResolveSQLSource(name, key string) ([]hatSql.SQLRow, error) {
	if name != "CACHE" || key != "logs" {
		return nil, nil
	}
	return []hatSql.SQLRow{
		{"message": "error opening socket", "detail": "id=42 retry=true"},
		{"message": "healthy", "detail": "status=ok"},
	}, nil
}
