package hatSql

import "testing"

func TestCH030JSONPathLiteralEvaluation(t *testing.T) {
	query, err := parseSQLQuery(`FROM CACHE('docs') AS src SELECT JSON_VALUE(src.doc, '$.profile.city')`)
	if err != nil {
		t.Fatalf("parseSQLQuery() error = %v", err)
	}
	if query.selects[0].expr.jsonPath == nil {
		t.Fatal("literal JSON path was not prepared during query binding")
	}
	row := newSQLSingleSourceExecRow("src", SQLRow{
		"doc": map[string]interface{}{
			"profile": map[string]interface{}{"city": "Singapore"},
		},
	})
	value := evalSQLExpr(query.selects[0].expr, []sqlExecRow{row}, row)
	if value != "Singapore" {
		t.Fatalf("JSON_VALUE() = %#v, want Singapore", value)
	}
}

func TestCH030JSONPathParameterIsPrepared(t *testing.T) {
	query, err := parseSQLQueryParameters(`FROM CACHE('docs') AS src SELECT JSON_VALUE(src.doc, $1)`, []interface{}{"$.profile.city"})
	if err != nil {
		t.Fatalf("parseSQLQueryParameters() error = %v", err)
	}
	if query.selects[0].expr.jsonPath == nil {
		t.Fatal("bound JSON path parameter was not prepared")
	}
	row := newSQLSingleSourceExecRow("src", SQLRow{
		"doc": map[string]interface{}{"profile": map[string]interface{}{"city": "Singapore"}},
	})
	value := evalSQLExpr(query.selects[0].expr, []sqlExecRow{row}, row)
	if value != "Singapore" {
		t.Fatalf("bound JSON_VALUE() = %#v, want Singapore", value)
	}
}

func TestCH030JSONPathDynamicPathFallsBackToRuntimeParsing(t *testing.T) {
	query, err := parseSQLQuery(`FROM CACHE('docs') AS src SELECT JSON_VALUE(src.doc, src.path)`)
	if err != nil {
		t.Fatalf("parseSQLQuery() error = %v", err)
	}
	if query.selects[0].expr.jsonPath != nil {
		t.Fatal("dynamic JSON path unexpectedly received a literal program")
	}
	row := newSQLSingleSourceExecRow("src", SQLRow{
		"doc":  map[string]interface{}{"profile": map[string]interface{}{"city": "Singapore"}},
		"path": "$.profile.city",
	})
	value := evalSQLExpr(query.selects[0].expr, []sqlExecRow{row}, row)
	if value != "Singapore" {
		t.Fatalf("dynamic JSON_VALUE() = %#v, want Singapore", value)
	}
}

func TestCH030JSONPathInvalidLiteralKeepsEvaluationError(t *testing.T) {
	query, err := parseSQLQuery(`FROM CACHE('docs') AS src SELECT JSON_VALUE(src.doc, 'profile.city')`)
	if err != nil {
		t.Fatalf("parseSQLQuery() error = %v", err)
	}
	row := newSQLSingleSourceExecRow("src", SQLRow{"doc": map[string]interface{}{}})
	value := evalSQLExpr(query.selects[0].expr, []sqlExecRow{row}, row)
	if err := sqlExpressionError(value); err == nil {
		t.Fatalf("invalid JSON_VALUE() = %#v, want evaluation error", value)
	}
}

func BenchmarkCH030JSONPathLiteralEvaluation(b *testing.B) {
	query, err := parseSQLQuery(`FROM CACHE('docs') AS src SELECT JSON_VALUE(src.doc, '$.profile.city')`)
	if err != nil {
		b.Fatalf("parseSQLQuery() error = %v", err)
	}
	row := newSQLSingleSourceExecRow("src", SQLRow{
		"doc": map[string]interface{}{
			"profile": map[string]interface{}{"city": "Singapore"},
		},
	})
	expr := query.selects[0].expr
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		value := evalSQLExpr(expr, []sqlExecRow{row}, row)
		if value != "Singapore" {
			b.Fatalf("JSON_VALUE() = %#v, want Singapore", value)
		}
	}
}
