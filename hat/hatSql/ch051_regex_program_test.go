package hatSql

import "testing"

func TestCH051RegexLiteralEvaluation(t *testing.T) {
	query, err := parseSQLQuery(`FROM CACHE('logs') AS src SELECT REGEXP_LIKE(src.message, '^error[0-9]+$') AS matched, REGEXP_EXTRACT(src.detail, 'id=([0-9]+)', 1) AS id`)
	if err != nil {
		t.Fatalf("parseSQLQuery() error = %v", err)
	}
	if query.selects[0].expr.regexProgram == nil || query.selects[1].expr.regexProgram == nil {
		t.Fatal("literal regex pattern was not prepared during query binding")
	}
	row := newSQLSingleSourceExecRow("src", SQLRow{
		"message": "error42",
		"detail":  "request id=31415",
	})
	matched := evalSQLExpr(query.selects[0].expr, []sqlExecRow{row}, row)
	if matched != true {
		t.Fatalf("REGEXP_LIKE() = %#v, want true", matched)
	}
	id := evalSQLExpr(query.selects[1].expr, []sqlExecRow{row}, row)
	if id != "31415" {
		t.Fatalf("REGEXP_EXTRACT() = %#v, want 31415", id)
	}
}

func TestCH051RegexPredicateLiteralEvaluation(t *testing.T) {
	query, err := parseSQLQuery(`FROM CACHE('logs') AS src WHERE src.message REGEXP '^error[0-9]+$' SELECT src.message`)
	if err != nil {
		t.Fatalf("parseSQLQuery() error = %v", err)
	}
	if query.where.regexProgram == nil {
		t.Fatal("literal regex predicate was not prepared during query binding")
	}
	row := newSQLSingleSourceExecRow("src", SQLRow{"message": "error42"})
	value := evalSQLExpr(query.where, []sqlExecRow{row}, row)
	if value != true {
		t.Fatalf("REGEXP predicate = %#v, want true", value)
	}
}

func TestCH051RegexDynamicPatternFallsBackToRuntimeCompilation(t *testing.T) {
	query, err := parseSQLQuery(`FROM CACHE('logs') AS src SELECT REGEXP_LIKE(src.message, src.pattern) AS matched`)
	if err != nil {
		t.Fatalf("parseSQLQuery() error = %v", err)
	}
	if query.selects[0].expr.regexProgram != nil {
		t.Fatal("dynamic regex pattern unexpectedly received a literal program")
	}
	row := newSQLSingleSourceExecRow("src", SQLRow{"message": "error42", "pattern": `^error[0-9]+$`})
	value := evalSQLExpr(query.selects[0].expr, []sqlExecRow{row}, row)
	if value != true {
		t.Fatalf("dynamic REGEXP_LIKE() = %#v, want true", value)
	}
}

func TestCH051RegexInvalidLiteralKeepsEvaluationError(t *testing.T) {
	query, err := parseSQLQuery(`FROM CACHE('logs') AS src SELECT REGEXP_LIKE(src.message, '[') AS matched`)
	if err != nil {
		t.Fatalf("parseSQLQuery() error = %v", err)
	}
	if query.selects[0].expr.regexProgram == nil || query.selects[0].expr.regexProgram.err == nil {
		t.Fatal("invalid literal regex was not retained as a failed program")
	}
	row := newSQLSingleSourceExecRow("src", SQLRow{"message": "error42"})
	value := evalSQLExpr(query.selects[0].expr, []sqlExecRow{row}, row)
	if err := sqlExpressionError(value); err == nil {
		t.Fatalf("invalid REGEXP_LIKE() = %#v, want evaluation error", value)
	}
}

func BenchmarkCH051RegexLiteralEvaluation(b *testing.B) {
	query, err := parseSQLQuery(`FROM CACHE('logs') AS src SELECT REGEXP_LIKE(src.message, '^error[0-9]+$') AS matched`)
	if err != nil {
		b.Fatalf("parseSQLQuery() error = %v", err)
	}
	row := newSQLSingleSourceExecRow("src", SQLRow{"message": "error42"})
	expr := query.selects[0].expr
	b.Run("function", func(b *testing.B) {
		b.ReportAllocs()
		for index := 0; index < b.N; index++ {
			if value := evalSQLExpr(expr, []sqlExecRow{row}, row); value != true {
				b.Fatalf("REGEXP_LIKE() = %#v, want true", value)
			}
		}
	})
}

func BenchmarkCH051RegexPredicateLiteralEvaluation(b *testing.B) {
	query, err := parseSQLQuery(`FROM CACHE('logs') AS src WHERE src.message REGEXP '^error[0-9]+$' SELECT src.message`)
	if err != nil {
		b.Fatalf("parseSQLQuery() error = %v", err)
	}
	row := newSQLSingleSourceExecRow("src", SQLRow{"message": "error42"})
	expr := query.where
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		if value := evalSQLExpr(expr, []sqlExecRow{row}, row); value != true {
			b.Fatalf("REGEXP predicate = %#v, want true", value)
		}
	}
}
