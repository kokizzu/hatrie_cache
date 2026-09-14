package hatSql

import (
	"errors"
	"testing"
)

func TestCH055LiteralBetweenIsPrepared(t *testing.T) {
	query, err := parseSQLQueryParameters(`FROM CACHE('events') AS src WHERE src.value BETWEEN 400 AND 600 SELECT src.value`, nil)
	if err != nil {
		t.Fatalf("parseSQLQueryParameters() error = %v", err)
	}
	if query.where.betweenProgram == nil {
		t.Fatal("literal BETWEEN bounds were not prepared during query binding")
	}

	row := sqlExecRow{sources: map[string]SQLRow{"src": {"value": int64(500)}}, order: []string{"src"}}
	if value := evalSQLExpr(query.where, []sqlExecRow{row}, row); value != true {
		t.Fatalf("BETWEEN result = %#v, want true", value)
	}

	row.sources["src"]["value"] = int64(700)
	if value := evalSQLExpr(query.where, []sqlExecRow{row}, row); value != false {
		t.Fatalf("out-of-range BETWEEN result = %#v, want false", value)
	}
}

func TestCH055DynamicBetweenBoundsRemainUnprepared(t *testing.T) {
	query, err := parseSQLQueryParameters(`FROM CACHE('events') AS src WHERE src.value BETWEEN src.lower AND 600 SELECT src.value`, nil)
	if err != nil {
		t.Fatalf("parseSQLQueryParameters() error = %v", err)
	}
	if query.where.betweenProgram != nil {
		t.Fatal("dynamic BETWEEN bound unexpectedly received a prepared program")
	}

	row := sqlExecRow{sources: map[string]SQLRow{"src": {"value": int64(500), "lower": int64(400)}}, order: []string{"src"}}
	if value := evalSQLExpr(query.where, []sqlExecRow{row}, row); value != true {
		t.Fatalf("dynamic BETWEEN result = %#v, want true", value)
	}
	row.sources["src"]["lower"] = int64(600)
	if value := evalSQLExpr(query.where, []sqlExecRow{row}, row); value != false {
		t.Fatalf("dynamic BETWEEN out-of-range result = %#v, want false", value)
	}
}

func TestCH055InvalidLiteralBoundRemainsUnprepared(t *testing.T) {
	expr := ch055LiteralBetweenExpression()
	expr.args[0].value = sqlEvalError{err: errors.New("invalid bound")}
	prepareSQLBetweenExpr(&expr)
	if expr.betweenProgram != nil {
		t.Fatal("invalid literal BETWEEN bound unexpectedly received a prepared program")
	}
}

func TestCH055PreparedBetweenPreservesNullAndNotBetweenSemantics(t *testing.T) {
	tests := []struct {
		name  string
		query string
		value interface{}
		want  interface{}
	}{
		{
			name:  "not between hit",
			query: `FROM CACHE('events') AS src WHERE src.value NOT BETWEEN 400 AND 600 SELECT src.value`,
			value: int64(700),
			want:  true,
		},
		{
			name:  "not between miss",
			query: `FROM CACHE('events') AS src WHERE src.value NOT BETWEEN 400 AND 600 SELECT src.value`,
			value: int64(500),
			want:  false,
		},
		{
			name:  "null left",
			query: `FROM CACHE('events') AS src WHERE src.value BETWEEN 400 AND 600 SELECT src.value`,
			value: nil,
			want:  nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			query, err := parseSQLQueryParameters(test.query, nil)
			if err != nil {
				t.Fatalf("parseSQLQueryParameters() error = %v", err)
			}
			if query.where.betweenProgram == nil {
				t.Fatal("literal BETWEEN bounds were not prepared")
			}
			row := sqlExecRow{sources: map[string]SQLRow{"src": {"value": test.value}}, order: []string{"src"}}
			if value := evalSQLExpr(query.where, []sqlExecRow{row}, row); value != test.want {
				t.Fatalf("BETWEEN result = %#v, want %#v", value, test.want)
			}
		})
	}
}

func TestCH055PreparedBetweenBindsParametersAndCollation(t *testing.T) {
	query, err := parseSQLQueryParameters(`FROM CACHE('events') AS src WHERE src.value BETWEEN $1 AND $2 SELECT src.value`, []interface{}{int64(400), int64(600)})
	if err != nil {
		t.Fatalf("parseSQLQueryParameters() error = %v", err)
	}
	if query.where.betweenProgram == nil {
		t.Fatal("bound BETWEEN bounds were not prepared")
	}
	row := sqlExecRow{sources: map[string]SQLRow{"src": {"value": int64(500)}}, order: []string{"src"}}
	if value := evalSQLExpr(query.where, []sqlExecRow{row}, row); value != true {
		t.Fatalf("bound BETWEEN result = %#v, want true", value)
	}

	query, err = parseSQLQueryParameters(`FROM CACHE('events') AS src WHERE src.value BETWEEN 'a' AND 'z' SELECT src.value`, nil)
	if err != nil {
		t.Fatalf("parseSQLQueryParameters() collation query error = %v", err)
	}
	applySQLQueryCollation(query, SQLCollationUnicodeCI)
	if query.where.betweenProgram == nil {
		t.Fatal("collated literal BETWEEN bounds were not prepared")
	}
	row.sources["src"]["value"] = "M"
	if value := evalSQLExpr(query.where, []sqlExecRow{row}, row); value != true {
		t.Fatalf("collated BETWEEN result = %#v, want true", value)
	}
}

func ch055LiteralBetweenExpression() sqlExpr {
	left := sqlExpr{kind: "field", qualifier: "src", name: "value"}
	return sqlExpr{
		kind: "between",
		op:   "between",
		left: &left,
		args: []sqlExpr{
			{kind: "literal", value: int64(400)},
			{kind: "literal", value: int64(600)},
		},
	}
}

func benchmarkCH055LiteralBetween(b *testing.B, prepared bool) {
	expr := ch055LiteralBetweenExpression()
	if prepared {
		prepareSQLBetweenExpr(&expr)
	}
	row := sqlExecRow{sources: map[string]SQLRow{"src": {"value": int64(500)}}, order: []string{"src"}}
	group := []sqlExecRow{row}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if value := evalSQLExpr(expr, group, row); value != true {
			b.Fatalf("BETWEEN result = %#v, want true", value)
		}
	}
}

func BenchmarkCH055LiteralBetween(b *testing.B) {
	b.Run("baseline", func(b *testing.B) {
		benchmarkCH055LiteralBetween(b, false)
	})
	b.Run("prepared", func(b *testing.B) {
		benchmarkCH055LiteralBetween(b, true)
	})
}
