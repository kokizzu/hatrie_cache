package hatSql

import "testing"

func TestSQLRewriteFoldsConstantScalarExpressions(t *testing.T) {
	cases := []struct {
		name   string
		source string
		where  bool
		want   interface{}
	}{
		{name: "cast", source: `FROM VALUES (1) AS src(n) SELECT CAST('42' AS TEXT) AS casted`, want: "42"},
		{name: "coalesce", source: `FROM VALUES (1) AS src(n) SELECT COALESCE(NULL, 'ok') AS fallback`, want: "ok"},
		{name: "case", source: `FROM VALUES (1) AS src(n) SELECT CASE WHEN 1 = 1 THEN 'hit' ELSE 'miss' END AS branch`, want: "hit"},
		{name: "in", source: `FROM VALUES (1) AS src(n) WHERE 2 IN (1, 2) SELECT src.n`, where: true, want: true},
		{name: "between", source: `FROM VALUES (1) AS src(n) WHERE 2 BETWEEN 1 AND 3 SELECT src.n`, where: true, want: true},
		{name: "is-not-null", source: `FROM VALUES (1) AS src(n) WHERE 5 IS NOT NULL SELECT src.n`, where: true, want: true},
	}

	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			query, err := parseSQLQuery(test.source)
			if err != nil {
				t.Fatalf("parse query: %v", err)
			}
			expression := query.selects[0].expr
			if test.where {
				expression = query.where
			}
			if expression.kind != "literal" {
				t.Fatalf("expression kind = %q, want literal", expression.kind)
			}
			if expression.value != test.want {
				t.Fatalf("literal = %#v, want %#v", expression.value, test.want)
			}
		})
	}
}

func TestSQLRewriteDoesNotFoldRowDependentOrAggregateExpressions(t *testing.T) {
	query, err := parseSQLQuery(`FROM VALUES (1) AS src(n) SELECT COUNT(*) AS total, LOWER(CAST(src.n AS TEXT)) AS name`)
	if err != nil {
		t.Fatalf("parse query: %v", err)
	}
	if query.selects[0].expr.kind != "func" || query.selects[0].expr.name != "COUNT" {
		t.Fatalf("aggregate expression = %#v, want COUNT function", query.selects[0].expr)
	}
	if query.selects[1].expr.kind != "func" || query.selects[1].expr.name != "LOWER" {
		t.Fatalf("row-dependent expression = %#v, want LOWER function", query.selects[1].expr)
	}
}

func TestSQLRewriteDoesNotFoldUnknownFunctions(t *testing.T) {
	query, err := parseSQLQuery(`FROM VALUES (1) AS src(n) SELECT CUSTOM('value') AS result`)
	if err != nil {
		t.Fatalf("parse query: %v", err)
	}
	if query.selects[0].expr.kind != "func" || query.selects[0].expr.name != "CUSTOM" {
		t.Fatalf("unknown function = %#v, want CUSTOM function", query.selects[0].expr)
	}
}

func TestSQLRewritePreservesConstantNullPredicateSemantics(t *testing.T) {
	query, err := parseSQLQuery(`FROM VALUES (1) AS src(n) WHERE NULL IN (1, NULL) SELECT src.n`)
	if err != nil {
		t.Fatalf("parse query: %v", err)
	}
	if query.where.kind != "literal" || query.where.value != nil {
		t.Fatalf("constant NULL predicate = %#v, want NULL literal", query.where)
	}
	result, err := ExecuteSQLQuery(`FROM VALUES (1) AS src(n) WHERE NULL IN (1, NULL) SELECT src.n`, nil)
	if err != nil {
		t.Fatalf("execute constant NULL predicate: %v", err)
	}
	if len(result.Rows) != 0 {
		t.Fatalf("constant NULL predicate rows = %#v, want empty", result.Rows)
	}
}
