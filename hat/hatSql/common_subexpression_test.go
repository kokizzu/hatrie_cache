package hatSql

import "testing"

func TestSQLRewriteEliminatesDuplicateDeterministicBooleanSubexpression(t *testing.T) {
	query, err := parseSQLQueryParameters(`FROM VALUES (1), (NULL), (3) AS src(score) WHERE (src.score >= 1) AND (src.score >= 1) SELECT src.score`, nil)
	if err != nil {
		t.Fatalf("parse query: %v", err)
	}
	if query.where.kind != "binary" || query.where.op != ">=" {
		t.Fatalf("rewritten WHERE = %#v, want one score >= 1 predicate", query.where)
	}

	query, err = parseSQLQueryParameters(`FROM VALUES (1) AS src(score) WHERE (src.score >= 1) OR (src.score >= 1) SELECT src.score`, nil)
	if err != nil {
		t.Fatalf("parse OR query: %v", err)
	}
	if query.where.kind != "binary" || query.where.op != ">=" {
		t.Fatalf("rewritten OR WHERE = %#v, want one score >= 1 predicate", query.where)
	}
}

func TestSQLRewriteKeepsNonDuplicateBooleanExpressions(t *testing.T) {
	query, err := parseSQLQueryParameters(`FROM VALUES (1) AS src(score) WHERE (src.score >= 1) AND (src.score >= 2) SELECT src.score`, nil)
	if err != nil {
		t.Fatalf("parse query: %v", err)
	}
	if query.where.kind != "binary" || query.where.op != "AND" {
		t.Fatalf("rewritten WHERE = %#v, want AND", query.where)
	}
}

func TestSQLRewriteDuplicatePredicatesPreserveThreeValuedFiltering(t *testing.T) {
	andResult, err := ExecuteSQLQuery(`FROM VALUES (NULL), (FALSE), (TRUE) AS src(flag) WHERE src.flag AND src.flag SELECT src.flag`, nil)
	if err != nil {
		t.Fatalf("execute AND query: %v", err)
	}
	if len(andResult.Rows) != 1 || andResult.Rows[0]["flag"] != true {
		t.Fatalf("AND rows = %#v, want only TRUE", andResult.Rows)
	}

	orResult, err := ExecuteSQLQuery(`FROM VALUES (NULL), (FALSE), (TRUE) AS src(flag) WHERE src.flag OR src.flag SELECT src.flag`, nil)
	if err != nil {
		t.Fatalf("execute OR query: %v", err)
	}
	if len(orResult.Rows) != 1 || orResult.Rows[0]["flag"] != true {
		t.Fatalf("OR rows = %#v, want only TRUE", orResult.Rows)
	}
}

func TestSQLRewriteDoesNotEliminateUserFunctionSubexpressions(t *testing.T) {
	function := sqlExpr{kind: "func", name: "user_function"}
	expression := sqlExpr{kind: "binary", op: "AND", left: &function, right: &function}
	rewritten := rewriteSQLExpr(expression)
	if rewritten.kind != "binary" || rewritten.op != "AND" {
		t.Fatalf("rewritten user function = %#v, want unchanged AND", rewritten)
	}
}

func TestSQLRewriteEliminatesDuplicateCastPredicate(t *testing.T) {
	query, err := parseSQLQueryParameters(`FROM VALUES (1) AS src(score) WHERE CAST(src.score AS NUMBER) >= 1 AND CAST(src.score AS NUMBER) >= 1 SELECT src.score`, nil)
	if err != nil {
		t.Fatalf("parse cast query: %v", err)
	}
	if query.where.kind != "binary" || query.where.op != ">=" {
		t.Fatalf("rewritten cast WHERE = %#v, want one comparison", query.where)
	}
}
