package hatSql_test

import (
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestSQLRewriteFoldsConstantsAndEliminatesDeadDerivedProjection(t *testing.T) {
	result, err := hatSql.ExecuteSQLQuery(`FROM VALUES (1) AS src(n) WHERE 1 = 0 AND src.n REGEXP '(' SELECT src.n`, nil)
	if err != nil {
		t.Fatalf("constant-false predicate should not evaluate dead branch: %v", err)
	}
	if len(result.Rows) != 0 {
		t.Fatalf("constant-false rows = %#v, want empty", result.Rows)
	}

	result, err = hatSql.ExecuteSQLQuery(`FROM (FROM VALUES (1) AS src(n) SELECT src.n AS n, REGEXP_EXTRACT(CAST(src.n AS TEXT), '(') AS dead) AS derived SELECT derived.n`, nil)
	if err != nil {
		t.Fatalf("dead derived projection should be eliminated: %v", err)
	}
	if len(result.Rows) != 1 || result.Rows[0]["n"] != int64(1) {
		t.Fatalf("derived rows = %#v, want n=1", result.Rows)
	}
}
