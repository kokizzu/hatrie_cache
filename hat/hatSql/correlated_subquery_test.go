package hatSql_test

import (
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestSQLCorrelatedExistsNotExistsAndScalarSubqueries(t *testing.T) {
	exists, err := hatSql.ExecuteSQLQuery(`FROM VALUES (1), (2), (3) AS outer_rows(id) WHERE EXISTS (FROM VALUES (1), (3) AS inner_rows(id) WHERE inner_rows.id = outer_rows.id SELECT inner_rows.id) SELECT outer_rows.id ORDER BY outer_rows.id`, nil)
	if err != nil {
		t.Fatalf("EXISTS query error = %v", err)
	}
	if len(exists.Rows) != 2 || exists.Rows[0]["id"] != int64(1) || exists.Rows[1]["id"] != int64(3) {
		t.Fatalf("EXISTS rows = %#v, want 1 and 3", exists.Rows)
	}

	notExists, err := hatSql.ExecuteSQLQuery(`FROM VALUES (1), (2), (3) AS outer_rows(id) WHERE NOT EXISTS (FROM VALUES (1), (3) AS inner_rows(id) WHERE inner_rows.id = outer_rows.id SELECT inner_rows.id) SELECT outer_rows.id`, nil)
	if err != nil {
		t.Fatalf("NOT EXISTS query error = %v", err)
	}
	if len(notExists.Rows) != 1 || notExists.Rows[0]["id"] != int64(2) {
		t.Fatalf("NOT EXISTS rows = %#v, want 2", notExists.Rows)
	}

	scalar, err := hatSql.ExecuteSQLQuery(`FROM VALUES (1), (2), (3) AS outer_rows(id) WHERE outer_rows.id = (FROM VALUES (2) AS inner_rows(id) SELECT inner_rows.id) SELECT outer_rows.id`, nil)
	if err != nil {
		t.Fatalf("scalar subquery error = %v", err)
	}
	if len(scalar.Rows) != 1 || scalar.Rows[0]["id"] != int64(2) {
		t.Fatalf("scalar rows = %#v, want 2", scalar.Rows)
	}
}
