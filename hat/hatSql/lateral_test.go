package hatSql_test

import (
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestSQLLateralJoinEvaluatesDerivedSourceForEachOuterRow(t *testing.T) {
	result, err := hatSql.ExecuteSQLQuery(`FROM VALUES (1), (2) AS outer_rows(id) CROSS JOIN LATERAL (FROM VALUES (1), (2), (3) AS inner_rows(value) WHERE inner_rows.value <= outer_rows.id SELECT inner_rows.value) AS expanded SELECT outer_rows.id, expanded.value ORDER BY outer_rows.id, expanded.value`, nil)
	if err != nil {
		t.Fatalf("LATERAL query error = %v", err)
	}
	if len(result.Rows) != 3 {
		t.Fatalf("rows = %#v, want three expanded rows", result.Rows)
	}
	want := [][2]int64{{1, 1}, {2, 1}, {2, 2}}
	for index, expected := range want {
		row := result.Rows[index]
		if row["id"] != expected[0] || row["value"] != expected[1] {
			t.Fatalf("row %d = %#v, want %#v", index, row, expected)
		}
	}
}
