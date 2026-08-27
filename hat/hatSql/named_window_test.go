package hatSql_test

import (
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestSQLNamedWindowsResolveForRankingAndLeadLag(t *testing.T) {
	result, err := hatSql.ExecuteSQLQuery(`FROM VALUES ('a', 10), ('a', 20), ('b', 5) AS src(category, amount) SELECT src.category, src.amount, ROW_NUMBER() OVER ranked AS row_number, LAG(src.amount, 1, 0) OVER ranked AS previous, LEAD(src.amount, 1, 0) OVER ranked AS following WINDOW ranked AS (PARTITION BY src.category ORDER BY src.amount) ORDER BY src.category, src.amount`, nil)
	if err != nil {
		t.Fatalf("named window query error = %v", err)
	}
	if len(result.Rows) != 3 {
		t.Fatalf("rows = %#v, want three rows", result.Rows)
	}
	if row := result.Rows[0]; row["row_number"] != int64(1) || sqlNamedWindowNumber(row["previous"]) != 0 || sqlNamedWindowNumber(row["following"]) != 20 {
		t.Fatalf("first row = %#v, want ranked lead/lag values", row)
	}
	if row := result.Rows[1]; row["row_number"] != int64(2) || sqlNamedWindowNumber(row["previous"]) != 10 || sqlNamedWindowNumber(row["following"]) != 0 {
		t.Fatalf("second row = %#v, want ranked lead/lag values", row)
	}
}

func sqlNamedWindowNumber(value interface{}) int64 {
	number, _ := hatSql.Number(value)
	return int64(number)
}
