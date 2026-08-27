package hatSql_test

import (
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestSQLAggregateFilterClause(t *testing.T) {
	result, err := hatSql.ExecuteSQLQuery(`FROM VALUES ('a', 5), ('a', -2), ('b', 4) AS src(category, amount) SELECT src.category, SUM(src.amount) FILTER (WHERE src.amount > 0) AS positive_total, COUNT(*) FILTER (WHERE src.amount > 0) AS positive_count GROUP BY src.category ORDER BY src.category`, nil)
	if err != nil {
		t.Fatalf("FILTER aggregate query error = %v", err)
	}
	if len(result.Rows) != 2 {
		t.Fatalf("rows = %#v, want two groups", result.Rows)
	}
	if row := result.Rows[0]; row["category"] != "a" || sqlAggregateInteger(row["positive_total"]) != 5 || sqlAggregateInteger(row["positive_count"]) != 1 {
		t.Fatalf("first row = %#v, want filtered a aggregate", row)
	}
	if row := result.Rows[1]; row["category"] != "b" || sqlAggregateInteger(row["positive_total"]) != 4 || sqlAggregateInteger(row["positive_count"]) != 1 {
		t.Fatalf("second row = %#v, want filtered b aggregate", row)
	}
}

func sqlAggregateInteger(value interface{}) int64 {
	number, _ := hatSql.Number(value)
	return int64(number)
}
