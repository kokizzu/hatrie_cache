package hatSql_test

import (
	"context"
	"strings"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestSQLWithTotalsReturnsGroupedRowsAndGrandTotal(t *testing.T) {
	result, err := hatSql.ExecuteSQLQuery(`FROM VALUES ('east', 2), ('east', 3), ('west', 5) AS src(region, amount) SELECT src.region, SUM(src.amount) AS total GROUP BY src.region WITH TOTALS`, nil)
	if err != nil {
		t.Fatalf("WITH TOTALS error = %v", err)
	}
	if len(result.Rows) != 2 {
		t.Fatalf("WITH TOTALS rows = %#v, want two grouped rows", result.Rows)
	}
	if !withTotalsRowExists(result.Rows, "east", 5) || !withTotalsRowExists(result.Rows, "west", 5) {
		t.Fatalf("WITH TOTALS grouped rows = %#v, want east=5 and west=5", result.Rows)
	}
	if result.Totals == nil || result.Totals["region"] != nil {
		t.Fatalf("WITH TOTALS total row = %#v, want a separate null-region total", result.Totals)
	}
	if total, ok := hatSql.Number(result.Totals["total"]); !ok || int64(total) != 10 {
		t.Fatalf("WITH TOTALS total value = %#v, want 10", result.Totals["total"])
	}
	if len(result.Columns) != 2 || result.Columns[0] != "region" || result.Columns[1] != "total" {
		t.Fatalf("WITH TOTALS columns = %#v, want public columns only", result.Columns)
	}
}

func TestSQLWithoutTotalsKeepsLegacyResultShape(t *testing.T) {
	result, err := hatSql.ExecuteSQLQuery(`FROM VALUES ('east', 2), ('west', 5) AS src(region, amount) SELECT src.region, SUM(src.amount) AS total GROUP BY src.region`, nil)
	if err != nil {
		t.Fatalf("legacy grouped query error = %v", err)
	}
	if result.Totals != nil {
		t.Fatalf("legacy grouped query totals = %#v, want nil", result.Totals)
	}
}

func TestSQLWithTotalsRequiresGrouping(t *testing.T) {
	_, err := hatSql.ExecuteSQLQuery(`FROM VALUES (1) AS src(value) SELECT SUM(src.value) AS total WITH TOTALS`, nil)
	if err == nil || !strings.Contains(strings.ToUpper(err.Error()), "GROUP") {
		t.Fatalf("ungrouped WITH TOTALS error = %v, want grouping diagnostic", err)
	}
}

func TestSQLWithTotalsRequiresMaterializedRowsAPI(t *testing.T) {
	err := hatSql.ExecuteSQLQueryRows(context.Background(), `FROM VALUES ('east', 2) AS src(region, amount) SELECT src.region, SUM(src.amount) AS total GROUP BY src.region WITH TOTALS`, nil, nil, hatSql.SQLQueryOptions{}, func([]string, hatSql.SQLRow) error {
		t.Fatal("WITH TOTALS unexpectedly called the streaming visitor")
		return nil
	})
	if err == nil || !strings.Contains(err.Error(), "separate totals section") {
		t.Fatalf("streaming WITH TOTALS error = %v, want explicit materialized API error", err)
	}
}

func withTotalsRowExists(rows []hatSql.Row, region string, total int64) bool {
	for _, row := range rows {
		if row["region"] != region {
			continue
		}
		value, ok := hatSql.Number(row["total"])
		if ok && int64(value) == total {
			return true
		}
	}
	return false
}
