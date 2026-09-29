package hatSql

import (
	"bytes"
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

func TestCHG05WithTotals(t *testing.T) {
	result, err := ExecuteSQLQuery(`
FROM VALUES ('a', 1), ('b', 2), ('a', 3) AS src(category, amount)
SELECT src.category, SUM(src.amount) AS total
GROUP BY src.category WITH TOTALS
ORDER BY src.category`, nil)
	if err != nil {
		t.Fatalf("WITH TOTALS query error = %v", err)
	}
	wantRows := []Row{
		{"category": "a", "total": float64(4)},
		{"category": "b", "total": float64(2)},
	}
	if !reflect.DeepEqual(result.Rows, wantRows) {
		t.Fatalf("group rows = %#v (total %T), want %#v (total %T)", result.Rows, result.Rows[0]["total"], wantRows, wantRows[0]["total"])
	}
	wantTotals := []Row{{"category": nil, "total": float64(6)}}
	if !reflect.DeepEqual(result.Totals, wantTotals) {
		t.Fatalf("totals = %#v, want %#v", result.Totals, wantTotals)
	}
}

func TestCHG05WithTotalsIgnoresHavingForGrandTotal(t *testing.T) {
	result, err := ExecuteSQLQuery(`
FROM VALUES ('a', 1), ('b', 2), ('a', 3) AS src(category, amount)
SELECT src.category, SUM(src.amount) AS total
GROUP BY src.category WITH TOTALS
HAVING SUM(src.amount) > 3`, nil)
	if err != nil {
		t.Fatalf("WITH TOTALS HAVING query error = %v", err)
	}
	wantRows := []Row{{"category": "a", "total": float64(4)}}
	if !reflect.DeepEqual(result.Rows, wantRows) {
		t.Fatalf("HAVING rows = %#v, want %#v", result.Rows, wantRows)
	}
	wantTotals := []Row{{"category": nil, "total": float64(6)}}
	if !reflect.DeepEqual(result.Totals, wantTotals) {
		t.Fatalf("HAVING totals = %#v, want %#v", result.Totals, wantTotals)
	}
}

func TestCHG05WithTotalsAggregatesAllFilteredRows(t *testing.T) {
	result, err := ExecuteSQLQuery(`
FROM VALUES ('a', 1), ('b', 2), ('a', 3) AS src(category, amount)
SELECT src.category, COUNT(*) AS count, SUM(src.amount) AS total
GROUP BY src.category WITH TOTALS`, nil)
	if err != nil {
		t.Fatalf("WITH TOTALS aggregate query error = %v", err)
	}
	want := []Row{{"category": nil, "count": int64(3), "total": float64(6)}}
	if !reflect.DeepEqual(result.Totals, want) {
		t.Fatalf("aggregate totals = %#v, want %#v", result.Totals, want)
	}
}

func TestCHG05WithTotalsPreservesLimitAndJSONShape(t *testing.T) {
	result, err := ExecuteSQLQuery(`
FROM VALUES ('a', 1), ('b', 2), ('a', 3) AS src(category, amount)
SELECT src.category, SUM(src.amount) AS total
GROUP BY src.category WITH TOTALS
ORDER BY total DESC
LIMIT 1`, nil)
	if err != nil {
		t.Fatalf("WITH TOTALS limited query error = %v", err)
	}
	if len(result.Rows) != 1 || result.Rows[0]["category"] != "a" {
		t.Fatalf("limited group rows = %#v, want only category a", result.Rows)
	}
	if !reflect.DeepEqual(result.Totals, []Row{{"category": nil, "total": float64(6)}}) {
		t.Fatalf("limited totals = %#v, want grand total 6", result.Totals)
	}
	payload, err := json.Marshal(result)
	if err != nil {
		t.Fatalf("marshal result: %v", err)
	}
	if !bytes.Contains(payload, []byte(`"totals":[`)) {
		t.Fatalf("JSON result = %s, want totals field", payload)
	}
}

func TestCHG05WithTotalsStreamingIsRejected(t *testing.T) {
	query := `
FROM VALUES ('a', 1), ('b', 2), ('a', 3) AS src(category, amount)
SELECT src.category, SUM(src.amount) AS total
GROUP BY src.category WITH TOTALS`
	err := ExecuteSQLQueryRows(context.Background(), query, nil, nil, SQLQueryOptions{}, func([]string, SQLRow) error {
		return nil
	})
	if err == nil || !strings.Contains(err.Error(), "only available for materialized query results") {
		t.Fatalf("streaming WITH TOTALS error = %v", err)
	}
}

func TestCHG05OrdinaryGroupedResultHasNoTotals(t *testing.T) {
	result, err := ExecuteSQLQuery(`
FROM VALUES ('a', 1), ('b', 2), ('a', 3) AS src(category, amount)
SELECT src.category, SUM(src.amount) AS total
GROUP BY src.category`, nil)
	if err != nil {
		t.Fatalf("ordinary grouped query error = %v", err)
	}
	if result.Totals != nil {
		t.Fatalf("ordinary grouped totals = %#v, want nil", result.Totals)
	}
}

func TestCHG05WithTotalsCloneIsIndependent(t *testing.T) {
	result, err := ExecuteSQLQuery(`
FROM VALUES ('a', 1), ('b', 2), ('a', 3) AS src(category, amount)
SELECT src.category, SUM(src.amount) AS total
GROUP BY src.category WITH TOTALS`, nil)
	if err != nil {
		t.Fatalf("WITH TOTALS clone query error = %v", err)
	}
	cloned := cloneQueryResult(result)
	cloned.Totals[0]["total"] = float64(99)
	if result.Totals[0]["total"] == float64(99) {
		t.Fatal("cloned totals mutated the original result")
	}
}

func TestCHG05WithTotalsRejectsGroupingSets(t *testing.T) {
	_, err := ExecuteSQLQuery(`
FROM VALUES ('east', 1), ('west', 2) AS src(region, amount)
SELECT src.region, SUM(src.amount) AS total
GROUP BY ROLLUP(src.region) WITH TOTALS`, nil)
	if err == nil || !strings.Contains(err.Error(), "requires a regular GROUP BY clause") {
		t.Fatalf("ROLLUP WITH TOTALS error = %v", err)
	}
}
