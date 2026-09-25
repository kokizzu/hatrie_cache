package hatSql

import (
	"context"
	"testing"
)

func TestSQLGroupingIDMultipleArguments(t *testing.T) {
	result, err := ExecuteSQLQueryContext(context.Background(), `
FROM VALUES
  ('east', 'book', 2),
  ('east', 'pen', 3),
  ('west', 'book', 5),
  ('west', 'pen', 7)
AS src(region, product, amount)
SELECT src.region AS region,
       src.product AS product,
       GROUPING_ID(src.region, src.product) AS grouping_id,
       SUM(src.amount) AS total
GROUP BY CUBE (src.region, src.product)`, nil, SQLQueryOptions{})
	if err != nil {
		t.Fatalf("GROUPING_ID query error: %v", err)
	}
	wantCounts := map[int64]int{0: 4, 1: 2, 2: 2, 3: 1}
	gotCounts := make(map[int64]int)
	for _, row := range result.Rows {
		value, ok := Number(row["grouping_id"])
		if !ok {
			t.Fatalf("grouping_id row = %#v, want numeric id", row)
		}
		groupingID := int64(value)
		gotCounts[groupingID]++
		switch groupingID {
		case 0:
			if row["region"] == nil || row["product"] == nil {
				t.Fatalf("detail row = %#v, want both dimensions", row)
			}
		case 1:
			if row["region"] == nil || row["product"] != nil {
				t.Fatalf("region subtotal row = %#v, want only product absent", row)
			}
		case 2:
			if row["region"] != nil || row["product"] == nil {
				t.Fatalf("product subtotal row = %#v, want only region absent", row)
			}
		case 3:
			if row["region"] != nil || row["product"] != nil {
				t.Fatalf("grand total row = %#v, want both dimensions absent", row)
			}
		default:
			t.Fatalf("unexpected grouping_id %d in row %#v", groupingID, row)
		}
	}
	if len(result.Rows) != 9 {
		t.Fatalf("rows = %d, want 9", len(result.Rows))
	}
	for groupingID, want := range wantCounts {
		if got := gotCounts[groupingID]; got != want {
			t.Errorf("grouping_id %d rows = %d, want %d", groupingID, got, want)
		}
	}
}

func TestSQLGroupingIDValidation(t *testing.T) {
	for _, query := range []string{
		`FROM VALUES ('east', 1) AS src(region, amount) SELECT GROUPING_ID() GROUP BY GROUPING SETS ((src.region), ())`,
		`FROM VALUES ('east', 1) AS src(region, amount) SELECT GROUPING_ID(src.missing) GROUP BY GROUPING SETS ((src.region), ())`,
		`FROM VALUES ('east', 1) AS src(region, amount) SELECT GROUPING_ID(src.region, src.amount) GROUP BY GROUPING SETS ((src.region), ())`,
	} {
		if _, err := ExecuteSQLQuery(query, nil); err == nil {
			t.Errorf("query unexpectedly succeeded: %s", query)
		}
	}
}

func TestSQLGroupingIDExpandedExecution(t *testing.T) {
	result, err := ExecuteSQLQueryContext(context.Background(), `
FROM VALUES
  ('east', 'book', 2),
  ('east', 'pen', 3),
  ('west', 'book', 5),
  ('west', 'pen', 7)
AS src(region, product, amount)
SELECT src.region AS region,
       src.product AS product,
       GROUPING_ID(src.region, src.product) AS grouping_id,
       SUM(src.amount) AS total
GROUP BY CUBE (src.region, src.product)`, nil, SQLQueryOptions{MaxGroupBytes: 1 << 20})
	if err != nil {
		t.Fatalf("expanded GROUPING_ID query error: %v", err)
	}
	counts := make(map[int64]int)
	for _, row := range result.Rows {
		value, ok := Number(row["grouping_id"])
		if !ok {
			t.Fatalf("expanded row = %#v, want numeric grouping_id", row)
		}
		counts[int64(value)]++
	}
	want := map[int64]int{0: 4, 1: 2, 2: 2, 3: 1}
	if len(result.Rows) != 9 {
		t.Fatalf("expanded rows = %d, want 9", len(result.Rows))
	}
	for groupingID, wantCount := range want {
		if counts[groupingID] != wantCount {
			t.Errorf("expanded grouping_id %d rows = %d, want %d", groupingID, counts[groupingID], wantCount)
		}
	}
}

func BenchmarkSQLGroupingIdentifierExisting(b *testing.B) {
	query := `
FROM VALUES
  ('east', 'book', 2),
  ('east', 'pen', 3),
  ('west', 'book', 5),
  ('west', 'pen', 7)
AS src(region, product, amount)
SELECT src.region AS region,
       src.product AS product,
       (GROUPING(src.region) * 2 + GROUPING(src.product)) AS grouping_id,
       SUM(src.amount) AS total
GROUP BY CUBE (src.region, src.product)`
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		if _, err := ExecuteSQLQuery(query, nil); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSQLGroupingIdentifierMultiple(b *testing.B) {
	query := `
FROM VALUES
  ('east', 'book', 2),
  ('east', 'pen', 3),
  ('west', 'book', 5),
  ('west', 'pen', 7)
AS src(region, product, amount)
SELECT src.region AS region,
       src.product AS product,
       GROUPING_ID(src.region, src.product) AS grouping_id,
       SUM(src.amount) AS total
GROUP BY CUBE (src.region, src.product)`
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		if _, err := ExecuteSQLQuery(query, nil); err != nil {
			b.Fatal(err)
		}
	}
}
