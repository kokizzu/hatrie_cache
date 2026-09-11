package hatSql

import (
	"fmt"
	"strings"
	"testing"
)

func TestSQLGroupingSetsGroupingIdentifiers(t *testing.T) {
	result, err := ExecuteSQLQuery(`
FROM VALUES
  ('east', 'a', 10),
  ('east', 'b', 20),
  ('west', 'a', 30)
AS src(region, product, amount)
SELECT src.region AS region,
       src.product AS product,
       SUM(src.amount) AS total,
       GROUPING(src.region) AS region_grouped,
       GROUPING(src.product) AS product_grouped
GROUP BY GROUPING SETS ((src.region, src.product), (src.region), ())`, nil)
	if err != nil {
		t.Fatal(err)
	}

	want := map[string]struct {
		total, regionGrouped, productGrouped int64
	}{
		"east/a":      {10, 0, 0},
		"east/b":      {20, 0, 0},
		"west/a":      {30, 0, 0},
		"east/<nil>":  {30, 0, 1},
		"west/<nil>":  {30, 0, 1},
		"<nil>/<nil>": {60, 1, 1},
	}
	if len(result.Rows) != len(want) {
		t.Fatalf("rows = %d, want %d: %#v", len(result.Rows), len(want), result.Rows)
	}
	for _, row := range result.Rows {
		key := fmt.Sprintf("%v/%v", row["region"], row["product"])
		got, ok := want[key]
		if !ok {
			t.Fatalf("unexpected grouping row %q: %#v", key, row)
		}
		total, totalOK := sqlGroupingIdentifierInteger(row["total"])
		regionGrouped, regionOK := sqlGroupingIdentifierInteger(row["region_grouped"])
		productGrouped, productOK := sqlGroupingIdentifierInteger(row["product_grouped"])
		if !totalOK || total != got.total || !regionOK || !productOK || regionGrouped != got.regionGrouped || productGrouped != got.productGrouped {
			t.Errorf("row %q = %#v, want total=%d region_grouped=%d product_grouped=%d", key, row, got.total, got.regionGrouped, got.productGrouped)
		}
		delete(want, key)
	}
	if len(want) != 0 {
		t.Fatalf("missing grouping rows: %#v", want)
	}
}

func TestSQLRollupAndCubeGroupingIdentifiers(t *testing.T) {
	queries := []struct {
		name  string
		query string
		want  int
	}{
		{
			name: "rollup",
			query: `
FROM VALUES
  ('east', 'a', 10),
  ('east', 'b', 20),
  ('west', 'a', 30)
AS src(region, product, amount)
SELECT src.region AS region, src.product AS product,
       GROUPING(src.region) AS region_grouped,
       GROUPING(src.product) AS product_grouped,
       SUM(src.amount) AS total
GROUP BY ROLLUP (src.region, src.product)`,
			want: 6,
		},
		{
			name: "cube",
			query: `
FROM VALUES
  ('east', 'a', 10),
  ('east', 'b', 20),
  ('west', 'a', 30)
AS src(region, product, amount)
SELECT src.region AS region, src.product AS product,
       GROUPING(src.region) AS region_grouped,
       GROUPING(src.product) AS product_grouped,
       SUM(src.amount) AS total
GROUP BY CUBE (src.region, src.product)`,
			want: 8,
		},
	}
	for _, test := range queries {
		t.Run(test.name, func(t *testing.T) {
			result, err := ExecuteSQLQuery(test.query, nil)
			if err != nil {
				t.Fatal(err)
			}
			if len(result.Rows) != test.want {
				t.Fatalf("rows = %d, want %d: %#v", len(result.Rows), test.want, result.Rows)
			}
			for _, row := range result.Rows {
				regionGrouped, regionOK := sqlGroupingIdentifierInteger(row["region_grouped"])
				productGrouped, productOK := sqlGroupingIdentifierInteger(row["product_grouped"])
				if !regionOK || !productOK || regionGrouped < 0 || regionGrouped > 1 || productGrouped < 0 || productGrouped > 1 {
					t.Fatalf("invalid grouping identifiers: %#v", row)
				}
			}
		})
	}
}

func TestSQLGroupingIdentifierNestedExpressionAndValidation(t *testing.T) {
	result, err := ExecuteSQLQuery(`
FROM VALUES ('east', 1), ('west', 2) AS src(region, amount)
SELECT COALESCE(GROUPING(src.region), 9) AS grouped, SUM(src.amount) AS total
GROUP BY ROLLUP (src.region)`, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 3 {
		t.Fatalf("rows = %d, want 3: %#v", len(result.Rows), result.Rows)
	}
	for _, row := range result.Rows {
		value, ok := sqlGroupingIdentifierInteger(row["grouped"])
		if !ok || value != 0 && value != 1 {
			t.Fatalf("grouped value = %#v, want 0 or 1", row["grouped"])
		}
	}

	for _, query := range []string{
		`FROM VALUES ('east', 1) AS src(region, amount) SELECT GROUPING(src.missing) GROUP BY GROUPING SETS ((src.region), ())`,
		`FROM VALUES ('east', 1) AS src(region, amount) SELECT GROUPING(src.region, src.amount) GROUP BY GROUPING SETS ((src.region), ())`,
	} {
		_, err := ExecuteSQLQuery(query, nil)
		if err == nil || !strings.Contains(err.Error(), "GROUPING") {
			t.Fatalf("query error = %v, want GROUPING validation error", err)
		}
	}
}

func TestSQLGroupingIdentifierOrdinaryGroupDefaultsToZero(t *testing.T) {
	result, err := ExecuteSQLQuery(`
FROM VALUES ('east', 1), ('east', 2) AS src(region, amount)
SELECT src.region AS region, GROUPING(src.region) AS grouped, SUM(src.amount) AS total
GROUP BY src.region`, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 1 {
		t.Fatalf("rows = %d, want 1: %#v", len(result.Rows), result.Rows)
	}
	if value, ok := sqlGroupingIdentifierInteger(result.Rows[0]["grouped"]); !ok || value != 0 {
		t.Fatalf("grouped value = %#v, want 0", result.Rows[0]["grouped"])
	}
}

func sqlGroupingIdentifierInteger(value interface{}) (int64, bool) {
	switch value := value.(type) {
	case int:
		return int64(value), true
	case int64:
		return value, true
	case float64:
		return int64(value), value == float64(int64(value))
	default:
		return 0, false
	}
}
