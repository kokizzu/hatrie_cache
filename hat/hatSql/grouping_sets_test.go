package hatSql_test

import (
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestSQLRollupCubeAndGroupingSets(t *testing.T) {
	rollup, err := hatSql.ExecuteSQLQuery(`FROM VALUES ('east', 'book', 2), ('east', 'pen', 3), ('west', 'book', 5) AS src(region, product, amount) SELECT src.region, src.product, SUM(src.amount) AS total GROUP BY ROLLUP(src.region, src.product)`, nil)
	if err != nil {
		t.Fatalf("ROLLUP error = %v", err)
	}
	if len(rollup.Rows) != 6 || !sqlGroupingRowExists(rollup.Rows, "east", nil, 5) || !sqlGroupingRowExists(rollup.Rows, nil, nil, 10) {
		t.Fatalf("ROLLUP rows = %#v, want detail, subtotal, and grand total rows", rollup.Rows)
	}

	cube, err := hatSql.ExecuteSQLQuery(`FROM VALUES ('east', 'book', 2), ('east', 'pen', 3), ('west', 'book', 5) AS src(region, product, amount) SELECT src.region, src.product, SUM(src.amount) AS total GROUP BY CUBE(src.region, src.product)`, nil)
	if err != nil {
		t.Fatalf("CUBE error = %v", err)
	}
	if len(cube.Rows) != 8 || !sqlGroupingRowExists(cube.Rows, nil, "book", 7) || !sqlGroupingRowExists(cube.Rows, nil, nil, 10) {
		t.Fatalf("CUBE rows = %#v, want product subtotal and grand total", cube.Rows)
	}

	sets, err := hatSql.ExecuteSQLQuery(`FROM VALUES ('east', 'book', 2), ('east', 'pen', 3), ('west', 'book', 5) AS src(region, product, amount) SELECT src.region, src.product, SUM(src.amount) AS total GROUP BY GROUPING SETS ((src.region), (src.product), ())`, nil)
	if err != nil {
		t.Fatalf("GROUPING SETS error = %v", err)
	}
	if len(sets.Rows) != 5 || !sqlGroupingRowExists(sets.Rows, "east", nil, 5) || !sqlGroupingRowExists(sets.Rows, nil, "book", 7) || !sqlGroupingRowExists(sets.Rows, nil, nil, 10) {
		t.Fatalf("GROUPING SETS rows = %#v, want requested sets", sets.Rows)
	}
}

func sqlGroupingRowExists(rows []hatSql.Row, region, product interface{}, total int64) bool {
	for _, row := range rows {
		value, ok := hatSql.Number(row["total"])
		if row["region"] == region && row["product"] == product && ok && int64(value) == total {
			return true
		}
	}
	return false
}
