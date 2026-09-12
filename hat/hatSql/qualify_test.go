package hatSql_test

import (
	"context"
	"fmt"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestSQLQualifyFiltersAfterWindow(t *testing.T) {
	result, err := hatSql.ExecuteSQLQuery(`FROM VALUES ('a', 20), ('a', 10), ('a', 30), ('b', 5) AS src(category, amount) SELECT src.category, src.amount, ROW_NUMBER() OVER (PARTITION BY src.category ORDER BY src.amount) AS row_number QUALIFY row_number <= 2 ORDER BY src.category, src.amount`, nil)
	if err != nil {
		t.Fatalf("QUALIFY query error = %v", err)
	}
	if len(result.Rows) != 3 {
		t.Fatalf("rows = %#v, want three rows", result.Rows)
	}
	want := []struct {
		category string
		amount   int64
		rank     int64
	}{
		{category: "a", amount: 10, rank: 1},
		{category: "a", amount: 20, rank: 2},
		{category: "b", amount: 5, rank: 1},
	}
	for index, expected := range want {
		row := result.Rows[index]
		if row["category"] != expected.category || row["amount"] != expected.amount || row["row_number"] != expected.rank {
			t.Fatalf("row %d = %#v, want category=%q amount=%d row_number=%d", index, row, expected.category, expected.amount, expected.rank)
		}
	}
}

func TestSQLQualifySupportsNestedPredicatesAndNamedWindows(t *testing.T) {
	result, err := hatSql.ExecuteSQLQuery(`FROM VALUES ('a', 20), ('a', 10), ('a', 30), ('b', 5) AS src(category, amount) SELECT src.category, src.amount, ROW_NUMBER() OVER ranked AS row_number WINDOW ranked AS (PARTITION BY src.category ORDER BY src.amount) QUALIFY row_number <= 2 AND src.amount >= 10 ORDER BY src.category, src.amount`, nil)
	if err != nil {
		t.Fatalf("QUALIFY query error = %v", err)
	}
	if len(result.Rows) != 2 {
		t.Fatalf("rows = %#v, want two rows", result.Rows)
	}
	if result.Rows[0]["amount"] != int64(10) || result.Rows[1]["amount"] != int64(20) {
		t.Fatalf("rows = %#v, want amounts 10 and 20", result.Rows)
	}
}

func TestSQLQualifyRequiresSelectedWindowAndAliasReference(t *testing.T) {
	if _, err := hatSql.ExecuteSQLQuery(`FROM VALUES (1) AS src(amount) SELECT src.amount QUALIFY src.amount > 0`, nil); err == nil {
		t.Fatal("QUALIFY without a selected window should fail")
	}
	if _, err := hatSql.ExecuteSQLQuery(`FROM VALUES (1) AS src(amount) SELECT src.amount, ROW_NUMBER() OVER (ORDER BY src.amount) AS row_number QUALIFY ROW_NUMBER() OVER (ORDER BY src.amount) = 1`, nil); err == nil {
		t.Fatal("direct QUALIFY window expression should fail")
	}
}

func TestSQLQualifyQueryRowsUsesMaterializedWindowPhase(t *testing.T) {
	var rows []hatSql.SQLRow
	err := hatSql.ExecuteSQLQueryRows(context.Background(), `FROM VALUES ('a', 20), ('a', 10), ('b', 5) AS src(category, amount) SELECT src.category, src.amount, ROW_NUMBER() OVER (PARTITION BY src.category ORDER BY src.amount) AS row_number QUALIFY row_number = 1 ORDER BY src.category`, nil, nil, hatSql.SQLQueryOptions{}, func(columns []string, row hatSql.SQLRow) error {
		if len(columns) != 3 {
			t.Fatalf("columns = %#v, want three columns", columns)
		}
		rows = append(rows, row)
		return nil
	})
	if err != nil {
		t.Fatalf("streamed QUALIFY query error = %v", err)
	}
	if len(rows) != 2 || rows[0]["row_number"] != int64(1) || rows[1]["row_number"] != int64(1) {
		t.Fatalf("rows = %#v, want one row per category", rows)
	}
}

func TestSQLQualifyMatchesSubqueryWorkaround(t *testing.T) {
	qualify, err := hatSql.ExecuteSQLQuery(`FROM VALUES ('a', 20), ('a', 10), ('a', 30), ('b', 5) AS src(category, amount) SELECT src.category, src.amount, ROW_NUMBER() OVER (PARTITION BY src.category ORDER BY src.amount) AS row_number QUALIFY row_number <= 2 ORDER BY src.category, src.amount`, nil)
	if err != nil {
		t.Fatalf("QUALIFY query error = %v", err)
	}
	workaround, err := hatSql.ExecuteSQLQuery(`FROM (FROM VALUES ('a', 20), ('a', 10), ('a', 30), ('b', 5) AS src(category, amount) SELECT src.category, src.amount, ROW_NUMBER() OVER (PARTITION BY src.category ORDER BY src.amount) AS row_number) AS ranked SELECT ranked.category, ranked.amount, ranked.row_number WHERE ranked.row_number <= 2 ORDER BY ranked.category, ranked.amount`, nil)
	if err != nil {
		t.Fatalf("subquery workaround error = %v", err)
	}
	if fmt.Sprintf("%#v", qualify.Rows) != fmt.Sprintf("%#v", workaround.Rows) {
		t.Fatalf("QUALIFY rows = %#v, workaround rows = %#v", qualify.Rows, workaround.Rows)
	}
}

func BenchmarkSQLQualifyAgainstSubquery(b *testing.B) {
	rows := make([]hatSql.Row, 1024)
	for index := range rows {
		rows[index] = hatSql.Row{"category": fmt.Sprintf("group-%02d", index%16), "amount": int64(index)}
	}
	resolver := hatSql.SourceResolverFunc(func(string, string) ([]hatSql.Row, error) {
		return rows, nil
	})
	qualifyQuery := `FROM CACHE('rows') AS src SELECT src.category, src.amount, ROW_NUMBER() OVER (PARTITION BY src.category ORDER BY src.amount) AS row_number QUALIFY row_number <= 2 ORDER BY src.category, src.amount`
	workaroundQuery := `FROM (FROM CACHE('rows') AS src SELECT src.category, src.amount, ROW_NUMBER() OVER (PARTITION BY src.category ORDER BY src.amount) AS row_number) AS ranked SELECT ranked.category, ranked.amount, ranked.row_number WHERE ranked.row_number <= 2 ORDER BY ranked.category, ranked.amount`
	for _, test := range []struct {
		name  string
		query string
	}{
		{name: "qualify", query: qualifyQuery},
		{name: "subquery_where", query: workaroundQuery},
	} {
		b.Run(test.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				result, err := hatSql.ExecuteSQLQuery(test.query, resolver)
				if err != nil {
					b.Fatal(err)
				}
				if len(result.Rows) != 32 {
					b.Fatalf("rows = %d, want 32", len(result.Rows))
				}
			}
		})
	}
}

func TestSQLQualifyExplainPlanIncludesPhase(t *testing.T) {
	result, err := hatSql.ExecuteSQLQuery(`EXPLAIN FROM VALUES ('a', 1) AS src(category, amount) SELECT src.category, ROW_NUMBER() OVER (ORDER BY src.amount) AS row_number QUALIFY row_number = 1`, nil)
	if err != nil {
		t.Fatalf("EXPLAIN QUALIFY query error = %v", err)
	}
	for _, row := range result.Rows {
		if row["node"] == "QUALIFY" {
			return
		}
	}
	t.Fatalf("EXPLAIN rows = %#v, missing QUALIFY phase", result.Rows)
}
