package hatSql_test

import (
	"fmt"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestSQLOrderByAll(t *testing.T) {
	result, err := hatSql.ExecuteSQLQuery(`FROM VALUES ('b', 2), ('a', 3), ('a', 1) AS src(region, amount) SELECT src.region, src.amount ORDER BY ALL`, nil)
	if err != nil {
		t.Fatal(err)
	}
	assertOrderByAllRows(t, result.Rows, []struct {
		region string
		amount string
	}{{"a", "1"}, {"a", "3"}, {"b", "2"}})

	descending, err := hatSql.ExecuteSQLQuery(`FROM VALUES ('b', 2), ('a', 3), ('a', 1) AS src(region, amount) SELECT src.region, src.amount ORDER BY ALL DESC`, nil)
	if err != nil {
		t.Fatal(err)
	}
	assertOrderByAllRows(t, descending.Rows, []struct {
		region string
		amount string
	}{{"b", "2"}, {"a", "3"}, {"a", "1"}})
}

func TestSQLOrderByAllSupportsExpressionsAndAggregateOnlyQueries(t *testing.T) {
	result, err := hatSql.ExecuteSQLQuery(`FROM VALUES ('west', 2), ('east', 5), ('east', 1) AS src(region, amount) SELECT LOWER(src.region) AS normalized, src.amount + 1 AS adjusted ORDER BY ALL`, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 3 {
		t.Fatalf("expression rows = %#v, want three rows", result.Rows)
	}
	for index, expected := range []struct {
		normalized string
		adjusted   string
	}{{"east", "2"}, {"east", "6"}, {"west", "3"}} {
		if got := fmt.Sprint(result.Rows[index]["normalized"]); got != expected.normalized {
			t.Fatalf("row %d normalized = %q, want %q; rows = %#v", index, got, expected.normalized, result.Rows)
		}
		if got := fmt.Sprint(result.Rows[index]["adjusted"]); got != expected.adjusted {
			t.Fatalf("row %d adjusted = %q, want %q; rows = %#v", index, got, expected.adjusted, result.Rows)
		}
	}

	aggregate, err := hatSql.ExecuteSQLQuery(`FROM VALUES ('east', 2), ('west', 5) AS src(region, amount) SELECT SUM(src.amount) AS total ORDER BY ALL`, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(aggregate.Rows) != 1 || fmt.Sprint(aggregate.Rows[0]["total"]) != "7" {
		t.Fatalf("aggregate ORDER BY ALL rows = %#v, want one total row", aggregate.Rows)
	}
}

func TestSQLOrderByAllCombinesWithGroupByAllAndNullPlacement(t *testing.T) {
	grouped, err := hatSql.ExecuteSQLQuery(`FROM VALUES ('west', 2), ('east', 5), ('east', 1) AS src(region, amount) SELECT src.region, SUM(src.amount) AS total GROUP BY ALL ORDER BY ALL`, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(grouped.Rows) != 2 {
		t.Fatalf("grouped rows = %#v, want two rows", grouped.Rows)
	}
	for index, expected := range []struct {
		region string
		total  string
	}{{"east", "6"}, {"west", "2"}} {
		if got := fmt.Sprint(grouped.Rows[index]["region"]); got != expected.region {
			t.Fatalf("grouped row %d region = %q, want %q; rows = %#v", index, got, expected.region, grouped.Rows)
		}
		if got := fmt.Sprint(grouped.Rows[index]["total"]); got != expected.total {
			t.Fatalf("grouped row %d total = %q, want %q; rows = %#v", index, got, expected.total, grouped.Rows)
		}
	}

	nullsLast, err := hatSql.ExecuteSQLQuery(`FROM VALUES (NULL), ('a') AS src(value) SELECT src.value ORDER BY ALL NULLS LAST`, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(nullsLast.Rows) != 2 || fmt.Sprint(nullsLast.Rows[0]["value"]) != "a" || nullsLast.Rows[1]["value"] != nil {
		t.Fatalf("NULLS LAST rows = %#v, want a then NULL", nullsLast.Rows)
	}
}

func TestSQLOrderByAllRejectsWildcardAndDuplicateClauses(t *testing.T) {
	if _, err := hatSql.ExecuteSQLQuery(`FROM VALUES ('east', 2), ('west', 5) AS src(region, amount) SELECT * ORDER BY ALL`, nil); err == nil {
		t.Fatal("SELECT * ORDER BY ALL should be rejected without schema expansion")
	}
	if _, err := hatSql.ExecuteSQLQuery(`FROM VALUES ('east', 2), ('west', 5) AS src(region, amount) SELECT src.region ORDER BY ALL ORDER BY src.region`, nil); err == nil {
		t.Fatal("duplicate ORDER BY clauses should be rejected")
	}
}

func assertOrderByAllRows(t *testing.T, rows []hatSql.Row, want []struct {
	region string
	amount string
}) {
	t.Helper()
	if len(rows) != len(want) {
		t.Fatalf("rows = %#v, want %d rows", rows, len(want))
	}
	for index, expected := range want {
		if got := fmt.Sprint(rows[index]["region"]); got != expected.region {
			t.Fatalf("row %d region = %q, want %q; rows = %#v", index, got, expected.region, rows)
		}
		if got := fmt.Sprint(rows[index]["amount"]); got != expected.amount {
			t.Fatalf("row %d amount = %q, want %q; rows = %#v", index, got, expected.amount, rows)
		}
	}
}

var orderByAllBenchmarkSink int

func BenchmarkSQLOrderByAll(b *testing.B) {
	rows := make([]hatSql.Row, 0, 4096)
	for index := 0; index < 4096; index++ {
		rows = append(rows, hatSql.Row{
			"region":  fmt.Sprintf("r%02d", index%8),
			"product": fmt.Sprintf("p%02d", index%32),
		})
	}
	resolver := hatSql.SourceResolverFunc(func(string, string) ([]hatSql.Row, error) {
		return rows, nil
	})
	queries := map[string]string{
		"explicit": "FROM CACHE('events') AS src SELECT src.region, src.product ORDER BY src.region, src.product",
		"all":      "FROM CACHE('events') AS src SELECT src.region, src.product ORDER BY ALL",
	}
	for name, query := range queries {
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for iteration := 0; iteration < b.N; iteration++ {
				result, err := hatSql.ExecuteSQLQuery(query, resolver)
				if err != nil {
					b.Fatal(err)
				}
				orderByAllBenchmarkSink += len(result.Rows)
			}
		})
	}
}
