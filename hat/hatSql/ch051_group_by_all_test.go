package hatSql_test

import (
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestSQLGroupByAll(t *testing.T) {
	result, err := hatSql.ExecuteSQLQuery(`FROM VALUES ('east', 'book', 2), ('east', 'pen', 3), ('west', 'book', 5) AS src(region, product, amount) SELECT src.region, src.product, SUM(src.amount) AS total GROUP BY ALL`, nil)
	if err != nil {
		t.Fatalf("GROUP BY ALL error = %v", err)
	}
	if len(result.Rows) != 3 {
		t.Fatalf("GROUP BY ALL rows = %#v, want three detail groups", result.Rows)
	}
	if !sqlGroupByAllRowExists(result.Rows, "east", "book", 2) || !sqlGroupByAllRowExists(result.Rows, "east", "pen", 3) || !sqlGroupByAllRowExists(result.Rows, "west", "book", 5) {
		t.Fatalf("GROUP BY ALL rows = %#v, want one aggregate per selected non-aggregate expression", result.Rows)
	}
}

func TestSQLGroupByAllAggregateOnlyAndRejectsStar(t *testing.T) {
	aggregate, err := hatSql.ExecuteSQLQuery(`FROM VALUES ('east', 2), ('west', 5) AS src(region, amount) SELECT SUM(src.amount) AS total GROUP BY ALL`, nil)
	if err != nil {
		t.Fatalf("aggregate-only GROUP BY ALL error = %v", err)
	}
	if len(aggregate.Rows) != 1 {
		t.Fatalf("aggregate-only GROUP BY ALL rows = %#v, want one row", aggregate.Rows)
	}
	if value, ok := hatSql.Number(aggregate.Rows[0]["total"]); !ok || int64(value) != 7 {
		t.Fatalf("aggregate-only GROUP BY ALL total = %#v, want 7", aggregate.Rows[0]["total"])
	}

	if _, err := hatSql.ExecuteSQLQuery(`FROM VALUES ('east', 2), ('west', 5) AS src(region, amount) SELECT * GROUP BY ALL`, nil); err == nil {
		t.Fatal("GROUP BY ALL with SELECT * succeeded, want a bounded diagnostic")
	}

	if _, err := hatSql.ExecuteSQLQuery(`FROM VALUES ('east', 2), ('west', 5) AS src(region, amount) SELECT src.region, SUM(src.amount) AS total GROUP BY ALL GROUP BY src.region`, nil); err == nil {
		t.Fatal("duplicate GROUP BY clause succeeded, want a diagnostic")
	}
}

func TestSQLGroupByAllSupportsScalarExpressions(t *testing.T) {
	result, err := hatSql.ExecuteSQLQuery(`FROM VALUES ('East', 2), ('east', 5), ('West', 7) AS src(region, amount) SELECT LOWER(src.region) AS normalized, SUM(src.amount) AS total GROUP BY ALL`, nil)
	if err != nil {
		t.Fatalf("scalar GROUP BY ALL error = %v", err)
	}
	if len(result.Rows) != 2 || !sqlGroupByAllRowExists(result.Rows, "east", "", 7) || !sqlGroupByAllRowExists(result.Rows, "west", "", 7) {
		t.Fatalf("scalar GROUP BY ALL rows = %#v, want case-folded groups", result.Rows)
	}

	mixed, err := hatSql.ExecuteSQLQuery(`FROM VALUES ('east', 2), ('east', 5), ('west', 7) AS src(region, amount) SELECT src.region, SUM(src.amount) + src.amount AS total GROUP BY ALL`, nil)
	if err != nil {
		t.Fatalf("mixed GROUP BY ALL error = %v", err)
	}
	if len(mixed.Rows) != 3 || !sqlGroupByAllRowExists(mixed.Rows, "east", "", 4) || !sqlGroupByAllRowExists(mixed.Rows, "east", "", 10) || !sqlGroupByAllRowExists(mixed.Rows, "west", "", 14) {
		t.Fatalf("mixed GROUP BY ALL rows = %#v, want non-aggregate components grouped", mixed.Rows)
	}
}

type groupByAllBenchmarkResolver struct {
	rows []hatSql.Row
}

func (resolver groupByAllBenchmarkResolver) ResolveSQLSource(string, string) ([]hatSql.Row, error) {
	return resolver.rows, nil
}

func BenchmarkSQLGroupByAll(b *testing.B) {
	rows := make([]hatSql.Row, 4096)
	for index := range rows {
		rows[index] = hatSql.Row{
			"region":  "region-" + string(rune('a'+index%8)),
			"product": "product-" + string(rune('a'+index%32)),
			"amount":  int64(index + 1),
		}
	}
	resolver := groupByAllBenchmarkResolver{rows: rows}
	queries := map[string]string{
		"explicit": `FROM CACHE('items') AS src SELECT src.region, src.product, SUM(src.amount) AS total GROUP BY src.region, src.product`,
		"all":      `FROM CACHE('items') AS src SELECT src.region, src.product, SUM(src.amount) AS total GROUP BY ALL`,
	}
	for name, query := range queries {
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				if _, err := hatSql.ExecuteSQLQuery(query, resolver); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func sqlGroupByAllRowExists(rows []hatSql.Row, region, product string, total int64) bool {
	for _, row := range rows {
		value, ok := hatSql.Number(row["total"])
		regionValue := row["region"]
		if normalized, exists := row["normalized"]; exists {
			regionValue = normalized
		}
		if regionValue == region && (product == "" || row["product"] == product) && ok && int64(value) == total {
			return true
		}
	}
	return false
}
