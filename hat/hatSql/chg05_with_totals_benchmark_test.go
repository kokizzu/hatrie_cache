package hatSql

import "testing"

var benchmarkCHG05Result SQLQueryResult

func BenchmarkCHG05Group(b *testing.B) {
	query := `
FROM VALUES ('a', 1), ('b', 2), ('a', 3), ('c', 4), ('b', 5) AS src(category, amount)
SELECT src.category, SUM(src.amount) AS total
GROUP BY src.category
ORDER BY src.category`
	b.ReportAllocs()
	for range b.N {
		result, err := ExecuteSQLQuery(query, nil)
		if err != nil {
			b.Fatal(err)
		}
		benchmarkCHG05Result = result
	}
}

func BenchmarkCHG05WithTotals(b *testing.B) {
	query := `
FROM VALUES ('a', 1), ('b', 2), ('a', 3), ('c', 4), ('b', 5) AS src(category, amount)
SELECT src.category, SUM(src.amount) AS total
GROUP BY src.category WITH TOTALS
ORDER BY src.category`
	b.ReportAllocs()
	for range b.N {
		result, err := ExecuteSQLQuery(query, nil)
		if err != nil {
			b.Fatal(err)
		}
		benchmarkCHG05Result = result
	}
}
