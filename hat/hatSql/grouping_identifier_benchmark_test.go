package hatSql

import "testing"

var benchmarkSQLGroupingIdentifierSink []Row

func BenchmarkSQLGroupingSetIdentifiers(b *testing.B) {
	queries := []struct {
		name  string
		query string
	}{
		{
			name: "grouping_sets",
			query: `
FROM VALUES
  ('east', 'a', 10),
  ('east', 'b', 20),
  ('west', 'a', 30)
AS src(region, product, amount)
SELECT src.region AS region, src.product AS product, SUM(src.amount) AS total
GROUP BY GROUPING SETS ((src.region, src.product), (src.region), ())`,
		},
		{
			name: "grouping_identifiers",
			query: `
FROM VALUES
  ('east', 'a', 10),
  ('east', 'b', 20),
  ('west', 'a', 30)
AS src(region, product, amount)
SELECT src.region AS region, src.product AS product,
       SUM(src.amount) AS total,
       GROUPING(src.region) AS region_grouped,
       GROUPING(src.product) AS product_grouped
GROUP BY GROUPING SETS ((src.region, src.product), (src.region), ())`,
		},
	}
	for _, test := range queries {
		b.Run(test.name, func(b *testing.B) {
			b.ReportAllocs()
			for index := 0; index < b.N; index++ {
				result, err := ExecuteSQLQuery(test.query, nil)
				if err != nil || len(result.Rows) != 6 {
					b.Fatalf("execute grouping query: err=%v rows=%d", err, len(result.Rows))
				}
				benchmarkSQLGroupingIdentifierSink = result.Rows
			}
		})
	}
}
