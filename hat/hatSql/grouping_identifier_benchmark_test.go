package hatSql

import "testing"

var benchmarkSQLGroupingIdentifierSink []Row

func BenchmarkSQLGroupingSetIdentifiers(b *testing.B) {
	queries := []struct {
		name  string
		query string
		rows  int
	}{
		{
			name: "grouping_sets",
			rows: 6,
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
			rows: 6,
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
		{
			name: "grouping_id",
			rows: 6,
			query: `
FROM VALUES
  ('east', 'a', 10),
  ('east', 'b', 20),
  ('west', 'a', 30)
AS src(region, product, amount)
SELECT src.region AS region, src.product AS product,
       SUM(src.amount) AS total,
       GROUPING_ID(src.region, src.product) AS grouping_id
GROUP BY GROUPING SETS ((src.region, src.product), (src.region), ())`,
		},
	}
	for _, test := range queries {
		b.Run(test.name, func(b *testing.B) {
			b.ReportAllocs()
			for index := 0; index < b.N; index++ {
				result, err := ExecuteSQLQuery(test.query, nil)
				if err != nil || len(result.Rows) != test.rows {
					b.Fatalf("execute grouping query: err=%v rows=%d want=%d", err, len(result.Rows), test.rows)
				}
				benchmarkSQLGroupingIdentifierSink = result.Rows
			}
		})
	}
}
