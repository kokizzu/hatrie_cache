package hatSql

import "testing"

const ch041GroupingIDQuery = `
FROM VALUES
  ('east', 'a', 10),
  ('east', 'b', 20),
  ('west', 'a', 30)
AS src(region, product, amount)
SELECT src.region AS region,
       src.product AS product,
       GROUPING_ID(src.region, src.product) AS grouping_id,
       SUM(src.amount) AS total
GROUP BY CUBE (src.region, src.product)`

const ch041GroupingIDComposedQuery = `
FROM VALUES
  ('east', 'a', 10),
  ('east', 'b', 20),
  ('west', 'a', 30)
AS src(region, product, amount)
SELECT src.region AS region,
       src.product AS product,
       GROUPING(src.region) * 2 + GROUPING(src.product) AS grouping_id,
       SUM(src.amount) AS total
GROUP BY CUBE (src.region, src.product)`

func BenchmarkCH041GroupingID(b *testing.B) {
	benchmarkCH041GroupingIDQuery(b, ch041GroupingIDQuery)
}

func BenchmarkCH041GroupingIDComposedControl(b *testing.B) {
	benchmarkCH041GroupingIDQuery(b, ch041GroupingIDComposedQuery)
}

func benchmarkCH041GroupingIDQuery(b *testing.B, query string) {
	b.Helper()
	b.ReportAllocs()
	for count := 0; count < b.N; count++ {
		result, err := ExecuteSQLQuery(query, nil)
		if err != nil {
			b.Fatalf("execute grouping query: %v", err)
		}
		if len(result.Rows) != 8 {
			b.Fatalf("rows = %d, want 8", len(result.Rows))
		}
	}
}
