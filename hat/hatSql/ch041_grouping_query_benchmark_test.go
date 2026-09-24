package hatSql

import "testing"

const ch041GroupingSetQuery = `
FROM VALUES
  ('east', 'a', 'web', 'new', 10),
  ('east', 'b', 'web', 'returning', 20),
  ('west', 'a', 'store', 'new', 30),
  ('west', 'b', 'store', 'returning', 40)
AS src(region, product, channel, customer_type, amount)
SELECT src.region, src.product, src.channel, src.customer_type, SUM(src.amount) AS total
GROUP BY CUBE(src.region, src.product, src.channel, src.customer_type)`

var benchmarkCH041GroupingSetQuerySink []SQLRow

func BenchmarkCH041GroupingSetQuery(b *testing.B) {
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		result, err := ExecuteSQLQuery(ch041GroupingSetQuery, nil)
		if err != nil || len(result.Rows) == 0 {
			b.Fatalf("execute CUBE query: err=%v rows=%d", err, len(result.Rows))
		}
		benchmarkCH041GroupingSetQuerySink = result.Rows
	}
}
