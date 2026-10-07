package hatSql_test

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"hatrie_cache/hat/hatSql"
)

var withTotalsBenchmarkSink hatSql.SQLQueryResult
var withTotalsBenchmarkWireSink []byte

func TestSQLWithTotalsJSONWireSize(t *testing.T) {
	grouped, err := hatSql.ExecuteSQLQuery(strings.TrimSuffix(withTotalsBenchmarkQuery(), " WITH TOTALS"), nil)
	if err != nil {
		t.Fatal(err)
	}
	totals, err := hatSql.ExecuteSQLQuery(withTotalsBenchmarkQuery(), nil)
	if err != nil {
		t.Fatal(err)
	}
	groupedPayload, err := json.Marshal(grouped)
	if err != nil {
		t.Fatal(err)
	}
	totalsPayload, err := json.Marshal(totals)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("grouped_wire_bytes=%d totals_wire_bytes=%d delta=%d", len(groupedPayload), len(totalsPayload), len(totalsPayload)-len(groupedPayload))
}

func BenchmarkSQLWithTotals(b *testing.B) {
	query := withTotalsBenchmarkQuery()
	grouped := strings.TrimSuffix(query, " WITH TOTALS")
	explicitGroupingSet := strings.Replace(grouped, "GROUP BY src.region", "GROUP BY GROUPING SETS ((src.region), ())", 1)
	benchmarks := []struct {
		name  string
		query string
	}{
		{name: "GroupedBaseline", query: grouped},
		{name: "ExplicitGroupingSet", query: explicitGroupingSet},
		{name: "WithTotals", query: query},
	}
	for _, benchmark := range benchmarks {
		b.Run(benchmark.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				result, err := hatSql.ExecuteSQLQuery(benchmark.query, nil)
				if err != nil {
					b.Fatal(err)
				}
				withTotalsBenchmarkSink = result
			}
		})
	}
}

func BenchmarkSQLWithTotalsJSON(b *testing.B) {
	grouped, err := hatSql.ExecuteSQLQuery(strings.TrimSuffix(withTotalsBenchmarkQuery(), " WITH TOTALS"), nil)
	if err != nil {
		b.Fatal(err)
	}
	totals, err := hatSql.ExecuteSQLQuery(withTotalsBenchmarkQuery(), nil)
	if err != nil {
		b.Fatal(err)
	}
	benchmarks := []struct {
		name   string
		result hatSql.SQLQueryResult
	}{
		{name: "GroupedBaseline", result: grouped},
		{name: "WithTotals", result: totals},
	}
	for _, benchmark := range benchmarks {
		b.Run(benchmark.name, func(b *testing.B) {
			payload, err := json.Marshal(benchmark.result)
			if err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.SetBytes(int64(len(payload)))
			b.ReportMetric(float64(len(payload)), "wire_bytes")
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				withTotalsBenchmarkWireSink, err = json.Marshal(benchmark.result)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func withTotalsBenchmarkQuery() string {
	values := make([]string, 0, 512)
	for i := 0; i < 512; i++ {
		values = append(values, fmt.Sprintf("('region-%02d', %d)", i%32, i))
	}
	return "FROM VALUES " + strings.Join(values, ", ") + " AS src(region, amount) SELECT src.region, SUM(src.amount) AS total GROUP BY src.region WITH TOTALS"
}
