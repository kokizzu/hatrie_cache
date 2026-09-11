package hatSql

import "testing"

var benchmarkSQLAggregateIfResult int64

func BenchmarkSQLAggregateIfVsFilter(b *testing.B) {
	rows := make([]Row, 10_000)
	for index := range rows {
		rows[index] = Row{
			"active": index%2 == 0,
			"value":  index,
		}
	}
	resolver := SourceResolverFunc(func(string, string) ([]Row, error) {
		return rows, nil
	})
	filterQuery := `FROM CACHE('events') AS event SELECT COUNT(*) FILTER (WHERE event.active) AS matched`
	aggregateIfQuery := `FROM CACHE('events') AS event SELECT COUNT_IF(event.active) AS matched`
	for _, query := range []string{filterQuery, aggregateIfQuery} {
		result, err := ExecuteSQLQuery(query, resolver)
		if err != nil {
			b.Fatal(err)
		}
		if len(result.Rows) != 1 || result.Rows[0]["matched"] != int64(5_000) {
			b.Fatalf("warmup result = %#v", result.Rows)
		}
	}

	b.Run("filter_clause", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			result, err := ExecuteSQLQuery(filterQuery, resolver)
			if err != nil {
				b.Fatal(err)
			}
			benchmarkSQLAggregateIfResult = result.Rows[0]["matched"].(int64)
		}
	})
	b.Run("aggregate_if", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			result, err := ExecuteSQLQuery(aggregateIfQuery, resolver)
			if err != nil {
				b.Fatal(err)
			}
			benchmarkSQLAggregateIfResult = result.Rows[0]["matched"].(int64)
		}
	})
	if benchmarkSQLAggregateIfResult != 5_000 {
		b.Fatalf("benchmark result = %d", benchmarkSQLAggregateIfResult)
	}
}
