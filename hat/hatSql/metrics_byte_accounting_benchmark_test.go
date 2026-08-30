package hatSql

import "testing"

type sqlMetricsByteBenchmarkResolver struct{ rows []Row }

func (resolver sqlMetricsByteBenchmarkResolver) ResolveSQLSource(name, key string) ([]Row, error) {
	return resolver.rows, nil
}

var sqlMetricsByteBenchmarkResult SQLQueryResult

func BenchmarkSQLMetricsDisabledFilteredQuery(b *testing.B) {
	rows := make([]Row, 20_000)
	for index := range rows {
		rows[index] = Row{"id": int64(index), "team": "core", "score": int64(index % 64)}
	}
	resolver := sqlMetricsByteBenchmarkResolver{rows: rows}
	const query = "FROM CACHE('events') AS event WHERE event.score >= 32 SELECT event.id"
	b.ReportAllocs()
	for iteration := 0; iteration < b.N; iteration++ {
		result, err := ExecuteSQLQuery(query, resolver)
		if err != nil {
			b.Fatal(err)
		}
		sqlMetricsByteBenchmarkResult = result
	}
}
