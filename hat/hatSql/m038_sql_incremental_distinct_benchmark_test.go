package hatSql

import "testing"

var m038SQLDistinctBenchmarkSink QueryResult

func m038SQLDistinctBenchmarkRows() []SQLRow {
	rows := make([]SQLRow, 4096)
	for index := range rows {
		rows[index] = SQLRow{
			"bucket":  int64(index % 256),
			"payload": "payload",
		}
	}
	return rows
}

func BenchmarkM038SQLIncrementalDistinctRebuild(b *testing.B) {
	rows := m038SQLDistinctBenchmarkRows()
	const query = "FROM CACHE('events') SELECT DISTINCT bucket"
	resolver := SQLSourceResolverFunc(func(string, string) ([]SQLRow, error) {
		return rows, nil
	})
	if _, err := ExecuteSQLQuery(query, resolver); err != nil {
		b.Fatalf("warmup query: %v", err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		result, err := ExecuteSQLQuery(query, resolver)
		if err != nil {
			b.Fatalf("rebuild query: %v", err)
		}
		m038SQLDistinctBenchmarkSink = result
	}
}

func BenchmarkM038SQLIncrementalDistinctApply(b *testing.B) {
	rows := m038SQLDistinctBenchmarkRows()
	compiled, err := CompileSQLQuery("FROM CACHE('events') SELECT DISTINCT bucket")
	if err != nil {
		b.Fatalf("compile query: %v", err)
	}
	operator, err := compiled.CompileIncrementalDistinct()
	if err != nil {
		b.Fatalf("compile incremental distinct: %v", err)
	}
	seed := make([]DifferentialRow, len(rows))
	for index, row := range rows {
		seed[index] = DifferentialRow{Key: "seed-" + string(rune(index)), Diff: 1, Row: row}
	}
	if _, err := operator.Apply(seed); err != nil {
		b.Fatalf("seed incremental distinct: %v", err)
	}
	delta := []DifferentialRow{
		{Key: "delta", Time: 1, Diff: 1, Row: m038SQLDistinctRow(1)},
		{Key: "delta", Time: 2, Diff: -1, Row: m038SQLDistinctRow(1)},
	}
	var changes []DifferentialRow
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		changes, err = operator.Apply(delta)
		if err != nil {
			b.Fatalf("incremental apply: %v", err)
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(len(changes)), "change_rows")
}
