package hatSql

import (
	"context"
	"testing"
)

func BenchmarkMZ039SQLDataflowBaseline(b *testing.B) {
	benchmarkMZ039SQLDataflow(b, SQLQueryOptions{})
}

func BenchmarkMZ039SQLDataflowYieldEvery64(b *testing.B) {
	benchmarkMZ039SQLDataflow(b, SQLQueryOptions{OperatorYieldEvery: 64})
}

func BenchmarkMZ039SQLDataflowYieldEvery1024(b *testing.B) {
	benchmarkMZ039SQLDataflow(b, SQLQueryOptions{OperatorYieldEvery: 1024})
}

func benchmarkMZ039SQLDataflow(b *testing.B, options SQLQueryOptions) {
	query, resolver := mz039BenchmarkFixture(b)
	control, cancel, err := newSQLExecutionControl(context.Background(), options)
	if err != nil {
		b.Fatal(err)
	}
	defer cancel()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, handled, err := executeSQLAutoNativeDataflow(control.ctx, query, resolver, options, control, false); err != nil {
			b.Fatal(err)
		} else if !handled {
			b.Fatal("benchmark query was not handled by native dataflow")
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(control.yields.Load())/float64(b.N), "yields/op")
}

func mz039BenchmarkFixture(tb testing.TB) (*sqlQuery, SQLSourceResolver) {
	tb.Helper()
	compiled, err := CompileSQLQuery("FROM CACHE('items') AS src SELECT src.id AS id, src.value AS value WHERE src.value >= 0")
	if err != nil {
		tb.Fatal(err)
	}
	rows := make([]SQLRow, 4096)
	for index := range rows {
		rows[index] = SQLRow{"id": int64(index), "value": int64(index % 100)}
	}
	resolver := SQLSourceResolverFunc(func(string, string) ([]Row, error) {
		return rows, nil
	})
	return compiled.template, resolver
}
