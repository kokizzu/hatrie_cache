package hatSql

import (
	"context"
	"testing"
)

var m052DataflowExecutorBenchmarkSink *SQLDataflowExecutor

func m052DataflowExecutorBenchmarkRunner(_ context.Context, _ SQLDataflowFragment, inputs SQLDataflowFragmentInputs) ([]SQLRow, error) {
	if inputs.Len() == 0 {
		return inputs.Initial(), nil
	}
	return inputs.Rows(0), nil
}

func BenchmarkM052DataflowExecutorCompile(b *testing.B) {
	query, err := CompileSQLQuery("FROM VALUES (1) AS src(id) SELECT src.id")
	if err != nil {
		b.Fatal(err)
	}
	_ = query.DataflowPlanView()
	b.Run("Clone", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			executor, err := query.CompileDataflow(m052DataflowExecutorBenchmarkRunner)
			if err != nil {
				b.Fatal(err)
			}
			m052DataflowExecutorBenchmarkSink = executor
		}
	})
	b.Run("Shared", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			executor, err := query.CompileReusableDataflow(m052DataflowExecutorBenchmarkRunner)
			if err != nil {
				b.Fatal(err)
			}
			m052DataflowExecutorBenchmarkSink = executor
		}
	})
}
