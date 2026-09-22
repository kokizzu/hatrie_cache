package hatSql

import (
	"context"
	"testing"
)

type c234QueryProfilerBenchmarkResolver struct {
	rows []SQLRow
}

func (resolver c234QueryProfilerBenchmarkResolver) ResolveSQLSource(name, key string) ([]SQLRow, error) {
	return resolver.rows, nil
}

var c234QueryProfilerBenchmarkResolverValue = func() c234QueryProfilerBenchmarkResolver {
	rows := make([]SQLRow, 512)
	for index := range rows {
		rows[index] = SQLRow{"id": index, "value": index % 17}
	}
	return c234QueryProfilerBenchmarkResolver{rows: rows}
}()

var c234QueryProfilerBenchmarkQuery = "FROM CACHE('events') AS events SELECT events.id, events.value WHERE events.value >= 0 ORDER BY events.id LIMIT 128"

var c234QueryProfilerBenchmarkSink SQLQueryResult

func BenchmarkC234QueryNoProfiler(b *testing.B) {
	query, err := CompileSQLQuery(c234QueryProfilerBenchmarkQuery)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		result, err := query.Execute(context.Background(), c234QueryProfilerBenchmarkResolverValue, nil, SQLQueryOptions{})
		if err != nil {
			b.Fatal(err)
		}
		c234QueryProfilerBenchmarkSink = result
	}
}

func BenchmarkC234QueryObserverBaseline(b *testing.B) {
	query, err := CompileSQLQuery(c234QueryProfilerBenchmarkQuery)
	if err != nil {
		b.Fatal(err)
	}
	observer := SQLQueryObserverFunc(func(event SQLQueryEvent) {
		if event.OutputRows == 0 {
			b.Fatal("observer saw no output rows")
		}
	})
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		result, err := query.Execute(context.Background(), c234QueryProfilerBenchmarkResolverValue, nil, SQLQueryOptions{
			QueryID:  "c234-baseline",
			Observer: observer,
		})
		if err != nil {
			b.Fatal(err)
		}
		c234QueryProfilerBenchmarkSink = result
	}
}

func BenchmarkC234QueryProfiler(b *testing.B) {
	query, err := CompileSQLQuery(c234QueryProfilerBenchmarkQuery)
	if err != nil {
		b.Fatal(err)
	}
	profiler, err := NewSQLQueryProfiler(SQLQueryProfilerOptions{})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		result, err := query.Execute(context.Background(), c234QueryProfilerBenchmarkResolverValue, nil, SQLQueryOptions{
			QueryID:  "c234-profiler",
			Profiler: profiler,
		})
		if err != nil {
			b.Fatal(err)
		}
		c234QueryProfilerBenchmarkSink = result
	}
}
