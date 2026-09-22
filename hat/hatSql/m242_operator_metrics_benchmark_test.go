package hatSql

import (
	"context"
	"testing"
)

const m242OperatorMetricsBenchmarkQuery = "SELECT * FROM VALUES (1), (2), (3), (4)"

var m242OperatorMetricsBenchmarkSink SQLQueryEvent

var m242OperatorMetricsBenchmarkObserver = SQLQueryObserverFunc(func(event SQLQueryEvent) {
	m242OperatorMetricsBenchmarkSink = event
})

func BenchmarkM242OperatorObserver(b *testing.B) {
	b.ReportAllocs()
	for range b.N {
		_, err := ExecuteSQLQueryContext(context.Background(), m242OperatorMetricsBenchmarkQuery, nil, SQLQueryOptions{
			Observer: m242OperatorMetricsBenchmarkObserver,
		})
		if err != nil {
			b.Fatal(err)
		}
	}
	if len(m242OperatorMetricsBenchmarkSink.Operators) == 0 {
		b.Fatal("observer did not receive operators")
	}
}
