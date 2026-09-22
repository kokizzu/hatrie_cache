package hatSql

import (
	"context"
	"encoding/json"
	"testing"
)

func BenchmarkM242OperatorMetrics(b *testing.B) {
	b.ReportAllocs()
	for range b.N {
		_, err := ExecuteSQLQueryContext(context.Background(), m242OperatorMetricsBenchmarkQuery, nil, SQLQueryOptions{
			Observer:        m242OperatorMetricsBenchmarkObserver,
			OperatorMetrics: true,
		})
		if err != nil {
			b.Fatal(err)
		}
	}
	if len(m242OperatorMetricsBenchmarkSink.OperatorMetrics) == 0 || m242OperatorMetricsBenchmarkSink.OperatorMetrics[0].BatchCount == 0 {
		b.Fatal("operator metrics were not recorded")
	}
}

func BenchmarkM242OperatorMetricsJSON(b *testing.B) {
	options := SQLQueryOptions{
		Observer:        m242OperatorMetricsBenchmarkObserver,
		OperatorMetrics: true,
	}
	if _, err := ExecuteSQLQueryContext(context.Background(), m242OperatorMetricsBenchmarkQuery, nil, options); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		wire, err := json.Marshal(m242OperatorMetricsBenchmarkSink)
		if err != nil {
			b.Fatal(err)
		}
		b.ReportMetric(float64(len(wire)), "wire_bytes")
	}
}
