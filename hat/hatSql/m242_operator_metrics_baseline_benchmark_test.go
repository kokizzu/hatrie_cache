package hatSql

import (
	"testing"
	"time"
)

func BenchmarkM242ThreeGenericOperatorUpdates(b *testing.B) {
	catalog, err := NewSQLDataflowMetricsCatalog(SQLDataflowMetricsCatalogOptions{})
	if err != nil {
		b.Fatal(err)
	}
	metrics := []SQLDataflowMetricPoint{
		{Name: "operator_updates_total", Unit: "updates"},
		{Name: "operator_batches_total", Unit: "batches"},
		{Name: "operator_frontier", Unit: "timestamp"},
	}
	if err := catalog.Upsert(SQLDataflowMetricsObject{Kind: SQLDataflowObjectCompute, Name: "join", Metrics: metrics}); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		value := float64(iteration + 1)
		updatedAt := time.Unix(int64(iteration+1), 0)
		for _, metric := range metrics {
			if err := catalog.UpdateMetric(SQLDataflowObjectCompute, "join", metric.Name, value, updatedAt); err != nil {
				b.Fatal(err)
			}
		}
	}
}
