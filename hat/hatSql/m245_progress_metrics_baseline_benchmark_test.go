package hatSql

import (
	"testing"
	"time"
)

func BenchmarkM245SixGenericMetricUpdates(b *testing.B) {
	catalog, err := NewSQLDataflowMetricsCatalog(SQLDataflowMetricsCatalogOptions{})
	if err != nil {
		b.Fatal(err)
	}
	metrics := []SQLDataflowMetricPoint{
		{Name: "input_timestamp", Unit: "timestamp"},
		{Name: "output_timestamp", Unit: "timestamp"},
		{Name: "input_output_timestamp_lag", Unit: "timestamp"},
		{Name: "input_timestamp_throughput", Unit: "timestamp/s"},
		{Name: "output_timestamp_throughput", Unit: "timestamp/s"},
		{Name: "input_to_output_latency", Unit: "s"},
	}
	if err := catalog.Upsert(SQLDataflowMetricsObject{Kind: SQLDataflowObjectSource, Name: "orders", Metrics: metrics}); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		value := float64(iteration + 1)
		updatedAt := time.Unix(int64(iteration+1), 0)
		for _, metric := range metrics {
			if err := catalog.UpdateMetric(SQLDataflowObjectSource, "orders", metric.Name, value, updatedAt); err != nil {
				b.Fatal(err)
			}
		}
	}
}
