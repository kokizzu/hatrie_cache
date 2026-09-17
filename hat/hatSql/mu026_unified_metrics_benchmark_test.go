package hatSql

import (
	"sort"
	"testing"
	"time"
)

var mu026BenchmarkSink int

type mu026SeparateMetricsControl struct {
	source  map[string]map[string]float64
	compute map[string]map[string]float64
	sink    map[string]map[string]float64
}

func newMU026SeparateMetricsControl() *mu026SeparateMetricsControl {
	metrics := map[string]float64{
		"bytes":        100,
		"errors":       2,
		"lag_seconds":  0.25,
		"rows":         1000,
		"retries":      3,
		"spills":       1,
		"watermark":    42,
		"work_seconds": 1.5,
	}
	return &mu026SeparateMetricsControl{
		source: map[string]map[string]float64{
			"orders":   copyMU026MetricValues(metrics),
			"payments": copyMU026MetricValues(metrics),
		},
		compute: map[string]map[string]float64{
			"rollup": copyMU026MetricValues(metrics),
		},
		sink: map[string]map[string]float64{
			"warehouse": copyMU026MetricValues(metrics),
		},
	}
}

func copyMU026MetricValues(values map[string]float64) map[string]float64 {
	copyOfValues := make(map[string]float64, len(values))
	for name, value := range values {
		copyOfValues[name] = value
	}
	return copyOfValues
}

func (control *mu026SeparateMetricsControl) rows() []SQLDataflowMetricsRow {
	objects := []struct {
		kind    SQLDataflowObjectKind
		name    string
		metrics map[string]float64
	}{
		{kind: SQLDataflowObjectSource, name: "orders", metrics: control.source["orders"]},
		{kind: SQLDataflowObjectSource, name: "payments", metrics: control.source["payments"]},
		{kind: SQLDataflowObjectCompute, name: "rollup", metrics: control.compute["rollup"]},
		{kind: SQLDataflowObjectSink, name: "warehouse", metrics: control.sink["warehouse"]},
	}
	rows := make([]SQLDataflowMetricsRow, 0, len(objects)*8)
	for _, object := range objects {
		metricNames := make([]string, 0, len(object.metrics))
		for metric := range object.metrics {
			metricNames = append(metricNames, metric)
		}
		sort.Strings(metricNames)
		for _, metric := range metricNames {
			value := object.metrics[metric]
			rows = append(rows, SQLDataflowMetricsRow{
				"object_kind": string(object.kind),
				"object_name": object.name,
				"metric":      metric,
				"value":       value,
				"unit":        mu026BenchmarkMetricUnit(metric),
				"updated_at":  time.Time{},
			})
		}
	}
	return rows
}

func mu026BenchmarkMetricUnit(metric string) string {
	switch metric {
	case "bytes":
		return "By"
	case "lag_seconds", "work_seconds":
		return "s"
	default:
		return "1"
	}
}

func newMU026UnifiedMetricsCatalogForBenchmark() *SQLDataflowMetricsCatalog {
	catalog, err := NewSQLDataflowMetricsCatalog(SQLDataflowMetricsCatalogOptions{
		MaxObjects:          8,
		MaxMetricsPerObject: 8,
		MaxMetricPoints:     32,
	})
	if err != nil {
		panic(err)
	}
	metrics := []SQLDataflowMetricPoint{
		{Name: "bytes", Unit: "By", Value: 100},
		{Name: "errors", Unit: "1", Value: 2},
		{Name: "lag_seconds", Unit: "s", Value: 0.25},
		{Name: "rows", Unit: "1", Value: 1000},
		{Name: "retries", Unit: "1", Value: 3},
		{Name: "spills", Unit: "1", Value: 1},
		{Name: "watermark", Unit: "1", Value: 42},
		{Name: "work_seconds", Unit: "s", Value: 1.5},
	}
	for _, object := range []struct {
		kind SQLDataflowObjectKind
		name string
	}{
		{kind: SQLDataflowObjectSource, name: "orders"},
		{kind: SQLDataflowObjectSource, name: "payments"},
		{kind: SQLDataflowObjectCompute, name: "rollup"},
		{kind: SQLDataflowObjectSink, name: "warehouse"},
	} {
		if err := catalog.Upsert(SQLDataflowMetricsObject{
			Kind:    object.kind,
			Name:    object.name,
			Metrics: metrics,
		}); err != nil {
			panic(err)
		}
	}
	return catalog
}

func BenchmarkMU026BeforeSeparateMapUpdate(b *testing.B) {
	control := newMU026SeparateMetricsControl()
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		control.source["orders"]["rows"] = float64(iteration)
	}
	mu026BenchmarkSink = int(control.source["orders"]["rows"])
}

func BenchmarkMU026BeforeSeparateMapRows(b *testing.B) {
	control := newMU026SeparateMetricsControl()
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		mu026BenchmarkSink = len(control.rows())
	}
}

func BenchmarkMU026AfterUnifiedCatalogUpsert(b *testing.B) {
	catalog := newMU026UnifiedMetricsCatalogForBenchmark()
	object := SQLDataflowMetricsObject{
		Kind: SQLDataflowObjectSource, Name: "orders",
		Metrics: []SQLDataflowMetricPoint{
			{Name: "rows", Unit: "1", Value: 0},
		},
	}
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		object.Metrics[0].Value = float64(iteration)
		if err := catalog.Upsert(object); err != nil {
			b.Fatal(err)
		}
	}
	mu026BenchmarkSink = int(object.Metrics[0].Value)
}

func BenchmarkMU026AfterUnifiedCatalogMetricUpdate(b *testing.B) {
	catalog := newMU026UnifiedMetricsCatalogForBenchmark()
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		if err := catalog.UpdateMetric(SQLDataflowObjectSource, "orders", "rows", float64(iteration), time.Time{}); err != nil {
			b.Fatal(err)
		}
	}
	mu026BenchmarkSink = int(catalog.Rows()[0]["value"].(float64))
}

func BenchmarkMU026AfterUnifiedCatalogRows(b *testing.B) {
	catalog := newMU026UnifiedMetricsCatalogForBenchmark()
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		mu026BenchmarkSink = len(catalog.Rows())
	}
}
