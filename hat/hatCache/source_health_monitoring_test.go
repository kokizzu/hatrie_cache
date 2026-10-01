package hatCache

import (
	"strconv"
	"strings"
	"testing"

	"hatrie_cache/hat/hatMetrics"
)

func TestWritePrometheusSourceHealthMetrics(t *testing.T) {
	registry := hatMetrics.NewSourceHealthRegistry(2)
	if err := registry.Record("orders", hatMetrics.SourceHealthFailed, 4, "upstream timeout"); err != nil {
		t.Fatal(err)
	}
	if err := registry.Record("users", hatMetrics.SourceHealthHealthy, 10, ""); err != nil {
		t.Fatal(err)
	}
	handler := &MonitoringHandler{options: MonitoringOptions{
		SourceHealth:         registry,
		SourceHealthObserved: func() uint64 { return 12 },
	}}

	var builder strings.Builder
	handler.writePrometheusSourceHealthMetrics(&builder, "node-a")
	output := builder.String()
	for _, want := range []string{
		`# HELP hatrie_cache_source_health_status Current health status for each configured source; exactly one status series is one.`,
		`hatrie_cache_source_health_status{node="node-a",source="orders",status="failed"} 1`,
		`hatrie_cache_source_health_status{node="node-a",source="users",status="healthy"} 1`,
		`hatrie_cache_source_health_frontier{node="node-a",source="orders"} 4`,
		`hatrie_cache_source_health_failures{node="node-a",source="orders"} 1`,
		`hatrie_cache_source_health_observed{node="node-a"} 12`,
		`hatrie_cache_source_health_lag{node="node-a",source="orders"} 8`,
	} {
		if !strings.Contains(output, want) {
			t.Fatalf("metrics missing %q:\n%s", want, output)
		}
	}
	if strings.Contains(output, "upstream timeout") {
		t.Fatalf("error text must not become a metric label or value: %s", output)
	}
}

func TestWritePrometheusSourceHealthMetricsFallsBackToFrontierObserved(t *testing.T) {
	registry := hatMetrics.NewSourceHealthRegistry(1)
	if err := registry.Record("orders", hatMetrics.SourceHealthHealthy, 4, ""); err != nil {
		t.Fatal(err)
	}
	handler := &MonitoringHandler{options: MonitoringOptions{
		SourceHealth:           registry,
		SourceFrontierObserved: func() uint64 { return 9 },
	}}

	var builder strings.Builder
	handler.writePrometheusSourceHealthMetrics(&builder, "node-a")
	if !strings.Contains(builder.String(), `hatrie_cache_source_health_lag{node="node-a",source="orders"} 5`) {
		t.Fatalf("expected fallback observed frontier in metrics:\n%s", builder.String())
	}
}

func TestWritePrometheusSourceHealthMetricsDisabledByDefault(t *testing.T) {
	handler := &MonitoringHandler{}
	var builder strings.Builder
	handler.writePrometheusSourceHealthMetrics(&builder, "node-a")
	if builder.Len() != 0 {
		t.Fatalf("nil registry must not emit metrics: %q", builder.String())
	}
}

func TestWritePrometheusSourceHealthMetricsEscapesNodeLabel(t *testing.T) {
	registry := hatMetrics.NewSourceHealthRegistry(1)
	if err := registry.Record("orders", hatMetrics.SourceHealthHealthy, 1, ""); err != nil {
		t.Fatal(err)
	}
	handler := &MonitoringHandler{options: MonitoringOptions{SourceHealth: registry}}

	var builder strings.Builder
	handler.writePrometheusSourceHealthMetrics(&builder, "node\"bad")
	output := builder.String()
	if strings.Contains(output, `node="node"bad`) {
		t.Fatalf("node label was not escaped: %s", output)
	}
	if !strings.Contains(output, `node="node\"bad"`) {
		t.Fatalf("escaped node label missing: %s", output)
	}
}

func BenchmarkWritePrometheusSourceHealthMetrics(b *testing.B) {
	registry := hatMetrics.NewSourceHealthRegistry(1024)
	for index := 0; index < 1024; index++ {
		if err := registry.Record("source-"+strconv.Itoa(index), hatMetrics.SourceHealthHealthy, uint64(index), ""); err != nil {
			b.Fatal(err)
		}
	}
	handler := &MonitoringHandler{options: MonitoringOptions{
		SourceHealth:         registry,
		SourceHealthObserved: func() uint64 { return 2048 },
	}}
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		var builder strings.Builder
		handler.writePrometheusSourceHealthMetrics(&builder, "node-a")
	}
}
