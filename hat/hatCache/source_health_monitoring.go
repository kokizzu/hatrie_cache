package hatCache

import (
	"fmt"
	"strings"
)

func (handler *MonitoringHandler) writePrometheusSourceHealthMetrics(builder *strings.Builder, node string) {
	registry := handler.options.SourceHealth
	if registry == nil {
		return
	}
	node = prometheusLabelValue(node)
	observed := uint64(0)
	observedFn := handler.options.SourceHealthObserved
	if observedFn == nil {
		observedFn = handler.options.SourceFrontierObserved
	}
	hasObserved := observedFn != nil
	if hasObserved {
		observed = observedFn()
	}
	rows := registry.Snapshot(observed)
	if len(rows) == 0 {
		return
	}

	writePrometheusHelp(builder, "hatrie_cache_source_health_status", "Current health status for each configured source; exactly one status series is one.")
	writePrometheusType(builder, "hatrie_cache_source_health_status", "gauge")
	for _, row := range rows {
		fmt.Fprintf(builder, "hatrie_cache_source_health_status{node=\"%s\",source=\"%s\",status=\"%s\"} 1\n", node, prometheusLabelValue(row.Source), prometheusLabelValue(string(row.Status)))
	}

	writePrometheusHelp(builder, "hatrie_cache_source_health_frontier", "Latest source frontier recorded with the health state.")
	writePrometheusType(builder, "hatrie_cache_source_health_frontier", "gauge")
	for _, row := range rows {
		fmt.Fprintf(builder, "hatrie_cache_source_health_frontier{node=\"%s\",source=\"%s\"} %d\n", node, prometheusLabelValue(row.Source), row.Frontier)
	}

	writePrometheusHelp(builder, "hatrie_cache_source_health_failures", "Consecutive degraded or failed health records since the last healthy record.")
	writePrometheusType(builder, "hatrie_cache_source_health_failures", "gauge")
	for _, row := range rows {
		fmt.Fprintf(builder, "hatrie_cache_source_health_failures{node=\"%s\",source=\"%s\"} %d\n", node, prometheusLabelValue(row.Source), row.ConsecutiveFailures)
	}

	writePrometheusHelp(builder, "hatrie_cache_source_health_updated_at_unix_nano", "Unix nanosecond timestamp of the latest source health record.")
	writePrometheusType(builder, "hatrie_cache_source_health_updated_at_unix_nano", "gauge")
	for _, row := range rows {
		fmt.Fprintf(builder, "hatrie_cache_source_health_updated_at_unix_nano{node=\"%s\",source=\"%s\"} %d\n", node, prometheusLabelValue(row.Source), row.UpdatedAtUnixNano)
	}

	if !hasObserved {
		return
	}
	writePrometheusHelp(builder, "hatrie_cache_source_health_observed", "Global observed frontier used to calculate source health lag.")
	writePrometheusType(builder, "hatrie_cache_source_health_observed", "gauge")
	fmt.Fprintf(builder, "hatrie_cache_source_health_observed{node=\"%s\"} %d\n", node, observed)
	writePrometheusHelp(builder, "hatrie_cache_source_health_lag", "Frontier distance between the observed point and each source health record.")
	writePrometheusType(builder, "hatrie_cache_source_health_lag", "gauge")
	for _, row := range rows {
		fmt.Fprintf(builder, "hatrie_cache_source_health_lag{node=\"%s\",source=\"%s\"} %d\n", node, prometheusLabelValue(row.Source), row.Lag)
	}
}
