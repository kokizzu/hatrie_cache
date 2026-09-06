package hatCache

import (
	"fmt"
	"strings"
)

func (handler *MonitoringHandler) writePrometheusSourceFrontierMetrics(builder *strings.Builder, node string) {
	registry := handler.options.SourceFrontier
	if registry == nil {
		return
	}
	observed := uint64(0)
	hasObserved := handler.options.SourceFrontierObserved != nil
	if hasObserved {
		observed = handler.options.SourceFrontierObserved()
	}
	rows := registry.Snapshot(observed)
	if len(rows) == 0 {
		return
	}

	writePrometheusHelp(builder, "hatrie_cache_source_frontier", "Current frontier for each configured source.")
	writePrometheusType(builder, "hatrie_cache_source_frontier", "gauge")
	for _, row := range rows {
		fmt.Fprintf(builder, "hatrie_cache_source_frontier{node=\"%s\",source=\"%s\"} %d\n", node, prometheusLabelValue(row.Source), row.Frontier)
	}
	if !hasObserved {
		return
	}

	writePrometheusHelp(builder, "hatrie_cache_source_observed", "Global observed frontier used to calculate source lag.")
	writePrometheusType(builder, "hatrie_cache_source_observed", "gauge")
	fmt.Fprintf(builder, "hatrie_cache_source_observed{node=\"%s\"} %d\n", node, observed)
	writePrometheusHelp(builder, "hatrie_cache_source_lag", "Frontier distance between the global observed point and each source.")
	writePrometheusType(builder, "hatrie_cache_source_lag", "gauge")
	for _, row := range rows {
		fmt.Fprintf(builder, "hatrie_cache_source_lag{node=\"%s\",source=\"%s\"} %d\n", node, prometheusLabelValue(row.Source), row.Lag)
	}
}
