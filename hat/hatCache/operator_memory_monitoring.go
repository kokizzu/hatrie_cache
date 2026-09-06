package hatCache

import (
	"fmt"
	"strings"
)

func (handler *MonitoringHandler) writePrometheusOperatorMemoryMetrics(builder *strings.Builder, node string) {
	registry := handler.options.OperatorMemory
	if registry == nil {
		return
	}
	rows := registry.Snapshot()
	if len(rows) == 0 {
		return
	}

	writePrometheusHelp(builder, "hatrie_cache_operator_retained_memory_bytes", "Retained memory bytes reported for each operator.")
	writePrometheusType(builder, "hatrie_cache_operator_retained_memory_bytes", "gauge")
	for _, row := range rows {
		fmt.Fprintf(builder, "hatrie_cache_operator_retained_memory_bytes{node=\"%s\",operator=\"%s\"} %d\n", node, prometheusLabelValue(row.Operator), row.RetainedBytes)
	}
}
