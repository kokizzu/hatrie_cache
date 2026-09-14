package hatCache

import (
	"strconv"
	"strings"
)

func (handler *MonitoringHandler) writePrometheusOperatorFrontierMetrics(builder *strings.Builder, node string) {
	registry := handler.options.OperatorFrontier
	if registry == nil {
		return
	}
	observed := uint64(0)
	hasObserved := handler.options.OperatorFrontierObserved != nil
	if hasObserved {
		observed = handler.options.OperatorFrontierObserved()
	}
	rows := registry.Snapshot(observed)
	if len(rows) == 0 {
		return
	}

	writePrometheusHelp(builder, "hatrie_cache_operator_frontier", "Current frontier for each configured operator.")
	writePrometheusType(builder, "hatrie_cache_operator_frontier", "gauge")
	for _, row := range rows {
		writePrometheusOperatorFrontierValue(builder, "hatrie_cache_operator_frontier", node, row.Operator, row.Frontier)
	}
	if !hasObserved {
		return
	}

	writePrometheusHelp(builder, "hatrie_cache_operator_observed", "Global observed frontier used to calculate operator lag.")
	writePrometheusType(builder, "hatrie_cache_operator_observed", "gauge")
	writePrometheusOperatorObservedValue(builder, "hatrie_cache_operator_observed", node, observed)
	writePrometheusHelp(builder, "hatrie_cache_operator_lag", "Frontier distance between the global observed point and each operator.")
	writePrometheusType(builder, "hatrie_cache_operator_lag", "gauge")
	for _, row := range rows {
		writePrometheusOperatorFrontierValue(builder, "hatrie_cache_operator_lag", node, row.Operator, row.Lag)
	}
}

func writePrometheusOperatorFrontierValue(builder *strings.Builder, metric, node, operator string, value uint64) {
	builder.WriteString(metric)
	builder.WriteString("{node=\"")
	builder.WriteString(node)
	builder.WriteString("\",operator=\"")
	builder.WriteString(prometheusLabelValue(operator))
	builder.WriteString("\"} ")
	writePrometheusUint64(builder, value)
	builder.WriteByte('\n')
}

func writePrometheusOperatorObservedValue(builder *strings.Builder, metric, node string, value uint64) {
	builder.WriteString(metric)
	builder.WriteString("{node=\"")
	builder.WriteString(node)
	builder.WriteString("\"} ")
	writePrometheusUint64(builder, value)
	builder.WriteByte('\n')
}

func writePrometheusUint64(builder *strings.Builder, value uint64) {
	var buffer [20]byte
	_, _ = builder.Write(strconv.AppendUint(buffer[:0], value, 10))
}
