package hatriecache

import "hatrie_cache/hat/hatMetrics"

// APIMetrics is retained at the root API for compatibility.
type APIMetrics = hatMetrics.APIMetrics

// APIMetricsSnapshot is retained at the root API for compatibility.
type APIMetricsSnapshot = hatMetrics.APIMetricsSnapshot

func NewAPIMetrics() *APIMetrics {
	return hatMetrics.NewAPIMetrics()
}
