package hatSql

import (
	"strings"
	"testing"
)

func TestM245TimestampTelemetryExportsThroughputAndLatency(t *testing.T) {
	telemetry := NewSQLTelemetry()
	telemetry.ObserveSQLTimestamp(SQLTimestampTelemetryEvent{
		InputTimestamp:   42,
		OutputTimestamp:  45,
		Updates:          7,
		Batches:          2,
		InputAtUnixNano:  1_000_000_000,
		OutputAtUnixNano: 1_250_000_000,
	})
	telemetry.ObserveSQLTimestamp(SQLTimestampTelemetryEvent{
		InputTimestamp:   46,
		OutputTimestamp:  44,
		Updates:          3,
		Batches:          1,
		InputAtUnixNano:  2_000_000_000,
		OutputAtUnixNano: 1_500_000_000,
	})

	snapshot := telemetry.Snapshot()
	if snapshot.TimestampObservationsTotal != 2 || snapshot.TimestampUpdatesTotal != 10 || snapshot.TimestampBatchesTotal != 3 {
		t.Fatalf("timestamp counters = %#v", snapshot)
	}
	if snapshot.LatestInputTimestamp != 46 || snapshot.LatestOutputTimestamp != 45 {
		t.Fatalf("latest timestamps = %#v, want input=46 output=45", snapshot)
	}
	if snapshot.InputToOutputLatencyNanosTotal != 250_000_000 || snapshot.InputToOutputLatencyCount != 1 {
		t.Fatalf("input-to-output latency = %#v", snapshot)
	}

	prometheus := telemetry.PrometheusMetrics()
	for _, metric := range []string{
		"hatrie_sql_timestamp_updates_total",
		"hatrie_sql_timestamp_batches_total",
		"hatrie_sql_timestamp_input",
		"hatrie_sql_timestamp_output",
		"hatrie_sql_input_to_output_latency_seconds",
	} {
		if !strings.Contains(prometheus, metric) {
			t.Fatalf("PrometheusMetrics() missing %q: %s", metric, prometheus)
		}
	}
	if !strings.Contains(prometheus, "hatrie_sql_timestamp_updates_total 10") || !strings.Contains(prometheus, "hatrie_sql_input_to_output_latency_seconds_sum 0.25") {
		t.Fatalf("PrometheusMetrics() values = %s", prometheus)
	}

	openTelemetry := telemetry.OpenTelemetryMetrics()
	if !m245MetricValue(openTelemetry, "hatrie.sql.timestamp.updates", 10) || !m245MetricValue(openTelemetry, "hatrie.sql.timestamp.batches", 3) || !m245MetricValue(openTelemetry, "hatrie.sql.timestamp.input_to_output_latency", 0.25) {
		t.Fatalf("OpenTelemetryMetrics() = %#v", openTelemetry)
	}
}

func TestM245TimestampTelemetryIgnoresUnavailableLatency(t *testing.T) {
	telemetry := NewSQLTelemetry()
	telemetry.ObserveSQLTimestamp(SQLTimestampTelemetryEvent{InputAtUnixNano: 20, OutputAtUnixNano: 10})
	telemetry.ObserveSQLTimestamp(SQLTimestampTelemetryEvent{InputAtUnixNano: 0, OutputAtUnixNano: 30})

	if snapshot := telemetry.Snapshot(); snapshot.InputToOutputLatencyNanosTotal != 0 || snapshot.InputToOutputLatencyCount != 0 {
		t.Fatalf("invalid timestamp latency was recorded: %#v", snapshot)
	}
}

func m245MetricValue(metrics []SQLTelemetryMetric, name string, want float64) bool {
	for _, metric := range metrics {
		if metric.Name == name && metric.Value == want {
			return true
		}
	}
	return false
}
