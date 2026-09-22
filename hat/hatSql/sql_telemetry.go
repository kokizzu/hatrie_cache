package hatSql

import (
	"strconv"
	"strings"
	"sync"
)

var defaultSQLTelemetryLatencyBounds = []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5}

// SQLTelemetry is an opt-in SQLQueryObserver that aggregates privacy-safe
// query completion events for Prometheus and OpenTelemetry adapters.
type SQLTelemetry struct {
	mu sync.Mutex

	bounds                  []float64
	buckets                 []uint64
	timestampLatencyBounds  []float64
	timestampLatencyBuckets []uint64
	SQLTelemetrySnapshot
}

// SQLTelemetrySnapshot contains cumulative query telemetry counters. Working
// bytes are planner-visible operator input/output bytes, not Go heap usage.
type SQLTelemetrySnapshot struct {
	QueriesTotal                   uint64
	ErrorsTotal                    uint64
	CanceledTotal                  uint64
	SlowTotal                      uint64
	IndexUsesTotal                 uint64
	SpillUsesTotal                 uint64
	ResultBytesTotal               uint64
	WorkingBytesTotal              uint64
	LatencyNanosTotal              uint64
	LatencyCount                   uint64
	LatencyBounds                  []float64
	LatencyBuckets                 []uint64
	TimestampObservationsTotal     uint64
	TimestampUpdatesTotal          uint64
	TimestampBatchesTotal          uint64
	LatestInputTimestamp           uint64
	LatestOutputTimestamp          uint64
	InputToOutputLatencyNanosTotal uint64
	InputToOutputLatencyCount      uint64
	InputToOutputLatencyBounds     []float64
	InputToOutputLatencyBuckets    []uint64
}

// SQLTimestampTelemetryEvent describes one logical timestamp's progress
// through an input-to-output path. Timestamp values are opaque logical units;
// Unix-nano values are wall-clock samples used only for freshness latency.
type SQLTimestampTelemetryEvent struct {
	InputTimestamp   uint64 `json:"input_timestamp,omitempty"`
	OutputTimestamp  uint64 `json:"output_timestamp,omitempty"`
	Updates          uint64 `json:"updates,omitempty"`
	Batches          uint64 `json:"batches,omitempty"`
	InputAtUnixNano  int64  `json:"input_at_unix_nano,omitempty"`
	OutputAtUnixNano int64  `json:"output_at_unix_nano,omitempty"`
}

// SQLTelemetryMetric is an SDK-neutral OpenTelemetry metric point. Exporters
// can map these names and units directly to their chosen OpenTelemetry SDK.
type SQLTelemetryMetric struct {
	Name       string
	Unit       string
	Value      float64
	Attributes map[string]string
}

// NewSQLTelemetry creates a telemetry observer with practical latency buckets.
func NewSQLTelemetry() *SQLTelemetry {
	bounds := append([]float64(nil), defaultSQLTelemetryLatencyBounds...)
	timestampLatencyBounds := append([]float64(nil), defaultSQLTelemetryLatencyBounds...)
	return &SQLTelemetry{
		bounds:                  bounds,
		buckets:                 make([]uint64, len(bounds)),
		timestampLatencyBounds:  timestampLatencyBounds,
		timestampLatencyBuckets: make([]uint64, len(timestampLatencyBounds)),
	}
}

// ObserveSQLQuery implements SQLQueryObserver.
func (telemetry *SQLTelemetry) ObserveSQLQuery(event SQLQueryEvent) {
	if telemetry == nil {
		return
	}
	telemetry.mu.Lock()
	defer telemetry.mu.Unlock()
	telemetry.QueriesTotal++
	if !event.OK {
		telemetry.ErrorsTotal++
	}
	if event.Canceled {
		telemetry.CanceledTotal++
	}
	if event.Slow {
		telemetry.SlowTotal++
	}
	telemetry.ResultBytesTotal += uint64(maxSQLTelemetryInt(event.ResultBytes))
	telemetry.LatencyNanosTotal += uint64(maxSQLTelemetryInt64(event.ElapsedNanos))
	telemetry.LatencyCount++
	seconds := float64(event.ElapsedNanos) / 1e9
	for index, bound := range telemetry.bounds {
		if seconds <= bound {
			telemetry.buckets[index]++
			break
		}
	}
	for _, operator := range event.Operators {
		if strings.Contains(operator.Node, "INDEX") {
			telemetry.IndexUsesTotal++
		}
		if strings.HasPrefix(operator.Node, "EXTERNAL ") {
			telemetry.SpillUsesTotal++
		}
		if operator.InputBytes != nil {
			telemetry.WorkingBytesTotal += uint64(maxSQLTelemetryInt(*operator.InputBytes))
		}
		if operator.OutputBytes != nil {
			telemetry.WorkingBytesTotal += uint64(maxSQLTelemetryInt(*operator.OutputBytes))
		}
	}
}

// ObserveSQLTimestamp records logical progress without changing query
// execution. Counter rates can be used as timestamp throughput, while the
// optional wall-clock pair measures input-to-output freshness latency.
func (telemetry *SQLTelemetry) ObserveSQLTimestamp(event SQLTimestampTelemetryEvent) {
	if telemetry == nil {
		return
	}
	telemetry.mu.Lock()
	defer telemetry.mu.Unlock()
	telemetry.TimestampObservationsTotal++
	telemetry.TimestampUpdatesTotal += event.Updates
	telemetry.TimestampBatchesTotal += event.Batches
	if event.InputTimestamp > telemetry.LatestInputTimestamp {
		telemetry.LatestInputTimestamp = event.InputTimestamp
	}
	if event.OutputTimestamp > telemetry.LatestOutputTimestamp {
		telemetry.LatestOutputTimestamp = event.OutputTimestamp
	}
	if event.InputAtUnixNano <= 0 || event.OutputAtUnixNano < event.InputAtUnixNano {
		return
	}
	latencyNanos := uint64(event.OutputAtUnixNano - event.InputAtUnixNano)
	telemetry.InputToOutputLatencyNanosTotal += latencyNanos
	telemetry.InputToOutputLatencyCount++
	seconds := float64(latencyNanos) / 1e9
	for index, bound := range telemetry.timestampLatencyBounds {
		if seconds <= bound {
			telemetry.timestampLatencyBuckets[index]++
			break
		}
	}
}

// Snapshot returns a stable copy for application metrics endpoints.
func (telemetry *SQLTelemetry) Snapshot() SQLTelemetrySnapshot {
	if telemetry == nil {
		return SQLTelemetrySnapshot{}
	}
	telemetry.mu.Lock()
	defer telemetry.mu.Unlock()
	snapshot := telemetry.SQLTelemetrySnapshot
	snapshot.LatencyBounds = append([]float64(nil), telemetry.bounds...)
	snapshot.LatencyBuckets = append([]uint64(nil), telemetry.buckets...)
	snapshot.InputToOutputLatencyBounds = append([]float64(nil), telemetry.timestampLatencyBounds...)
	snapshot.InputToOutputLatencyBuckets = append([]uint64(nil), telemetry.timestampLatencyBuckets...)
	return snapshot
}

// PrometheusMetrics renders the collector in Prometheus text exposition format.
func (telemetry *SQLTelemetry) PrometheusMetrics() string {
	snapshot := telemetry.Snapshot()
	var out strings.Builder
	writeSQLTelemetryCounter(&out, "hatrie_sql_queries_total", "Completed SQL queries.", snapshot.QueriesTotal)
	writeSQLTelemetryCounter(&out, "hatrie_sql_query_errors_total", "Failed SQL queries.", snapshot.ErrorsTotal)
	writeSQLTelemetryCounter(&out, "hatrie_sql_query_canceled_total", "Canceled SQL queries.", snapshot.CanceledTotal)
	writeSQLTelemetryCounter(&out, "hatrie_sql_query_slow_total", "SQL queries at or above their configured slow threshold.", snapshot.SlowTotal)
	writeSQLTelemetryCounter(&out, "hatrie_sql_query_index_uses_total", "SQL query operators using an index.", snapshot.IndexUsesTotal)
	writeSQLTelemetryCounter(&out, "hatrie_sql_query_spill_uses_total", "SQL query operators using external spill work.", snapshot.SpillUsesTotal)
	writeSQLTelemetryCounter(&out, "hatrie_sql_query_result_bytes_total", "SQL result bytes emitted or materialized.", snapshot.ResultBytesTotal)
	writeSQLTelemetryCounter(&out, "hatrie_sql_query_working_bytes_total", "Planner-visible SQL operator input and output bytes.", snapshot.WorkingBytesTotal)
	writeSQLTelemetryHistogram(&out, "hatrie_sql_query_latency_seconds", "SQL query completion latency.", snapshot.LatencyBounds, snapshot.LatencyBuckets, snapshot.LatencyCount, snapshot.LatencyNanosTotal)
	writeSQLTelemetryCounter(&out, "hatrie_sql_timestamp_observations_total", "Logical timestamp progress observations.", snapshot.TimestampObservationsTotal)
	writeSQLTelemetryCounter(&out, "hatrie_sql_timestamp_updates_total", "Updates associated with logical timestamp progress.", snapshot.TimestampUpdatesTotal)
	writeSQLTelemetryCounter(&out, "hatrie_sql_timestamp_batches_total", "Batches associated with logical timestamp progress.", snapshot.TimestampBatchesTotal)
	writeSQLTelemetryGauge(&out, "hatrie_sql_timestamp_input", "Latest logical input timestamp observed.", snapshot.LatestInputTimestamp)
	writeSQLTelemetryGauge(&out, "hatrie_sql_timestamp_output", "Latest logical output timestamp observed.", snapshot.LatestOutputTimestamp)
	writeSQLTelemetryHistogram(&out, "hatrie_sql_input_to_output_latency_seconds", "Input-to-output freshness latency.", snapshot.InputToOutputLatencyBounds, snapshot.InputToOutputLatencyBuckets, snapshot.InputToOutputLatencyCount, snapshot.InputToOutputLatencyNanosTotal)
	return out.String()
}

// OpenTelemetryMetrics returns SDK-neutral cumulative metric points. It avoids
// importing an OpenTelemetry SDK so applications retain version control.
func (telemetry *SQLTelemetry) OpenTelemetryMetrics() []SQLTelemetryMetric {
	snapshot := telemetry.Snapshot()
	return []SQLTelemetryMetric{
		{Name: "hatrie.sql.queries", Unit: "1", Value: float64(snapshot.QueriesTotal)},
		{Name: "hatrie.sql.query.errors", Unit: "1", Value: float64(snapshot.ErrorsTotal)},
		{Name: "hatrie.sql.query.index_uses", Unit: "1", Value: float64(snapshot.IndexUsesTotal)},
		{Name: "hatrie.sql.query.spill_uses", Unit: "1", Value: float64(snapshot.SpillUsesTotal)},
		{Name: "hatrie.sql.query.result_bytes", Unit: "By", Value: float64(snapshot.ResultBytesTotal)},
		{Name: "hatrie.sql.query.working_bytes", Unit: "By", Value: float64(snapshot.WorkingBytesTotal)},
		{Name: "hatrie.sql.query.latency", Unit: "s", Value: float64(snapshot.LatencyNanosTotal) / 1e9},
		{Name: "hatrie.sql.timestamp.observations", Unit: "1", Value: float64(snapshot.TimestampObservationsTotal)},
		{Name: "hatrie.sql.timestamp.updates", Unit: "1", Value: float64(snapshot.TimestampUpdatesTotal)},
		{Name: "hatrie.sql.timestamp.batches", Unit: "1", Value: float64(snapshot.TimestampBatchesTotal)},
		{Name: "hatrie.sql.timestamp.input", Unit: "1", Value: float64(snapshot.LatestInputTimestamp)},
		{Name: "hatrie.sql.timestamp.output", Unit: "1", Value: float64(snapshot.LatestOutputTimestamp)},
		{Name: "hatrie.sql.timestamp.input_to_output_latency", Unit: "s", Value: float64(snapshot.InputToOutputLatencyNanosTotal) / 1e9},
	}
}

func writeSQLTelemetryHistogram(out *strings.Builder, name, help string, bounds []float64, buckets []uint64, count, sumNanos uint64) {
	out.WriteString("# HELP ")
	out.WriteString(name)
	out.WriteByte(' ')
	out.WriteString(help)
	out.WriteString("\n# TYPE ")
	out.WriteString(name)
	out.WriteString(" histogram\n")
	var cumulative uint64
	for index, bound := range bounds {
		cumulative += buckets[index]
		out.WriteString(name)
		out.WriteString("_bucket{le=\"")
		out.WriteString(strconv.FormatFloat(bound, 'f', -1, 64))
		out.WriteString("\"} ")
		out.WriteString(strconv.FormatUint(cumulative, 10))
		out.WriteByte('\n')
	}
	out.WriteString(name)
	out.WriteString("_bucket{le=\"+Inf\"} ")
	out.WriteString(strconv.FormatUint(count, 10))
	out.WriteByte('\n')
	out.WriteString(name)
	out.WriteString("_sum ")
	out.WriteString(strconv.FormatFloat(float64(sumNanos)/1e9, 'f', -1, 64))
	out.WriteByte('\n')
	out.WriteString(name)
	out.WriteString("_count ")
	out.WriteString(strconv.FormatUint(count, 10))
	out.WriteByte('\n')
}

func writeSQLTelemetryGauge(out *strings.Builder, name, help string, value uint64) {
	out.WriteString("# HELP ")
	out.WriteString(name)
	out.WriteByte(' ')
	out.WriteString(help)
	out.WriteString("\n# TYPE ")
	out.WriteString(name)
	out.WriteString(" gauge\n")
	out.WriteString(name)
	out.WriteByte(' ')
	out.WriteString(strconv.FormatUint(value, 10))
	out.WriteByte('\n')
}

func writeSQLTelemetryCounter(out *strings.Builder, name, help string, value uint64) {
	out.WriteString("# HELP ")
	out.WriteString(name)
	out.WriteByte(' ')
	out.WriteString(help)
	out.WriteString("\n# TYPE ")
	out.WriteString(name)
	out.WriteString(" counter\n")
	out.WriteString(name)
	out.WriteByte(' ')
	out.WriteString(strconv.FormatUint(value, 10))
	out.WriteByte('\n')
}

func maxSQLTelemetryInt(value int) int {
	if value < 0 {
		return 0
	}
	return value
}

func maxSQLTelemetryInt64(value int64) int64 {
	if value < 0 {
		return 0
	}
	return value
}
