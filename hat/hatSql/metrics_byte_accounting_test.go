package hatSql

import (
	"testing"
	"time"
)

func TestSQLMetricsByteAccountingRunsOnlyWhenMetricsAreEnabled(t *testing.T) {
	t.Parallel()
	rows := wrapSQLSource(sqlSource{alias: "event"}, []Row{{"id": int64(1), "team": "core"}})
	if got := sqlMetricsExecRowsBytes(nil, rows); got != -1 {
		t.Fatalf("sqlMetricsExecRowsBytes(nil) = %d, want -1", got)
	}
	metrics := &sqlExecutionMetrics{}
	if got, want := sqlMetricsExecRowsBytes(metrics, rows), sqlExecRowsBytes(rows); got != want || got <= 0 {
		t.Fatalf("sqlMetricsExecRowsBytes(metrics) = %d, want %d", got, want)
	}
	metrics.recordScanRows(sqlSource{kind: "CACHE", key: "events"}, []Row{{"id": int64(1)}}, time.Now())
	if len(metrics.steps) != 1 || metrics.steps[0].ActualOutputBytes == nil || *metrics.steps[0].ActualOutputBytes <= 0 {
		t.Fatalf("recordScanRows() = %#v", metrics.steps)
	}
}

func TestSQLObservationResultBytesRunOnlyWhenObserved(t *testing.T) {
	t.Parallel()
	rows := []Row{{"id": int64(1), "team": "core"}}
	if got := (sqlQueryObservation{}).resultBytes(rows); got != -1 {
		t.Fatalf("unobserved result bytes = %d, want -1", got)
	}
	observed := sqlQueryObservation{recorder: NewSQLSlowQueryRecorder(1)}
	if got, want := observed.resultBytes(rows), sqlRowsBytes(rows); got != want || got <= 0 {
		t.Fatalf("observed result bytes = %d, want %d", got, want)
	}
}
