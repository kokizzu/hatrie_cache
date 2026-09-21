package hatSql

import (
	"errors"
	"testing"
	"time"
)

func TestCH234SQLQueryProfilerAggregatesBoundedStageMetrics(t *testing.T) {
	profiler, err := NewSQLQueryProfiler(SQLQueryProfilerOptions{
		MaxQueries:        1,
		MaxStagesPerQuery: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	observations := []SQLQueryStageSample{
		{Stage: "scan", CPUTime: 2 * time.Millisecond, BlockedTime: time.Microsecond, Rows: 10, Bytes: 100, AllocatedBytes: 50, PeakBytes: 80, RetainedBytes: 20},
		{Stage: "scan", CPUTime: 3 * time.Millisecond, Rows: 5, Bytes: 50, AllocatedBytes: 60, PeakBytes: 70, RetainedBytes: 30},
		{Stage: "filter", CPUTime: time.Millisecond, Rows: 5, Bytes: 50, AllocatedBytes: 10, PeakBytes: 40, RetainedBytes: 12},
		{Stage: "project", CPUTime: time.Millisecond},
	}
	for _, observation := range observations {
		captured, err := profiler.RecordStage("q1", observation)
		if err != nil {
			t.Fatalf("RecordStage(%q) error = %v", observation.Stage, err)
		}
		if observation.Stage == "project" && captured {
			t.Fatal("stage over capacity was captured")
		}
	}

	profile, ok := profiler.StageProfile("q1")
	if !ok {
		t.Fatal("StageProfile(q1) not found")
	}
	if len(profile.Stages) != 2 || profile.DroppedObservations != 1 {
		t.Fatalf("stage profile bounds = %#v", profile)
	}
	if profile.Stages[0].Stage != "filter" || profile.Stages[1].Stage != "scan" {
		t.Fatalf("stage order = %#v", profile.Stages)
	}
	scan := profile.Stages[1]
	if scan.Observations != 2 || scan.CPUTime != 5*time.Millisecond || scan.BlockedTime != time.Microsecond || scan.Rows != 15 || scan.Bytes != 150 || scan.AllocatedBytes != 110 || scan.PeakBytes != 80 || scan.MaxRetainedBytes != 30 {
		t.Fatalf("scan metrics = %#v", scan)
	}
	stats := profiler.Stats()
	if stats.StageQueryCount != 1 || stats.StageObservationCount != 3 || stats.DroppedStageObservationCount != 1 {
		t.Fatalf("stage stats = %#v", stats)
	}
}

func TestCH234SQLQueryProfilerStageValidationAndClose(t *testing.T) {
	if _, err := NewSQLQueryProfiler(SQLQueryProfilerOptions{MaxStagesPerQuery: -1}); !errors.Is(err, ErrSQLQueryProfilerLimitInvalid) {
		t.Fatalf("invalid stage limit error = %v", err)
	}
	profiler, err := NewSQLQueryProfiler(SQLQueryProfilerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := profiler.RecordStage("q1", SQLQueryStageSample{}); !errors.Is(err, ErrSQLQueryProfilerStageRequired) {
		t.Fatalf("empty stage error = %v", err)
	}
	profiler.Close()
	if _, err := profiler.RecordStage("q1", SQLQueryStageSample{Stage: "scan"}); !errors.Is(err, ErrSQLQueryProfilerClosed) {
		t.Fatalf("closed stage error = %v", err)
	}
}
