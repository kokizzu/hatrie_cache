package hatSql

import (
	"errors"
	"sync"
	"testing"
	"time"
)

func TestSQLDataflowMetricsProgress(t *testing.T) {
	catalog, err := NewSQLDataflowMetricsCatalog(SQLDataflowMetricsCatalogOptions{MaxObjects: 2})
	if err != nil {
		t.Fatalf("NewSQLDataflowMetricsCatalog() error = %v", err)
	}
	firstInputAt := time.Unix(100, 0).UTC()
	firstOutputAt := firstInputAt.Add(2 * time.Second)
	if err := catalog.ObserveProgress(SQLDataflowObjectSource, "orders", SQLDataflowProgressObservation{
		InputTimestamp:   100,
		OutputTimestamp:  90,
		InputObservedAt:  firstInputAt,
		OutputObservedAt: firstOutputAt,
	}); err != nil {
		t.Fatalf("ObserveProgress(first) error = %v", err)
	}

	secondInputAt := firstInputAt.Add(10 * time.Second)
	secondOutputAt := secondInputAt.Add(2 * time.Second)
	if err := catalog.ObserveProgress(SQLDataflowObjectSource, "orders", SQLDataflowProgressObservation{
		InputTimestamp:   130,
		OutputTimestamp:  120,
		InputObservedAt:  secondInputAt,
		OutputObservedAt: secondOutputAt,
	}); err != nil {
		t.Fatalf("ObserveProgress(second) error = %v", err)
	}

	snapshot, err := catalog.Progress(SQLDataflowObjectSource, "orders")
	if err != nil {
		t.Fatalf("Progress() error = %v", err)
	}
	if snapshot.InputTimestamp != 130 || snapshot.OutputTimestamp != 120 || snapshot.TimestampLag != 10 {
		t.Fatalf("progress timestamps = %#v", snapshot)
	}
	if snapshot.InputTimestampThroughput != 3 || snapshot.OutputTimestampThroughput != 3 {
		t.Fatalf("timestamp throughput = %#v, want 3/3", snapshot)
	}
	if snapshot.InputToOutputLatency != 2*time.Second {
		t.Fatalf("input-to-output latency = %s, want 2s", snapshot.InputToOutputLatency)
	}
	if snapshot.InputObservedAt != secondInputAt || snapshot.OutputObservedAt != secondOutputAt {
		t.Fatalf("observed timestamps = %#v", snapshot)
	}

	stats := catalog.Stats()
	if stats.Objects != 1 || stats.MetricPoints != 6 {
		t.Fatalf("catalog stats = %#v, want one progress object with six metrics", stats)
	}
	rows := catalog.Rows()
	if len(rows) != 6 {
		t.Fatalf("progress rows = %d, want 6", len(rows))
	}

	if err := catalog.ObserveProgress(SQLDataflowObjectSource, "orders", SQLDataflowProgressObservation{
		InputTimestamp:   129,
		OutputTimestamp:  121,
		InputObservedAt:  secondInputAt,
		OutputObservedAt: secondOutputAt,
	}); !errors.Is(err, ErrSQLDataflowMetricsInvalid) {
		t.Fatalf("decreasing progress error = %v, want ErrSQLDataflowMetricsInvalid", err)
	}
	if _, err := catalog.Progress(SQLDataflowObjectSource, "missing"); !errors.Is(err, ErrSQLDataflowMetricsNotFound) {
		t.Fatalf("missing progress error = %v, want ErrSQLDataflowMetricsNotFound", err)
	}
}

func TestSQLDataflowMetricsProgressValidatesBoundsAndLifecycle(t *testing.T) {
	catalog, err := NewSQLDataflowMetricsCatalog(SQLDataflowMetricsCatalogOptions{MaxObjects: 1, MaxMetricsPerObject: 7, MaxMetricPoints: 7})
	if err != nil {
		t.Fatalf("NewSQLDataflowMetricsCatalog() error = %v", err)
	}
	if err := catalog.ObserveProgress(SQLDataflowObjectSource, "orders", SQLDataflowProgressObservation{
		InputTimestamp:   1,
		OutputTimestamp:  2,
		InputObservedAt:  time.Unix(100, 0),
		OutputObservedAt: time.Unix(99, 0),
	}); !errors.Is(err, ErrSQLDataflowMetricsInvalid) {
		t.Fatalf("invalid first progress error = %v, want ErrSQLDataflowMetricsInvalid", err)
	}

	if err := catalog.ObserveProgress(SQLDataflowObjectSource, "orders", SQLDataflowProgressObservation{InputTimestamp: 10, OutputTimestamp: 8}); err != nil {
		t.Fatalf("ObserveProgress(without wall clock) error = %v", err)
	}
	snapshot, err := catalog.Progress(SQLDataflowObjectSource, "orders")
	if err != nil {
		t.Fatalf("Progress() after wall-clock-free update error = %v", err)
	}
	if snapshot.InputTimestampThroughput != 0 || snapshot.OutputTimestampThroughput != 0 || snapshot.InputToOutputLatency != 0 {
		t.Fatalf("wall-clock-free derived metrics = %#v, want zero rates and latency", snapshot)
	}
	if err := catalog.Upsert(SQLDataflowMetricsObject{
		Kind:    SQLDataflowObjectSource,
		Name:    "orders",
		Metrics: []SQLDataflowMetricPoint{{Name: "custom", Unit: "count"}},
	}); err != nil {
		t.Fatalf("Upsert() error = %v", err)
	}
	if _, err := catalog.Progress(SQLDataflowObjectSource, "orders"); !errors.Is(err, ErrSQLDataflowMetricsNotFound) {
		t.Fatalf("upserted progress error = %v, want ErrSQLDataflowMetricsNotFound", err)
	}
	if err := catalog.ObserveProgress(SQLDataflowObjectSource, "orders", SQLDataflowProgressObservation{InputTimestamp: 20, OutputTimestamp: 19}); err != nil {
		t.Fatalf("ObserveProgress(after Upsert) error = %v", err)
	}

	if err := catalog.ObserveProgress(SQLDataflowObjectSink, "archive", SQLDataflowProgressObservation{InputTimestamp: 1, OutputTimestamp: 1}); !errors.Is(err, ErrSQLDataflowMetricsLimit) {
		t.Fatalf("object limit error = %v, want ErrSQLDataflowMetricsLimit", err)
	}
	if err := catalog.Remove(SQLDataflowObjectSource, "orders"); err != nil {
		t.Fatalf("Remove() error = %v", err)
	}
	if _, err := catalog.Progress(SQLDataflowObjectSource, "orders"); !errors.Is(err, ErrSQLDataflowMetricsNotFound) {
		t.Fatalf("removed progress error = %v, want ErrSQLDataflowMetricsNotFound", err)
	}
}

func TestSQLDataflowMetricsProgressConcurrentReadAndUpdate(t *testing.T) {
	catalog, err := NewSQLDataflowMetricsCatalog(SQLDataflowMetricsCatalogOptions{})
	if err != nil {
		t.Fatalf("NewSQLDataflowMetricsCatalog() error = %v", err)
	}
	if err := catalog.ObserveProgress(SQLDataflowObjectCompute, "aggregate", SQLDataflowProgressObservation{InputTimestamp: 1, OutputTimestamp: 1}); err != nil {
		t.Fatalf("ObserveProgress(initial) error = %v", err)
	}
	var group sync.WaitGroup
	group.Add(2)
	go func() {
		defer group.Done()
		for range 1000 {
			if err := catalog.ObserveProgress(SQLDataflowObjectCompute, "aggregate", SQLDataflowProgressObservation{InputTimestamp: 1, OutputTimestamp: 1}); err != nil {
				t.Errorf("ObserveProgress(concurrent) error = %v", err)
				return
			}
		}
	}()
	go func() {
		defer group.Done()
		for range 1000 {
			if _, err := catalog.Progress(SQLDataflowObjectCompute, "aggregate"); err != nil {
				t.Errorf("Progress(concurrent) error = %v", err)
				return
			}
		}
	}()
	group.Wait()
}

func TestSQLDataflowMetricsProgressRejectsTooSmallMetricBounds(t *testing.T) {
	for name, options := range map[string]SQLDataflowMetricsCatalogOptions{
		"metrics per object": {MaxMetricsPerObject: 5},
		"metric points":      {MaxMetricPoints: 5},
	} {
		t.Run(name, func(t *testing.T) {
			catalog, err := NewSQLDataflowMetricsCatalog(options)
			if err != nil {
				t.Fatalf("NewSQLDataflowMetricsCatalog() error = %v", err)
			}
			err = catalog.ObserveProgress(SQLDataflowObjectSource, "orders", SQLDataflowProgressObservation{InputTimestamp: 1, OutputTimestamp: 1})
			if !errors.Is(err, ErrSQLDataflowMetricsLimit) {
				t.Fatalf("ObserveProgress() error = %v, want ErrSQLDataflowMetricsLimit", err)
			}
			if got := catalog.Stats(); got.Objects != 0 || got.MetricPoints != 0 {
				t.Fatalf("failed progress stats = %#v, want empty catalog", got)
			}
		})
	}
}
