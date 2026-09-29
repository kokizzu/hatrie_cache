package hatSql

import (
	"errors"
	"sync"
	"testing"
	"time"
)

func TestSQLDataflowMetricsOperator(t *testing.T) {
	catalog, err := NewSQLDataflowMetricsCatalog(SQLDataflowMetricsCatalogOptions{MaxObjects: 2})
	if err != nil {
		t.Fatalf("NewSQLDataflowMetricsCatalog() error = %v", err)
	}
	firstAt := time.Unix(100, 0).UTC()
	if err := catalog.ObserveOperator(SQLDataflowObjectCompute, "join", SQLDataflowOperatorObservation{
		Updates:    100,
		Batches:    10,
		Frontier:   90,
		ObservedAt: firstAt,
	}); err != nil {
		t.Fatalf("ObserveOperator(first) error = %v", err)
	}
	secondAt := firstAt.Add(10 * time.Second)
	if err := catalog.ObserveOperator(SQLDataflowObjectCompute, "join", SQLDataflowOperatorObservation{
		Updates:    130,
		Batches:    13,
		Frontier:   120,
		ObservedAt: secondAt,
	}); err != nil {
		t.Fatalf("ObserveOperator(second) error = %v", err)
	}

	snapshot, err := catalog.Operator(SQLDataflowObjectCompute, "join")
	if err != nil {
		t.Fatalf("Operator() error = %v", err)
	}
	if snapshot.Updates != 130 || snapshot.Batches != 13 || snapshot.Frontier != 120 || !snapshot.UpdatedAt.Equal(secondAt) {
		t.Fatalf("operator snapshot = %#v", snapshot)
	}
	stats := catalog.Stats()
	if stats.Objects != 1 || stats.MetricPoints != 3 {
		t.Fatalf("catalog stats = %#v, want one object with three metrics", stats)
	}
	if rows := catalog.Rows(); len(rows) != 3 {
		t.Fatalf("operator rows = %d, want 3", len(rows))
	}

	if err := catalog.ObserveOperator(SQLDataflowObjectCompute, "join", SQLDataflowOperatorObservation{
		Updates:    129,
		Batches:    14,
		Frontier:   121,
		ObservedAt: secondAt,
	}); !errors.Is(err, ErrSQLDataflowMetricsInvalid) {
		t.Fatalf("decreasing operator counters error = %v, want ErrSQLDataflowMetricsInvalid", err)
	}
	if _, err := catalog.Operator(SQLDataflowObjectCompute, "missing"); !errors.Is(err, ErrSQLDataflowMetricsNotFound) {
		t.Fatalf("missing operator error = %v, want ErrSQLDataflowMetricsNotFound", err)
	}
}

func TestSQLDataflowMetricsOperatorBoundsAndLifecycle(t *testing.T) {
	catalog, err := NewSQLDataflowMetricsCatalog(SQLDataflowMetricsCatalogOptions{MaxObjects: 1, MaxMetricsPerObject: 3, MaxMetricPoints: 3})
	if err != nil {
		t.Fatalf("NewSQLDataflowMetricsCatalog() error = %v", err)
	}
	if err := catalog.ObserveOperator(SQLDataflowObjectCompute, "join", SQLDataflowOperatorObservation{Updates: 1, Batches: 1, Frontier: 1}); err != nil {
		t.Fatalf("ObserveOperator() error = %v", err)
	}
	if err := catalog.Upsert(SQLDataflowMetricsObject{
		Kind:    SQLDataflowObjectCompute,
		Name:    "join",
		Metrics: []SQLDataflowMetricPoint{{Name: "custom", Unit: "count"}},
	}); err != nil {
		t.Fatalf("Upsert() error = %v", err)
	}
	if _, err := catalog.Operator(SQLDataflowObjectCompute, "join"); !errors.Is(err, ErrSQLDataflowMetricsNotFound) {
		t.Fatalf("upserted operator error = %v, want ErrSQLDataflowMetricsNotFound", err)
	}
	if err := catalog.ObserveOperator(SQLDataflowObjectCompute, "join", SQLDataflowOperatorObservation{Updates: 2, Batches: 2, Frontier: 2}); !errors.Is(err, ErrSQLDataflowMetricsLimit) {
		t.Fatalf("operator append bound error = %v, want ErrSQLDataflowMetricsLimit", err)
	}
	if err := catalog.Remove(SQLDataflowObjectCompute, "join"); err != nil {
		t.Fatalf("Remove() error = %v", err)
	}
	if _, err := catalog.Operator(SQLDataflowObjectCompute, "join"); !errors.Is(err, ErrSQLDataflowMetricsNotFound) {
		t.Fatalf("removed operator error = %v, want ErrSQLDataflowMetricsNotFound", err)
	}

	for name, options := range map[string]SQLDataflowMetricsCatalogOptions{
		"metrics per object": {MaxMetricsPerObject: 2},
		"metric points":      {MaxMetricPoints: 2},
	} {
		t.Run(name, func(t *testing.T) {
			limited, err := NewSQLDataflowMetricsCatalog(options)
			if err != nil {
				t.Fatalf("NewSQLDataflowMetricsCatalog() error = %v", err)
			}
			if err := limited.ObserveOperator(SQLDataflowObjectCompute, "join", SQLDataflowOperatorObservation{Updates: 1, Batches: 1, Frontier: 1}); !errors.Is(err, ErrSQLDataflowMetricsLimit) {
				t.Fatalf("ObserveOperator() error = %v, want ErrSQLDataflowMetricsLimit", err)
			}
		})
	}
}

func TestSQLDataflowMetricsOperatorConcurrentReadAndUpdate(t *testing.T) {
	catalog, err := NewSQLDataflowMetricsCatalog(SQLDataflowMetricsCatalogOptions{})
	if err != nil {
		t.Fatalf("NewSQLDataflowMetricsCatalog() error = %v", err)
	}
	if err := catalog.ObserveOperator(SQLDataflowObjectCompute, "aggregate", SQLDataflowOperatorObservation{Updates: 1, Batches: 1, Frontier: 1}); err != nil {
		t.Fatalf("ObserveOperator(initial) error = %v", err)
	}
	var group sync.WaitGroup
	group.Add(2)
	go func() {
		defer group.Done()
		for range 1000 {
			if err := catalog.ObserveOperator(SQLDataflowObjectCompute, "aggregate", SQLDataflowOperatorObservation{Updates: 1, Batches: 1, Frontier: 1}); err != nil {
				t.Errorf("ObserveOperator(concurrent) error = %v", err)
				return
			}
		}
	}()
	go func() {
		defer group.Done()
		for range 1000 {
			if _, err := catalog.Operator(SQLDataflowObjectCompute, "aggregate"); err != nil {
				t.Errorf("Operator(concurrent) error = %v", err)
				return
			}
		}
	}()
	group.Wait()
}
