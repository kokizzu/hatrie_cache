package hatSql

import (
	"errors"
	"math"
	"sync"
	"testing"
	"time"
)

func TestSQLDataflowMetricsCatalogUnifiesObjectsAndRows(t *testing.T) {
	updatedAt := time.Unix(1_700_000_000, 123).UTC()
	catalog, err := NewSQLDataflowMetricsCatalog(SQLDataflowMetricsCatalogOptions{
		MaxObjects:          4,
		MaxMetricsPerObject: 4,
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, object := range []SQLDataflowMetricsObject{
		{
			Kind: SQLDataflowObjectSink, Name: "warehouse",
			Metrics: []SQLDataflowMetricPoint{{Name: "rows_sent", Unit: "rows", Value: 8, UpdatedAt: updatedAt}},
		},
		{
			Kind: SQLDataflowObjectCompute, Name: "daily_rollup",
			Metrics: []SQLDataflowMetricPoint{{Name: "rows_out", Unit: "rows", Value: 12, UpdatedAt: updatedAt}},
		},
		{
			Kind: SQLDataflowObjectSource, Name: "orders",
			Metrics: []SQLDataflowMetricPoint{{Name: "rows_read", Unit: "rows", Value: 20, UpdatedAt: updatedAt}},
		},
	} {
		if err := catalog.Upsert(object); err != nil {
			t.Fatalf("Upsert(%q) error = %v", object.Name, err)
		}
	}

	rows := catalog.Rows()
	if len(rows) != 3 {
		t.Fatalf("Rows() returned %d rows, want 3", len(rows))
	}
	wantKinds := []string{"source", "compute", "sink"}
	wantNames := []string{"orders", "daily_rollup", "warehouse"}
	for index, row := range rows {
		if row["object_kind"] != wantKinds[index] || row["object_name"] != wantNames[index] {
			t.Fatalf("row %d = %#v, want %s/%s", index, row, wantKinds[index], wantNames[index])
		}
		if row["value"] != float64([]float64{20, 12, 8}[index]) || row["unit"] != "rows" {
			t.Fatalf("row %d metric fields = %#v", index, row)
		}
	}

	snapshot := catalog.Snapshot()
	if len(snapshot.Objects) != 3 || snapshot.Objects[0].Metrics[0].UpdatedAt != updatedAt {
		t.Fatalf("Snapshot() = %#v", snapshot)
	}
	snapshot.Objects[0].Metrics[0].Value = 999
	if got := catalog.Rows()[0]["value"]; got != float64(20) {
		t.Fatalf("snapshot mutation changed catalog value to %v", got)
	}
	if stats := catalog.Stats(); stats.Objects != 3 || stats.MetricPoints != 3 {
		t.Fatalf("Stats() = %#v", stats)
	}
}

func TestSQLDataflowMetricsCatalogReplacementIsAtomicAndValidated(t *testing.T) {
	catalog, err := NewSQLDataflowMetricsCatalog(SQLDataflowMetricsCatalogOptions{
		MaxObjects:          2,
		MaxMetricsPerObject: 3,
	})
	if err != nil {
		t.Fatal(err)
	}
	valid := SQLDataflowMetricsObject{
		Kind: SQLDataflowObjectSource, Name: "orders",
		Metrics: []SQLDataflowMetricPoint{{Name: "rows_read", Unit: "rows", Value: 10}},
	}
	if err := catalog.Upsert(valid); err != nil {
		t.Fatal(err)
	}
	for _, invalid := range []SQLDataflowMetricsObject{
		{Kind: SQLDataflowObjectKind("unknown"), Name: "orders", Metrics: valid.Metrics},
		{Kind: SQLDataflowObjectSource, Name: "", Metrics: valid.Metrics},
		{Kind: SQLDataflowObjectSource, Name: "orders", Metrics: []SQLDataflowMetricPoint{
			{Name: "rows_read", Value: 1}, {Name: "rows_read", Value: 2},
		}},
		{Kind: SQLDataflowObjectSource, Name: "orders", Metrics: []SQLDataflowMetricPoint{{Name: "bad value", Value: 1}}},
		{Kind: SQLDataflowObjectSource, Name: "orders", Metrics: []SQLDataflowMetricPoint{{Name: "bad", Value: math.NaN()}}},
	} {
		if err := catalog.Upsert(invalid); !errors.Is(err, ErrSQLDataflowMetricsInvalid) {
			t.Fatalf("Upsert(%#v) error = %v, want invalid", invalid, err)
		}
	}
	if got := catalog.Rows()[0]["value"]; got != float64(10) {
		t.Fatalf("invalid replacement changed existing value to %v", got)
	}
}

func TestSQLDataflowMetricsCatalogBoundsAndRemoval(t *testing.T) {
	catalog, err := NewSQLDataflowMetricsCatalog(SQLDataflowMetricsCatalogOptions{
		MaxObjects:          1,
		MaxMetricsPerObject: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	object := SQLDataflowMetricsObject{
		Kind: SQLDataflowObjectSource, Name: "orders",
		Metrics: []SQLDataflowMetricPoint{{Name: "rows", Value: 1}},
	}
	if err := catalog.Upsert(object); err != nil {
		t.Fatal(err)
	}
	if err := catalog.Upsert(SQLDataflowMetricsObject{
		Kind: SQLDataflowObjectCompute, Name: "rollup",
		Metrics: []SQLDataflowMetricPoint{{Name: "rows", Value: 1}},
	}); !errors.Is(err, ErrSQLDataflowMetricsLimit) {
		t.Fatalf("object limit error = %v, want limit", err)
	}
	if err := catalog.Upsert(SQLDataflowMetricsObject{
		Kind: SQLDataflowObjectSource, Name: "orders",
		Metrics: []SQLDataflowMetricPoint{{Name: "rows", Value: 1}, {Name: "bytes", Value: 2}},
	}); !errors.Is(err, ErrSQLDataflowMetricsLimit) {
		t.Fatalf("metric limit error = %v, want limit", err)
	}
	if err := catalog.Remove(SQLDataflowObjectSource, "orders"); err != nil {
		t.Fatal(err)
	}
	if err := catalog.Remove(SQLDataflowObjectSource, "orders"); !errors.Is(err, ErrSQLDataflowMetricsNotFound) {
		t.Fatalf("second Remove() error = %v, want not found", err)
	}
	if len(catalog.Rows()) != 0 {
		t.Fatal("removed object still appears in Rows()")
	}
}

func TestSQLDataflowMetricsCatalogRejectsInvalidOptions(t *testing.T) {
	for _, options := range []SQLDataflowMetricsCatalogOptions{
		{MaxObjects: -1},
		{MaxMetricsPerObject: -1},
	} {
		if _, err := NewSQLDataflowMetricsCatalog(options); !errors.Is(err, ErrSQLDataflowMetricsInvalid) {
			t.Fatalf("options %#v error = %v, want invalid", options, err)
		}
	}
}

func TestSQLDataflowMetricsCatalogConcurrentReadersAndWriters(t *testing.T) {
	catalog, err := NewSQLDataflowMetricsCatalog(SQLDataflowMetricsCatalogOptions{
		MaxObjects:          8,
		MaxMetricsPerObject: 2,
		MaxMetricPoints:     8,
	})
	if err != nil {
		t.Fatal(err)
	}

	var writers sync.WaitGroup
	for worker := 0; worker < 4; worker++ {
		worker := worker
		writers.Add(1)
		go func() {
			defer writers.Done()
			kind := []SQLDataflowObjectKind{
				SQLDataflowObjectSource,
				SQLDataflowObjectCompute,
				SQLDataflowObjectSink,
				SQLDataflowObjectSource,
			}[worker]
			name := []string{"orders", "rollup", "warehouse", "payments"}[worker]
			for iteration := 0; iteration < 100; iteration++ {
				if err := catalog.Upsert(SQLDataflowMetricsObject{
					Kind: kind,
					Name: name,
					Metrics: []SQLDataflowMetricPoint{
						{Name: "rows", Unit: "rows", Value: float64(iteration)},
					},
				}); err != nil {
					t.Errorf("worker %d Upsert() error = %v", worker, err)
					return
				}
			}
		}()
	}

	var readers sync.WaitGroup
	for reader := 0; reader < 4; reader++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for iteration := 0; iteration < 100; iteration++ {
				_ = catalog.Rows()
				_ = catalog.Snapshot()
				_ = catalog.Stats()
			}
		}()
	}
	writers.Wait()
	readers.Wait()

	if stats := catalog.Stats(); stats.Objects != 4 || stats.MetricPoints != 4 {
		t.Fatalf("Stats() = %#v, want four objects and metric points", stats)
	}
}

func TestSQLDataflowMetricsCatalogUpdatesRegisteredMetric(t *testing.T) {
	catalog, err := NewSQLDataflowMetricsCatalog(SQLDataflowMetricsCatalogOptions{})
	if err != nil {
		t.Fatal(err)
	}
	updatedAt := time.Unix(1_700_000_100, 0).UTC()
	if err := catalog.Upsert(SQLDataflowMetricsObject{
		Kind: SQLDataflowObjectCompute,
		Name: "rollup",
		Metrics: []SQLDataflowMetricPoint{
			{Name: "rows", Unit: "rows", Value: 1},
		},
	}); err != nil {
		t.Fatal(err)
	}
	if err := catalog.UpdateMetric(SQLDataflowObjectCompute, "rollup", "rows", 99, updatedAt); err != nil {
		t.Fatal(err)
	}
	row := catalog.Rows()[0]
	if row["value"] != float64(99) || row["updated_at"] != updatedAt {
		t.Fatalf("updated row = %#v", row)
	}
	if err := catalog.UpdateMetric(SQLDataflowObjectCompute, "rollup", "missing", 1, updatedAt); !errors.Is(err, ErrSQLDataflowMetricsNotFound) {
		t.Fatalf("missing metric error = %v, want not found", err)
	}
	if err := catalog.UpdateMetric(SQLDataflowObjectCompute, "rollup", "rows", math.Inf(1), updatedAt); !errors.Is(err, ErrSQLDataflowMetricsInvalid) {
		t.Fatalf("non-finite update error = %v, want invalid", err)
	}
	if got := catalog.Rows()[0]["value"]; got != float64(99) {
		t.Fatalf("invalid update changed value to %v", got)
	}
}
