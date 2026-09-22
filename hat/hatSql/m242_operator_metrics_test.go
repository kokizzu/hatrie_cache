package hatSql

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
)

type m242SnapshotResolver struct{}

func (m242SnapshotResolver) ResolveSQLSource(string, string) ([]Row, error) {
	return nil, nil
}

func (m242SnapshotResolver) BeginSQLSnapshotAt(context.Context, uint64) (SQLSourceResolver, func(), error) {
	return m242SnapshotResolver{}, func() {}, nil
}

func TestM242OperatorMetricsReportUpdatesBatchesAndFrontier(t *testing.T) {
	frontier := uint64(42)
	var observed SQLQueryEvent
	result, err := ExecuteSQLQueryContext(context.Background(), "SELECT * FROM VALUES (1), (2)", m242SnapshotResolver{}, SQLQueryOptions{
		AsOfFrontier:    &frontier,
		OperatorMetrics: true,
		Observer: SQLQueryObserverFunc(func(event SQLQueryEvent) {
			observed = event
		}),
	})
	if err != nil {
		t.Fatalf("ExecuteSQLQueryContext() error = %v", err)
	}
	if len(result.Rows) != 2 {
		t.Fatalf("result rows = %d, want 2", len(result.Rows))
	}
	var scan *SQLQueryOperatorMetrics
	for index := range observed.OperatorMetrics {
		if observed.OperatorMetrics[index].Node == "SCAN" {
			scan = &observed.OperatorMetrics[index]
			break
		}
	}
	if scan == nil {
		t.Fatalf("operator metrics = %#v, want SCAN", observed.OperatorMetrics)
	}
	if scan.UpdateCount != 2 {
		t.Fatalf("SCAN UpdateCount = %d, want 2", scan.UpdateCount)
	}
	if scan.BatchCount != 1 {
		t.Fatalf("SCAN BatchCount = %d, want 1", scan.BatchCount)
	}
	if scan.Frontier == nil || *scan.Frontier != frontier {
		t.Fatalf("SCAN Frontier = %#v, want %d", scan.Frontier, frontier)
	}
	wire, err := json.Marshal(observed)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	if !strings.Contains(string(wire), "operator_metrics") || !strings.Contains(string(wire), "update_count") || !strings.Contains(string(wire), "frontier") {
		t.Fatalf("enabled event omitted operator metrics: %s", wire)
	}
}

func TestM242OperatorMetricsAreDefaultOff(t *testing.T) {
	var observed SQLQueryEvent
	_, err := ExecuteSQLQueryContext(context.Background(), "SELECT * FROM VALUES (1)", nil, SQLQueryOptions{
		Observer: SQLQueryObserverFunc(func(event SQLQueryEvent) {
			observed = event
		}),
	})
	if err != nil {
		t.Fatalf("ExecuteSQLQueryContext() error = %v", err)
	}
	if len(observed.Operators) == 0 {
		t.Fatal("default observer did not report operators")
	}
	if len(observed.OperatorMetrics) != 0 {
		t.Fatalf("default operator metrics = %#v, want disabled", observed.OperatorMetrics)
	}
	wire, err := json.Marshal(observed)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	if strings.Contains(string(wire), "operator_metrics") || strings.Contains(string(wire), "update_count") || strings.Contains(string(wire), "batch_count") {
		t.Fatalf("default event unexpectedly contains opt-in metrics: %s", wire)
	}
}

func TestM242OperatorMetricFrontierIsCloned(t *testing.T) {
	frontier := uint64(7)
	cloned := cloneSQLQueryOperatorMetrics([]SQLQueryOperatorMetrics{{Frontier: &frontier}})
	if len(cloned) != 1 || cloned[0].Frontier == nil || *cloned[0].Frontier != frontier {
		t.Fatalf("cloned frontier = %#v, want %d", cloned, frontier)
	}
	if cloned[0].Frontier == &frontier {
		t.Fatal("cloned frontier shares input pointer")
	}
}
