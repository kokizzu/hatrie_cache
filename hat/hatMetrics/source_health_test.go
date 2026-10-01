package hatMetrics

import (
	"errors"
	"reflect"
	"testing"
)

func TestSourceHealthRegistryRecordsBoundedSortedState(t *testing.T) {
	registry := NewSourceHealthRegistry(2)
	if err := registry.Record("zeta", SourceHealthHealthy, 10, ""); err != nil {
		t.Fatal(err)
	}
	if err := registry.Record("alpha", SourceHealthFailed, 4, "upstream timeout"); err != nil {
		t.Fatal(err)
	}
	if err := registry.Record("alpha", SourceHealthDegraded, 5, "retrying"); err != nil {
		t.Fatal(err)
	}
	rows := registry.Snapshot(12)
	want := []SourceHealth{
		{Source: "alpha", Status: SourceHealthDegraded, Frontier: 5, Observed: 12, Lag: 7, ConsecutiveFailures: 2, LastError: "retrying"},
		{Source: "zeta", Status: SourceHealthHealthy, Frontier: 10, Observed: 12, Lag: 2},
	}
	for index := range rows {
		rows[index].UpdatedAtUnixNano = 0
	}
	if !reflect.DeepEqual(rows, want) {
		t.Fatalf("health snapshot = %#v, want %#v", rows, want)
	}
	if err := registry.Record("alpha", SourceHealthHealthy, 8, ""); err != nil {
		t.Fatal(err)
	}
	if got := registry.Snapshot(9)[0]; got.Status != SourceHealthHealthy || got.ConsecutiveFailures != 0 || got.LastError != "" || got.Frontier != 8 {
		t.Fatalf("recovered source = %#v", got)
	}
}

func TestSourceHealthRegistryRejectsInvalidAndOverCapacityRecords(t *testing.T) {
	registry := NewSourceHealthRegistry(1)
	if err := registry.Record("", SourceHealthHealthy, 1, ""); !errors.Is(err, ErrSourceNameRequired) {
		t.Fatalf("empty source error = %v", err)
	}
	if err := registry.Record("source", SourceHealthStatus("bad"), 1, ""); !errors.Is(err, ErrSourceHealthStatusInvalid) {
		t.Fatalf("invalid status error = %v", err)
	}
	if err := registry.Record("source", SourceHealthHealthy, 1, ""); err != nil {
		t.Fatal(err)
	}
	if err := registry.Record("other", SourceHealthHealthy, 1, ""); !errors.Is(err, ErrSourceHealthCapacity) {
		t.Fatalf("capacity error = %v", err)
	}
}
