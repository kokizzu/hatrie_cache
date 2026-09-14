package hatMetrics

import (
	"errors"
	"reflect"
	"testing"
)

func TestOperatorFrontierRegistryTracksMonotoneProgress(t *testing.T) {
	var registry OperatorFrontierRegistry

	if err := registry.Advance("  sort  ", 7); err != nil {
		t.Fatalf("Advance(sort) error = %v", err)
	}
	if err := registry.Advance("scan", 11); err != nil {
		t.Fatalf("Advance(scan) error = %v", err)
	}
	if err := registry.Advance("sort", 6); !errors.Is(err, ErrOperatorFrontierRegressed) {
		t.Fatalf("Advance(regressed) error = %v, want ErrOperatorFrontierRegressed", err)
	}

	frontier, ok := registry.Frontier("sort")
	if !ok || frontier != 7 {
		t.Fatalf("Frontier(sort) = (%d, %t), want (7, true)", frontier, ok)
	}

	got := registry.Snapshot(10)
	want := []OperatorFrontier{
		{Operator: "scan", Frontier: 11, Observed: 10, Lag: 0},
		{Operator: "sort", Frontier: 7, Observed: 10, Lag: 3},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Snapshot() = %#v, want %#v", got, want)
	}
}

func TestOperatorFrontierRegistryRejectsBlankNamesAndHandlesNil(t *testing.T) {
	var registry OperatorFrontierRegistry
	if err := registry.Advance(" \t", 1); !errors.Is(err, ErrOperatorNameRequired) {
		t.Fatalf("Advance(blank) error = %v, want ErrOperatorNameRequired", err)
	}
	if got, ok := registry.Frontier("missing"); ok || got != 0 {
		t.Fatalf("Frontier(missing) = (%d, %t), want (0, false)", got, ok)
	}

	var nilRegistry *OperatorFrontierRegistry
	if err := nilRegistry.Advance("scan", 1); err == nil {
		t.Fatal("nil registry Advance() error = nil, want an error")
	}
	if got := nilRegistry.Snapshot(1); got != nil {
		t.Fatalf("nil registry Snapshot() = %#v, want nil", got)
	}
}

func TestOperatorFrontierRegistryDeleteReleasesOperator(t *testing.T) {
	registry := NewOperatorFrontierRegistry()
	if err := registry.Advance("sort", 7); err != nil {
		t.Fatalf("Advance(sort) error = %v", err)
	}
	if !registry.Delete(" sort ") {
		t.Fatal("Delete(sort) = false, want true")
	}
	if registry.Delete("sort") {
		t.Fatal("second Delete(sort) = true, want false")
	}
	if got := registry.Snapshot(10); len(got) != 0 {
		t.Fatalf("Snapshot() after delete = %#v, want empty", got)
	}
}
