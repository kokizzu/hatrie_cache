package hatStorage_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"hatrie_cache/hat/hatStorage"
)

func newCHU30ColumnReferences(t *testing.T) []hatStorage.RemotePartColumnReference {
	t.Helper()
	columns := make([]hatStorage.RemotePartColumnReference, 0, 3)
	for _, column := range []struct {
		name string
		size uint64
	}{
		{name: "id", size: 2},
		{name: "body", size: 3},
		{name: "unused", size: 4},
	} {
		reference, err := hatStorage.NewRemotePartReference(
			"s3://bucket/parts/"+column.name,
			"parts/"+column.name+".bin",
			"sha256:"+column.name,
			column.size,
		)
		if err != nil {
			t.Fatal(err)
		}
		columns = append(columns, hatStorage.RemotePartColumnReference{Name: column.name, Reference: reference})
	}
	return columns
}

func TestCHU30ColumnPrefetchPlansRequestedColumnsWithinBudget(t *testing.T) {
	columns := newCHU30ColumnReferences(t)
	plan, err := hatStorage.PlanRemotePartColumnPrefetch(columns, []string{"id", "body", "unused"}, hatStorage.RemotePartColumnPrefetchOptions{MaxBytes: 5})
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.References) != 2 || len(plan.SelectedColumns) != 2 || len(plan.SkippedColumns) != 1 {
		t.Fatalf("plan = %#v, want two selected and one skipped column", plan)
	}
	if plan.SelectedColumns[0] != "id" || plan.SelectedColumns[1] != "body" || plan.SkippedColumns[0] != "unused" {
		t.Fatalf("plan column order = %#v/%#v", plan.SelectedColumns, plan.SkippedColumns)
	}
	if plan.SelectedBytes != 5 || plan.SkippedBytes != 4 {
		t.Fatalf("plan bytes = %d/%d, want 5/4", plan.SelectedBytes, plan.SkippedBytes)
	}
}

func TestCHU30ColumnPrefetchUsesExistingSingleFlightCache(t *testing.T) {
	columns := newCHU30ColumnReferences(t)
	cache, err := hatStorage.NewRemotePartCache(hatStorage.RemotePartCacheOptions{MaxBytes: 5, MaxEntries: 2})
	if err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	loader := func(_ context.Context, reference hatStorage.RemotePartReference) ([]byte, error) {
		calls.Add(1)
		return make([]byte, int(reference.SizeBytes())), nil
	}
	options := hatStorage.RemotePartColumnPrefetchOptions{MaxBytes: 5, MaxConcurrent: 1, Priority: 7}
	plan, err := cache.PrefetchColumns(context.Background(), columns, []string{"id", "body", "unused"}, options, loader)
	if err != nil {
		t.Fatal(err)
	}
	if plan.SelectedBytes != 5 || plan.SkippedBytes != 4 || calls.Load() != 2 {
		t.Fatalf("first plan/calls = %#v/%d, want 5 selected bytes, 4 skipped, two loads", plan, calls.Load())
	}
	if _, err := cache.PrefetchColumns(context.Background(), columns, []string{"id", "body"}, options, loader); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 2 {
		t.Fatalf("second prefetch calls = %d, want cached columns", calls.Load())
	}
	stats := cache.Stats()
	if stats.Entries != 2 || stats.Bytes != 5 || stats.Hits != 2 {
		t.Fatalf("cache stats = %#v, want two entries, five bytes, two hits", stats)
	}
}

func TestCHU30ColumnPrefetchRejectsAmbiguousRequests(t *testing.T) {
	columns := newCHU30ColumnReferences(t)
	for name, requested := range map[string][]string{
		"missing":   {"id", "nope"},
		"duplicate": {"id", "id"},
		"empty":     {"id", " "},
	} {
		if _, err := hatStorage.PlanRemotePartColumnPrefetch(columns, requested, hatStorage.RemotePartColumnPrefetchOptions{}); !errors.Is(err, hatStorage.ErrRemotePartColumnInvalid) {
			t.Errorf("%s error = %v, want ErrRemotePartColumnInvalid", name, err)
		}
	}
	invalid := append(columns, hatStorage.RemotePartColumnReference{Name: "id", Reference: columns[0].Reference})
	if _, err := hatStorage.PlanRemotePartColumnPrefetch(invalid, []string{"id"}, hatStorage.RemotePartColumnPrefetchOptions{}); !errors.Is(err, hatStorage.ErrRemotePartColumnInvalid) {
		t.Fatalf("duplicate source error = %v, want ErrRemotePartColumnInvalid", err)
	}
}
