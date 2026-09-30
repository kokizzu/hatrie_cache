package hatSql

import (
	"context"
	"strconv"
	"testing"
)

type m236BenchmarkRefreshEntry struct {
	object  SQLMaintainedObject
	refresh SQLMaintainedRefreshFunc
}

func newM236BenchmarkFixture(b *testing.B) (*SQLMaintainedRefreshRegistry, []m236BenchmarkRefreshEntry) {
	b.Helper()
	registry := NewSQLMaintainedRefreshRegistry()
	entries := make([]m236BenchmarkRefreshEntry, 0, 8192)
	refresh := SQLMaintainedRefreshFunc(func(context.Context, SQLMaintainedObject) error { return nil })
	for index := 0; index < 8192; index++ {
		kind := SQLMaintainedObjectView
		if index%2 == 0 {
			kind = SQLMaintainedObjectIndex
		}
		name := "object-" + strconv.Itoa(index)
		dependency := "source-" + strconv.Itoa(index)
		if index < 8 {
			dependency = "hot"
		}
		object := SQLMaintainedObject{Kind: kind, Name: name, Dependencies: []string{dependency}}
		if err := registry.Register(kind, name, []string{dependency}, refresh); err != nil {
			b.Fatalf("Register(%q) error = %v", name, err)
		}
		entries = append(entries, m236BenchmarkRefreshEntry{object: object, refresh: refresh})
	}
	return registry, entries
}

func BenchmarkM236LinearInvalidatedRefresh(b *testing.B) {
	_, entries := newM236BenchmarkFixture(b)
	changed := "hot"
	refreshed := 0
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		for _, entry := range entries {
			if entry.object.Dependencies[0] != changed {
				continue
			}
			if err := entry.refresh(ctx, entry.object); err != nil {
				b.Fatal(err)
			}
			refreshed++
		}
	}
	b.StopTimer()
	if refreshed == 0 {
		b.Fatal("no entries refreshed")
	}
}

func BenchmarkM236IndexedInvalidatedRefresh(b *testing.B) {
	registry, _ := newM236BenchmarkFixture(b)
	changed := []string{"hot"}
	refreshed := 0
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		objects, err := registry.RefreshChanged(ctx, changed)
		if err != nil {
			b.Fatal(err)
		}
		refreshed += len(objects)
	}
	b.StopTimer()
	if refreshed == 0 {
		b.Fatal("no entries refreshed")
	}
}

func BenchmarkM236IndexedInvalidatedRefreshInto(b *testing.B) {
	registry, _ := newM236BenchmarkFixture(b)
	changed := []string{"hot"}
	buffer := make([]SQLMaintainedObject, 0, 8)
	refreshed := 0
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		var err error
		buffer, err = registry.RefreshChangedInto(ctx, buffer, changed)
		if err != nil {
			b.Fatal(err)
		}
		refreshed += len(buffer)
	}
	b.StopTimer()
	if refreshed == 0 {
		b.Fatal("no entries refreshed")
	}
}
