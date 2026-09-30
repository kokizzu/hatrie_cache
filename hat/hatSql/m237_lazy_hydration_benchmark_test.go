package hatSql

import (
	"context"
	"strconv"
	"testing"
)

type m237BenchmarkHydrationEntry struct {
	object  SQLMaintainedObject
	hydrate SQLLazyHydrationFunc
}

func newM237BenchmarkFixture(b *testing.B) (*SQLLazyHydrationRegistry, []m237BenchmarkHydrationEntry) {
	b.Helper()
	registry := NewSQLLazyHydrationRegistry()
	entries := make([]m237BenchmarkHydrationEntry, 0, 8192)
	hydrate := SQLLazyHydrationFunc(func(context.Context, SQLMaintainedObject) error { return nil })
	for index := 0; index < 8192; index++ {
		kind := SQLMaintainedObjectView
		if index%2 == 0 {
			kind = SQLMaintainedObjectIndex
		}
		object := SQLMaintainedObject{
			Kind:         kind,
			Name:         "object-" + strconv.Itoa(index),
			Dependencies: []string{"source-" + strconv.Itoa(index)},
		}
		if err := registry.Register(object, hydrate); err != nil {
			b.Fatalf("Register(%q) error = %v", object.Name, err)
		}
		entries = append(entries, m237BenchmarkHydrationEntry{object: object, hydrate: hydrate})
	}
	return registry, entries
}

func BenchmarkM237EagerHydrateAll8192(b *testing.B) {
	_, entries := newM237BenchmarkFixture(b)
	ctx := context.Background()
	hydrated := 0
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		for _, entry := range entries {
			if err := entry.hydrate(ctx, entry.object); err != nil {
				b.Fatal(err)
			}
			hydrated++
		}
	}
	b.StopTimer()
	if hydrated == 0 {
		b.Fatal("no objects hydrated")
	}
}

func BenchmarkM237LazyHydrateOneOf8192(b *testing.B) {
	registry, _ := newM237BenchmarkFixture(b)
	ctx := context.Background()
	hydrated := 0
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		if err := registry.Invalidate(SQLMaintainedObjectView, "object-1"); err != nil {
			b.Fatal(err)
		}
		if err := registry.Ensure(ctx, SQLMaintainedObjectView, "object-1"); err != nil {
			b.Fatal(err)
		}
		hydrated++
	}
	b.StopTimer()
	if hydrated == 0 {
		b.Fatal("no objects hydrated")
	}
}

func BenchmarkM237LazyReadyRead(b *testing.B) {
	registry, _ := newM237BenchmarkFixture(b)
	ctx := context.Background()
	if err := registry.Ensure(ctx, SQLMaintainedObjectView, "object-1"); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		if err := registry.Ensure(ctx, SQLMaintainedObjectView, "object-1"); err != nil {
			b.Fatal(err)
		}
	}
}
