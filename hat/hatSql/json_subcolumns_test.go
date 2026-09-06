package hatSql_test

import (
	"errors"
	"reflect"
	"sync"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestJSONSubcolumnRegistrySharesRepeatedPaths(t *testing.T) {
	registry := hatSql.NewJSONSubcolumnRegistry()
	first, created, err := registry.Intern(" $.user.id ")
	if err != nil || !created || first == 0 {
		t.Fatalf("first Intern() = id=%d created=%v err=%v", first, created, err)
	}
	second, created, err := registry.Intern("$.user.id")
	if err != nil || created || second != first {
		t.Fatalf("duplicate Intern() = id=%d created=%v err=%v, want id=%d existing", second, created, err, first)
	}
	third, created, err := registry.Intern("$.user.name")
	if err != nil || !created || third == first {
		t.Fatalf("second path Intern() = id=%d created=%v err=%v", third, created, err)
	}

	if id, ok := registry.Lookup("$.user.id"); !ok || id != first {
		t.Fatalf("Lookup() = %d, %v", id, ok)
	}
	if path, ok := registry.Path(third); !ok || path != "$.user.name" {
		t.Fatalf("Path() = %q, %v", path, ok)
	}
	expected := []hatSql.JSONSubcolumn{{ID: first, Path: "$.user.id"}, {ID: third, Path: "$.user.name"}}
	if got := registry.Snapshot(); !reflect.DeepEqual(got, expected) {
		t.Fatalf("Snapshot() = %#v, want %#v", got, expected)
	}
	if _, ok := registry.Path(0); ok {
		t.Fatal("zero ID unexpectedly resolved")
	}
	if _, ok := registry.Path(third + 1); ok {
		t.Fatal("unknown ID unexpectedly resolved")
	}
}

func TestJSONSubcolumnRegistryRejectsInvalidPathsAndSerializesInterning(t *testing.T) {
	registry := hatSql.NewJSONSubcolumnRegistry()
	for _, path := range []string{"", " ", "$.bad\x00path"} {
		if _, _, err := registry.Intern(path); !errors.Is(err, hatSql.ErrJSONSubcolumnPathInvalid) {
			t.Fatalf("Intern(%q) error = %v", path, err)
		}
	}
	var waitGroup sync.WaitGroup
	ids := make(chan uint32, 8)
	waitGroup.Add(8)
	for range 8 {
		go func() {
			defer waitGroup.Done()
			id, _, err := registry.Intern("$.shared")
			if err != nil {
				t.Errorf("Intern concurrent error = %v", err)
				return
			}
			ids <- id
		}()
	}
	waitGroup.Wait()
	close(ids)
	for id := range ids {
		if id == 0 {
			t.Fatal("concurrent Intern returned zero ID")
		}
	}
	if len(registry.Snapshot()) != 1 {
		t.Fatalf("concurrent snapshot = %#v, want one path", registry.Snapshot())
	}

	var nilRegistry *hatSql.JSONSubcolumnRegistry
	if id, created, err := nilRegistry.Intern("$.ignored"); err != nil || created || id != 0 {
		t.Fatalf("nil Intern() = id=%d created=%v err=%v", id, created, err)
	}
	if nilRegistry.Snapshot() != nil {
		t.Fatal("nil registry snapshot is not nil")
	}
}

func BenchmarkJSONSubcolumnRegistryInternExisting(b *testing.B) {
	registry := hatSql.NewJSONSubcolumnRegistry()
	if _, _, err := registry.Intern("$.user.id"); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, _, err := registry.Intern("$.user.id"); err != nil {
			b.Fatal(err)
		}
	}
}
