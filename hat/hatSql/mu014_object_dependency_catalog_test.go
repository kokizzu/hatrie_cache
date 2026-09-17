package hatSql

import (
	"context"
	"errors"
	"testing"
)

func TestMU014CatalogResolverExposesVersionedObjectsAndDependencies(t *testing.T) {
	resolver := CatalogResolver{Catalog: Catalog{
		Version:    42,
		Namespaces: []string{"public"},
		Sources: []CatalogSource{{
			Namespace: "public",
			Name:      "people",
			Kind:      "CACHE",
			Fields:    []CatalogField{{Name: "id", Type: "INTEGER"}},
		}},
		Indexes: []CatalogIndex{{
			Namespace: "public",
			Source:    "people",
			Name:      "people_id",
			Kind:      "hash",
			Columns:   []string{"id"},
		}},
		Objects: []CatalogObject{{
			Namespace: "public",
			Name:      "people_view",
			Kind:      CatalogObjectKindView,
			Type:      "materialized",
			Version:   7,
			State:     "ready",
		}},
		Dependencies: []CatalogDependency{{
			Namespace:          "public",
			Object:             "people_view",
			ObjectKind:         CatalogObjectKindView,
			DependsOnNamespace: "public",
			DependsOn:          "people",
			DependsOnKind:      CatalogObjectKindSource,
		}},
	}}

	objects, err := ExecuteSQLQueryParameters(context.Background(), `FROM CACHE('information_schema.objects') SELECT catalog_version, namespace, name, kind, type, object_version, state ORDER BY kind, name`, resolver, nil, SQLQueryOptions{})
	if err != nil {
		t.Fatalf("objects query error = %v", err)
	}
	if len(objects.Rows) != 3 {
		t.Fatalf("objects rows = %#v, want source, index, and view", objects.Rows)
	}
	wantObjects := map[string]map[string]interface{}{
		"source:people":    {"catalog_version": uint64(42), "type": "CACHE", "object_version": uint64(42), "state": "ready"},
		"index:people_id":  {"catalog_version": uint64(42), "type": "hash", "object_version": uint64(42), "state": "ready"},
		"view:people_view": {"catalog_version": uint64(42), "type": "materialized", "object_version": uint64(7), "state": "ready"},
	}
	for _, row := range objects.Rows {
		key := row["kind"].(string) + ":" + row["name"].(string)
		want, ok := wantObjects[key]
		if !ok {
			t.Fatalf("unexpected object row = %#v", row)
		}
		for field, value := range want {
			if row[field] != value {
				t.Errorf("object %s field %s = %#v, want %#v", key, field, row[field], value)
			}
		}
	}

	dependencies, err := ExecuteSQLQueryParameters(context.Background(), `FROM CACHE('information_schema.dependencies') SELECT catalog_version, object, object_kind, depends_on, depends_on_kind, ordinal_position ORDER BY object`, resolver, nil, SQLQueryOptions{})
	if err != nil {
		t.Fatalf("dependencies query error = %v", err)
	}
	if len(dependencies.Rows) != 2 {
		t.Fatalf("dependencies rows = %#v, want derived index and explicit view edges", dependencies.Rows)
	}
	if dependencies.Rows[0]["object"] != "people_id" || dependencies.Rows[0]["object_kind"] != CatalogObjectKindIndex || dependencies.Rows[0]["depends_on"] != "people" {
		t.Errorf("first dependency = %#v, want people_id index -> people source", dependencies.Rows[0])
	}
	if dependencies.Rows[1]["object"] != "people_view" || dependencies.Rows[1]["object_kind"] != CatalogObjectKindView || dependencies.Rows[1]["depends_on"] != "people" {
		t.Errorf("second dependency = %#v, want people_view -> people source", dependencies.Rows[1])
	}
	if dependencies.Rows[0]["catalog_version"] != uint64(42) || dependencies.Rows[1]["catalog_version"] != uint64(42) {
		t.Errorf("dependency catalog versions = %#v, want 42", dependencies.Rows)
	}

	for _, shortcut := range []string{"SHOW OBJECTS", "SHOW DEPENDENCIES"} {
		compiled, err := CompileSQLShortcut(shortcut)
		if err != nil {
			t.Fatalf("CompileSQLShortcut(%q) error = %v", shortcut, err)
		}
		if compiled == shortcut || len(compiled) == 0 {
			t.Fatalf("CompileSQLShortcut(%q) = %q, want information_schema query", shortcut, compiled)
		}
		result, err := ExecuteSQLQueryParameters(context.Background(), shortcut, resolver, nil, SQLQueryOptions{})
		if err != nil || len(result.Rows) == 0 {
			t.Fatalf("execute %s = %#v, %v", shortcut, result, err)
		}
	}
}

func TestMU014CatalogDependencyClosureBoundsCycles(t *testing.T) {
	catalog := Catalog{
		Version: 1,
		Objects: []CatalogObject{
			{Namespace: "public", Name: "a", Kind: CatalogObjectKindView},
			{Namespace: "public", Name: "b", Kind: CatalogObjectKindView},
			{Namespace: "public", Name: "c", Kind: CatalogObjectKindView},
		},
		Dependencies: []CatalogDependency{
			{Namespace: "public", Object: "a", ObjectKind: CatalogObjectKindView, DependsOnNamespace: "public", DependsOn: "b", DependsOnKind: CatalogObjectKindView},
			{Namespace: "public", Object: "b", ObjectKind: CatalogObjectKindView, DependsOnNamespace: "public", DependsOn: "c", DependsOnKind: CatalogObjectKindView},
			{Namespace: "public", Object: "c", ObjectKind: CatalogObjectKindView, DependsOnNamespace: "public", DependsOn: "a", DependsOnKind: CatalogObjectKindView},
		},
	}

	paths, err := catalog.DependencyClosure(CatalogObjectRef{Namespace: "public", Name: "a", Kind: CatalogObjectKindView}, CatalogDependencyTraversalOptions{MaxDepth: 8, MaxRows: 8})
	if err != nil {
		t.Fatalf("DependencyClosure() error = %v", err)
	}
	if len(paths) != 3 || paths[0].Depth != 1 || paths[1].Depth != 2 || paths[2].Depth != 3 {
		t.Fatalf("DependencyClosure() = %#v, want three deterministic cycle edges at depths 1, 2, 3", paths)
	}
	if paths[2].DependsOn.Name != "a" {
		t.Fatalf("cycle closure final edge = %#v, want c -> a", paths[2])
	}

	if _, err := catalog.DependencyClosure(CatalogObjectRef{Namespace: "public", Name: "a", Kind: CatalogObjectKindView}, CatalogDependencyTraversalOptions{MaxRows: 2}); !errors.Is(err, ErrCatalogDependencyLimitExceeded) {
		t.Fatalf("row limit error = %v, want ErrCatalogDependencyLimitExceeded", err)
	}
	if _, err := catalog.DependencyClosure(CatalogObjectRef{Namespace: "public", Name: "a", Kind: CatalogObjectKindView}, CatalogDependencyTraversalOptions{MaxDepth: 1}); !errors.Is(err, ErrCatalogDependencyDepthExceeded) {
		t.Fatalf("depth limit error = %v, want ErrCatalogDependencyDepthExceeded", err)
	}
	if _, err := catalog.DependencyClosure(CatalogObjectRef{Namespace: "public", Name: "a", Kind: CatalogObjectKindView}, CatalogDependencyTraversalOptions{MaxRows: -1}); !errors.Is(err, ErrCatalogDependencyInvalid) {
		t.Fatalf("negative limit error = %v, want ErrCatalogDependencyInvalid", err)
	}
}

func TestMU014CatalogDependencyRowsDeduplicateDerivedIndexEdge(t *testing.T) {
	resolver := CatalogResolver{Catalog: Catalog{
		Sources: []CatalogSource{{Namespace: "public", Name: "people", Kind: "CACHE"}},
		Indexes: []CatalogIndex{{Namespace: "public", Source: "people", Name: "people_id", Kind: "hash"}},
		Dependencies: []CatalogDependency{{
			Namespace:          "public",
			Object:             "people_id",
			ObjectKind:         CatalogObjectKindIndex,
			DependsOnNamespace: "public",
			DependsOn:          "people",
			DependsOnKind:      CatalogObjectKindSource,
		}},
	}}
	result, err := ExecuteSQLQueryParameters(context.Background(), "FROM CACHE('information_schema.dependencies') SELECT object, depends_on", resolver, nil, SQLQueryOptions{})
	if err != nil {
		t.Fatalf("dependencies query error = %v", err)
	}
	if len(result.Rows) != 1 {
		t.Fatalf("deduplicated dependencies = %#v, want one edge", result.Rows)
	}
}

type mu014PartitionPruningSource struct {
	called bool
}

func (source *mu014PartitionPruningSource) ResolveSQLSource(string, string) ([]Row, error) {
	return nil, nil
}

func (source *mu014PartitionPruningSource) ResolveSQLSourcePartitionsForPredicate(string, string, SQLPartitionPredicate) ([]SQLSourcePartition, bool, error) {
	source.called = true
	return []SQLSourcePartition{{Name: "unexpected"}}, true, nil
}

func TestMU014CatalogResolverKeepsNewVirtualSourcesLocalToPredicatePruning(t *testing.T) {
	source := &mu014PartitionPruningSource{}
	resolver := CatalogResolver{Source: source}
	partitions, available, err := resolver.ResolveSQLSourcePartitionsForPredicate("CACHE", "information_schema.dependencies", SQLPartitionPredicate{})
	if err != nil {
		t.Fatalf("predicate pruning error = %v", err)
	}
	if available || partitions != nil || source.called {
		t.Fatalf("predicate pruning for virtual catalog source = %#v/%v, called=%v; want unavailable and local", partitions, available, source.called)
	}
}
