package hatSql

import (
	"context"
	"testing"
)

var mu014CatalogObjectsSink []Row
var mu014CatalogDependenciesSink []CatalogDependencyPath

func BenchmarkMU014CatalogObjects(b *testing.B) {
	resolver := mu014CatalogBenchmarkResolver()
	query := "FROM CACHE('information_schema.objects') SELECT catalog_version, namespace, name, kind, type, object_version, state ORDER BY namespace, name, kind"

	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		result, err := ExecuteSQLQueryParameters(context.Background(), query, resolver, nil, SQLQueryOptions{})
		if err != nil {
			b.Fatal(err)
		}
		mu014CatalogObjectsSink = result.Rows
	}
}

func BenchmarkMU014CatalogDependencyClosure(b *testing.B) {
	catalog := mu014CatalogBenchmarkResolver().Catalog
	root := CatalogObjectRef{Namespace: "public", Name: "view_07", Kind: CatalogObjectKindView}

	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		paths, err := catalog.DependencyClosure(root, CatalogDependencyTraversalOptions{})
		if err != nil {
			b.Fatal(err)
		}
		mu014CatalogDependenciesSink = paths
	}
}

func mu014CatalogBenchmarkResolver() CatalogResolver {
	sources := make([]CatalogSource, 32)
	indexes := make([]CatalogIndex, 32)
	objects := make([]CatalogObject, 16)
	dependencies := make([]CatalogDependency, 16)
	for index := range sources {
		name := "source_" + string(rune('a'+index))
		sources[index] = CatalogSource{Namespace: "public", Name: name, Kind: "CACHE", Fields: []CatalogField{{Name: "id", Type: "INTEGER"}}}
		indexes[index] = CatalogIndex{Namespace: "public", Source: name, Name: name + "_id", Kind: "hash", Columns: []string{"id"}}
	}
	for index := range objects {
		name := "view_" + string(rune('a'+index))
		objects[index] = CatalogObject{Namespace: "public", Name: name, Kind: CatalogObjectKindView, Type: "materialized", Version: uint64(index + 1)}
		dependencies[index] = CatalogDependency{Namespace: "public", Object: name, ObjectKind: CatalogObjectKindView, DependsOnNamespace: "public", DependsOn: sources[index].Name, DependsOnKind: CatalogObjectKindSource}
	}
	return CatalogResolver{Catalog: Catalog{Version: 9, Sources: sources, Indexes: indexes, Objects: objects, Dependencies: dependencies}}
}
