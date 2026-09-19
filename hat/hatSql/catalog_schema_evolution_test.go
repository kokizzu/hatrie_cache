package hatSql_test

import (
	"errors"
	"reflect"
	"sync"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestCatalogSchemaEvolutionAllowsNullableAddAndProjectsRows(t *testing.T) {
	before := catalogSchemaEvolutionFixture([]hatSql.CatalogField{
		{Name: "id", Type: "int64"},
		{Name: "total", Type: "number", Nullable: true},
	})
	after := catalogSchemaEvolutionFixture([]hatSql.CatalogField{
		{Name: "id", Type: "int64"},
		{Name: "total", Type: "number", Nullable: true},
		{Name: "currency", Type: "string", Nullable: true},
	})

	plan, err := hatSql.PlanCatalogSchemaEvolution(before, after, hatSql.CatalogSchemaEvolutionOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Changes) != 1 || plan.Changes[0].Kind != hatSql.CatalogSchemaFieldAdded {
		t.Fatalf("changes = %#v, want one field-add change", plan.Changes)
	}
	rows := []hatSql.Row{{"id": int64(1), "total": 2.5}}
	projected, err := plan.ProjectRows("default", "orders", rows)
	if err != nil {
		t.Fatal(err)
	}
	want := []hatSql.Row{{"id": int64(1), "total": 2.5, "currency": nil}}
	if !reflect.DeepEqual(projected, want) {
		t.Fatalf("projected rows = %#v, want %#v", projected, want)
	}
	if !reflect.DeepEqual(rows, []hatSql.Row{{"id": int64(1), "total": 2.5}}) {
		t.Fatalf("input rows were mutated: %#v", rows)
	}
}

func TestCatalogSchemaEvolutionRejectsBreakingChangesAndDependencies(t *testing.T) {
	before := catalogSchemaEvolutionFixture([]hatSql.CatalogField{
		{Name: "id", Type: "int64"},
		{Name: "total", Type: "number", Nullable: true},
	})
	removedIndexed := catalogSchemaEvolutionFixture([]hatSql.CatalogField{
		{Name: "total", Type: "number", Nullable: true},
	})
	if _, err := hatSql.PlanCatalogSchemaEvolution(before, removedIndexed, hatSql.CatalogSchemaEvolutionOptions{AllowFieldRemoval: true}); !errors.Is(err, hatSql.ErrCatalogSchemaDependency) {
		t.Fatalf("indexed field removal error = %v, want dependency error", err)
	}

	removedUnindexed := catalogSchemaEvolutionFixture([]hatSql.CatalogField{{Name: "id", Type: "int64"}})
	if _, err := hatSql.PlanCatalogSchemaEvolution(before, removedUnindexed, hatSql.CatalogSchemaEvolutionOptions{}); !errors.Is(err, hatSql.ErrCatalogSchemaIncompatible) {
		t.Fatalf("field removal error = %v, want incompatible error", err)
	}

	requiredAdd := catalogSchemaEvolutionFixture([]hatSql.CatalogField{
		{Name: "id", Type: "int64"},
		{Name: "total", Type: "number", Nullable: true},
		{Name: "region", Type: "string"},
	})
	if _, err := hatSql.PlanCatalogSchemaEvolution(before, requiredAdd, hatSql.CatalogSchemaEvolutionOptions{}); !errors.Is(err, hatSql.ErrCatalogSchemaIncompatible) {
		t.Fatalf("required field addition error = %v, want incompatible error", err)
	}

	badIndex := catalogSchemaEvolutionFixture([]hatSql.CatalogField{
		{Name: "id", Type: "int64"},
		{Name: "total", Type: "number", Nullable: true},
	})
	badIndex.Indexes[0].Columns = []string{"missing"}
	if _, err := hatSql.PlanCatalogSchemaEvolution(before, badIndex, hatSql.CatalogSchemaEvolutionOptions{}); !errors.Is(err, hatSql.ErrCatalogSchemaInvalid) {
		t.Fatalf("invalid index error = %v, want invalid schema error", err)
	}
}

func TestCatalogSchemaEvolutionNumericWideningAndDeterminism(t *testing.T) {
	before := catalogSchemaEvolutionFixture([]hatSql.CatalogField{{Name: "id", Type: "int64"}})
	after := catalogSchemaEvolutionFixture([]hatSql.CatalogField{{Name: "id", Type: "number"}})
	if _, err := hatSql.PlanCatalogSchemaEvolution(before, after, hatSql.CatalogSchemaEvolutionOptions{}); !errors.Is(err, hatSql.ErrCatalogSchemaIncompatible) {
		t.Fatalf("numeric change without option = %v, want incompatible error", err)
	}
	plan, err := hatSql.PlanCatalogSchemaEvolution(before, after, hatSql.CatalogSchemaEvolutionOptions{AllowNumericWidening: true})
	if err != nil {
		t.Fatal(err)
	}
	projected, err := plan.ProjectRows("default", "orders", []hatSql.Row{{"id": int64(7)}})
	if err != nil {
		t.Fatal(err)
	}
	if want := []hatSql.Row{{"id": float64(7)}}; !reflect.DeepEqual(projected, want) {
		t.Fatalf("widened rows = %#v, want %#v", projected, want)
	}
	if _, err := plan.ProjectRows("default", "orders", []hatSql.Row{{"id": int64(1<<53 + 1)}}); !errors.Is(err, hatSql.ErrCatalogSchemaValueIncompatible) {
		t.Fatalf("lossy numeric widening error = %v, want value incompatibility", err)
	}

	permutedBefore := catalogSchemaEvolutionFixture([]hatSql.CatalogField{{Name: "id", Type: "int64"}})
	permutedBefore.Sources[0].Fields = append(permutedBefore.Sources[0].Fields, hatSql.CatalogField{Name: "amount", Type: "number", Nullable: true})
	permutedAfter := catalogSchemaEvolutionFixture([]hatSql.CatalogField{{Name: "amount", Type: "number", Nullable: true}, {Name: "id", Type: "int64"}, {Name: "region", Type: "string", Nullable: true}})
	permutedBefore.Sources[0].Fields = []hatSql.CatalogField{{Name: "amount", Type: "number", Nullable: true}, {Name: "id", Type: "int64"}}
	first, err := hatSql.PlanCatalogSchemaEvolution(permutedBefore, permutedAfter, hatSql.CatalogSchemaEvolutionOptions{})
	if err != nil {
		t.Fatal(err)
	}
	permutedBefore.Sources[0].Fields[0], permutedBefore.Sources[0].Fields[1] = permutedBefore.Sources[0].Fields[1], permutedBefore.Sources[0].Fields[0]
	second, err := hatSql.PlanCatalogSchemaEvolution(permutedBefore, permutedAfter, hatSql.CatalogSchemaEvolutionOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if first.FromFingerprint != second.FromFingerprint || first.ToFingerprint != second.ToFingerprint || !reflect.DeepEqual(first.Changes, second.Changes) {
		t.Fatalf("permuted plans differ: first=%#v second=%#v", first, second)
	}
}

func TestCatalogSchemaRegistryReplaceAndRollbackIsAtomic(t *testing.T) {
	before := catalogSchemaEvolutionFixture([]hatSql.CatalogField{{Name: "id", Type: "int64"}})
	after := catalogSchemaEvolutionFixture([]hatSql.CatalogField{{Name: "id", Type: "int64"}, {Name: "region", Type: "string", Nullable: true}})
	registry, err := hatSql.NewCatalogSchemaRegistry(before)
	if err != nil {
		t.Fatal(err)
	}
	initial, generation := registry.Snapshot()
	if generation != 1 || !reflect.DeepEqual(initial, before) {
		t.Fatalf("initial snapshot = %#v/%d", initial, generation)
	}
	plan, nextGeneration, err := registry.Replace(after, hatSql.CatalogSchemaEvolutionOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if nextGeneration != 2 || len(plan.Changes) != 1 {
		t.Fatalf("replace result = %#v/%d", plan, nextGeneration)
	}
	if _, _, err := registry.Replace(before, hatSql.CatalogSchemaEvolutionOptions{}); !errors.Is(err, hatSql.ErrCatalogSchemaIncompatible) {
		t.Fatalf("breaking replace error = %v, want incompatible error", err)
	}
	unchanged, unchangedGeneration := registry.Snapshot()
	if unchangedGeneration != 2 || !reflect.DeepEqual(unchanged, after) {
		t.Fatalf("failed replace mutated registry = %#v/%d", unchanged, unchangedGeneration)
	}
	if _, err := registry.Rollback(1, before, hatSql.CatalogSchemaEvolutionOptions{AllowFieldRemoval: true}); !errors.Is(err, hatSql.ErrCatalogSchemaGenerationConflict) {
		t.Fatalf("stale rollback error = %v, want generation conflict", err)
	}
	rolledBackGeneration, err := registry.Rollback(2, before, hatSql.CatalogSchemaEvolutionOptions{AllowFieldRemoval: true})
	if err != nil || rolledBackGeneration != 3 {
		t.Fatalf("rollback result = %d/%v", rolledBackGeneration, err)
	}
	rolledBack, finalGeneration := registry.Snapshot()
	if finalGeneration != 3 || !reflect.DeepEqual(rolledBack, before) {
		t.Fatalf("rolled back snapshot = %#v/%d", rolledBack, finalGeneration)
	}
}

func TestCatalogSchemaRegistryConcurrentSnapshots(t *testing.T) {
	before := catalogSchemaEvolutionFixture([]hatSql.CatalogField{{Name: "id", Type: "int64"}})
	after := catalogSchemaEvolutionFixture([]hatSql.CatalogField{
		{Name: "id", Type: "int64"},
		{Name: "region", Type: "string", Nullable: true},
	})
	registry, err := hatSql.NewCatalogSchemaRegistry(before)
	if err != nil {
		t.Fatal(err)
	}

	var group sync.WaitGroup
	for reader := 0; reader < 8; reader++ {
		group.Add(1)
		go func() {
			defer group.Done()
			for iteration := 0; iteration < 100; iteration++ {
				catalog, generation := registry.Snapshot()
				if generation == 0 || len(catalog.Sources) != 1 || len(catalog.Sources[0].Fields) < 1 {
					t.Errorf("invalid concurrent snapshot = %#v/%d", catalog, generation)
				}
			}
		}()
	}
	group.Add(1)
	go func() {
		defer group.Done()
		for iteration := 0; iteration < 100; iteration++ {
			target := before
			options := hatSql.CatalogSchemaEvolutionOptions{AllowFieldRemoval: true}
			if iteration%2 == 0 {
				target = after
				options = hatSql.CatalogSchemaEvolutionOptions{}
			}
			if _, _, err := registry.Replace(target, options); err != nil {
				t.Errorf("concurrent replace: %v", err)
				return
			}
		}
	}()
	group.Wait()
}

func catalogSchemaEvolutionFixture(fields []hatSql.CatalogField) hatSql.Catalog {
	return hatSql.Catalog{
		Namespaces: []string{"default"},
		Sources:    []hatSql.CatalogSource{{Namespace: "default", Name: "orders", Kind: "table", Fields: fields}},
		Indexes:    []hatSql.CatalogIndex{{Namespace: "default", Source: "orders", Name: "orders_id", Kind: "primary", Columns: []string{"id"}}},
	}
}
