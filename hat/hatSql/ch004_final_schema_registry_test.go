package hatSql

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
)

func TestCH004FinalSchemaRegistryResolvesReplacingDefinition(t *testing.T) {
	registry, err := NewSQLFinalSchemaRegistry(SQLFinalSchemaRegistryOptions{})
	if err != nil {
		t.Fatalf("new registry: %v", err)
	}
	if err := registry.Upsert("cache", "events", SQLFinalSchemaDefinition{
		Mode:         SQLFinalReplacing,
		KeyFields:    []string{"tenant_id", "id"},
		VersionField: "version",
	}); err != nil {
		t.Fatalf("upsert replacing definition: %v", err)
	}

	options, configured, err := registry.Resolve("cache", "events")
	if err != nil {
		t.Fatalf("resolve replacing definition: %v", err)
	}
	if !configured {
		t.Fatal("replacing definition was not configured")
	}
	rows := []SQLRow{
		{"tenant_id": "acme", "id": "1", "version": uint64(1), "value": "old"},
		{"tenant_id": "acme", "id": "1", "version": uint64(2), "value": "new"},
		{"tenant_id": "other", "id": "1", "version": uint64(1), "value": "other"},
	}
	got, err := applySQLFinalOptions(options, rows)
	if err != nil {
		t.Fatalf("apply replacing definition: %v", err)
	}
	want := []SQLRow{
		{"tenant_id": "acme", "id": "1", "version": uint64(2), "value": "new"},
		{"tenant_id": "other", "id": "1", "version": uint64(1), "value": "other"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("replacing rows = %#v, want %#v", got, want)
	}
}

func TestCH004FinalSchemaRegistryResolvesCollapsingDefinition(t *testing.T) {
	registry, err := NewSQLFinalSchemaRegistry(SQLFinalSchemaRegistryOptions{})
	if err != nil {
		t.Fatalf("new registry: %v", err)
	}
	if err := registry.Upsert("cache", "changes", SQLFinalSchemaDefinition{
		Mode:      SQLFinalCollapsing,
		KeyFields: []string{"id"},
		SignField: "sign",
	}); err != nil {
		t.Fatalf("upsert collapsing definition: %v", err)
	}

	options, configured, err := registry.Resolver().Resolve("cache", "changes")
	if err != nil {
		t.Fatalf("resolve collapsing definition: %v", err)
	}
	if !configured {
		t.Fatal("collapsing definition was not configured")
	}
	rows := []SQLRow{
		{"id": "1", "sign": int64(1), "value": "created"},
		{"id": "1", "sign": int64(-1), "value": "deleted"},
		{"id": "2", "sign": int64(1), "value": "kept"},
	}
	got, err := applySQLFinalOptions(options, rows)
	if err != nil {
		t.Fatalf("apply collapsing definition: %v", err)
	}
	want := []SQLRow{{"id": "2", "sign": int64(1), "value": "kept"}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("collapsing rows = %#v, want %#v", got, want)
	}
}

func TestCH004FinalSchemaRegistrySeparatesCompositeKeyValues(t *testing.T) {
	registry, err := NewSQLFinalSchemaRegistry(SQLFinalSchemaRegistryOptions{})
	if err != nil {
		t.Fatalf("new registry: %v", err)
	}
	if err := registry.Upsert("cache", "events", SQLFinalSchemaDefinition{
		Mode:         SQLFinalReplacing,
		KeyFields:    []string{"left", "right"},
		VersionField: "version",
	}); err != nil {
		t.Fatalf("upsert definition: %v", err)
	}
	options, configured, err := registry.Resolve("cache", "events")
	if err != nil || !configured {
		t.Fatalf("resolve definition = %#v, configured=%v", err, configured)
	}
	rows := []SQLRow{
		{"left": "a", "right": "bc", "version": uint64(1)},
		{"left": "ab", "right": "c", "version": uint64(1)},
		{"left": "a", "right": "bc", "version": uint64(2)},
	}
	got, err := applySQLFinalOptions(options, rows)
	if err != nil {
		t.Fatalf("apply definition: %v", err)
	}
	want := []SQLRow{
		{"left": "a", "right": "bc", "version": uint64(2)},
		{"left": "ab", "right": "c", "version": uint64(1)},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("composite-key rows = %#v, want %#v", got, want)
	}
}

func TestCH004FinalSchemaRegistrySeparatesMixedSingleKeyValues(t *testing.T) {
	registry, err := NewSQLFinalSchemaRegistry(SQLFinalSchemaRegistryOptions{})
	if err != nil {
		t.Fatalf("new registry: %v", err)
	}
	if err := registry.Upsert("cache", "events", SQLFinalSchemaDefinition{
		Mode:         SQLFinalReplacing,
		KeyFields:    []string{"id"},
		VersionField: "version",
	}); err != nil {
		t.Fatalf("upsert definition: %v", err)
	}
	options, configured, err := registry.Resolve("cache", "events")
	if err != nil || !configured {
		t.Fatalf("resolve definition = %#v, configured=%v", err, configured)
	}
	rows := []SQLRow{
		{"id": "i1;", "version": uint64(1)},
		{"id": int64(1), "version": uint64(1)},
		{"id": "\x00i1;", "version": uint64(1)},
	}
	got, err := applySQLFinalOptions(options, rows)
	if err != nil {
		t.Fatalf("apply definition: %v", err)
	}
	if len(got) != len(rows) {
		t.Fatalf("mixed-key rows = %#v, want %d distinct rows", got, len(rows))
	}
}

func TestCH004FinalSchemaRegistryValidatesDefinitionsAndLimit(t *testing.T) {
	if _, err := NewSQLFinalSchemaRegistry(SQLFinalSchemaRegistryOptions{MaxDefinitions: -1}); !errors.Is(err, ErrSQLFinalSchemaRegistryOptionsInvalid) {
		t.Fatalf("negative max definitions error = %v, want %v", err, ErrSQLFinalSchemaRegistryOptionsInvalid)
	}
	registry, err := NewSQLFinalSchemaRegistry(SQLFinalSchemaRegistryOptions{MaxDefinitions: 1})
	if err != nil {
		t.Fatalf("new limited registry: %v", err)
	}
	cases := []struct {
		name       string
		definition SQLFinalSchemaDefinition
	}{
		{name: "unknown mode", definition: SQLFinalSchemaDefinition{Mode: SQLFinalMode(99), KeyFields: []string{"id"}, VersionField: "version"}},
		{name: "missing key", definition: SQLFinalSchemaDefinition{Mode: SQLFinalReplacing, VersionField: "version"}},
		{name: "replacing sign", definition: SQLFinalSchemaDefinition{Mode: SQLFinalReplacing, KeyFields: []string{"id"}, VersionField: "version", SignField: "sign"}},
		{name: "collapsing version", definition: SQLFinalSchemaDefinition{Mode: SQLFinalCollapsing, KeyFields: []string{"id"}, VersionField: "version", SignField: "sign"}},
		{name: "duplicate field", definition: SQLFinalSchemaDefinition{Mode: SQLFinalReplacing, KeyFields: []string{"id", "id"}, VersionField: "version"}},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			if err := registry.Upsert("cache", test.name, test.definition); !errors.Is(err, ErrSQLFinalSchemaDefinitionInvalid) {
				t.Fatalf("upsert error = %v, want %v", err, ErrSQLFinalSchemaDefinitionInvalid)
			}
		})
	}
	valid := SQLFinalSchemaDefinition{Mode: SQLFinalReplacing, KeyFields: []string{"id"}, VersionField: "version"}
	if err := registry.Upsert("cache", "first", valid); err != nil {
		t.Fatalf("upsert first definition: %v", err)
	}
	if err := registry.Upsert("cache", "second", valid); !errors.Is(err, ErrSQLFinalSchemaRegistryFull) {
		t.Fatalf("second definition error = %v, want %v", err, ErrSQLFinalSchemaRegistryFull)
	}
	if err := registry.Upsert("cache", "first", valid); err != nil {
		t.Fatalf("updating existing definition: %v", err)
	}
}

func TestCH004FinalSchemaRegistryCopiesDefinitionsAndSupportsDelete(t *testing.T) {
	registry, err := NewSQLFinalSchemaRegistry(SQLFinalSchemaRegistryOptions{})
	if err != nil {
		t.Fatalf("new registry: %v", err)
	}
	definition := SQLFinalSchemaDefinition{
		Mode:         SQLFinalReplacing,
		KeyFields:    []string{"id"},
		VersionField: "version",
	}
	if err := registry.Upsert("cache", "events", definition); err != nil {
		t.Fatalf("upsert definition: %v", err)
	}
	definition.KeyFields[0] = "mutated"
	snapshot := registry.Snapshot()
	if len(snapshot) != 1 || !reflect.DeepEqual(snapshot[0].Definition.KeyFields, []string{"id"}) {
		t.Fatalf("snapshot = %#v, want original key field", snapshot)
	}
	snapshot[0].Definition.KeyFields[0] = "snapshot-mutated"
	snapshotAgain := registry.Snapshot()
	if len(snapshotAgain) != 1 || !reflect.DeepEqual(snapshotAgain[0].Definition.KeyFields, []string{"id"}) {
		t.Fatalf("snapshot isolation failed: %#v", snapshotAgain)
	}
	if !registry.Delete("cache", "events") {
		t.Fatal("delete did not report an existing definition")
	}
	if registry.Delete("cache", "events") {
		t.Fatal("delete reported a missing definition")
	}
	if _, configured, err := registry.Resolve("cache", "events"); err != nil || configured {
		t.Fatalf("deleted definition resolve = err %v, configured %v", err, configured)
	}
}

func TestCH004FinalSchemaRegistryReturnsRowValueErrors(t *testing.T) {
	registry, err := NewSQLFinalSchemaRegistry(SQLFinalSchemaRegistryOptions{})
	if err != nil {
		t.Fatalf("new registry: %v", err)
	}
	if err := registry.Upsert("cache", "events", SQLFinalSchemaDefinition{
		Mode:         SQLFinalReplacing,
		KeyFields:    []string{"id"},
		VersionField: "version",
	}); err != nil {
		t.Fatalf("upsert definition: %v", err)
	}
	options, _, err := registry.Resolve("cache", "events")
	if err != nil {
		t.Fatalf("resolve definition: %v", err)
	}
	if _, err := applySQLFinalOptions(options, []SQLRow{{"id": "1", "version": "not-a-version"}}); !errors.Is(err, ErrSQLFinalSchemaRowValue) {
		t.Fatalf("invalid version error = %v, want %v", err, ErrSQLFinalSchemaRowValue)
	}
}

func TestCH004FinalSchemaRegistryIntegratesWithFinalQuery(t *testing.T) {
	registry, err := NewSQLFinalSchemaRegistry(SQLFinalSchemaRegistryOptions{})
	if err != nil {
		t.Fatalf("new registry: %v", err)
	}
	if err := registry.Upsert("cache", "events", SQLFinalSchemaDefinition{
		Mode:         SQLFinalReplacing,
		KeyFields:    []string{"id"},
		VersionField: "version",
	}); err != nil {
		t.Fatalf("upsert definition: %v", err)
	}
	rows := []Row{
		{"id": "1", "version": uint64(1), "value": "old"},
		{"id": "1", "version": uint64(2), "value": "new"},
	}
	resolver := SourceResolverFunc(func(_, _ string) ([]Row, error) { return rows, nil })
	result, err := ExecuteSQLQueryContext(
		context.Background(),
		"FROM CACHE('events') AS event FINAL SELECT event.id, event.value",
		resolver,
		SQLQueryOptions{FinalSourceOptions: registry.Resolver()},
	)
	if err != nil {
		t.Fatalf("FINAL query: %v", err)
	}
	want := []SQLRow{{"id": "1", "value": "new"}}
	if !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("FINAL query rows = %#v, want %#v", result.Rows, want)
	}
}

func TestCH004FinalSchemaRegistryConcurrentAccess(t *testing.T) {
	registry, err := NewSQLFinalSchemaRegistry(SQLFinalSchemaRegistryOptions{MaxDefinitions: 2})
	if err != nil {
		t.Fatalf("new registry: %v", err)
	}
	definition := SQLFinalSchemaDefinition{
		Mode:         SQLFinalReplacing,
		KeyFields:    []string{"id"},
		VersionField: "version",
	}
	if err := registry.Upsert("cache", "events", definition); err != nil {
		t.Fatalf("upsert definition: %v", err)
	}
	failures := make(chan error, 1)
	reportFailure := func(err error) {
		if err != nil {
			select {
			case failures <- err:
			default:
			}
		}
	}
	var waitGroup sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			for iteration := 0; iteration < 100; iteration++ {
				_, configured, err := registry.Resolve("CACHE", "events")
				if err != nil {
					reportFailure(err)
					return
				}
				if !configured {
					reportFailure(errors.New("registry definition was not configured"))
					return
				}
				if len(registry.Snapshot()) != 1 {
					reportFailure(errors.New("unexpected registry snapshot size"))
					return
				}
			}
		}()
	}
	for worker := 0; worker < 2; worker++ {
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			for iteration := 0; iteration < 100; iteration++ {
				reportFailure(registry.Upsert("cache", "events", definition))
			}
		}()
	}
	waitGroup.Wait()
	select {
	case err := <-failures:
		t.Fatal(err)
	default:
	}
}
