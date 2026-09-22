package hatSql

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
)

func TestCH004FinalSchemaRegistryResolvesConfiguredSource(t *testing.T) {
	rows := []SQLRow{
		{"id": "a", "version": uint64(1), "value": "old"},
		{"id": "a", "version": uint64(2), "value": "new"},
		{"id": "b", "version": uint64(1), "value": "b"},
	}
	resolver := SourceResolverFunc(func(_, _ string) ([]Row, error) {
		return rows, nil
	})
	registry := NewSQLFinalSchemaRegistry()
	if err := registry.Register("CACHE", "events", SQLFinalOptions{
		Mode: SQLFinalReplacing,
		Key: func(row SQLRow) string {
			return row["id"].(string)
		},
		Version: func(row SQLRow) (uint64, error) {
			return row["version"].(uint64), nil
		},
	}); err != nil {
		t.Fatalf("Register() error = %v", err)
	}

	result, err := ExecuteSQLQueryContext(context.Background(),
		"FROM CACHE('events') AS event FINAL SELECT event.id, event.value ORDER BY event.id",
		resolver, SQLQueryOptions{FinalSchema: registry})
	if err != nil {
		t.Fatalf("schema-bound FINAL query: %v", err)
	}
	want := []SQLRow{
		{"id": "a", "value": "new"},
		{"id": "b", "value": "b"},
	}
	if !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("schema-bound FINAL rows = %#v, want %#v", result.Rows, want)
	}
}

func TestCH004FinalSchemaRegistryPreservesExplicitResolverPrecedence(t *testing.T) {
	registry := NewSQLFinalSchemaRegistry()
	if err := registry.Register("CACHE", "events", SQLFinalOptions{
		Mode: SQLFinalReplacing,
		Key:  func(SQLRow) string { return "schema" },
	}); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	wantErr := errors.New("explicit resolver error")
	resolver := SourceResolverFunc(func(_, _ string) ([]Row, error) {
		return []Row{{"id": "a"}}, nil
	})
	_, err := ExecuteSQLQueryContext(context.Background(),
		"FROM CACHE('events') AS event FINAL SELECT event.id",
		resolver, SQLQueryOptions{
			FinalSchema: registry,
			FinalSourceOptions: ch004FinalSourceOptionsResolver(func(string, string) (SQLFinalOptions, bool, error) {
				return SQLFinalOptions{}, false, wantErr
			}),
		})
	if !errors.Is(err, wantErr) {
		t.Fatalf("explicit resolver error = %v, want %v", err, wantErr)
	}
}

func TestCH004FinalSchemaRegistryRejectsInvalidAndSupportsRemoval(t *testing.T) {
	registry := NewSQLFinalSchemaRegistry()
	if err := registry.Register("CACHE", "events", SQLFinalOptions{Mode: SQLFinalReplacing}); !errors.Is(err, ErrSQLFinalOptionsInvalid) {
		t.Fatalf("invalid Register() error = %v, want %v", err, ErrSQLFinalOptionsInvalid)
	}
	options := SQLFinalOptions{
		Mode: SQLFinalCollapsing,
		Key:  func(SQLRow) string { return "a" },
		Sign: func(SQLRow) (int, error) { return 1, nil },
	}
	if err := registry.Register("CACHE", "events", options); err != nil {
		t.Fatalf("valid Register() error = %v", err)
	}
	if _, configured, err := registry.Resolve("CACHE", "events"); err != nil || !configured {
		t.Fatalf("Resolve() = configured=%v error=%v, want configured", configured, err)
	}
	if !registry.Unregister("CACHE", "events") {
		t.Fatal("Unregister() = false, want true")
	}
	if registry.Unregister("CACHE", "events") {
		t.Fatal("second Unregister() = true, want false")
	}
	if _, configured, err := registry.Resolve("CACHE", "events"); err != nil || configured {
		t.Fatalf("Resolve() after removal = configured=%v error=%v, want missing", configured, err)
	}
}

func TestCH004FinalSchemaRegistryConcurrentAccess(t *testing.T) {
	registry := NewSQLFinalSchemaRegistry()
	options := SQLFinalOptions{
		Mode: SQLFinalReplacing,
		Key:  func(SQLRow) string { return "a" },
	}
	var wait sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		wait.Add(1)
		go func(worker int) {
			defer wait.Done()
			for iteration := 0; iteration < 200; iteration++ {
				if worker&1 == 0 {
					if err := registry.Register("cache", "events", options); err != nil {
						t.Errorf("Register() error = %v", err)
					}
				} else {
					if _, _, err := registry.Resolve("CACHE", "events"); err != nil {
						t.Errorf("Resolve() error = %v", err)
					}
				}
			}
		}(worker)
	}
	wait.Wait()
}
