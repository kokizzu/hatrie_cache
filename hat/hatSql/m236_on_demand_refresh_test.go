package hatSql

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

func TestM236RefreshRegistryRunsOnlyAffectedMaintainedObjects(t *testing.T) {
	registry := NewSQLMaintainedRefreshRegistry()
	var calls []string
	register := func(kind SQLMaintainedObjectKind, name string, dependencies []string) {
		t.Helper()
		if err := registry.Register(kind, name, dependencies, func(_ context.Context, object SQLMaintainedObject) error {
			calls = append(calls, string(object.Kind)+":"+object.Name)
			if len(object.Dependencies) > 0 {
				object.Dependencies[0] = "mutated-by-callback"
			}
			return nil
		}); err != nil {
			t.Fatalf("Register(%q) error = %v", name, err)
		}
	}
	register(SQLMaintainedObjectView, "orders_view", []string{"orders"})
	register(SQLMaintainedObjectIndex, "orders_by_email", []string{"orders"})
	register(SQLMaintainedObjectView, "audit_view", []string{"audit"})

	buffer := make([]SQLMaintainedObject, 0, 3)
	refreshed, err := registry.RefreshChangedInto(context.Background(), buffer, []string{"orders", "orders"})
	if err != nil {
		t.Fatalf("RefreshChangedInto() error = %v", err)
	}
	got := make([]string, 0, len(refreshed))
	for _, object := range refreshed {
		got = append(got, string(object.Kind)+":"+object.Name)
	}
	if want := []string{"index:orders_by_email", "view:orders_view"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("refreshed = %#v, want %#v", got, want)
	}
	if want := []string{"index:orders_by_email", "view:orders_view"}; !reflect.DeepEqual(calls, want) {
		t.Fatalf("callbacks = %#v, want %#v", calls, want)
	}

	definitions := registry.Snapshot()
	ordersViewDependencies := []string(nil)
	for _, definition := range definitions {
		if definition.Name == "orders_view" {
			ordersViewDependencies = definition.Dependencies
		}
	}
	if len(definitions) != 3 || len(ordersViewDependencies) != 1 || ordersViewDependencies[0] != "orders" {
		t.Fatalf("Snapshot() = %#v, want callback mutations isolated", definitions)
	}
	calls = calls[:0]
	refreshed, err = registry.RefreshChanged(context.Background(), []string{"audit"})
	if err != nil {
		t.Fatalf("unrelated RefreshChanged() error = %v", err)
	}
	if len(refreshed) != 1 || refreshed[0].Name != "audit_view" || !reflect.DeepEqual(calls, []string{"view:audit_view"}) {
		t.Fatalf("audit refresh = %#v, callbacks = %#v", refreshed, calls)
	}
	if refreshed, err = registry.RefreshChanged(context.Background(), nil); err != nil || refreshed != nil {
		t.Fatalf("empty RefreshChanged() = %#v, %v; want nil, nil", refreshed, err)
	}
}

func TestM236RefreshRegistryValidatesCallbacksAndStopsOnError(t *testing.T) {
	registry := NewSQLMaintainedRefreshRegistry()
	if err := registry.Register(SQLMaintainedObjectView, "missing", []string{"source"}, nil); !errors.Is(err, ErrSQLMaintainedRefreshRequired) {
		t.Fatalf("nil callback error = %v, want %v", err, ErrSQLMaintainedRefreshRequired)
	}
	wantErr := errors.New("refresh failed")
	calls := 0
	if err := registry.Register(SQLMaintainedObjectIndex, "first", []string{"source"}, func(context.Context, SQLMaintainedObject) error {
		calls++
		return wantErr
	}); err != nil {
		t.Fatalf("first Register() error = %v", err)
	}
	if err := registry.Register(SQLMaintainedObjectView, "second", []string{"source"}, func(context.Context, SQLMaintainedObject) error {
		calls++
		return nil
	}); err != nil {
		t.Fatalf("second Register() error = %v", err)
	}
	if err := registry.Register(SQLMaintainedObjectView, "second", []string{"other"}, func(context.Context, SQLMaintainedObject) error {
		return nil
	}); !errors.Is(err, ErrSQLDependencyGraphAlreadyRegistered) {
		t.Fatalf("duplicate Register() error = %v, want %v", err, ErrSQLDependencyGraphAlreadyRegistered)
	}
	refreshed, err := registry.RefreshChanged(context.Background(), []string{"source"})
	if !errors.Is(err, wantErr) {
		t.Fatalf("RefreshChanged() error = %v, want %v", err, wantErr)
	}
	if len(refreshed) != 0 || calls != 1 {
		t.Fatalf("failed refresh = %#v, calls = %d; want no committed result and one callback", refreshed, calls)
	}
	if err := registry.Unregister(SQLMaintainedObjectIndex, "first"); err != nil {
		t.Fatalf("Unregister() error = %v", err)
	}
	if registry.Len() != 1 {
		t.Fatalf("Len() = %d, want 1", registry.Len())
	}
}
