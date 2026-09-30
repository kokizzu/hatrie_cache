package hatSql

import (
	"errors"
	"reflect"
	"testing"
)

func TestM235DependencyGraphReturnsOnlyAffectedViewsAndIndexes(t *testing.T) {
	graph := NewSQLDependencyGraph()
	if err := graph.Register(SQLMaintainedObjectView, "orders_view", "orders", "customers"); err != nil {
		t.Fatal(err)
	}
	if err := graph.Register(SQLMaintainedObjectIndex, "orders_by_email", "orders"); err != nil {
		t.Fatal(err)
	}
	if err := graph.Register(SQLMaintainedObjectView, "audit_view", "audit"); err != nil {
		t.Fatal(err)
	}

	got := graph.Affected([]string{"orders", "orders"})
	want := []SQLMaintainedObject{
		{Kind: SQLMaintainedObjectIndex, Name: "orders_by_email", Dependencies: []string{"orders"}},
		{Kind: SQLMaintainedObjectView, Name: "orders_view", Dependencies: []string{"customers", "orders"}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Affected() = %#v, want %#v", got, want)
	}

	got[0].Dependencies[0] = "mutated"
	if snapshot := graph.Snapshot(); snapshot[0].Dependencies[0] == "mutated" {
		t.Fatal("Affected() exposed graph-owned dependency storage")
	}
	if got := graph.Affected([]string{"unrelated"}); len(got) != 0 {
		t.Fatalf("unrelated Affected() = %#v, want empty", got)
	}
}

func TestM235DependencyGraphValidatesRegistrationAndSupportsReuse(t *testing.T) {
	graph := NewSQLDependencyGraph()
	for _, test := range []struct {
		name string
		kind SQLMaintainedObjectKind
		id   string
		deps []string
		want error
	}{
		{name: "kind required", id: "object", deps: []string{"source"}, want: ErrSQLDependencyGraphKindRequired},
		{name: "kind unsupported", kind: "table", id: "object", deps: []string{"source"}, want: ErrSQLDependencyGraphKindUnsupported},
		{name: "name required", kind: SQLMaintainedObjectView, deps: []string{"source"}, want: ErrSQLDependencyGraphNameRequired},
		{name: "dependency required", kind: SQLMaintainedObjectView, id: "object", want: ErrSQLDependencyGraphDependencyRequired},
		{name: "duplicate dependency", kind: SQLMaintainedObjectView, id: "object", deps: []string{"source", "source"}, want: ErrSQLDependencyGraphDuplicateDependency},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := graph.Register(test.kind, test.id, test.deps...); !errors.Is(err, test.want) {
				t.Fatalf("Register() error = %v, want %v", err, test.want)
			}
		})
	}

	if err := graph.Register(SQLMaintainedObjectView, "orders_view", "orders"); err != nil {
		t.Fatal(err)
	}
	if err := graph.Register(SQLMaintainedObjectView, "orders_view", "orders"); !errors.Is(err, ErrSQLDependencyGraphAlreadyRegistered) {
		t.Fatalf("duplicate Register() error = %v, want %v", err, ErrSQLDependencyGraphAlreadyRegistered)
	}
	buffer := make([]SQLMaintainedObject, 0, 4)
	buffer = graph.AffectedInto(buffer, []string{"orders"})
	if len(buffer) != 1 || buffer[0].Name != "orders_view" {
		t.Fatalf("AffectedInto() = %#v", buffer)
	}
	if err := graph.Unregister(SQLMaintainedObjectView, "orders_view"); err != nil {
		t.Fatal(err)
	}
	if err := graph.Unregister(SQLMaintainedObjectView, "orders_view"); !errors.Is(err, ErrSQLDependencyGraphNotFound) {
		t.Fatalf("duplicate Unregister() error = %v, want %v", err, ErrSQLDependencyGraphNotFound)
	}
	buffer = graph.AffectedInto(buffer, []string{"orders"})
	if len(buffer) != 0 {
		t.Fatalf("AffectedInto() after unregister = %#v, want empty", buffer)
	}
}
