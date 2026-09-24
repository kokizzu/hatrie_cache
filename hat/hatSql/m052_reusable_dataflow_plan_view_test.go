package hatSql

import (
	"reflect"
	"testing"
)

func TestM052ReusableDataflowPlanViewIsReadOnlyAndComplete(t *testing.T) {
	query, err := CompileSQLQuery("FROM CACHE('items') SELECT id WHERE id >= 2 ORDER BY id LIMIT 4")
	if err != nil {
		t.Fatalf("CompileSQLQuery() error = %v", err)
	}
	view := query.DataflowPlanView()
	if !view.Valid() {
		t.Fatal("DataflowPlanView().Valid() = false, want true")
	}
	if view.Format() == "" || view.Source() == "" || view.FragmentCount() == 0 {
		t.Fatalf("view metadata = format %q source %q fragments %d", view.Format(), view.Source(), view.FragmentCount())
	}
	if view.Root() < 0 || view.Root() >= view.FragmentCount() {
		t.Fatalf("view.Root() = %d, fragment count = %d", view.Root(), view.FragmentCount())
	}
	fragment, ok := view.Fragment(0)
	if !ok || fragment.Kind() == "" || fragment.ID() != 0 {
		t.Fatalf("view.Fragment(0) = %#v/%v", fragment, ok)
	}
	if _, ok := view.Fragment(-1); ok {
		t.Fatal("view.Fragment(-1) reported a fragment")
	}
	if _, ok := view.Fragment(view.FragmentCount()); ok {
		t.Fatal("view.Fragment(count) reported a fragment")
	}
	for index := 0; index < fragment.InputCount(); index++ {
		if input, ok := fragment.Input(index); !ok || input < 0 || input >= view.FragmentCount() {
			t.Fatalf("fragment input %d = %d/%v", index, input, ok)
		}
	}

	copyPlan := query.LowerDataflow()
	copyPlan.Fragments[0].Kind = "MUTATED"
	got, ok := view.Fragment(0)
	if !ok || !got.Valid() || got.Kind() == "MUTATED" {
		t.Fatal("view shared mutable LowerDataflow storage")
	}
	if got := query.DataflowPlanView().FragmentCount(); got != view.FragmentCount() {
		t.Fatalf("repeated view fragment count = %d, want %d", got, view.FragmentCount())
	}
}

func TestM052ReusableDataflowPlanViewZeroValue(t *testing.T) {
	var view SQLDataflowPlanView
	if view.Valid() || view.FragmentCount() != 0 || view.Root() != -1 {
		t.Fatalf("zero view metadata = valid %v count %d root %d", view.Valid(), view.FragmentCount(), view.Root())
	}
	if _, ok := view.Fragment(0); ok {
		t.Fatal("zero view returned a fragment")
	}
	var query *CompiledSQLQuery
	if query.DataflowPlanView().Valid() {
		t.Fatal("nil query returned a valid view")
	}
	if got := view.Source(); !reflect.DeepEqual(got, "") {
		t.Fatalf("zero view source = %q, want empty", got)
	}
}
