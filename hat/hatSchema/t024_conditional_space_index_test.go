package hatSchema

import (
	"errors"
	"fmt"
	"reflect"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestT024ConditionalFunctionalIndexLifecycleAndPlannerMetadata(t *testing.T) {
	source := NewMaterializedSource([]DerivedColumn{
		{Name: "id"},
		{Name: "name"},
		{Name: "active"},
	})
	for _, row := range []Row{
		{"id": int64(1), "name": "Ada", "active": true},
		{"id": int64(2), "name": "Ada", "active": false},
		{"id": int64(3), "name": "Grace", "active": true},
		{"id": int64(4), "name": "Ada", "active": true},
	} {
		if _, err := source.Insert(row); err != nil {
			t.Fatal(err)
		}
	}

	report, err := source.BuildConditionalFunctionalIndex(
		"active_name",
		[]string{"name", "active"},
		ConditionalFunctionalIndexOptions{
			Predicate: "active = true",
			Matches: func(row Row) (bool, error) {
				active, ok := row["active"].(bool)
				if !ok {
					return false, fmt.Errorf("active is not boolean")
				}
				return active, nil
			},
		},
		func(row Row) (interface{}, error) {
			return row["name"], nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if report.Name != "active_name" || report.Rows != 4 || report.IndexedRows != 3 || report.Attempts != 1 || report.Predicate != "active = true" {
		t.Fatalf("build report = %#v", report)
	}
	rows := source.Lookup("active_name", "Ada")
	if got, want := rowIDs(rows), []int64{1, 4}; !reflect.DeepEqual(got, want) {
		t.Fatalf("conditional lookup = %v, want %v", got, want)
	}

	if _, err := source.Insert(Row{"id": int64(5), "name": "Ada", "active": false}); err != nil {
		t.Fatal(err)
	}
	if _, err := source.Insert(Row{"id": int64(6), "name": "Ada", "active": true}); err != nil {
		t.Fatal(err)
	}
	if got, want := rowIDs(source.Lookup("active_name", "Ada")), []int64{1, 4, 6}; !reflect.DeepEqual(got, want) {
		t.Fatalf("maintained conditional lookup = %v, want %v", got, want)
	}

	metadata := source.ConditionalIndexMetadata()
	if got, want := metadata, []hatSql.SQLConditionalIndexMetadata{{
		Name:      "active_name",
		Fields:    []string{"name", "active"},
		Predicate: "active = true",
	}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("source metadata = %#v, want %#v", got, want)
	}
	metadata[0].Fields[0] = "mutated"
	if source.ConditionalIndexMetadata()[0].Fields[0] != "name" {
		t.Fatal("conditional metadata was not cloned")
	}

	adapter := SQLResolverAdapter{Sources: map[string]*MaterializedSource{"people": source}}
	published, available, err := adapter.SQLConditionalIndexMetadata("people")
	if err != nil || !available || !reflect.DeepEqual(published, source.ConditionalIndexMetadata()) {
		t.Fatalf("adapter metadata = %#v/%v/%v", published, available, err)
	}
	if _, available, err := adapter.SQLConditionalIndexMetadata("missing"); err != nil || available {
		t.Fatalf("missing metadata = %v/%v, want unavailable", err, available)
	}

	if !source.DropIndex("active_name") || source.DropIndex("active_name") || source.HasIndex("active_name") {
		t.Fatal("conditional index drop lifecycle failed")
	}
	if got := source.ConditionalIndexMetadata(); len(got) != 0 {
		t.Fatalf("metadata after drop = %#v", got)
	}
}

func TestT024ConditionalFunctionalIndexValidationAndAtomicFailure(t *testing.T) {
	source := NewMaterializedSource([]DerivedColumn{{Name: "id"}, {Name: "name"}, {Name: "active"}})
	if _, err := source.Insert(Row{"id": int64(1), "name": "Ada", "active": true}); err != nil {
		t.Fatal(err)
	}
	validOptions := ConditionalFunctionalIndexOptions{
		Predicate: "active = true",
		Matches:   func(Row) (bool, error) { return true, nil },
	}
	if _, err := source.BuildConditionalFunctionalIndex("active_name", []string{"name"}, validOptions, func(row Row) (interface{}, error) {
		return row["name"], nil
	}); err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name    string
		options ConditionalFunctionalIndexOptions
		want    error
	}{
		{name: "missing predicate", options: ConditionalFunctionalIndexOptions{Matches: validOptions.Matches}, want: ErrMaterializedSourceConditionalPredicateRequired},
		{name: "missing matcher", options: ConditionalFunctionalIndexOptions{Predicate: "active = true"}, want: ErrMaterializedSourceConditionalMatcherRequired},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			_, err := source.BuildConditionalFunctionalIndex("invalid_"+test.name, []string{"name"}, test.options, func(row Row) (interface{}, error) {
				return row["name"], nil
			})
			if !errors.Is(err, test.want) {
				t.Fatalf("error = %v, want %v", err, test.want)
			}
		})
	}
	evaluationErr := errors.New("evaluation failed")
	_, err := source.BuildConditionalFunctionalIndex("broken", []string{"name"}, validOptions, func(Row) (interface{}, error) {
		return nil, evaluationErr
	})
	if !errors.Is(err, evaluationErr) {
		t.Fatalf("failed evaluator error = %v, want %v", err, evaluationErr)
	}
	if source.HasIndex("broken") {
		t.Fatal("failed build was published")
	}
}

func TestT024ConditionalFunctionalIndexMatcherFailureIsAtomic(t *testing.T) {
	source := NewMaterializedSource([]DerivedColumn{{Name: "id"}, {Name: "active"}})
	if _, err := source.BuildConditionalFunctionalIndex(
		"active_id",
		[]string{"id", "active"},
		ConditionalFunctionalIndexOptions{
			Predicate: "active = true",
			Matches: func(row Row) (bool, error) {
				if row["id"] == int64(2) {
					return false, errors.New("matcher failed")
				}
				return row["active"] == true, nil
			},
		},
		func(row Row) (interface{}, error) { return row["id"], nil },
	); err != nil {
		t.Fatal(err)
	}
	if _, err := source.Insert(Row{"id": int64(1), "active": true}); err != nil {
		t.Fatal(err)
	}
	before := source.Rows()
	if _, err := source.Insert(Row{"id": int64(2), "active": true}); err == nil {
		t.Fatal("matcher failure was not returned")
	}
	after := source.Rows()
	if !reflect.DeepEqual(after, before) {
		t.Fatalf("failed conditional insert changed rows: before=%#v after=%#v", before, after)
	}
	if got := rowIDs(source.Lookup("active_id", int64(2))); len(got) != 0 {
		t.Fatalf("failed conditional insert published index row: %v", got)
	}
}

func TestT024SpaceCatalogConditionalIndexMetadata(t *testing.T) {
	catalog, err := NewSpaceCatalog(nil)
	if err != nil {
		t.Fatal(err)
	}
	definition := SpaceDefinition{
		Name: "orders",
		Source: Source{
			Name:    "orders",
			Columns: []Column{{Name: "id", Type: TypeText}, {Name: "active", Type: TypeBoolean}},
		},
		Indexes: []IndexDefinition{{
			Name:       "active_id",
			Kind:       IndexKindFunctional,
			Expression: "id",
			Predicate:  "active = true",
			Columns:    []string{"id", "active"},
		}},
	}
	if err := catalog.Upsert(definition); err != nil {
		t.Fatal(err)
	}
	stored, ok := catalog.Lookup("orders")
	if !ok || stored.Indexes[0].Predicate != "active = true" {
		t.Fatalf("conditional catalog definition = %#v/%v", stored, ok)
	}
	invalid := definition
	invalid.Name = "invalid"
	invalid.Source.Name = "invalid"
	invalid.Indexes = []IndexDefinition{{
		Name:      "hash_active",
		Kind:      IndexKindHash,
		Columns:   []string{"id"},
		Predicate: "active = true",
	}}
	if err := catalog.Upsert(invalid); !errors.Is(err, ErrSpaceCatalogInvalid) {
		t.Fatalf("non-functional conditional index error = %v", err)
	}
}

func rowIDs(rows []Row) []int64 {
	ids := make([]int64, 0, len(rows))
	for _, row := range rows {
		id, _ := row["id"].(int64)
		ids = append(ids, id)
	}
	return ids
}
