package hatSchema

import (
	"errors"
	"reflect"
	"testing"
)

func TestTT027ValidatedGeneratedColumnsEvaluateDependencyOrder(t *testing.T) {
	source, err := NewValidatedMaterializedSource([]DerivedColumn{
		{Name: "base"},
		{
			Name:                  "quadrupled",
			GeneratedDependencies: []string{"doubled"},
			Generated: func(row Row) (interface{}, error) {
				value, ok := row["doubled"].(int64)
				if !ok {
					return nil, errors.New("doubled is not int64")
				}
				return value * 2, nil
			},
		},
		{
			Name:                  "doubled",
			GeneratedDependencies: []string{"base"},
			Generated: func(row Row) (interface{}, error) {
				value, ok := row["base"].(int64)
				if !ok {
					return nil, errors.New("base is not int64")
				}
				return value * 2, nil
			},
		},
	})
	if err != nil {
		t.Fatalf("NewValidatedMaterializedSource() error = %v", err)
	}
	row, err := source.Insert(Row{"base": int64(3)})
	if err != nil {
		t.Fatalf("Insert() error = %v", err)
	}
	want := Row{"base": int64(3), "doubled": int64(6), "quadrupled": int64(12)}
	if !reflect.DeepEqual(row, want) {
		t.Fatalf("Insert() row = %#v, want %#v", row, want)
	}
}

func TestTT027ValidatedGeneratedColumnsRejectInvalidDependenciesAtomically(t *testing.T) {
	tests := []struct {
		name    string
		columns []DerivedColumn
	}{
		{
			name:    "unknown",
			columns: []DerivedColumn{{Name: "value", GeneratedDependencies: []string{"missing"}, Generated: func(Row) (interface{}, error) { return int64(1), nil }}},
		},
		{
			name:    "duplicate",
			columns: []DerivedColumn{{Name: "value", GeneratedDependencies: []string{"base", "base"}, Generated: func(Row) (interface{}, error) { return int64(1), nil }}, {Name: "base"}},
		},
		{
			name: "cycle",
			columns: []DerivedColumn{
				{Name: "left", GeneratedDependencies: []string{"right"}, Generated: func(Row) (interface{}, error) { return int64(1), nil }},
				{Name: "right", GeneratedDependencies: []string{"left"}, Generated: func(Row) (interface{}, error) { return int64(1), nil }},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if source, err := NewValidatedMaterializedSource(test.columns); err == nil || source != nil {
				t.Fatalf("NewValidatedMaterializedSource() = %#v, %v; want nil and error", source, err)
			}
		})
	}
}

func TestTT027LegacyGeneratedColumnsRemainDeclarationOrdered(t *testing.T) {
	source := NewMaterializedSource([]DerivedColumn{
		{Name: "base"},
		{Name: "derived", Generated: func(row Row) (interface{}, error) { return row["base"], nil }},
	})
	row, err := source.Insert(Row{"base": "legacy"})
	if err != nil || row["derived"] != "legacy" {
		t.Fatalf("legacy Insert() = %#v, %v", row, err)
	}
}
