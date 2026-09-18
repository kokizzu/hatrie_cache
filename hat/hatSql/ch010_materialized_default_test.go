package hatSql

import "testing"

func TestCH010DefaultGeneratedColumnsPreserveExplicitValuesAndComputeMissing(t *testing.T) {
	table, err := NewTypedTable(TypedTableSchema{
		Name: "defaults",
		Columns: []TypedTableColumn{
			{Name: "base", Kind: TypedTableInt64},
			{
				Name:          "derived",
				Kind:          TypedTableInt64,
				GeneratedMode: TypedTableGeneratedDefault,
				Generated: func(values []TypedTableValue) (TypedTableValue, error) {
					return TypedInt64(values[0].Int64 * 10), nil
				},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := table.Upsert("explicit", []TypedTableValue{TypedInt64(3), TypedInt64(99)}); err != nil {
		t.Fatal(err)
	}
	if _, err := table.Upsert("missing", []TypedTableValue{TypedInt64(4), TypedNull()}); err != nil {
		t.Fatal(err)
	}
	rows := table.Rows()
	if len(rows) != 2 {
		t.Fatalf("rows = %#v, want 2 rows", rows)
	}
	if rows[0]["derived"] != int64(99) || rows[1]["derived"] != int64(40) {
		t.Fatalf("default values = %#v and %#v, want 99 and 40", rows[0]["derived"], rows[1]["derived"])
	}
}

func TestCH010MaterializedGeneratedColumnsIgnoreExplicitValues(t *testing.T) {
	table, err := NewTypedTable(TypedTableSchema{
		Name: "materialized",
		Columns: []TypedTableColumn{
			{Name: "base", Kind: TypedTableInt64},
			{
				Name: "derived",
				Kind: TypedTableInt64,
				Generated: func(values []TypedTableValue) (TypedTableValue, error) {
					return TypedInt64(values[0].Int64 * 10), nil
				},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	change, err := table.Upsert("row", []TypedTableValue{TypedInt64(3), TypedInt64(99)})
	if err != nil {
		t.Fatal(err)
	}
	if change.After[1] != TypedInt64(30) {
		t.Fatalf("materialized value = %#v, want %#v", change.After[1], TypedInt64(30))
	}
}

func TestCH010GeneratedDependenciesControlEvaluationOrder(t *testing.T) {
	table, err := NewTypedTable(TypedTableSchema{
		Name: "ordered",
		Columns: []TypedTableColumn{
			{Name: "base", Kind: TypedTableInt64},
			{
				Name:                  "final",
				Kind:                  TypedTableInt64,
				GeneratedDependencies: []string{"middle"},
				Generated: func(values []TypedTableValue) (TypedTableValue, error) {
					return TypedInt64(values[2].Int64 + 1), nil
				},
			},
			{
				Name: "middle",
				Kind: TypedTableInt64,
				Generated: func(values []TypedTableValue) (TypedTableValue, error) {
					return TypedInt64(values[0].Int64 * 2), nil
				},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	change, err := table.Upsert("row", []TypedTableValue{TypedInt64(3), TypedNull(), TypedNull()})
	if err != nil {
		t.Fatal(err)
	}
	if change.After[1] != TypedInt64(7) || change.After[2] != TypedInt64(6) {
		t.Fatalf("generated values = %#v, want final=7 middle=6", change.After)
	}
}

func TestCH010DefaultGeneratedColumnValidatesExplicitType(t *testing.T) {
	table, err := NewTypedTable(TypedTableSchema{
		Name: "typed-default",
		Columns: []TypedTableColumn{
			{Name: "base", Kind: TypedTableInt64},
			{
				Name:          "derived",
				Kind:          TypedTableInt64,
				GeneratedMode: TypedTableGeneratedDefault,
				Generated: func(values []TypedTableValue) (TypedTableValue, error) {
					return TypedInt64(values[0].Int64), nil
				},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := table.Upsert("wrong", []TypedTableValue{TypedInt64(1), TypedString("bad")}); err == nil {
		t.Fatal("default generated column accepted an explicit value with the wrong kind")
	}
	if rows := table.Rows(); len(rows) != 0 {
		t.Fatalf("invalid default row retained = %#v", rows)
	}
}

func TestCH010GeneratedDependenciesRejectUnknownAndCycles(t *testing.T) {
	for name, columns := range map[string][]TypedTableColumn{
		"unknown": {
			{Name: "base", Kind: TypedTableInt64},
			{
				Name:                  "derived",
				Kind:                  TypedTableInt64,
				GeneratedDependencies: []string{"missing"},
				Generated: func([]TypedTableValue) (TypedTableValue, error) {
					return TypedInt64(1), nil
				},
			},
		},
		"duplicate": {
			{Name: "base", Kind: TypedTableInt64},
			{
				Name:                  "derived",
				Kind:                  TypedTableInt64,
				GeneratedDependencies: []string{"base", " base "},
				Generated: func([]TypedTableValue) (TypedTableValue, error) {
					return TypedInt64(1), nil
				},
			},
		},
		"cycle": {
			{
				Name:                  "left",
				Kind:                  TypedTableInt64,
				GeneratedDependencies: []string{"right"},
				Generated: func([]TypedTableValue) (TypedTableValue, error) {
					return TypedInt64(1), nil
				},
			},
			{
				Name:                  "right",
				Kind:                  TypedTableInt64,
				GeneratedDependencies: []string{"left"},
				Generated: func([]TypedTableValue) (TypedTableValue, error) {
					return TypedInt64(1), nil
				},
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := NewTypedTable(TypedTableSchema{Name: name, Columns: columns}); err == nil {
				t.Fatalf("invalid generated dependency schema was accepted: %s", name)
			}
		})
	}
}
