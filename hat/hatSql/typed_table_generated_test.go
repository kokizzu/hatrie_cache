package hatSql

import (
	"errors"
	"testing"
)

func TestTypedTableGeneratedColumnsAreComputedForRowsAndChanges(t *testing.T) {
	table, err := NewTypedTable(TypedTableSchema{
		Name: "orders",
		Columns: []TypedTableColumn{
			{Name: "price", Kind: TypedTableInt64},
			{Name: "quantity", Kind: TypedTableInt64},
			{
				Name: "total",
				Kind: TypedTableInt64,
				Generated: func(values []TypedTableValue) (TypedTableValue, error) {
					return TypedInt64(values[0].Int64 * values[1].Int64), nil
				},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	change, err := table.Upsert("a", []TypedTableValue{TypedInt64(4), TypedInt64(3), TypedInt64(999)})
	if err != nil {
		t.Fatal(err)
	}
	if change.After[2] != TypedInt64(12) {
		t.Fatalf("insert generated value = %#v, want %#v", change.After[2], TypedInt64(12))
	}
	rows, err := table.ResolveSQLSource("CACHE", "orders")
	if err != nil || len(rows) != 1 || rows[0]["total"] != int64(12) {
		t.Fatalf("resolved rows = %#v, error = %v", rows, err)
	}

	change, err = table.Upsert("a", []TypedTableValue{TypedInt64(5), TypedInt64(2), TypedNull()})
	if err != nil {
		t.Fatal(err)
	}
	if change.Before[2] != TypedInt64(12) || change.After[2] != TypedInt64(10) {
		t.Fatalf("update generated values = before %#v after %#v", change.Before[2], change.After[2])
	}
}

func TestTypedTableGeneratedColumnRejectsWrongTypeAndCallbackError(t *testing.T) {
	callbackErr := errors.New("generated callback failed")
	table, err := NewTypedTable(TypedTableSchema{
		Name: "events",
		Columns: []TypedTableColumn{
			{Name: "value", Kind: TypedTableInt64},
			{
				Name: "derived",
				Kind: TypedTableString,
				Generated: func(values []TypedTableValue) (TypedTableValue, error) {
					if values[0].Int64 == 2 {
						return TypedNull(), callbackErr
					}
					return TypedInt64(values[0].Int64), nil
				},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := table.Upsert("wrong-type", []TypedTableValue{TypedInt64(1), TypedNull()}); err == nil {
		t.Fatal("wrong generated type was accepted")
	}
	if _, err := table.Upsert("callback-error", []TypedTableValue{TypedInt64(2), TypedNull()}); !errors.Is(err, callbackErr) {
		t.Fatalf("generated callback error = %v, want %v", err, callbackErr)
	}
	if rows := table.Rows(); len(rows) != 0 {
		t.Fatalf("failed generated rows retained = %#v", rows)
	}
}
