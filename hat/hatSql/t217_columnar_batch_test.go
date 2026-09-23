//go:build t217

package hatSql

import (
	"errors"
	"reflect"
	"testing"
)

func TestTypedTableAppendColumnarBatch(t *testing.T) {
	table := newT217Table(t)
	input := TypedTableColumnarBatch{
		Keys: []string{" e1 ", "e2", "e3"},
		Columns: [][]TypedTableValue{
			{TypedString("apac"), TypedString("emea"), TypedString("apac")},
			{TypedInt64(11), TypedInt64(22), TypedInt64(33)},
			{TypedBool(true), TypedBool(false), TypedBool(true)},
		},
	}
	rows, err := table.AppendColumnarBatch(input)
	if err != nil {
		t.Fatal(err)
	}
	if rows != len(input.Keys) {
		t.Fatalf("AppendColumnarBatch rows = %d, want %d", rows, len(input.Keys))
	}
	if got := table.Stats().RowCount; got != 3 {
		t.Fatalf("row count = %d, want 3", got)
	}

	columnar, found, err := table.ResolveSQLColumnarSource("cache", "events", []string{"region", "score", "active"})
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("ResolveSQLColumnarSource did not find table")
	}
	if columnar.Rows != 3 {
		t.Fatalf("columnar rows = %d, want 3", columnar.Rows)
	}
	want := map[string][]interface{}{
		"region": {"apac", "emea", "apac"},
		"score":  {int64(11), int64(22), int64(33)},
		"active": {true, false, true},
	}
	if !reflect.DeepEqual(columnar.Columns, want) {
		t.Fatalf("columnar values = %#v, want %#v", columnar.Columns, want)
	}
	changes, tail, err := table.ChangesAfter(0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if tail != 3 || len(changes) != 3 {
		t.Fatalf("changes = %#v, tail = %d, want 3 insert changes and tail 3", changes, tail)
	}
	for index, change := range changes {
		if change.Sequence != uint64(index+1) || change.Operation != "INSERT" {
			t.Fatalf("change[%d] = %#v, want ordered INSERT", index, change)
		}
	}
}

func TestTypedTableAppendColumnarBatchIsAtomic(t *testing.T) {
	table := newT217Table(t)
	valid := newT217Batch(2)
	if _, err := table.AppendColumnarBatch(valid); err != nil {
		t.Fatal(err)
	}

	cases := map[string]struct {
		batch    TypedTableColumnarBatch
		wantIs   error
		wantRows int
	}{
		"column count": {
			batch:    TypedTableColumnarBatch{Keys: []string{"new"}, Columns: [][]TypedTableValue{{TypedString("x")}}},
			wantIs:   ErrTypedTableColumnarBatchInvalid,
			wantRows: 2,
		},
		"column length": {
			batch: TypedTableColumnarBatch{
				Keys:    []string{"new", "newer"},
				Columns: [][]TypedTableValue{{TypedString("x")}, {TypedInt64(1), TypedInt64(2)}, {TypedBool(true), TypedBool(false)}},
			},
			wantIs:   ErrTypedTableColumnarBatchInvalid,
			wantRows: 2,
		},
		"value kind": {
			batch: TypedTableColumnarBatch{
				Keys:    []string{"new"},
				Columns: [][]TypedTableValue{{TypedString("x")}, {TypedString("wrong")}, {TypedBool(true)}},
			},
			wantIs:   ErrTypedTableColumnarBatchInvalid,
			wantRows: 2,
		},
		"duplicate key": {
			batch: TypedTableColumnarBatch{
				Keys:    []string{"new", "new"},
				Columns: [][]TypedTableValue{{TypedString("x"), TypedString("y")}, {TypedInt64(1), TypedInt64(2)}, {TypedBool(true), TypedBool(false)}},
			},
			wantIs:   ErrTypedTableColumnarBatchInvalid,
			wantRows: 2,
		},
		"existing key": {
			batch: TypedTableColumnarBatch{
				Keys:    []string{"event-0"},
				Columns: [][]TypedTableValue{{TypedString("x")}, {TypedInt64(1)}, {TypedBool(true)}},
			},
			wantIs:   ErrTypedTableColumnarBatchKeyExists,
			wantRows: 2,
		},
	}
	for name, test := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := table.AppendColumnarBatch(test.batch); !errors.Is(err, test.wantIs) {
				t.Fatalf("error = %v, want errors.Is(..., %v)", err, test.wantIs)
			}
			if got := table.Stats().RowCount; got != test.wantRows {
				t.Fatalf("row count = %d after rejected batch, want %d", got, test.wantRows)
			}
		})
	}
}

func TestTypedTableAppendColumnarBatchMaterializesGeneratedValues(t *testing.T) {
	table, err := NewTypedTable(TypedTableSchema{
		Name: "derived",
		Columns: []TypedTableColumn{
			{Name: "amount", Kind: TypedTableInt64},
			{
				Name:                  "double_amount",
				Kind:                  TypedTableInt64,
				GeneratedDependencies: []string{"amount"},
				Generated: func(values []TypedTableValue) (TypedTableValue, error) {
					return TypedInt64(values[0].Int64 * 2), nil
				},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = table.AppendColumnarBatch(TypedTableColumnarBatch{
		Keys: []string{"a", "b"},
		Columns: [][]TypedTableValue{
			{TypedInt64(4), TypedInt64(7)},
			{TypedNull(), TypedNull()},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	columnar, found, err := table.ResolveSQLColumnarSource("CACHE", "derived", []string{"amount", "double_amount"})
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("ResolveSQLColumnarSource did not find generated table")
	}
	want := map[string][]interface{}{
		"amount":        {int64(4), int64(7)},
		"double_amount": {int64(8), int64(14)},
	}
	if !reflect.DeepEqual(columnar.Columns, want) {
		t.Fatalf("generated columnar values = %#v, want %#v", columnar.Columns, want)
	}
}

func TestTypedTableAppendColumnarBatchMemoryBudgetIsAtomic(t *testing.T) {
	values := []TypedTableValue{TypedString("apac"), TypedInt64(11), TypedBool(true)}
	maxBytes := typedTableEstimatedRowBytes("a", values)
	table, err := NewTypedTable(TypedTableSchema{
		Name:         "budgeted",
		MemoryBudget: TypedTableMemoryBudgetOptions{MaxBytes: maxBytes},
		Columns: []TypedTableColumn{
			{Name: "region", Kind: TypedTableString},
			{Name: "score", Kind: TypedTableInt64},
			{Name: "active", Kind: TypedTableBool},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = table.AppendColumnarBatch(TypedTableColumnarBatch{
		Keys: []string{"a", "b"},
		Columns: [][]TypedTableValue{
			{TypedString("apac"), TypedString("emea")},
			{TypedInt64(11), TypedInt64(22)},
			{TypedBool(true), TypedBool(false)},
		},
	})
	if !errors.Is(err, ErrTypedTableMemoryBudgetExceeded) {
		t.Fatalf("error = %v, want memory budget error", err)
	}
	if got := table.Stats().RowCount; got != 0 {
		t.Fatalf("row count after rejected budget batch = %d, want 0", got)
	}
}
