package hatSql

import (
	"reflect"
	"strings"
	"testing"
)

func TestTypedTableAppendColumnarPreservesRowsAndChanges(t *testing.T) {
	table, err := NewTypedTable(chu22BenchmarkSchema())
	if err != nil {
		t.Fatal(err)
	}
	batch := ColumnarBatch{
		Columns: map[string][]interface{}{
			"name":   {"alpha", nil, "gamma"},
			"score":  {int64(10), int64(20), nil},
			"ratio":  {1.5, nil, 3.5},
			"active": {true, false, true},
		},
		Rows: 3,
	}
	batch.PackCompressedColumns()
	keys := []string{"a", "b", "c"}

	changes, err := table.AppendColumnar(keys, batch)
	if err != nil {
		t.Fatalf("AppendColumnar() error = %v", err)
	}
	if len(changes) != len(keys) {
		t.Fatalf("AppendColumnar() returned %d changes, want %d", len(changes), len(keys))
	}
	wantAfter := [][]TypedTableValue{
		{TypedString("alpha"), TypedInt64(10), TypedFloat64(1.5), TypedBool(true)},
		{TypedNull(), TypedInt64(20), TypedNull(), TypedBool(false)},
		{TypedString("gamma"), TypedNull(), TypedFloat64(3.5), TypedBool(true)},
	}
	for index, change := range changes {
		if change.Sequence != uint64(index+1) || change.Operation != "INSERT" || change.Key != keys[index] {
			t.Fatalf("change[%d] = %#v", index, change)
		}
		if !reflect.DeepEqual(change.After, wantAfter[index]) {
			t.Fatalf("change[%d].After = %#v, want %#v", index, change.After, wantAfter[index])
		}
	}
	wantRows := []Row{
		{"name": "alpha", "score": int64(10), "ratio": 1.5, "active": true},
		{"name": nil, "score": int64(20), "ratio": nil, "active": false},
		{"name": "gamma", "score": nil, "ratio": 3.5, "active": true},
	}
	if got := table.Rows(); !reflect.DeepEqual(got, wantRows) {
		t.Fatalf("Rows() = %#v, want %#v", got, wantRows)
	}
}

func TestTypedTableAppendColumnarGeneratedColumns(t *testing.T) {
	generatedCalls := 0
	table, err := NewTypedTable(TypedTableSchema{
		Name: "generated",
		Columns: []TypedTableColumn{
			{Name: "base", Kind: TypedTableInt64},
			{
				Name: "double",
				Kind: TypedTableInt64,
				Generated: func(values []TypedTableValue) (TypedTableValue, error) {
					generatedCalls++
					return TypedInt64(values[0].Int64 * 2), nil
				},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	changes, err := table.AppendColumnar([]string{"a", "b"}, ColumnarBatch{
		Columns: map[string][]interface{}{
			"base":   {int64(4), int64(7)},
			"double": {nil, nil},
		},
		Rows: 2,
	})
	if err != nil {
		t.Fatalf("AppendColumnar() error = %v", err)
	}
	if generatedCalls != 2 {
		t.Fatalf("generated callback calls = %d, want 2", generatedCalls)
	}
	want := []TypedTableValue{TypedInt64(7), TypedInt64(14)}
	if !reflect.DeepEqual(changes[1].After, want) {
		t.Fatalf("generated After = %#v, want %#v", changes[1].After, want)
	}
}

func TestTypedTableAppendColumnarRejectsInvalidInputAtomically(t *testing.T) {
	tests := []struct {
		name  string
		keys  []string
		batch ColumnarBatch
		want  string
	}{
		{
			name: "missing field",
			keys: []string{"a", "b"},
			batch: ColumnarBatch{
				Columns: map[string][]interface{}{
					"name":  {"a", "b"},
					"score": {int64(1), int64(2)},
					"ratio": {1.0, 2.0},
				},
				Rows: 2,
			},
			want: "active",
		},
		{
			name: "wrong type",
			keys: []string{"a", "b"},
			batch: ColumnarBatch{
				Columns: map[string][]interface{}{
					"name":   {"a", "b"},
					"score":  {int64(1), "bad"},
					"ratio":  {1.0, 2.0},
					"active": {true, false},
				},
				Rows: 2,
			},
			want: "score",
		},
		{
			name:  "duplicate key",
			keys:  []string{"a", "a"},
			batch: chu22BenchmarkBatch(2),
			want:  "duplicate",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			table, err := NewTypedTable(chu22BenchmarkSchema())
			if err != nil {
				t.Fatal(err)
			}
			if _, err := table.AppendColumnar(test.keys, test.batch); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("AppendColumnar() error = %v, want substring %q", err, test.want)
			}
			if got := table.Rows(); len(got) != 0 {
				t.Fatalf("Rows() length = %d after rejected append, want 0", len(got))
			}
			if _, tail, err := table.ChangesAfter(0, 10); err != nil || tail != 0 {
				t.Fatalf("ChangesAfter() tail = %d, err = %v, want 0/nil", tail, err)
			}
		})
	}

	table, err := NewTypedTable(chu22BenchmarkSchema())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := table.Upsert("existing", []TypedTableValue{TypedString("old"), TypedInt64(1), TypedFloat64(1), TypedBool(true)}); err != nil {
		t.Fatal(err)
	}
	if _, err := table.AppendColumnar([]string{"existing", "new"}, chu22BenchmarkBatch(2)); err == nil || !strings.Contains(err.Error(), "existing") {
		t.Fatalf("existing-key append error = %v", err)
	}
	if got := table.Rows(); len(got) != 1 {
		t.Fatalf("Rows() length after existing-key rejection = %d, want 1", len(got))
	}
}
