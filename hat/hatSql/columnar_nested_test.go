package hatSql_test

import (
	"errors"
	"reflect"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestColumnarListColumnUsesOffsetsAndValues(t *testing.T) {
	column, err := hatSql.NewColumnarListColumn([][]interface{}{
		{int64(1), int64(2)},
		{},
		{int64(3)},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(column.Offsets, []uint32{0, 2, 2, 3}) || !reflect.DeepEqual(column.Values, []interface{}{int64(1), int64(2), int64(3)}) {
		t.Fatalf("list layout = %#v, want offsets [0 2 2 3] and flat values", column)
	}
	batch := hatSql.ColumnarBatch{ListColumns: map[string]hatSql.ColumnarListColumn{"tags": column}, Rows: 3}
	if got := batch.FieldRows("tags"); got != 3 {
		t.Fatalf("FieldRows(tags) = %d, want 3", got)
	}
	value, ok := batch.Value("tags", 0)
	if !ok || !reflect.DeepEqual(value, []interface{}{int64(1), int64(2)}) {
		t.Fatalf("Value(tags, 0) = %#v/%v", value, ok)
	}
	value.([]interface{})[0] = int64(99)
	value, _ = batch.Value("tags", 0)
	if !reflect.DeepEqual(value, []interface{}{int64(1), int64(2)}) {
		t.Fatalf("list value mutation changed column = %#v", value)
	}
}

func TestColumnarNestedColumnUsesSharedOffsetsAndChildColumns(t *testing.T) {
	column, err := hatSql.NewColumnarNestedColumn([]map[string][]interface{}{
		{"name": {"ada", "lin"}, "score": {int64(7), int64(9)}},
		{"name": {}, "score": {}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(column.Offsets, []uint32{0, 2, 2}) || !reflect.DeepEqual(column.Fields["name"], []interface{}{"ada", "lin"}) || !reflect.DeepEqual(column.Fields["score"], []interface{}{int64(7), int64(9)}) {
		t.Fatalf("nested layout = %#v", column)
	}
	batch := hatSql.ColumnarBatch{NestedColumns: map[string]hatSql.ColumnarNestedColumn{"events": column}, Rows: 2}
	value, ok := batch.Value("events", 0)
	if !ok || !reflect.DeepEqual(value, []map[string]interface{}{{"name": "ada", "score": int64(7)}, {"name": "lin", "score": int64(9)}}) {
		t.Fatalf("Value(events, 0) = %#v/%v", value, ok)
	}
}

func TestColumnarNestedLayoutsRejectMalformedOffsetsAndChildLengths(t *testing.T) {
	if _, err := hatSql.NewColumnarListColumn(nil); err != nil {
		t.Fatal(err)
	}
	for name, column := range map[string]hatSql.ColumnarListColumn{
		"missing initial offset": {Offsets: []uint32{1}, Values: []interface{}{int64(1)}},
		"decreasing offsets":     {Offsets: []uint32{0, 2, 1}, Values: []interface{}{int64(1), int64(2)}},
		"short values":           {Offsets: []uint32{0, 2}, Values: []interface{}{int64(1)}},
		"trailing values":        {Offsets: []uint32{0, 1}, Values: []interface{}{int64(1), int64(2)}},
	} {
		t.Run(name, func(t *testing.T) {
			if err := column.Validate(1); !errors.Is(err, hatSql.ErrColumnarNestedLayoutInvalid) {
				t.Fatalf("Validate() error = %v, want ErrColumnarNestedLayoutInvalid", err)
			}
		})
	}
	if _, err := hatSql.NewColumnarNestedColumn([]map[string][]interface{}{{"name": {"ada"}, "score": {}}}); !errors.Is(err, hatSql.ErrColumnarNestedLayoutInvalid) {
		t.Fatalf("child length error = %v, want ErrColumnarNestedLayoutInvalid", err)
	}
	if err := (hatSql.ColumnarNestedColumn{Offsets: []uint32{0, 1}}).Validate(1); !errors.Is(err, hatSql.ErrColumnarNestedLayoutInvalid) {
		t.Fatalf("missing child fields error = %v, want ErrColumnarNestedLayoutInvalid", err)
	}
}

func TestMergeColumnarPartsReadsArrayAndNestedLayouts(t *testing.T) {
	list, err := hatSql.NewColumnarListColumn([][]interface{}{{"a"}, {"b", "c"}})
	if err != nil {
		t.Fatal(err)
	}
	nested, err := hatSql.NewColumnarNestedColumn([]map[string][]interface{}{{"name": {"ada"}}, {"name": {"lin"}}})
	if err != nil {
		t.Fatal(err)
	}
	merged, err := hatSql.MergeColumnarParts([]hatSql.ColumnarMergePart{
		hatSql.ColumnarBatchPart{Batch: hatSql.ColumnarBatch{
			ListColumns:   map[string]hatSql.ColumnarListColumn{"tags": list},
			NestedColumns: map[string]hatSql.ColumnarNestedColumn{"events": nested},
			Rows:          2,
		}},
	}, []string{"tags", "events"})
	if err != nil {
		t.Fatal(err)
	}
	if got, ok := merged.Value("tags", 1); !ok || !reflect.DeepEqual(got, []interface{}{"b", "c"}) {
		t.Fatalf("merged tags = %#v/%v", got, ok)
	}
	if got, ok := merged.Value("events", 0); !ok || !reflect.DeepEqual(got, []map[string]interface{}{{"name": "ada"}}) {
		t.Fatalf("merged events = %#v/%v", got, ok)
	}
}

func BenchmarkColumnarListLayout(b *testing.B) {
	rows := make([][]interface{}, 4096)
	for row := range rows {
		rows[row] = []interface{}{int64(row), "tag", row%2 == 0}
	}
	b.ReportAllocs()
	b.Run("legacy-slices", func(b *testing.B) {
		for index := 0; index < b.N; index++ {
			legacy := make([][]interface{}, len(rows))
			for row, values := range rows {
				legacy[row] = append([]interface{}(nil), values...)
			}
			if len(legacy) != len(rows) {
				b.Fatal("legacy layout changed row count")
			}
		}
	})
	b.Run("offset-values", func(b *testing.B) {
		for index := 0; index < b.N; index++ {
			column, err := hatSql.NewColumnarListColumn(rows)
			if err != nil || len(column.Offsets) != len(rows)+1 || len(column.Values) != len(rows)*3 {
				b.Fatalf("offset layout = %#v, %v", column, err)
			}
		}
	})
}
