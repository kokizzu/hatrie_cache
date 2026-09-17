package hatSql

import "testing"

var chu22BenchmarkTableSink *TypedTable

func chu22BenchmarkSchema() TypedTableSchema {
	return TypedTableSchema{
		Name: "events",
		Columns: []TypedTableColumn{
			{Name: "name", Kind: TypedTableString},
			{Name: "score", Kind: TypedTableInt64},
			{Name: "ratio", Kind: TypedTableFloat64},
			{Name: "active", Kind: TypedTableBool},
		},
	}
}

func chu22BenchmarkBatch(rows int) ColumnarBatch {
	names := make([]interface{}, rows)
	scores := make([]interface{}, rows)
	ratios := make([]interface{}, rows)
	active := make([]interface{}, rows)
	for row := 0; row < rows; row++ {
		names[row] = "event"
		scores[row] = int64(row)
		ratios[row] = float64(row) / 10
		active[row] = row%2 == 0
	}
	return ColumnarBatch{
		Columns: map[string][]interface{}{
			"name":   names,
			"score":  scores,
			"ratio":  ratios,
			"active": active,
		},
		Rows: rows,
	}
}

func chu22BenchmarkKeys(rows int) []string {
	keys := make([]string, rows)
	for row := range keys {
		keys[row] = "key-" + formatChu22BenchmarkInteger(row)
	}
	return keys
}

func formatChu22BenchmarkInteger(value int) string {
	if value == 0 {
		return "0"
	}
	var buffer [20]byte
	index := len(buffer)
	for value > 0 {
		index--
		buffer[index] = byte('0' + value%10)
		value /= 10
	}
	return string(buffer[index:])
}

func chu22BenchmarkTypedValue(batch ColumnarBatch, field string, row int, kind TypedTableKind) TypedTableValue {
	raw, valid := batch.Value(field, row)
	if !valid || raw == nil {
		return TypedNull()
	}
	switch kind {
	case TypedTableString:
		return TypedString(raw.(string))
	case TypedTableInt64:
		return TypedInt64(raw.(int64))
	case TypedTableFloat64:
		return TypedFloat64(raw.(float64))
	case TypedTableBool:
		return TypedBool(raw.(bool))
	default:
		panic("unsupported benchmark kind")
	}
}

func BenchmarkCHU22RowWiseUpsert(b *testing.B) {
	const rows = 4096
	schema := chu22BenchmarkSchema()
	batch := chu22BenchmarkBatch(rows)
	keys := chu22BenchmarkKeys(rows)
	benchmarkCHU22RowWiseUpsert(b, schema, batch, keys)
}

func BenchmarkCHU22RowWiseUpsertPacked(b *testing.B) {
	const rows = 4096
	schema := chu22BenchmarkSchema()
	batch := chu22BenchmarkBatch(rows)
	batch.PackCompressedColumns()
	keys := chu22BenchmarkKeys(rows)
	benchmarkCHU22RowWiseUpsert(b, schema, batch, keys)
}

func benchmarkCHU22RowWiseUpsert(b *testing.B, schema TypedTableSchema, batch ColumnarBatch, keys []string) {
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		table, err := NewTypedTable(schema)
		if err != nil {
			b.Fatal(err)
		}
		for row := 0; row < len(keys); row++ {
			values := make([]TypedTableValue, len(schema.Columns))
			for column, definition := range schema.Columns {
				values[column] = chu22BenchmarkTypedValue(batch, definition.Name, row, definition.Kind)
			}
			if _, err := table.Upsert(keys[row], values); err != nil {
				b.Fatal(err)
			}
		}
		chu22BenchmarkTableSink = table
	}
}

func BenchmarkCHU22AppendColumnar(b *testing.B) {
	const rows = 4096
	schema := chu22BenchmarkSchema()
	keys := chu22BenchmarkKeys(rows)
	plain := chu22BenchmarkBatch(rows)
	packed := chu22BenchmarkBatch(rows)
	packed.PackCompressedColumns()
	for _, test := range []struct {
		name  string
		batch ColumnarBatch
	}{
		{name: "plain", batch: plain},
		{name: "packed", batch: packed},
	} {
		b.Run(test.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				table, err := NewTypedTable(schema)
				if err != nil {
					b.Fatal(err)
				}
				if _, err := table.AppendColumnar(keys, test.batch); err != nil {
					b.Fatal(err)
				}
				chu22BenchmarkTableSink = table
			}
		})
	}
}
