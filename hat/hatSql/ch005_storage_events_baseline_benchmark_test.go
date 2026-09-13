package hatSql

import "testing"

func BenchmarkCH005BaselineTypedTablePatchDeleteUpsert(b *testing.B) {
	const rowCount = 256
	keys := make([]string, rowCount)
	for index := range keys {
		keys[index] = "key-" + string(rune('a'+index%26)) + string(rune('0'+index/26))
	}
	table, err := NewTypedTable(TypedTableSchema{
		Name: "benchmark-events",
		Columns: []TypedTableColumn{
			{Name: "value", Kind: TypedTableInt64},
		},
		PatchParts: TypedTablePatchOptions{
			Enabled:        true,
			MergeThreshold: rowCount + 1,
		},
	})
	if err != nil {
		b.Fatal(err)
	}
	for index, key := range keys {
		if _, err := table.Upsert(key, []TypedTableValue{TypedInt64(int64(index))}); err != nil {
			b.Fatal(err)
		}
	}
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		keyIndex := index % rowCount
		if _, err := table.Delete(keys[keyIndex]); err != nil {
			b.Fatal(err)
		}
		if _, err := table.Upsert(keys[keyIndex], []TypedTableValue{TypedInt64(int64(keyIndex))}); err != nil {
			b.Fatal(err)
		}
	}
}
