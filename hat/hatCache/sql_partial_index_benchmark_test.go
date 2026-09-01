package hatCache

import "testing"

func BenchmarkSQLJSONPartialIndexRefresh(b *testing.B) {
	rows := make([]SQLRow, 10_000)
	for index := range rows {
		rows[index] = SQLRow{"state": "queued", "active": index%10 == 0, "id": index}
	}
	source := sqlJSONSource{raw: "fixture"}
	conditionKey, _ := sqlIndexValueKey(true)
	lookupKey, _ := sqlIndexValueKey("queued")
	b.Run("partial_active_true", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			index := &sqlJSONPartialIndex{field: "state", conditionField: "active", conditionKey: conditionKey}
			if err := refreshSQLJSONPartialIndexSource(index, source, rows); err != nil || len(index.rows[lookupKey]) != 1_000 {
				b.Fatalf("refresh partial = %#v, %v", index.rows, err)
			}
		}
	})
	b.Run("composite_all_rows", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			index := &sqlJSONCompositeIndex{fields: []string{"state", "active"}}
			if err := refreshSQLJSONCompositeIndexSourceRows(index, source, rows); err != nil {
				b.Fatal(err)
			}
			count := 0
			for _, posting := range index.rows {
				count += len(posting)
			}
			if count != len(rows) {
				b.Fatalf("composite rows = %d", count)
			}
		}
	})
}
