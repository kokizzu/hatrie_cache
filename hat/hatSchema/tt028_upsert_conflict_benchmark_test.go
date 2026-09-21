package hatSchema

import "testing"

func BenchmarkTT028LegacyManualMerge(b *testing.B) {
	rows := tt028BenchmarkRows(1024)
	incoming := Row{"id": "item-512", "name": "updated", "count": int64(3)}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		merged := make([]Row, len(rows))
		for rowIndex, row := range rows {
			merged[rowIndex] = cloneRow(row)
		}
		for _, row := range merged {
			if row["id"] != incoming["id"] {
				continue
			}
			row["name"] = incoming["name"]
			row["count"] = row["count"].(int64) + incoming["count"].(int64)
			break
		}
		rows = merged
	}
	tt028BenchmarkSink = rows[len(rows)-1]
}

func BenchmarkTT028Upsert(b *testing.B) {
	source := NewMaterializedSource([]DerivedColumn{
		{Name: "id", Indexed: true},
		{Name: "name", Indexed: true},
		{Name: "count"},
	})
	for _, row := range tt028BenchmarkRows(1024) {
		if _, err := source.Insert(row); err != nil {
			b.Fatal(err)
		}
	}
	if _, err := source.BuildUniqueIndex("id"); err != nil {
		b.Fatal(err)
	}
	incoming := Row{"id": "item-512", "name": "updated", "count": int64(3)}
	options := MaterializedUpsertOptions{
		ConflictField: "id",
		OnConflict: func(existing, incoming Row) (Row, error) {
			return Row{
				"id":    existing["id"],
				"name":  incoming["name"],
				"count": existing["count"].(int64) + incoming["count"].(int64),
			}, nil
		},
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		result, err := source.Upsert(incoming, options)
		if err != nil {
			b.Fatal(err)
		}
		tt028BenchmarkSink = result.Row
	}
}

var tt028BenchmarkSink Row

func tt028BenchmarkRows(count int) []Row {
	rows := make([]Row, count)
	for index := range rows {
		rows[index] = Row{
			"id":    "item-" + benchmarkDecimal(index),
			"name":  "name-" + benchmarkDecimal(index),
			"count": int64(index),
		}
	}
	return rows
}

func benchmarkDecimal(value int) string {
	if value == 0 {
		return "0"
	}
	var digits [20]byte
	position := len(digits)
	for value > 0 {
		position--
		digits[position] = byte('0' + value%10)
		value /= 10
	}
	return string(digits[position:])
}
