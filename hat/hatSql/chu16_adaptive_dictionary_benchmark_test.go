package hatSql

import (
	"fmt"
	"testing"
)

func BenchmarkCHU16TypedTableStringStorage(b *testing.B) {
	for _, test := range []struct {
		name               string
		dictionaryEncoded  bool
		dictionaryAdaptive bool
		unique             bool
	}{
		{name: "plain-repeated", unique: false},
		{name: "plain-unique", unique: true},
		{name: "dictionary-repeated", dictionaryEncoded: true, unique: false},
		{name: "dictionary-unique", dictionaryEncoded: true, unique: true},
		{name: "adaptive-repeated", dictionaryAdaptive: true, unique: false},
		{name: "adaptive-unique", dictionaryAdaptive: true, unique: true},
	} {
		b.Run(test.name, func(b *testing.B) {
			const rows = 512
			keys := make([]string, rows)
			values := make([]TypedTableValue, rows)
			for index := range values {
				keys[index] = fmt.Sprintf("key-%d", index)
				value := "team-a"
				if test.unique {
					value = fmt.Sprintf("value-%d", index)
				}
				values[index] = TypedTableValue{Kind: TypedTableString, Valid: true, String: value}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				table, err := NewTypedTable(TypedTableSchema{
					Name: "events",
					Columns: []TypedTableColumn{{
						Name:               "team",
						Kind:               TypedTableString,
						DictionaryEncoded:  test.dictionaryEncoded,
						DictionaryAdaptive: test.dictionaryAdaptive,
					}},
				})
				if err != nil {
					b.Fatal(err)
				}
				for index := range values {
					if _, err := table.Upsert(keys[index], values[index:index+1]); err != nil {
						b.Fatal(err)
					}
				}
			}
		})
	}
}
