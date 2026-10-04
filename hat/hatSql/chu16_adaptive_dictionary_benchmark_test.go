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

func BenchmarkCHU16AdaptiveDictionaryPostChurn(b *testing.B) {
	const rows = typedTableDictionaryProbeRows
	const churn = typedTableDictionaryProbeRows/2 + 1
	table, err := NewTypedTable(TypedTableSchema{
		Name:    "events",
		Columns: []TypedTableColumn{{Name: "value", Kind: TypedTableString, DictionaryAdaptive: true}},
	})
	if err != nil {
		b.Fatal(err)
	}
	for index := 0; index < rows; index++ {
		if _, err := table.Upsert(fmt.Sprintf("key-%d", index), []TypedTableValue{TypedString("team-a")}); err != nil {
			b.Fatal(err)
		}
	}
	for index := 0; index < churn; index++ {
		if _, err := table.Upsert(fmt.Sprintf("key-%d", index), []TypedTableValue{TypedString(fmt.Sprintf("value-%d", index))}); err != nil {
			b.Fatal(err)
		}
	}
	keys := make([]string, rows)
	values := make([]TypedTableValue, churn)
	for index := range keys {
		keys[index] = fmt.Sprintf("key-%d", index)
	}
	for index := range values {
		values[index] = TypedString(fmt.Sprintf("value-%d", index))
	}
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		index := iteration % rows
		if _, err := table.Upsert(keys[index], []TypedTableValue{values[index%churn]}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCHU16AdaptiveDictionaryDemotion(b *testing.B) {
	const rows = typedTableDictionaryProbeRows
	const churn = typedTableDictionaryProbeRows/2 + 1
	b.ReportAllocs()
	b.StopTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		table, err := NewTypedTable(TypedTableSchema{
			Name:    "events",
			Columns: []TypedTableColumn{{Name: "value", Kind: TypedTableString, DictionaryAdaptive: true}},
		})
		if err != nil {
			b.Fatal(err)
		}
		for index := 0; index < rows; index++ {
			if _, err := table.Upsert(fmt.Sprintf("key-%d", index), []TypedTableValue{TypedString("team-a")}); err != nil {
				b.Fatal(err)
			}
		}
		b.StartTimer()
		for index := 0; index < churn; index++ {
			if _, err := table.Upsert(fmt.Sprintf("key-%d", index), []TypedTableValue{TypedString(fmt.Sprintf("value-%d", index))}); err != nil {
				b.Fatal(err)
			}
		}
		b.StopTimer()
	}
}
