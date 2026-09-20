package hatSql

import (
	"fmt"
	"testing"
)

var chu16DemotionBenchmarkSink int

func BenchmarkCHU16AdaptivePromotionThenChurn(b *testing.B) {
	const (
		stableRows = 256
		churnRows  = typedTableDictionaryProbeMaxDistinct + 1
	)
	keys := make([]string, stableRows+churnRows)
	values := make([]TypedTableValue, stableRows+churnRows)
	for index := 0; index < stableRows; index++ {
		keys[index] = fmt.Sprintf("stable-%d", index)
		values[index] = TypedString("team-a")
	}
	for index := 0; index < churnRows; index++ {
		keys[stableRows+index] = fmt.Sprintf("churn-%d", index)
		values[stableRows+index] = TypedString(fmt.Sprintf("churn-%d", index))
	}
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		table, err := NewTypedTable(TypedTableSchema{
			Name:    "events",
			Columns: []TypedTableColumn{{Name: "team", Kind: TypedTableString, DictionaryAdaptive: true}},
		})
		if err != nil {
			b.Fatal(err)
		}
		for index, key := range keys {
			if _, err := table.Upsert(key, values[index:index+1]); err != nil {
				b.Fatal(err)
			}
		}
		storage := &table.columns[0]
		chu16DemotionBenchmarkSink += len(storage.dictionaryValues) + len(storage.strings)
		b.ReportMetric(float64(len(storage.dictionaryValues)), "dictionary-values/op")
		b.ReportMetric(float64(len(storage.strings)), "string-slots/op")
	}
}

func BenchmarkCHU16AdaptiveLongChurn(b *testing.B) {
	const (
		stableRows = 256
		churnRows  = 4096
	)
	keys := make([]string, stableRows+churnRows)
	values := make([]TypedTableValue, stableRows+churnRows)
	for index := 0; index < stableRows; index++ {
		keys[index] = fmt.Sprintf("stable-%d", index)
		values[index] = TypedString("team-a")
	}
	for index := 0; index < churnRows; index++ {
		keys[stableRows+index] = fmt.Sprintf("churn-%d", index)
		values[stableRows+index] = TypedString(fmt.Sprintf("churn-%d", index))
	}
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		table, err := NewTypedTable(TypedTableSchema{
			Name:    "events",
			Columns: []TypedTableColumn{{Name: "team", Kind: TypedTableString, DictionaryAdaptive: true}},
		})
		if err != nil {
			b.Fatal(err)
		}
		for index, key := range keys {
			if _, err := table.Upsert(key, values[index:index+1]); err != nil {
				b.Fatal(err)
			}
		}
		storage := &table.columns[0]
		chu16DemotionBenchmarkSink += len(storage.dictionaryValues) + len(storage.strings)
		b.ReportMetric(float64(len(storage.dictionaryValues)), "dictionary-values/op")
		b.ReportMetric(float64(len(storage.strings)), "string-slots/op")
	}
}
