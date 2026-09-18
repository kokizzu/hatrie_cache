package hatSchema

import (
	"fmt"
	"strconv"
	"testing"
)

func BenchmarkTT021LegacyPublicScanIndexBuild(b *testing.B) {
	source := benchmarkTT021MaterializedSource(b)
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		rows := source.Rows()
		index := make(map[string][]int, len(rows))
		for position, row := range rows {
			key := fmt.Sprintf("%T:%v", row["region"], row["region"])
			index[key] = append(index[key], position)
		}
		if len(index) == 0 {
			b.Fatal("legacy index build produced no entries")
		}
	}
}

func benchmarkTT021MaterializedSource(b *testing.B) *MaterializedSource {
	b.Helper()
	source := NewMaterializedSource([]DerivedColumn{
		{Name: "id"},
		{Name: "region"},
		{Name: "payload"},
	})
	for index := 0; index < 10_000; index++ {
		if _, err := source.Insert(Row{
			"id":      int64(index),
			"region":  "region-" + strconv.Itoa(index%64),
			"payload": "payload-" + strconv.Itoa(index),
		}); err != nil {
			b.Fatal(err)
		}
	}
	return source
}
