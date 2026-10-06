package hatDataStructure

import (
	"strconv"
	"testing"
)

type tu23ExistingBenchmarkRow struct {
	Tags []string
	Code int
}

func tu23ExistingBenchmarkRows(size int) []tu23ExistingBenchmarkRow {
	rows := make([]tu23ExistingBenchmarkRow, size)
	for i := range rows {
		rows[i] = tu23ExistingBenchmarkRow{
			Tags: []string{
				"tag-" + strconv.Itoa(i%1000),
				"group-" + strconv.Itoa(i%100),
				"kind-" + strconv.Itoa(i%32),
				"bucket-" + strconv.Itoa(i%256),
			},
			Code: i,
		}
	}
	return rows
}

func tu23NewExistingMultikeyIndex(capacity int) *StringMultikeyIndex {
	return NewStringMultikeyIndex(StringMultikeyIndexOptions{
		MaxKeysPerItem: 8,
		MaxItems:       capacity,
	})
}

func BenchmarkTU23BaselineUpsert(b *testing.B) {
	const size = 10_000
	rows := tu23ExistingBenchmarkRows(size)
	index := tu23NewExistingMultikeyIndex(size)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		row := rows[i%size]
		if err := index.Set(uint64(i%size+1), row.Tags); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTU23BaselineBuild10000(b *testing.B) {
	const size = 10_000
	rows := tu23ExistingBenchmarkRows(size)
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		index := tu23NewExistingMultikeyIndex(size)
		for i, row := range rows {
			if err := index.Set(uint64(i+1), row.Tags); err != nil {
				b.Fatal(err)
			}
		}
	}
}
