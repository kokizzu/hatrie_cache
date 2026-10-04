package functional_multikey_contract_test

import (
	"strconv"
	"testing"

	"hatrie_cache/hat/hatDataStructure"
)

type benchmarkRow struct {
	Tags []string
	Code int
}

func benchmarkRows(size int) []benchmarkRow {
	rows := make([]benchmarkRow, size)
	for i := range rows {
		rows[i] = benchmarkRow{
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

func newBaselineMultikeyIndex(capacity int) *hatDataStructure.StringMultikeyIndex {
	return hatDataStructure.NewStringMultikeyIndex(hatDataStructure.StringMultikeyIndexOptions{
		MaxKeysPerItem: 8,
		MaxItems:       capacity,
	})
}

func BenchmarkTU23BaselineUpsert(b *testing.B) {
	const size = 10_000
	rows := benchmarkRows(size)
	index := newBaselineMultikeyIndex(size)
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
	rows := benchmarkRows(size)
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		index := newBaselineMultikeyIndex(size)
		for i, row := range rows {
			if err := index.Set(uint64(i+1), row.Tags); err != nil {
				b.Fatal(err)
			}
		}
	}
}
