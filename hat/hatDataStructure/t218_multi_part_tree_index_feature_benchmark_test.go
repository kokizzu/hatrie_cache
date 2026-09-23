//go:build t218

package hatDataStructure_test

import (
	"testing"

	"hatrie_cache/hat/hatDataStructure"
)

func BenchmarkT218MultiPartTreePrefixScan(b *testing.B) {
	records := newT218Records(32_768)
	index, err := hatDataStructure.NewMultiPartTreeIndex(
		func(record t218Record) []string {
			return []string{record.Region, stringIndex(record.Order)}
		},
		t218CompareString,
		2,
		len(records),
	)
	if err != nil {
		b.Fatal(err)
	}
	for _, record := range records {
		if err := index.Upsert(record.ID, record); err != nil {
			b.Fatal(err)
		}
	}
	prefix := []string{"region-07"}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		iterator, ok, err := index.Prefix(prefix)
		if err != nil || !ok {
			b.Fatalf("Prefix() = %v, %v", ok, err)
		}
		matches := 0
		for {
			entry, next, err := iterator.Next()
			if err != nil {
				b.Fatal(err)
			}
			if !next {
				break
			}
			matches += entry.Value.Order
		}
		benchmarkT218Matches = matches
	}
}
