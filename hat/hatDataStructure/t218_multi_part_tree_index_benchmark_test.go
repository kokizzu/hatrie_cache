package hatDataStructure_test

import (
	"strconv"
	"testing"
)

type t218Record struct {
	ID     uint64
	Region string
	Order  int
}

var benchmarkT218Matches int

func newT218Records(count int) []t218Record {
	records := make([]t218Record, count)
	for index := range records {
		records[index] = t218Record{
			ID:     uint64(index + 1),
			Region: "region-" + strconv.Itoa(index%16),
			Order:  index,
		}
	}
	return records
}

func BenchmarkT218PrefixScanBaseline(b *testing.B) {
	records := newT218Records(32_768)
	const region = "region-07"
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		matches := 0
		for _, record := range records {
			if record.Region == region {
				matches += record.Order
			}
		}
		benchmarkT218Matches = matches
	}
}
