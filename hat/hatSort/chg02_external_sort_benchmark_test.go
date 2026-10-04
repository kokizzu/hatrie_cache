package hatSort

import (
	"bytes"
	"context"
	"sort"
	"testing"
)

var chg02BenchmarkSink []Record

func BenchmarkCHG02InMemoryStableSort(b *testing.B) {
	input := chg02BenchmarkRecords(8192)
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		records := cloneRecords(input)
		sort.SliceStable(records, func(left, right int) bool {
			return bytes.Compare(records[left].Key, records[right].Key) < 0
		})
		chg02BenchmarkSink = records
	}
}

func BenchmarkCHG02ExternalSortNoSpill(b *testing.B) {
	input := chg02BenchmarkRecords(8192)
	options := Options{MaxMemoryBytes: 1 << 20, MaxSpillBytes: 1 << 30}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		records, _, err := ExternalSort(context.Background(), input, options)
		if err != nil {
			b.Fatal(err)
		}
		chg02BenchmarkSink = records
	}
}

func BenchmarkCHG02ExternalSortSpill(b *testing.B) {
	input := chg02BenchmarkRecords(8192)
	options := Options{MaxMemoryBytes: 4 << 10, MaxSpillBytes: 1 << 30, SpillDirectory: b.TempDir()}
	var last Stats
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		records, stats, err := ExternalSort(context.Background(), input, options)
		if err != nil {
			b.Fatal(err)
		}
		last = stats
		chg02BenchmarkSink = records
	}
	b.StopTimer()
	b.ReportMetric(float64(last.SpilledBytes), "spill-bytes/op")
	b.ReportMetric(float64(last.PeakMemory), "peak-run-bytes/op")
	b.ReportMetric(float64(last.Runs), "runs/op")
}

func BenchmarkCHG02ExternalSortIntoSpill(b *testing.B) {
	input := chg02BenchmarkRecords(8192)
	options := Options{MaxMemoryBytes: 4 << 10, MaxSpillBytes: 1 << 30, SpillDirectory: b.TempDir()}
	var last Stats
	var streamed int
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		stats, err := ExternalSortInto(context.Background(), input, options, func(Record) error {
			streamed++
			return nil
		})
		if err != nil {
			b.Fatal(err)
		}
		last = stats
	}
	b.StopTimer()
	if streamed != b.N*len(input) {
		b.Fatalf("streamed records = %d, want %d", streamed, b.N*len(input))
	}
	b.ReportMetric(float64(last.SpilledBytes), "spill-bytes/op")
	b.ReportMetric(float64(last.PeakMemory), "peak-run-bytes/op")
	b.ReportMetric(float64(last.Runs), "runs/op")
}

func chg02BenchmarkRecords(count int) []Record {
	records := make([]Record, count)
	for index := range records {
		records[index] = Record{
			Key:   []byte{byte('a' + index%64), byte(index % 17)},
			Value: []byte{byte(index), byte(index >> 8), 'v', 'a', 'l', 'u', 'e'},
		}
	}
	return records
}

func cloneRecords(records []Record) []Record {
	cloned := make([]Record, len(records))
	for index, record := range records {
		cloned[index] = cloneRecord(record)
	}
	return cloned
}
