package hatReplication

import (
	"context"
	"strconv"
	"testing"
	"time"
)

var ch034ParallelReplicaRangeBenchmarkSink ParallelReplicaRangeReadResult

func BenchmarkCH034SequentialReplicaRangeRead(b *testing.B) {
	ranges := ch034BenchmarkRanges(32)
	read := ch034BenchmarkRangeRead()
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		result := ParallelReplicaRangeReadResult{Ranges: make([]ParallelReplicaRangeResult, len(ranges))}
		for rangeIndex, item := range ranges {
			value, err := read(context.Background(), "replica-a", item)
			if err != nil {
				b.Fatal(err)
			}
			result.Ranges[rangeIndex] = ParallelReplicaRangeResult{ID: item.ID, Node: "replica-a", Value: value}
		}
		ch034ParallelReplicaRangeBenchmarkSink = result
	}
}

func BenchmarkCH034BoundedParallelReplicaRangeRead(b *testing.B) {
	ranges := ch034BenchmarkRanges(32)
	read := ch034BenchmarkRangeRead()
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		result, err := ExecuteParallelReplicaRangeRead(
			context.Background(),
			[]string{"replica-a", "replica-b", "replica-c", "replica-d"},
			ranges,
			ParallelReplicaRangeReadOptions{MaxConcurrency: 8},
			read,
		)
		if err != nil {
			b.Fatal(err)
		}
		ch034ParallelReplicaRangeBenchmarkSink = result
	}
}

func ch034BenchmarkRanges(count int) []ParallelReplicaRange {
	ranges := make([]ParallelReplicaRange, count)
	for index := range ranges {
		ranges[index] = ParallelReplicaRange{ID: "range-" + strconv.Itoa(index), Payload: index}
	}
	return ranges
}

func ch034BenchmarkRangeRead() ParallelReplicaRangeReadFunc {
	return func(context.Context, string, ParallelReplicaRange) (any, error) {
		time.Sleep(time.Millisecond)
		return int64(1), nil
	}
}
