package hatSql

import (
	"sort"
	"testing"
)

func BenchmarkSQLAsofJoinOptimized(b *testing.B) {
	left, right := sqlAsofJoinBenchmarkInput()
	buckets := make(map[string][]sqlAsofJoinCandidate, 128)
	for _, row := range right {
		buckets[row.key] = append(buckets[row.key], sqlAsofJoinCandidate{time: row.at})
	}
	for key, bucket := range buckets {
		sort.SliceStable(bucket, func(left, right int) bool {
			return sqlCompare(bucket[left].time, bucket[right].time) < 0
		})
		buckets[key] = bucket
	}
	b.ResetTimer()
	for range b.N {
		checksum := int64(0)
		for _, row := range left {
			index, ok := sqlAsofJoinCandidateIndex(buckets[row.key], row.at, "<=")
			if ok {
				checksum += buckets[row.key][index].time.(int64)
			}
		}
		sqlAsofJoinBenchmarkSink = checksum
	}
}
