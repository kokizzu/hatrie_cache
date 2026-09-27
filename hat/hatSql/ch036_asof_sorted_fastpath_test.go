package hatSql

import (
	"sort"
	"testing"
)

func TestCH036AsofPreparationPreservesStableSortedBuckets(t *testing.T) {
	buckets := map[string][]sqlAsofJoinCandidate{
		"a": {
			{time: int64(1), row: ch036AsofTestRow("first")},
			{time: int64(1), row: ch036AsofTestRow("second")},
			{time: int64(3), row: ch036AsofTestRow("third")},
		},
	}
	prepareSQLAsofJoinBuckets(buckets)
	got := buckets["a"]
	if got[0].row.sources["r"]["id"] != "first" || got[1].row.sources["r"]["id"] != "second" {
		t.Fatalf("equal-time ASOF candidates lost stable order: %#v", got)
	}
}

func TestCH036AsofPreparationSortsUnorderedBuckets(t *testing.T) {
	buckets := map[string][]sqlAsofJoinCandidate{
		"a": {
			{time: int64(3), row: ch036AsofTestRow("third")},
			{time: int64(1), row: ch036AsofTestRow("first")},
			{time: int64(2), row: ch036AsofTestRow("second")},
		},
	}
	prepareSQLAsofJoinBuckets(buckets)
	got := buckets["a"]
	if got[0].time != int64(1) || got[1].time != int64(2) || got[2].time != int64(3) {
		t.Fatalf("unordered ASOF candidates = %#v, want ascending times", got)
	}
}

func BenchmarkCH036AsofBucketSortBaseline(b *testing.B) {
	base := ch036AsofBenchmarkBuckets()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		buckets := cloneCH036AsofBuckets(base)
		for key, bucket := range buckets {
			sort.SliceStable(bucket, func(left, right int) bool {
				return sqlCompare(bucket[left].time, bucket[right].time) < 0
			})
			buckets[key] = bucket
		}
	}
}

func BenchmarkCH036AsofBucketSortFastPath(b *testing.B) {
	base := ch036AsofBenchmarkBuckets()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		prepareSQLAsofJoinBuckets(cloneCH036AsofBuckets(base))
	}
}

func BenchmarkCH036AsofBucketSortUnorderedBaseline(b *testing.B) {
	base := ch036AsofUnorderedBenchmarkBuckets()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		buckets := cloneCH036AsofBuckets(base)
		for key, bucket := range buckets {
			sort.SliceStable(bucket, func(left, right int) bool {
				return sqlCompare(bucket[left].time, bucket[right].time) < 0
			})
			buckets[key] = bucket
		}
	}
}

func BenchmarkCH036AsofBucketSortUnorderedFastPath(b *testing.B) {
	base := ch036AsofUnorderedBenchmarkBuckets()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		prepareSQLAsofJoinBuckets(cloneCH036AsofBuckets(base))
	}
}

func ch036AsofTestRow(id string) sqlExecRow {
	return sqlExecRow{
		sources: map[string]SQLRow{"r": {"id": id}},
		order:   []string{"r"},
	}
}

func ch036AsofBenchmarkBuckets() map[string][]sqlAsofJoinCandidate {
	const groups = 32
	const rowsPerGroup = 256
	buckets := make(map[string][]sqlAsofJoinCandidate, groups)
	for group := 0; group < groups; group++ {
		key := string(rune('a' + group))
		bucket := make([]sqlAsofJoinCandidate, rowsPerGroup)
		for row := range bucket {
			bucket[row] = sqlAsofJoinCandidate{time: int64(row), row: ch036AsofTestRow(key)}
		}
		buckets[key] = bucket
	}
	return buckets
}

func ch036AsofUnorderedBenchmarkBuckets() map[string][]sqlAsofJoinCandidate {
	buckets := ch036AsofBenchmarkBuckets()
	for key, bucket := range buckets {
		for left, right := 0, len(bucket)-1; left < right; left, right = left+1, right-1 {
			bucket[left], bucket[right] = bucket[right], bucket[left]
		}
		buckets[key] = bucket
	}
	return buckets
}

func cloneCH036AsofBuckets(source map[string][]sqlAsofJoinCandidate) map[string][]sqlAsofJoinCandidate {
	clone := make(map[string][]sqlAsofJoinCandidate, len(source))
	for key, bucket := range source {
		clone[key] = append([]sqlAsofJoinCandidate(nil), bucket...)
	}
	return clone
}
