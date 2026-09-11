package hatSql

import (
	"strconv"
	"testing"
)

type sqlAsofJoinBenchmarkRow struct {
	key string
	at  int64
}

var sqlAsofJoinBenchmarkSink int64

func BenchmarkSQLAsofJoinBaseline(b *testing.B) {
	left, right := sqlAsofJoinBenchmarkInput()
	b.ResetTimer()
	for range b.N {
		sqlAsofJoinBenchmarkSink = sqlAsofJoinNestedChecksum(left, right)
	}
}

func sqlAsofJoinBenchmarkInput() ([]sqlAsofJoinBenchmarkRow, []sqlAsofJoinBenchmarkRow) {
	const (
		leftCount  = 2048
		rightCount = 8192
		keyCount   = 128
	)
	keys := make([]string, keyCount)
	for index := range keys {
		keys[index] = "key-" + strconv.Itoa(index)
	}
	left := make([]sqlAsofJoinBenchmarkRow, leftCount)
	for index := range left {
		left[index] = sqlAsofJoinBenchmarkRow{key: keys[index%keyCount], at: int64((index*17)%80 + 8)}
	}
	right := make([]sqlAsofJoinBenchmarkRow, rightCount)
	for index := range right {
		right[index] = sqlAsofJoinBenchmarkRow{key: keys[index%keyCount], at: int64(index / keyCount)}
	}
	return left, right
}

func sqlAsofJoinNestedChecksum(left, right []sqlAsofJoinBenchmarkRow) int64 {
	checksum := int64(0)
	for _, leftRow := range left {
		best := int64(0)
		found := false
		for _, rightRow := range right {
			if rightRow.key != leftRow.key || rightRow.at > leftRow.at {
				continue
			}
			if !found || rightRow.at > best {
				best = rightRow.at
				found = true
			}
		}
		if found {
			checksum += best
		}
	}
	return checksum
}
