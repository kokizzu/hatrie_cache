//go:build mz006baseline

package hatPipeline

import "testing"

type mz006BaselineTimestamp struct {
	epoch  uint64
	offset uint64
}

var (
	mz006BaselineSink     int
	mz006BaselineRetained []mz006BaselineTimestamp
)

func mz006BaselineLessEqual(left, right mz006BaselineTimestamp) bool {
	return left.epoch <= right.epoch && left.offset <= right.offset
}

func mz006BaselineFrontier(size int) []mz006BaselineTimestamp {
	frontier := make([]mz006BaselineTimestamp, 0, size)
	for value := size; value > 0; value-- {
		frontier = append(frontier, mz006BaselineTimestamp{uint64(value), uint64(value)})
	}
	return frontier
}

func BenchmarkMZ006NaiveFrontierInsert(b *testing.B) {
	b.ReportAllocs()
	for range b.N {
		frontier := mz006BaselineFrontier(4096)
		mz006BaselineRetained = frontier
		mz006BaselineSink += len(frontier)
	}
}

func BenchmarkMZ006NaiveFrontierCovers(b *testing.B) {
	frontier := mz006BaselineFrontier(4096)
	target := mz006BaselineTimestamp{}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		covered := false
		for _, timestamp := range frontier {
			if mz006BaselineLessEqual(timestamp, target) {
				covered = true
				break
			}
		}
		if covered {
			mz006BaselineSink++
		}
	}
}
