package hatDataStructure

import (
	"strconv"
	"testing"
)

var t247BenchmarkSink int

func BenchmarkT247PlainFIFOAdmission(b *testing.B) {
	ids := t247BenchmarkIDs(100_000, 100)
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		queue := make([]int, 0, len(ids))
		for index := range ids {
			queue = append(queue, index)
		}
		checksum := 0
		for _, value := range queue {
			checksum += value
		}
		t247BenchmarkSink = checksum
	}
}

func BenchmarkT247DeduplicatingAdmission(b *testing.B) {
	ids := t247BenchmarkIDs(100_000, 100)
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		queue := NewDeduplicatingQueue[string, int](100)
		for index, id := range ids {
			queue.Enqueue(id, index)
		}
		checksum := 0
		for {
			_, value, ok := queue.Dequeue()
			if !ok {
				break
			}
			checksum += value
		}
		t247BenchmarkSink = checksum
	}
}

func BenchmarkT247PlainFIFOWithWork(b *testing.B) {
	ids := t247BenchmarkIDs(100_000, 100)
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		queue := make([]int, 0, len(ids))
		for index := range ids {
			queue = append(queue, index)
		}
		checksum := 0
		for _, value := range queue {
			checksum += t247BenchmarkWork(value)
		}
		t247BenchmarkSink = checksum
	}
}

func BenchmarkT247DeduplicatingWithWork(b *testing.B) {
	ids := t247BenchmarkIDs(100_000, 100)
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		queue := NewDeduplicatingQueue[string, int](100)
		for index, id := range ids {
			queue.Enqueue(id, index)
		}
		checksum := 0
		for {
			_, value, ok := queue.Dequeue()
			if !ok {
				break
			}
			checksum += t247BenchmarkWork(value)
		}
		t247BenchmarkSink = checksum
	}
}

func t247BenchmarkWork(value int) int {
	for index := 0; index < 64; index++ {
		value = value*1664525 + 1013904223 + index
	}
	return value
}

func t247BenchmarkIDs(count, unique int) []string {
	ids := make([]string, count)
	for index := range ids {
		ids[index] = "task-" + strconv.Itoa(index%unique)
	}
	return ids
}
