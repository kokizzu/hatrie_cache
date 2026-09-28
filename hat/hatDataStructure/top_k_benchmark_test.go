package hatDataStructure

import (
	"math/rand"
	"sort"
	"strconv"
	"testing"
)

var topKBenchmarkSink []string
var topKBenchmarkEntrySink []TopKEntry[string]

func BenchmarkTopKBaselineMapSort(b *testing.B) {
	values := topKBenchmarkValues()
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		counts := make(map[string]uint64, 20_000)
		for _, value := range values {
			counts[value]++
		}
		keys := make([]string, 0, len(counts))
		for key := range counts {
			keys = append(keys, key)
		}
		sort.Slice(keys, func(left, right int) bool {
			if counts[keys[left]] != counts[keys[right]] {
				return counts[keys[left]] > counts[keys[right]]
			}
			return keys[left] < keys[right]
		})
		topKBenchmarkSink = keys[:100]
	}
}

func BenchmarkTopKSpaceSaving(b *testing.B) {
	values := topKBenchmarkValues()
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		top, err := NewTopK[string](100)
		if err != nil {
			b.Fatal(err)
		}
		for _, value := range values {
			top.Add(value)
		}
		topKBenchmarkEntrySink = top.Entries()
	}
}

func topKBenchmarkValues() []string {
	random := rand.New(rand.NewSource(222))
	values := make([]string, 100_000)
	for index := range values {
		var key int
		if index%10 < 7 {
			key = index % 100
		} else {
			key = 100 + random.Intn(19_900)
		}
		values[index] = strconv.Itoa(key)
	}
	return values
}
