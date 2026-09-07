package hatPipeline_test

import (
	"context"
	"testing"

	"hatrie_cache/hat/hatPipeline"
)

func BenchmarkOrderedMap(b *testing.B) {
	input := orderedMapBenchmarkInput()
	b.ReportAllocs()
	for range b.N {
		if _, err := hatPipeline.OrderedMap(context.Background(), input, 4, func(_ context.Context, value int) (int, error) {
			return value * 2, nil
		}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSequentialMap(b *testing.B) {
	input := orderedMapBenchmarkInput()
	b.ReportAllocs()
	for range b.N {
		output := make([]int, len(input))
		for index, value := range input {
			output[index] = value * 2
		}
	}
}

func orderedMapBenchmarkInput() []int {
	input := make([]int, 1024)
	for index := range input {
		input[index] = index
	}
	return input
}
