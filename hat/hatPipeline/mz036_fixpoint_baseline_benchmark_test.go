package hatPipeline

import (
	"context"
	"testing"
)

const mz036BaselineNodes = 2048

func BenchmarkMZ036FullScanBaseline(b *testing.B) {
	graph := make([][]int, mz036BaselineNodes)
	for index := 1; index < len(graph); index++ {
		graph[index] = []int{index - 1}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		reached := make([]bool, len(graph))
		reached[len(reached)-1] = true
		for changed := true; changed; {
			changed = false
			for from, dependents := range graph {
				if !reached[from] {
					continue
				}
				for _, dependent := range dependents {
					if reached[dependent] {
						continue
					}
					reached[dependent] = true
					changed = true
				}
			}
		}
		if !reached[0] {
			b.Fatal("full scan did not reach the terminal node")
		}
	}
}

func BenchmarkMZ036FixpointScheduler(b *testing.B) {
	step := func(_ context.Context, item FixpointItem[int, int], emit FixpointEmitter[int, int]) error {
		if item.Key == 0 {
			return nil
		}
		return emit(FixpointItem[int, int]{Key: item.Key - 1, Value: item.Value + 1})
	}
	initial := []FixpointItem[int, int]{{Key: mz036BaselineNodes - 1, Value: 0}}
	options := FixpointOptions[int, int]{Step: step}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		result, err := RunFixpoint(context.Background(), initial, options)
		if err != nil || result.Steps != mz036BaselineNodes {
			b.Fatalf("RunFixpoint() result/error = %#v/%v", result, err)
		}
	}
}
