package hatPipeline

import (
	"context"
	"strconv"
	"testing"
)

func BenchmarkDataflowGraphSnapshot(b *testing.B) {
	graph := newBenchmarkDataflowGraph()
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		_ = graph.Snapshot()
	}
}

func BenchmarkDataflowGraphTopologicalOrder(b *testing.B) {
	graph := newBenchmarkDataflowGraph()
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		_, _ = graph.TopologicalOrder()
	}
}

func BenchmarkDataflowGraphImpact(b *testing.B) {
	graph := newBenchmarkDataflowGraph()
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		_, _ = graph.Impact("node-0")
	}
}

func BenchmarkDataflowGraphQueries(b *testing.B) {
	graph := newBenchmarkDataflowGraph()
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		_, _ = graph.Dependencies("node-1024")
		_, _ = graph.Dependents("node-1024")
	}
}

func BenchmarkPipelineDescribe(b *testing.B) {
	pipeline, err := NewPipeline(
		Stage[int]{Name: "read", Workers: 2, Queue: 8, Process: func(_ context.Context, value int) (int, error) { return value, nil }},
		Stage[int]{Name: "filter", Workers: 2, Queue: 8, Process: func(_ context.Context, value int) (int, error) { return value, nil }},
		Stage[int]{Name: "write", Workers: 1, Queue: 0, Process: func(_ context.Context, value int) (int, error) { return value, nil }},
	)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		_ = pipeline.Describe()
	}
}

func newBenchmarkDataflowGraph() *DataflowGraph {
	graph, err := NewDataflowGraph(DataflowGraphOptions{MaxNodes: 2048, MaxEdges: 4096})
	if err != nil {
		panic(err)
	}
	for index := 0; index < 2048; index++ {
		if err := graph.AddNode(DataflowNode{ID: "node-" + strconv.Itoa(index), Kind: "operator"}); err != nil {
			panic(err)
		}
	}
	for index := 0; index < 2047; index++ {
		if err := graph.AddEdge(DataflowEdge{From: "node-" + strconv.Itoa(index), To: "node-" + strconv.Itoa(index+1)}); err != nil {
			panic(err)
		}
	}
	return graph
}
