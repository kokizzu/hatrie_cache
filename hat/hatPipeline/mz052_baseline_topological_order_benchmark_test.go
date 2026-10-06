package hatPipeline

import (
	"strconv"
	"testing"
)

var mz052BaselineOrderSink []string

func newMZ052BaselineGraph(b *testing.B) *DataflowGraph {
	b.Helper()
	graph, err := NewDataflowGraph(DataflowGraphOptions{MaxNodes: 2048, MaxEdges: 2047})
	if err != nil {
		b.Fatal(err)
	}
	for i := 0; i < 2048; i++ {
		if err := graph.AddNode(DataflowNode{ID: "node-" + strconv.Itoa(i)}); err != nil {
			b.Fatal(err)
		}
	}
	for i := 0; i < 2047; i++ {
		if err := graph.AddEdge(DataflowEdge{
			From: "node-" + strconv.Itoa(i),
			To:   "node-" + strconv.Itoa(i+1),
		}); err != nil {
			b.Fatal(err)
		}
	}
	return graph
}

func BenchmarkMZ052BaselineTopologicalOrder(b *testing.B) {
	graph := newMZ052BaselineGraph(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		order, err := graph.TopologicalOrder()
		if err != nil {
			b.Fatal(err)
		}
		mz052BaselineOrderSink = order
	}
}
