package hatPipeline

import (
	"strconv"
	"testing"
)

var mz052SnapshotBaselineSink DataflowGraphSnapshot

func newMZ052SnapshotBaselineGraph(b *testing.B) *DataflowGraph {
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

func BenchmarkMZ052SnapshotBaseline(b *testing.B) {
	graph := newMZ052SnapshotBaselineGraph(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		snapshot := graph.Snapshot()
		if len(snapshot.Nodes) != 2048 || len(snapshot.Edges) != 2047 {
			b.Fatalf("snapshot sizes = %d nodes, %d edges", len(snapshot.Nodes), len(snapshot.Edges))
		}
		mz052SnapshotBaselineSink = snapshot
	}
}
