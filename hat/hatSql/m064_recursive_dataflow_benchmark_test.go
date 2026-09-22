package hatSql

import (
	"context"
	"testing"
)

var m064RecursiveDataflowSink []int

func BenchmarkM064RecursiveDataflowDelta(b *testing.B) {
	const nodeCount = 1024
	edges := make([][]int, nodeCount)
	for index := 0; index < nodeCount; index++ {
		if index+1 < nodeCount {
			edges[index] = append(edges[index], index+1)
		}
		if index+2 < nodeCount {
			edges[index] = append(edges[index], index+2)
		}
	}
	flow, err := NewSQLRecursiveDataflow([]int{0}, func(_ context.Context, _ []int, delta []int) ([]int, error) {
		next := make([]int, 0, len(delta)*2)
		for _, source := range delta {
			next = append(next, edges[source]...)
		}
		return next, nil
	}, SQLRecursiveDataflowOptions{})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		rows, err := flow.Run(context.Background())
		if err != nil {
			b.Fatal(err)
		}
		m064RecursiveDataflowSink = rows
	}
}
