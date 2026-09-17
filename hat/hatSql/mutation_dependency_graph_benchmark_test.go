package hatSql

import (
	"bytes"
	"strconv"
	"testing"
)

func BenchmarkSQLMutationDependencyGraphClaimReady(b *testing.B) {
	graph, err := NewSQLMutationDependencyGraph(512)
	if err != nil {
		b.Fatal(err)
	}
	for index := 0; index < 512; index++ {
		if err := graph.Add(SQLMutationTask{ID: mutationDependencyBenchmarkID(index)}); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		claimed := graph.ClaimReady(64)
		if len(claimed) != 64 {
			b.Fatalf("ClaimReady() returned %d tasks, want 64", len(claimed))
		}
		if got := graph.RequeueRunning(); got != 64 {
			b.Fatalf("RequeueRunning() returned %d tasks, want 64", got)
		}
	}
}

func BenchmarkSQLMutationDependencyGraphSave(b *testing.B) {
	graph, err := NewSQLMutationDependencyGraph(256)
	if err != nil {
		b.Fatal(err)
	}
	for index := 0; index < 256; index++ {
		if err := graph.Add(SQLMutationTask{ID: mutationDependencyBenchmarkID(index)}); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		var output bytes.Buffer
		if err := graph.Save(&output); err != nil {
			b.Fatal(err)
		}
		b.SetBytes(int64(output.Len()))
	}
}

func mutationDependencyBenchmarkID(index int) string {
	return "mutation:" + strconv.Itoa(index)
}
