package hatPipeline

import (
	"fmt"
	"sort"
	"testing"
)

func TestMutationDependencyGraphReadyIntoReusesBuffer(t *testing.T) {
	graph := NewMutationDependencyGraph()
	if err := graph.Add("mutation-0000"); err != nil {
		t.Fatal(err)
	}
	ready := make([]string, 0, 1)
	allocs := testing.AllocsPerRun(100, func() {
		ready = graph.ReadyInto(ready, 1)
	})
	if allocs != 0 {
		t.Fatalf("ReadyInto allocated %.0f times with a reusable buffer", allocs)
	}
	if len(ready) != 1 || ready[0] != "mutation-0000" {
		t.Fatalf("unexpected ready list: %#v", ready)
	}
}

func newMutationDependencyGraphBenchmarkFixture(b *testing.B) *MutationDependencyGraph {
	b.Helper()
	graph := NewMutationDependencyGraph()
	if err := graph.Add("mutation-0000"); err != nil {
		b.Fatal(err)
	}
	for i := 1; i < 4096; i++ {
		id := fmt.Sprintf("mutation-%04d", i)
		dependency := fmt.Sprintf("mutation-%04d", i-1)
		if err := graph.Add(id, dependency); err != nil {
			b.Fatal(err)
		}
	}
	return graph
}

// BenchmarkMutationDependencyGraphReadySet measures polling the indexed ready set
// when only one of 4096 registered mutations is runnable.
func BenchmarkMutationDependencyGraphReadySet(b *testing.B) {
	graph := newMutationDependencyGraphBenchmarkFixture(b)
	ready := make([]string, 0, 1)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ready = graph.ReadyInto(ready, 1)
	}
	b.StopTimer()
	if len(ready) != 1 {
		b.Fatalf("expected one ready mutation, got %d", len(ready))
	}
}

// BenchmarkMutationDependencyGraphReadyLinearScan is the equivalent control that
// scans every registered node on each poll instead of using the ready set.
func BenchmarkMutationDependencyGraphReadyLinearScan(b *testing.B) {
	graph := newMutationDependencyGraphBenchmarkFixture(b)
	ready := make([]string, 0, 1)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ready = ready[:0]
		for id, node := range graph.nodes {
			if node.state == MutationPending && node.remaining == 0 {
				ready = append(ready, id)
			}
		}
		sort.Strings(ready)
	}
	b.StopTimer()
	if len(ready) != 1 {
		b.Fatalf("expected one ready mutation, got %d", len(ready))
	}
}
